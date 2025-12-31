"""
JobInsight Archive DAG - Archive old data from PostgreSQL to MinIO
Schedule: Weekly on Sunday at 2:00 AM Vietnam time

HYBRID Strategy:
1. Export old data (>30 days) to Parquet
2. Upload to MinIO (jobinsight-archive bucket)
3. VERIFY: Download and compare row count
4. If verified → Hard delete from PostgreSQL
5. If failed → Keep data + alert
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import sys

sys.path.insert(0, '/opt/airflow')

logger = logging.getLogger(__name__)

# Retention period in days
RETENTION_DAYS = 30

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
}


def check_old_data(**kwargs):
    """Check if there's old data to archive"""
    from src.storage.archive import get_old_records
    
    df = get_old_records(days=RETENTION_DAYS)
    
    if df.empty:
        logger.info("No old data to archive")
        return "skip_archive"
    
    logger.info(f"Found {len(df)} records to archive")
    
    # Store data info in XCom
    kwargs['ti'].xcom_push(key='record_count', value=len(df))
    kwargs['ti'].xcom_push(key='job_ids', value=df['job_id'].tolist())
    
    return "export_to_parquet"


def export_to_parquet_task(**kwargs):
    """Export old data to Parquet format"""
    from src.storage.archive import get_old_records, export_to_parquet
    
    ti = kwargs['ti']
    
    # Get old records
    df = get_old_records(days=RETENTION_DAYS)
    
    if df.empty:
        logger.warning("No data to export")
        return {"exported": 0}
    
    # Export to Parquet bytes
    parquet_data = export_to_parquet(df)
    
    logger.info(f"Exported {len(df)} records to Parquet ({len(parquet_data)} bytes)")
    
    # Store in XCom (base64 encoded for safety)
    import base64
    ti.xcom_push(key='parquet_data', value=base64.b64encode(parquet_data).decode('utf-8'))
    ti.xcom_push(key='record_count', value=len(df))
    ti.xcom_push(key='job_ids', value=df['job_id'].tolist())
    
    return {"exported": len(df), "size": len(parquet_data)}


def upload_to_minio_task(**kwargs):
    """Upload Parquet archive to MinIO"""
    from src.storage.archive import upload_archive_to_minio
    import base64
    
    ti = kwargs['ti']
    
    # Get Parquet data from XCom
    parquet_b64 = ti.xcom_pull(key='parquet_data', task_ids='export_to_parquet')
    
    if not parquet_b64:
        logger.error("No Parquet data found in XCom")
        raise ValueError("No Parquet data to upload")
    
    parquet_data = base64.b64decode(parquet_b64)
    
    # Upload to MinIO
    archive_date = datetime.now()
    result = upload_archive_to_minio(parquet_data, archive_date)
    
    if not result.get("success"):
        raise Exception(f"Upload failed: {result.get('error')}")
    
    # Store object name for verification
    ti.xcom_push(key='object_name', value=result['object'])
    
    logger.info(f"Uploaded to MinIO: {result['object']}")
    return result


def verify_archive_task(**kwargs):
    """Verify uploaded archive matches expected row count"""
    from src.storage.archive import verify_archive
    
    ti = kwargs['ti']
    
    object_name = ti.xcom_pull(key='object_name', task_ids='upload_to_minio')
    expected_count = ti.xcom_pull(key='record_count', task_ids='export_to_parquet')
    
    if not object_name:
        raise ValueError("No object name found")
    
    result = verify_archive(object_name, expected_count)
    
    if not result.get("verified"):
        logger.error(f"Archive verification FAILED: {result}")
        raise Exception(f"Verification failed: expected {expected_count}, got {result.get('actual')}")
    
    logger.info(f"Archive verified successfully: {expected_count} rows")
    return result


def delete_old_data_task(**kwargs):
    """Delete archived data from PostgreSQL"""
    from src.storage.archive import delete_old_records
    
    ti = kwargs['ti']
    
    job_ids = ti.xcom_pull(key='job_ids', task_ids='export_to_parquet')
    
    if not job_ids:
        logger.warning("No job_ids to delete")
        return {"deleted": 0}
    
    result = delete_old_records(job_ids)
    
    logger.info(f"Deleted {result['deleted']} records from PostgreSQL")
    return result


def alert_failure_task(**kwargs):
    """Alert on archive failure - keep data safe"""
    logger.error("ARCHIVE FAILED - Data kept in PostgreSQL for safety")
    logger.error("Please investigate and retry manually")
    # TODO: Add email/Slack notification here
    return {"status": "failed", "action": "data_preserved"}


with DAG(
    'jobinsight_archive',
    default_args=default_args,
    description='Weekly archive of old job data to MinIO',
    schedule_interval='0 2 * * 0',  # Sunday 2:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'archive'],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Check if there's data to archive
    check_data = BranchPythonOperator(
        task_id='check_old_data',
        python_callable=check_old_data,
    )
    
    # Skip path
    skip_archive = EmptyOperator(task_id='skip_archive')
    
    # Export to Parquet
    export_parquet = PythonOperator(
        task_id='export_to_parquet',
        python_callable=export_to_parquet_task,
    )
    
    # Upload to MinIO
    upload_minio = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio_task,
    )
    
    # Verify archive
    verify = PythonOperator(
        task_id='verify_archive',
        python_callable=verify_archive_task,
    )
    
    # Delete from PostgreSQL (only if verified)
    delete_data = PythonOperator(
        task_id='delete_old_data',
        python_callable=delete_old_data_task,
        trigger_rule='all_success',
    )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
    )
    
    # DAG flow
    start >> check_data
    check_data >> skip_archive >> end
    check_data >> export_parquet >> upload_minio >> verify >> delete_data >> end
