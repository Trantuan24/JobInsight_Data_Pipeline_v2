"""
JobInsight Maintenance DAG - Cleanup and Backup
Schedule: Daily at 3:00 AM Vietnam time

Tasks:
1. Backup PostgreSQL to MinIO
2. Cleanup HTML files > RETENTION_HTML_DAYS from jobinsight-raw
3. Cleanup DWH backups > RETENTION_BACKUP_DAYS from jobinsight-backup
4. Cleanup PostgreSQL backups > RETENTION_BACKUP_DAYS
5. Log storage stats
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import os
import subprocess
import sys

sys.path.insert(0, '/opt/airflow')

logger = logging.getLogger(__name__)

# Retention policies (from env vars)
RAW_HTML_RETENTION = int(os.environ.get('RETENTION_HTML_DAYS', '15'))
DWH_BACKUP_RETENTION = int(os.environ.get('RETENTION_BACKUP_DAYS', '7'))
PG_BACKUP_RETENTION = int(os.environ.get('RETENTION_BACKUP_DAYS', '7'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


def backup_postgres_task(**kwargs):
    """Backup PostgreSQL database to MinIO."""
    from minio import Minio
    import io
    
    db_host = os.environ.get('DB_HOST', 'postgres')
    db_port = os.environ.get('DB_PORT', '5432')
    db_user = os.environ.get('DB_USER', 'jobinsight')
    db_name = os.environ.get('DB_NAME', 'jobinsight')
    db_password = os.environ.get('DB_PASSWORD', 'jobinsight')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dump_file = f'/tmp/jobinsight_{timestamp}.dump'
    
    # Run pg_dump
    env = os.environ.copy()
    env['PGPASSWORD'] = db_password
    
    cmd = [
        'pg_dump',
        '-h', db_host,
        '-p', db_port,
        '-U', db_user,
        '-Fc',  # Custom format (compressed)
        '-f', dump_file,
        db_name
    ]
    
    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"pg_dump failed: {result.stderr}")
            raise Exception(f"pg_dump failed: {result.stderr}")
        
        # Get file size
        file_size = os.path.getsize(dump_file)
        logger.info(f"pg_dump completed: {dump_file} ({file_size / 1024 / 1024:.2f} MB)")
        
        # Upload to MinIO
        client = Minio(
            os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False
        )
        
        bucket = os.environ.get('MINIO_BUCKET_BACKUP', 'jobinsight-backup')
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        object_name = f'pg_backups/jobinsight_{timestamp}.dump'
        client.fput_object(bucket, object_name, dump_file)
        logger.info(f"Uploaded PostgreSQL backup: {object_name}")
        
        # Cleanup local file
        os.remove(dump_file)
        
        return {'backup': object_name, 'size_mb': round(file_size / 1024 / 1024, 2)}
        
    except subprocess.TimeoutExpired:
        logger.error("pg_dump timed out after 5 minutes")
        raise
    except Exception as e:
        logger.error(f"PostgreSQL backup failed: {e}")
        # Cleanup on failure
        if os.path.exists(dump_file):
            os.remove(dump_file)
        raise


def cleanup_pg_backups_task(**kwargs):
    """Cleanup PostgreSQL backups older than RETENTION_BACKUP_DAYS."""
    from minio import Minio
    from datetime import timezone
    
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False
    )
    
    bucket = os.environ.get('MINIO_BUCKET_BACKUP', 'jobinsight-backup')
    cutoff = datetime.now(timezone.utc) - timedelta(days=PG_BACKUP_RETENTION)
    
    deleted = 0
    total_size = 0
    
    try:
        objects = client.list_objects(bucket, prefix='pg_backups/', recursive=True)
        for obj in objects:
            if obj.last_modified < cutoff:
                client.remove_object(bucket, obj.object_name)
                deleted += 1
                total_size += obj.size
                logger.info(f"Deleted: {obj.object_name}")
    except Exception as e:
        logger.error(f"Error cleaning PostgreSQL backups: {e}")
        raise
    
    logger.info(f"Cleanup PostgreSQL backups: deleted {deleted} files, freed {total_size / 1024 / 1024:.2f} MB")
    return {'deleted': deleted, 'freed_mb': round(total_size / 1024 / 1024, 2)}


def cleanup_raw_html_task(**kwargs):
    """Cleanup HTML files older than RETENTION_HTML_DAYS from jobinsight-raw bucket."""
    from minio import Minio
    from datetime import timezone
    
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False
    )
    
    bucket = os.environ.get('MINIO_BUCKET_RAW', 'jobinsight-raw')
    cutoff = datetime.now(timezone.utc) - timedelta(days=RAW_HTML_RETENTION)
    
    deleted = 0
    total_size = 0
    
    try:
        objects = client.list_objects(bucket, prefix='html/', recursive=True)
        for obj in objects:
            if obj.last_modified < cutoff:
                client.remove_object(bucket, obj.object_name)
                deleted += 1
                total_size += obj.size
                logger.info(f"Deleted: {obj.object_name}")
    except Exception as e:
        logger.error(f"Error cleaning raw HTML: {e}")
        raise
    
    logger.info(f"Cleanup raw HTML: deleted {deleted} files, freed {total_size / 1024 / 1024:.2f} MB")
    return {'deleted': deleted, 'freed_mb': round(total_size / 1024 / 1024, 2)}


def cleanup_dwh_backups_task(**kwargs):
    """Cleanup DWH backups older than RETENTION_BACKUP_DAYS from jobinsight-backup bucket."""
    from minio import Minio
    from datetime import timezone
    
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False
    )
    
    bucket = os.environ.get('MINIO_BUCKET_BACKUP', 'jobinsight-backup')
    cutoff = datetime.now(timezone.utc) - timedelta(days=DWH_BACKUP_RETENTION)
    
    deleted = 0
    total_size = 0
    
    try:
        objects = client.list_objects(bucket, prefix='dwh_backups/', recursive=True)
        for obj in objects:
            if obj.last_modified < cutoff:
                client.remove_object(bucket, obj.object_name)
                deleted += 1
                total_size += obj.size
                logger.info(f"Deleted: {obj.object_name}")
    except Exception as e:
        logger.error(f"Error cleaning DWH backups: {e}")
        raise
    
    logger.info(f"Cleanup DWH backups: deleted {deleted} files, freed {total_size / 1024 / 1024:.2f} MB")
    return {'deleted': deleted, 'freed_mb': round(total_size / 1024 / 1024, 2)}


def get_storage_stats_task(**kwargs):
    """Get storage statistics for all buckets."""
    from minio import Minio
    
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False
    )
    
    buckets = [
        os.environ.get('MINIO_BUCKET_RAW', 'jobinsight-raw'),
        os.environ.get('MINIO_BUCKET_ARCHIVE', 'jobinsight-archive'),
        os.environ.get('MINIO_BUCKET_BACKUP', 'jobinsight-backup'),
        os.environ.get('MINIO_BUCKET_WAREHOUSE', 'jobinsight-warehouse')
    ]
    stats = {}
    
    for bucket in buckets:
        try:
            objects = list(client.list_objects(bucket, recursive=True))
            total_size = sum(obj.size for obj in objects)
            stats[bucket] = {
                'objects': len(objects),
                'size_mb': round(total_size / 1024 / 1024, 2)
            }
        except Exception as e:
            stats[bucket] = {'error': str(e)}
    
    logger.info(f"Storage stats: {stats}")
    return stats


with DAG(
    'jobinsight_maintenance',
    default_args=default_args,
    description='Daily backup and cleanup',
    schedule_interval='0 3 * * *',  # 3:00 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'cleanup', 'backup'],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Backup tasks
    backup_postgres = PythonOperator(
        task_id='backup_postgres',
        python_callable=backup_postgres_task,
    )
    
    # Cleanup tasks
    cleanup_html = PythonOperator(
        task_id='cleanup_raw_html',
        python_callable=cleanup_raw_html_task,
    )
    
    cleanup_dwh_backups = PythonOperator(
        task_id='cleanup_dwh_backups',
        python_callable=cleanup_dwh_backups_task,
    )
    
    cleanup_pg_backups = PythonOperator(
        task_id='cleanup_pg_backups',
        python_callable=cleanup_pg_backups_task,
    )
    
    storage_stats = PythonOperator(
        task_id='get_storage_stats',
        python_callable=get_storage_stats_task,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flow: start → backup_postgres → [cleanup tasks] → storage_stats → end
    start >> backup_postgres >> [cleanup_html, cleanup_dwh_backups, cleanup_pg_backups] >> storage_stats >> end
