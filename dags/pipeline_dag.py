"""
JobInsight DWH DAG - Staging → Data Warehouse
Schedule: Daily at 7:00 AM Vietnam time (sau pipeline_dag)

Flow:
1. Download DuckDB từ MinIO
2. Backup DuckDB lên MinIO
3. Process Dimensions (SCD Type 2)
4. Process Facts (Pure Periodic Snapshot - 1 record/job/ngày)
5. Process Bridges
6. Upload DuckDB lên MinIO
7. Export Parquet lên MinIO

Design: Pure Periodic Snapshot (Kimball methodology)
- Grain: 1 job × 1 ngày crawl
- Mỗi ngày crawl chỉ tạo 1 record cho ngày đó
- KHÔNG tạo projected records

Data stored on MinIO:
- jobinsight-warehouse/dwh/jobinsight.duckdb
- jobinsight-warehouse/parquet/load_month=YYYY-MM/facts.parquet
- jobinsight-backup/dwh_backups/...
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import logging
import os
import sys

sys.path.insert(0, '/opt/airflow')

logger = logging.getLogger(__name__)

# PostgreSQL connection string
PG_CONN_STRING = os.environ.get(
    'PG_CONN_STRING',
    'postgresql://jobinsight:jobinsight@postgres:5432/jobinsight'
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


def run_dwh_etl_task(**kwargs):
    """
    Run full DWH ETL pipeline.
    Downloads DuckDB from MinIO, processes data, uploads back.
    
    Logic mới:
    1. Carry forward: Tạo facts cho jobs còn hạn từ ngày trước
    2. Process staging: Chỉ lấy jobs crawl ngày hôm nay
    """
    from src.etl.warehouse import run_etl
    from src.monitoring import ETLMetricsLogger
    from datetime import date
    
    dag_run_id = kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None
    force_new = kwargs.get('force_new', False)
    
    crawl_date = kwargs.get('crawl_date')
    if crawl_date is None:
        crawl_date = date.today()
    elif isinstance(crawl_date, str):
        crawl_date = date.fromisoformat(crawl_date)
    
    with ETLMetricsLogger(PG_CONN_STRING).track('jobinsight_dwh', 'run_dwh_etl', dag_run_id) as metrics:
        logger.info(f"Starting DWH ETL (crawl_date={crawl_date}, force_new={force_new})...")
        
        result = run_etl(
            pg_conn_string=PG_CONN_STRING,
            crawl_date=crawl_date,
            force_new=force_new
        )
        
        if result['success']:
            stats = result.get('stats', {})
            metrics.rows_out = stats.get('facts', {}).get('inserted', 0)
            metrics.rows_inserted = stats.get('facts', {}).get('inserted', 0)
            metrics.metadata = {
                'dim_job': stats.get('dim_job', {}),
                'dim_company': stats.get('dim_company', {}),
                'dim_location': stats.get('dim_location', {}),
                'facts': stats.get('facts', {}),
                'bridges': stats.get('bridges', {})
            }
            logger.info(f"DWH ETL completed in {result['duration_seconds']:.2f}s")
            logger.info(f"Stats: {stats}")
        else:
            logger.error(f"DWH ETL failed: {result.get('message', 'Unknown error')}")
            raise Exception(result.get('message', 'DWH ETL failed'))
        
        return result


def validate_dwh_task(**kwargs):
    """Validate DWH data quality after ETL."""
    from src.storage.minio import download_duckdb, get_duckdb_connection
    import os
    
    logger.info("Validating DWH data...")
    
    # Download DuckDB for validation
    local_path = download_duckdb()
    
    try:
        with get_duckdb_connection(local_path) as conn:
            # Check record counts
            counts = {}
            for table in ['DimJob', 'DimCompany', 'DimLocation', 'DimDate', 'FactJobPostingDaily', 'FactJobLocationBridge']:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0
            
            logger.info(f"Table counts: {counts}")
            
            # Check for orphan facts
            orphan_jobs = conn.execute("""
                SELECT COUNT(*) FROM FactJobPostingDaily f
                LEFT JOIN DimJob j ON f.job_sk = j.job_sk
                WHERE j.job_sk IS NULL
            """).fetchone()[0]
            
            orphan_companies = conn.execute("""
                SELECT COUNT(*) FROM FactJobPostingDaily f
                LEFT JOIN DimCompany c ON f.company_sk = c.company_sk
                WHERE c.company_sk IS NULL
            """).fetchone()[0]
            
            # Check facts by date (Pure Periodic Snapshot)
            facts_by_date = conn.execute("""
                SELECT 
                    date_id,
                    COUNT(*) as count
                FROM FactJobPostingDaily
                WHERE date_id >= CURRENT_DATE - 7
                GROUP BY date_id
                ORDER BY date_id DESC
            """).fetchdf()
            
            logger.info(f"Facts by date (last 7 days): {facts_by_date.to_dict()}")
            
            validation_result = {
                'counts': counts,
                'orphan_jobs': orphan_jobs,
                'orphan_companies': orphan_companies,
                'is_valid': orphan_jobs == 0 and orphan_companies == 0
            }
            
            if not validation_result['is_valid']:
                logger.warning(f"Validation issues: orphan_jobs={orphan_jobs}, orphan_companies={orphan_companies}")
            else:
                logger.info("Validation passed!")
            
            return validation_result
            
    finally:
        # Cleanup local file
        if os.path.exists(local_path):
            os.remove(local_path)


def get_dwh_stats_task(**kwargs):
    """Get DWH statistics for monitoring."""
    from src.storage.minio import download_duckdb, get_duckdb_connection
    import os
    
    logger.info("Getting DWH statistics...")
    
    local_path = download_duckdb()
    
    try:
        with get_duckdb_connection(local_path) as conn:
            # Today's stats (Pure Periodic Snapshot - no projected)
            today_stats = conn.execute("""
                SELECT 
                    COUNT(DISTINCT job_sk) as unique_jobs,
                    COUNT(*) as total_facts
                FROM FactJobPostingDaily
                WHERE date_id = CURRENT_DATE
            """).fetchone()
            
            # Load month stats
            load_month_stats = conn.execute("""
                SELECT 
                    load_month,
                    COUNT(*) as fact_count,
                    COUNT(DISTINCT job_sk) as unique_jobs,
                    COUNT(DISTINCT date_id) as days_with_data
                FROM FactJobPostingDaily
                GROUP BY load_month
                ORDER BY load_month DESC
                LIMIT 3
            """).fetchdf()
            
            stats = {
                'today': {
                    'unique_jobs': today_stats[0] if today_stats else 0,
                    'total_facts': today_stats[1] if today_stats else 0,
                },
                'by_month': load_month_stats.to_dict('records') if not load_month_stats.empty else []
            }
            
            logger.info(f"DWH Stats: {stats}")
            return stats
            
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


with DAG(
    'jobinsight_dwh',
    default_args=default_args,
    description='Daily Staging → DWH ETL pipeline (MinIO storage)',
    schedule_interval='0 7 * * *',  # 7:00 AM daily (sau pipeline_dag)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['production', 'dwh', 'etl', 'minio'],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Wait for pipeline_dag to complete before running DWH ETL
    # pipeline_dag runs at 6:00 AM (execution_date = previous day 23:00 UTC)
    # dwh_dag runs at 7:00 AM (execution_date = current day 00:00 UTC)
    # Delta = 1 hour to match execution dates
    wait_for_pipeline = ExternalTaskSensor(
        task_id='wait_for_pipeline',
        external_dag_id='jobinsight_pipeline',
        external_task_id='end',
        execution_delta=timedelta(hours=1),  # dwh 7AM - pipeline 6AM = 1 hour
        timeout=3600,              # Wait max 1 hour
        poke_interval=60,          # Check every 1 minute
        mode='reschedule',         # Free up worker while waiting
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )
    
    run_etl = PythonOperator(
        task_id='run_dwh_etl',
        python_callable=run_dwh_etl_task,
        op_kwargs={
            'crawl_date': None,  # Default: today
            'force_new': False,  # Incremental mode (giữ data cũ)
        },
    )
    
    validate = PythonOperator(
        task_id='validate_dwh',
        python_callable=validate_dwh_task,
    )
    
    get_stats = PythonOperator(
        task_id='get_dwh_stats',
        python_callable=get_dwh_stats_task,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flow: start → wait_for_pipeline → run_etl → validate → get_stats → end
    start >> wait_for_pipeline >> run_etl >> validate >> get_stats >> end
