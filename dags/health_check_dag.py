"""
Health Check DAG - Monitor PostgreSQL, MinIO, DuckDB
Schedule: Every hour
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import sys

sys.path.insert(0, '/opt/airflow')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def health_check_task(**kwargs):
    """Run health check on all services."""
    from src.monitoring import check_all_services
    
    result = check_all_services()
    
    # Log results
    logger.info(f"Health check: {result['overall']}")
    for service, status in result['services'].items():
        s = status.get('status')
        if s == 'healthy':
            logger.info(f"  ✅ {service}: healthy")
        elif s == 'degraded':
            logger.warning(f"  ⚠️ {service}: degraded")
        else:
            logger.error(f"  ❌ {service}: {status.get('error', 'unhealthy')}")
    
    # Fail task only if unhealthy (not degraded)
    if result['overall'] == 'unhealthy':
        unhealthy = [s for s, v in result['services'].items() if v.get('status') == 'unhealthy']
        raise Exception(f"Services unhealthy: {unhealthy}")
    
    return result


with DAG(
    'jobinsight_health_check',
    default_args=default_args,
    description='Health check for PostgreSQL, MinIO, DuckDB, Grafana, Superset',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring', 'health'],
    max_active_runs=1,
) as dag:
    
    health_check = PythonOperator(
        task_id='health_check',
        python_callable=health_check_task,
    )
