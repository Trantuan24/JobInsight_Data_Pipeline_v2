"""
JobInsight Pipeline DAG - Crawl TopCV → Raw → Staging
Schedule: Daily at 6:00 AM Vietnam time

Flow:
1. Crawl TopCV pages
2. Upload HTML to MinIO (backup)
3. Parse HTML → jobs
4. Upsert to raw_jobs
5. Transform to staging_jobs
6. Get stats
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import sys

sys.path.insert(0, '/opt/airflow')

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


def crawl_task(**kwargs):
    """Crawl TopCV pages"""
    import asyncio
    from src.data_sources.topcv import scrape_pages
    
    num_pages = kwargs.get('num_pages', 10)
    logger.info(f"Starting crawl of {num_pages} pages...")
    
    results = asyncio.run(scrape_pages(num_pages=num_pages, parallel=True))
    
    success_count = sum(1 for r in results if r.get("success"))
    logger.info(f"Crawl completed: {success_count}/{num_pages} pages successful")
    
    html_data = [
        {"page": r["page"], "html": r["html"], "size": r["size"]}
        for r in results if r.get("success")
    ]
    
    kwargs['ti'].xcom_push(key='html_data', value=html_data)
    return {"success": success_count, "failed": len(results) - success_count}


def upload_minio_task(**kwargs):
    """Upload HTML to MinIO"""
    from src.storage import upload_html_to_minio
    
    ti = kwargs['ti']
    html_data = ti.xcom_pull(key='html_data', task_ids='crawl')
    
    if not html_data:
        logger.warning("No HTML data to upload")
        return {"uploaded": 0}
    
    uploaded = 0
    for item in html_data:
        result = upload_html_to_minio(item["html"], item["page"])
        if result.get("success"):
            uploaded += 1
    
    logger.info(f"Uploaded {uploaded} files to MinIO")
    return {"uploaded": uploaded}


def parse_task(**kwargs):
    """Parse HTML and extract jobs"""
    from src.data_sources.topcv import parse_html
    
    ti = kwargs['ti']
    html_data = ti.xcom_pull(key='html_data', task_ids='crawl')
    
    if not html_data:
        logger.warning("No HTML data to parse")
        return {"jobs": 0}
    
    all_jobs = []
    for item in html_data:
        jobs = parse_html(item["html"])
        logger.info(f"Page {item['page']}: parsed {len(jobs)} jobs")
        all_jobs.extend(jobs)
    
    seen = set()
    unique_jobs = [j for j in all_jobs if j['job_id'] not in seen and not seen.add(j['job_id'])]
    
    logger.info(f"Total unique jobs: {len(unique_jobs)}")
    ti.xcom_push(key='jobs', value=unique_jobs)
    return {"jobs_parsed": len(unique_jobs)}


def upsert_raw_task(**kwargs):
    """Upsert jobs to raw_jobs table"""
    from src.data_sources.topcv import jobs_to_dataframe
    from src.storage import bulk_upsert
    
    ti = kwargs['ti']
    jobs = ti.xcom_pull(key='jobs', task_ids='parse')
    
    if not jobs:
        logger.warning("No jobs to insert")
        return {"inserted": 0, "updated": 0, "unchanged": 0}
    
    df = jobs_to_dataframe(jobs)
    logger.info(f"DataFrame shape: {df.shape}")
    
    result = bulk_upsert(df)
    logger.info(f"Raw DB: {result['inserted']} inserted, {result['updated']} updated")
    return result


def transform_staging_task(**kwargs):
    """Transform raw_jobs to staging_jobs"""
    from src.etl.staging import run_staging_pipeline
    
    result = run_staging_pipeline()
    logger.info(f"Staging result: {result}")
    return result


with DAG(
    'jobinsight_pipeline',
    default_args=default_args,
    description='Daily TopCV crawl → raw → staging pipeline',
    schedule_interval='0 6 * * *',  # 6:00 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['production', 'crawler', 'etl'],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    crawl = PythonOperator(
        task_id='crawl',
        python_callable=crawl_task,
        op_kwargs={'num_pages': 10},
    )
    
    upload_minio = PythonOperator(
        task_id='upload_minio',
        python_callable=upload_minio_task,
    )
    
    parse = PythonOperator(
        task_id='parse',
        python_callable=parse_task,
    )
    
    upsert_raw = PythonOperator(
        task_id='upsert_raw',
        python_callable=upsert_raw_task,
    )
    
    transform_staging = PythonOperator(
        task_id='transform_staging',
        python_callable=transform_staging_task,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flow: crawl → [upload_minio, parse] → upsert_raw → transform_staging → end
    start >> crawl >> [upload_minio, parse]
    parse >> upsert_raw >> transform_staging >> end
    upload_minio >> end
