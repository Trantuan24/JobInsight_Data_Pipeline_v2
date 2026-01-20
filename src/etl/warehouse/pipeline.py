"""
ETL Pipeline: Staging to DWH.
Main orchestrator for ETL process.
"""

import logging
import os
import re
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional

import pandas as pd

from src.storage.minio import (
    download_duckdb,
    upload_duckdb,
    backup_duckdb,
    export_parquet,
    export_all_parquet,
    get_duckdb_connection
)
from .cache import init_dimension_caches
from .dimensions import (
    process_dim_job,
    process_dim_company,
    process_dim_location,
    process_dim_date
)
from .facts import process_facts, process_bridges, cleanup_duplicate_facts

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_PATH = 'sql/schemas/dwh_schema.sql'


def _setup_schema(conn, schema_path: str = DEFAULT_SCHEMA_PATH) -> bool:
    """
    Setup schema - only create tables if they don't exist.
    Does NOT drop existing tables to preserve data.
    """
    try:
        # Check if schema already exists by checking for DimJob table
        result = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'DimJob'
        """).fetchone()
        
        if result and result[0] > 0:
            logger.info("Schema already exists, skipping setup")
            return True
        
        # Schema doesn't exist, create it
        logger.info("Creating new schema...")
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # Remove comments
        sql = re.sub(r'--.*\n', '', sql)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Execute each statement
        for stmt in [s.strip() for s in sql.split(';') if s.strip()]:
            try:
                conn.execute(stmt)
            except Exception as e:
                logger.debug(f"Statement warning: {e}")
        
        logger.info("Schema setup complete")
        return True
    except Exception as e:
        logger.error(f"Schema setup failed: {e}")
        return False


def _get_staging_data(pg_conn_string: str, crawl_date: Optional[date] = None) -> pd.DataFrame:
    """ Get staging data from PostgreSQL """
    import psycopg2
    
    if crawl_date is None:
        crawl_date = date.today()
    
    query = """
        SELECT *
        FROM jobinsight_staging.staging_jobs
        WHERE DATE(crawled_at) = %s
    """
    
    try:
        with psycopg2.connect(pg_conn_string) as conn:
            df = pd.read_sql(query, conn, params=[crawl_date])
        logger.info(f"Loaded {len(df)} records from staging for {crawl_date}")
        return df
    except Exception as e:
        logger.error(f"Failed to get staging data: {e}")
        return pd.DataFrame()


def run_etl(
    pg_conn_string: str,
    crawl_date: Optional[date] = None,
    force_new: bool = False
) -> Dict[str, Any]:
    """
    Run full ETL pipeline: Staging -> DWH (MinIO).
    
    Flow:
    1. Download DuckDB from MinIO (or create new)
    2. Backup to MinIO
    3. Get staging data from PostgreSQL (only today's crawl)
    4. Process dimensions & facts (carry forward + new)
    5. Upload DuckDB back to MinIO
    6. Export Parquet to MinIO
    """
    start_time = datetime.now()
    result = {
        'success': False,
        'start_time': start_time.isoformat(),
        'stats': {}
    }
    
    local_db_path = None
    
    try:
        logger.info("=" * 60)
        logger.info(f"ETL START: {start_time}")
        logger.info("=" * 60)
        
        # 1. Download DuckDB from MinIO
        local_db_path = download_duckdb(force_new=force_new)
        
        # 2. Backup existing database
        if not force_new:
            result['backup_object'] = backup_duckdb(local_db_path)
        
        # 3. Get staging data (only today's crawl)
        staging_df = _get_staging_data(pg_conn_string, crawl_date)
        result['stats']['staging_count'] = len(staging_df)
        
        if staging_df.empty:
            logger.info("No new data to process")
            result['success'] = True
            result['message'] = 'No new data'
            return result
        
        # 4. Connect to DuckDB and setup schema
        with get_duckdb_connection(local_db_path) as conn:
            if not _setup_schema(conn):
                result['message'] = 'Schema setup failed'
                return result
            
            # 5. Process dimensions
            logger.info("Processing dimensions...")
            result['stats']['dim_job'] = process_dim_job(conn, staging_df)
            result['stats']['dim_company'] = process_dim_company(conn, staging_df)
            result['stats']['dim_location'] = process_dim_location(conn, staging_df)
            result['stats']['dim_date'] = process_dim_date(conn, staging_df)
            
            # 6. Initialize caches
            caches = init_dimension_caches(conn)
            
            # 7. Process facts
            logger.info("Processing facts...")
            result['stats']['facts'] = process_facts(conn, staging_df, caches)
            
            # 8. Process bridges
            logger.info("Processing bridges...")
            result['stats']['bridges'] = process_bridges(conn, staging_df, caches)
            
            # 9. Cleanup duplicates
            result['stats']['cleanup'] = cleanup_duplicate_facts(conn)
            
            # 10. Export all tables to Parquet
            load_month = datetime.now().strftime('%Y-%m')
            result['stats']['parquet_export'] = export_all_parquet(conn, load_month)
            result['load_month'] = load_month
        
        # 11. Upload DuckDB back to MinIO
        upload_duckdb(local_db_path)
        
        result['success'] = True
        result['message'] = 'ETL completed successfully'
        
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        result['message'] = str(e)
    
    finally:
        if local_db_path and os.path.exists(local_db_path):
            try:
                os.remove(local_db_path)
            except:
                pass
        
        end_time = datetime.now()
        result['end_time'] = end_time.isoformat()
        result['duration_seconds'] = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"ETL END: Duration {result['duration_seconds']:.2f}s")
        logger.info(f"Status: {'SUCCESS' if result['success'] else 'FAILED'}")
        logger.info("=" * 60)
    
    return result
