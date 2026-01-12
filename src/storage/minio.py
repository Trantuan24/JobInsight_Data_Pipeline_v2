"""
MinIO storage operations.

Buckets:
- jobinsight-raw: Raw HTML from crawler
- jobinsight-archive: Archived old data (Parquet)
- jobinsight-backup: DuckDB backups
- jobinsight-warehouse: DWH DuckDB + Parquet exports
"""

import io
import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, Any, List, Optional

import duckdb
from minio import Minio
from minio.error import S3Error

from src.config import MINIO_CONFIG

logger = logging.getLogger(__name__)

# Buckets
RAW_BUCKET = "jobinsight-raw"
ARCHIVE_BUCKET = "jobinsight-archive"
BACKUP_BUCKET = "jobinsight-backup"
WAREHOUSE_BUCKET = "jobinsight-warehouse"

ALL_BUCKETS = [RAW_BUCKET, ARCHIVE_BUCKET, BACKUP_BUCKET, WAREHOUSE_BUCKET]

# DWH paths
DUCKDB_OBJECT = 'dwh/jobinsight.duckdb'
PARQUET_PREFIX = 'parquet'
BACKUP_PREFIX = 'dwh_backups'
LOCAL_TEMP_DIR = '/tmp/jobinsight_dwh'


def get_minio_client() -> Minio:
    """Get MinIO client."""
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"]
    )


def init_minio_buckets():
    """Initialize all MinIO buckets on startup."""
    try:
        client = get_minio_client()
        
        for bucket in ALL_BUCKETS:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.info(f"Bucket exists: {bucket}")
        
        logger.info("MinIO initialization completed")
        
    except S3Error as e:
        logger.error(f"MinIO initialization error: {e}")
        raise


# =============================================================================
# RAW HTML OPERATIONS (jobinsight-raw)
# =============================================================================

def upload_html(html: str, page_num: int) -> Dict[str, Any]:
    """Upload HTML content to MinIO raw bucket."""
    try:
        client = get_minio_client()
        
        if not client.bucket_exists(RAW_BUCKET):
            client.make_bucket(RAW_BUCKET)
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        object_name = f"html/it_p{page_num}_{timestamp}.html"
        
        data = html.encode('utf-8')
        client.put_object(RAW_BUCKET, object_name, io.BytesIO(data), len(data), content_type='text/html')
        
        logger.info(f"Uploaded {object_name} ({len(data)} bytes)")
        return {"success": True, "bucket": RAW_BUCKET, "object": object_name, "size": len(data)}
        
    except Exception as e:
        logger.error(f"Upload HTML error: {e}")
        return {"success": False, "error": str(e)}


def list_html_files(prefix: str = "html/") -> List[str]:
    """List HTML files in raw bucket."""
    try:
        client = get_minio_client()
        objects = client.list_objects(RAW_BUCKET, prefix=prefix)
        return [obj.object_name for obj in objects]
    except Exception as e:
        logger.error(f"List HTML error: {e}")
        return []


def download_html(object_name: str) -> str:
    """Download HTML content from raw bucket."""
    try:
        client = get_minio_client()
        response = client.get_object(RAW_BUCKET, object_name)
        return response.read().decode('utf-8')
    except Exception as e:
        logger.error(f"Download HTML error: {e}")
        return ""


# =============================================================================
# DWH OPERATIONS (jobinsight-warehouse, jobinsight-backup)
# =============================================================================

def reset_dwh() -> bool:
    """Delete existing DuckDB file on MinIO and local temp."""
    try:
        client = get_minio_client()
        
        try:
            client.remove_object(WAREHOUSE_BUCKET, DUCKDB_OBJECT)
            logger.info(f"Deleted DuckDB on MinIO")
        except:
            pass
        
        local_path = os.path.join(LOCAL_TEMP_DIR, 'jobinsight.duckdb')
        for ext in ['', '.wal']:
            path = local_path + ext
            if os.path.exists(path):
                os.remove(path)
        
        return True
    except Exception as e:
        logger.error(f"Reset DWH error: {e}")
        return False


def download_duckdb(force_new: bool = False) -> str:
    """Download DuckDB file from MinIO. Returns local path."""
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_TEMP_DIR, 'jobinsight.duckdb')
    
    for ext in ['', '.wal', '.tmp']:
        path = local_path + ext
        if os.path.exists(path):
            os.remove(path)
    
    if force_new:
        logger.info("Creating fresh DuckDB")
        return local_path
    
    try:
        client = get_minio_client()
        try:
            client.stat_object(WAREHOUSE_BUCKET, DUCKDB_OBJECT)
            client.fget_object(WAREHOUSE_BUCKET, DUCKDB_OBJECT, local_path)
            logger.info("Downloaded DuckDB from MinIO")
        except:
            logger.info("No existing DuckDB, will create new")
        return local_path
    except Exception as e:
        logger.error(f"Download DuckDB error: {e}")
        raise


def upload_duckdb(local_path: str):
    """Upload DuckDB file to MinIO."""
    try:
        client = get_minio_client()
        if not client.bucket_exists(WAREHOUSE_BUCKET):
            client.make_bucket(WAREHOUSE_BUCKET)
        client.fput_object(WAREHOUSE_BUCKET, DUCKDB_OBJECT, local_path)
        logger.info("Uploaded DuckDB to MinIO")
    except Exception as e:
        logger.error(f"Upload DuckDB error: {e}")
        raise


def get_duckdb_connection(local_path: str = None) -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection from local temp file."""
    if local_path is None:
        local_path = os.path.join(LOCAL_TEMP_DIR, 'jobinsight.duckdb')
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    return duckdb.connect(local_path)


def backup_duckdb(local_path: str) -> Optional[str]:
    """Backup DuckDB to MinIO. Keeps last 5 backups."""
    if not os.path.exists(local_path):
        return None
    
    try:
        client = get_minio_client()
        if not client.bucket_exists(BACKUP_BUCKET):
            client.make_bucket(BACKUP_BUCKET)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_object = f'{BACKUP_PREFIX}/jobinsight_{timestamp}.duckdb'
        
        client.fput_object(BACKUP_BUCKET, backup_object, local_path)
        logger.info(f"Backed up DuckDB: {backup_object}")
        
        # Cleanup old backups
        objects = list(client.list_objects(BACKUP_BUCKET, prefix=BACKUP_PREFIX))
        backups = sorted([o.object_name for o in objects if o.object_name.endswith('.duckdb')])
        while len(backups) > 5:
            client.remove_object(BACKUP_BUCKET, backups.pop(0))
        
        return backup_object
    except Exception as e:
        logger.error(f"Backup DuckDB error: {e}")
        return None


def export_parquet(conn: duckdb.DuckDBPyConnection, load_month: str) -> bool:
    """Export facts to Parquet and upload to MinIO."""
    try:
        client = get_minio_client()
        if not client.bucket_exists(WAREHOUSE_BUCKET):
            client.make_bucket(WAREHOUSE_BUCKET)
        
        df = conn.execute(f"""
            SELECT f.*, j.job_id, j.title, c.company_name
            FROM FactJobPostingDaily f
            JOIN DimJob j ON f.job_sk = j.job_sk
            JOIN DimCompany c ON f.company_sk = c.company_sk
            WHERE f.load_month = '{load_month}'
        """).fetchdf()
        
        if not df.empty:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                df.to_parquet(tmp.name, index=False)
                object_name = f'{PARQUET_PREFIX}/load_month={load_month}/facts.parquet'
                client.fput_object(WAREHOUSE_BUCKET, object_name, tmp.name)
                os.unlink(tmp.name)
                logger.info(f"Exported {len(df)} records to Parquet")
        
        return True
    except Exception as e:
        logger.error(f"Export Parquet error: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_minio_buckets()
