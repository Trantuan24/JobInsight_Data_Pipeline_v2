"""Archive storage operations - Export old data to Parquet and upload to MinIO"""
import io
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from src.config import DB_CONFIG, MINIO_CONFIG
from src.storage.postgres import get_db_connection

logger = logging.getLogger(__name__)

ARCHIVE_BUCKET = "jobinsight-archive"


def get_minio_client() -> Minio:
    """Get MinIO client"""
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"]
    )


def get_old_records(days: int = 30) -> pd.DataFrame:
    """
    Query records older than X days from PostgreSQL
    
    Args:
        days: Number of days to keep (default 30)
        
    Returns:
        DataFrame with old records
    """
    conn = None
    try:
        conn = get_db_connection()
        
        query = """
            SELECT * FROM raw_jobs 
            WHERE crawled_at < NOW() - INTERVAL '%s days'
            ORDER BY crawled_at ASC
        """
        
        df = pd.read_sql(query, conn, params=(days,))
        logger.info(f"Found {len(df)} records older than {days} days")
        return df
        
    except Exception as e:
        logger.error(f"Error querying old records: {e}")
        raise
    finally:
        if conn:
            conn.close()


def export_to_parquet(df: pd.DataFrame) -> bytes:
    """
    Export DataFrame to Parquet bytes
    
    Args:
        df: DataFrame to export
        
    Returns:
        Parquet file as bytes
    """
    if df.empty:
        return b""
    
    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Write to bytes buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    
    buffer.seek(0)
    return buffer.getvalue()


def upload_archive_to_minio(data: bytes, archive_date: datetime) -> Dict[str, Any]:
    """
    Upload Parquet archive to MinIO
    
    Args:
        data: Parquet file bytes
        archive_date: Date for partitioning
        
    Returns:
        Dict with upload result
    """
    try:
        client = get_minio_client()
        
        # Ensure bucket exists
        if not client.bucket_exists(ARCHIVE_BUCKET):
            client.make_bucket(ARCHIVE_BUCKET)
            logger.info(f"Created bucket: {ARCHIVE_BUCKET}")
        
        # Create object name with date partitioning
        year = archive_date.strftime("%Y")
        month = archive_date.strftime("%m")
        timestamp = archive_date.strftime("%Y%m%d_%H%M%S")
        object_name = f"year={year}/month={month}/raw_jobs_{timestamp}.parquet"
        
        # Upload
        client.put_object(
            ARCHIVE_BUCKET,
            object_name,
            io.BytesIO(data),
            len(data),
            content_type='application/octet-stream'
        )
        
        logger.info(f"Uploaded archive to {ARCHIVE_BUCKET}/{object_name} ({len(data)} bytes)")
        
        return {
            "success": True,
            "bucket": ARCHIVE_BUCKET,
            "object": object_name,
            "size": len(data)
        }
        
    except Exception as e:
        logger.error(f"MinIO archive upload error: {e}")
        return {"success": False, "error": str(e)}


def verify_archive(object_name: str, expected_count: int) -> Dict[str, Any]:
    """
    Download and verify archived Parquet file
    
    Args:
        object_name: MinIO object name
        expected_count: Expected row count
        
    Returns:
        Dict with verification result
    """
    try:
        client = get_minio_client()
        
        # Download file
        response = client.get_object(ARCHIVE_BUCKET, object_name)
        data = response.read()
        
        # Read Parquet and count rows
        buffer = io.BytesIO(data)
        table = pq.read_table(buffer)
        actual_count = table.num_rows
        
        verified = actual_count == expected_count
        
        if verified:
            logger.info(f"Archive verified: {actual_count} rows match expected {expected_count}")
        else:
            logger.error(f"Archive verification FAILED: got {actual_count}, expected {expected_count}")
        
        return {
            "verified": verified,
            "expected": expected_count,
            "actual": actual_count,
            "object": object_name
        }
        
    except Exception as e:
        logger.error(f"Archive verification error: {e}")
        return {"verified": False, "error": str(e)}


def delete_old_records(job_ids: List[str]) -> Dict[str, Any]:
    """
    Delete records by job_ids from PostgreSQL
    
    Args:
        job_ids: List of job_ids to delete
        
    Returns:
        Dict with delete result
    """
    if not job_ids:
        return {"deleted": 0}
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Delete in batches to avoid long transactions
        batch_size = 1000
        total_deleted = 0
        
        for i in range(0, len(job_ids), batch_size):
            batch = job_ids[i:i + batch_size]
            placeholders = ",".join(["%s"] * len(batch))
            
            cur.execute(
                f"DELETE FROM raw_jobs WHERE job_id IN ({placeholders})",
                batch
            )
            total_deleted += cur.rowcount
        
        conn.commit()
        logger.info(f"Deleted {total_deleted} records from PostgreSQL")
        
        return {"deleted": total_deleted}
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Delete error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def list_archives(year: Optional[str] = None, month: Optional[str] = None) -> List[str]:
    """
    List archive files in MinIO
    
    Args:
        year: Filter by year (optional)
        month: Filter by month (optional)
        
    Returns:
        List of object names
    """
    try:
        client = get_minio_client()
        
        if not client.bucket_exists(ARCHIVE_BUCKET):
            return []
        
        prefix = ""
        if year:
            prefix = f"year={year}/"
            if month:
                prefix = f"year={year}/month={month}/"
        
        objects = client.list_objects(ARCHIVE_BUCKET, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
        
    except Exception as e:
        logger.error(f"List archives error: {e}")
        return []


def restore_from_archive(object_name: str) -> Dict[str, Any]:
    """
    Restore data from archive back to PostgreSQL
    
    Args:
        object_name: MinIO object name
        
    Returns:
        Dict with restore result
    """
    try:
        client = get_minio_client()
        
        # Download Parquet
        response = client.get_object(ARCHIVE_BUCKET, object_name)
        data = response.read()
        
        # Read to DataFrame
        buffer = io.BytesIO(data)
        df = pq.read_table(buffer).to_pandas()
        
        if df.empty:
            return {"restored": 0}
        
        # Import bulk_upsert
        from src.storage.postgres import bulk_upsert
        
        # Upsert back to DB
        result = bulk_upsert(df)
        
        logger.info(f"Restored {len(df)} records from {object_name}")
        
        return {
            "restored": len(df),
            "inserted": result.get("inserted", 0),
            "updated": result.get("updated", 0),
            "object": object_name
        }
        
    except Exception as e:
        logger.error(f"Restore error: {e}")
        return {"restored": 0, "error": str(e)}
