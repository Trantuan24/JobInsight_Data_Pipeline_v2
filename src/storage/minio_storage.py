"""MinIO storage operations"""
import io
import logging
from datetime import datetime
from typing import Dict, Any, List

from minio import Minio

from src.config import MINIO_CONFIG

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Get MinIO client"""
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"]
    )


def upload_html_to_minio(html: str, page_num: int) -> Dict[str, Any]:
    """Upload HTML content to MinIO"""
    try:
        client = get_minio_client()
        bucket = MINIO_CONFIG["bucket"]
        
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        object_name = f"html/it_p{page_num}_{timestamp}.html"
        
        data = html.encode('utf-8')
        client.put_object(
            bucket,
            object_name,
            io.BytesIO(data),
            len(data),
            content_type='text/html'
        )
        
        logger.info(f"Uploaded {object_name} to MinIO ({len(data)} bytes)")
        
        return {
            "success": True,
            "bucket": bucket,
            "object": object_name,
            "size": len(data)
        }
        
    except Exception as e:
        logger.error(f"MinIO upload error: {e}")
        return {"success": False, "error": str(e)}


def list_html_files(prefix: str = "html/") -> List[str]:
    """List HTML files in MinIO bucket"""
    try:
        client = get_minio_client()
        bucket = MINIO_CONFIG["bucket"]
        
        objects = client.list_objects(bucket, prefix=prefix)
        return [obj.object_name for obj in objects]
    except Exception as e:
        logger.error(f"MinIO list error: {e}")
        return []


def download_html_from_minio(object_name: str) -> str:
    """Download HTML content from MinIO"""
    try:
        client = get_minio_client()
        bucket = MINIO_CONFIG["bucket"]
        
        response = client.get_object(bucket, object_name)
        return response.read().decode('utf-8')
    except Exception as e:
        logger.error(f"MinIO download error: {e}")
        return ""
