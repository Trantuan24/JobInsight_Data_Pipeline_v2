"""MinIO initialization functions"""
import logging
from minio import Minio
from minio.error import S3Error

from src.config import MINIO_CONFIG

logger = logging.getLogger(__name__)

# Buckets cần tạo
BUCKETS = [
    "jobinsight-raw",
    "jobinsight-archive", 
    "jobinsight-backup",
    "jobinsight-warehouse"
]


def get_minio_client() -> Minio:
    """Get MinIO client"""
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"]
    )


def init_minio_buckets():
    """
    Initialize MinIO buckets on startup.
    Called by airflow-init in docker-compose.yml
    """
    try:
        client = get_minio_client()
        
        for bucket in BUCKETS:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.info(f"Bucket already exists: {bucket}")
        
        logger.info("MinIO initialization completed")
        
    except S3Error as e:
        logger.error(f"MinIO initialization error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during MinIO init: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_minio_buckets()
