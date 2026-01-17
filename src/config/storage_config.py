"""Storage configuration (MinIO)"""
import os

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
}

# Bucket names
BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "jobinsight-raw")
BUCKET_WAREHOUSE = os.getenv("MINIO_BUCKET_WAREHOUSE", "jobinsight-warehouse")
BUCKET_ARCHIVE = os.getenv("MINIO_BUCKET_ARCHIVE", "jobinsight-archive")
BUCKET_BACKUP = os.getenv("MINIO_BUCKET_BACKUP", "jobinsight-backup")
