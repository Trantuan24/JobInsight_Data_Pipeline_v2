"""
Storage module exports.

Files:
- minio.py: All MinIO operations (raw, dwh, backup)
- archive.py: Archive logic (query + export + restore)
- postgres.py: PostgreSQL operations
"""

from .postgres import bulk_upsert, get_db_connection
from .minio import (
    # Client & init
    get_minio_client,
    init_minio_buckets,
    # Buckets
    RAW_BUCKET,
    ARCHIVE_BUCKET,
    BACKUP_BUCKET,
    WAREHOUSE_BUCKET,
    # Raw HTML
    upload_html,
    list_html_files,
    download_html,
    # DWH
    reset_dwh,
    download_duckdb,
    upload_duckdb,
    backup_duckdb,
    export_parquet,
    get_duckdb_connection,
)
from .archive import (
    get_old_records,
    export_to_parquet,
    upload_archive_to_minio,
    verify_archive,
    delete_old_records,
    list_archives,
    restore_from_archive
)

__all__ = [
    # PostgreSQL
    'bulk_upsert',
    'get_db_connection',
    # MinIO client
    'get_minio_client',
    'init_minio_buckets',
    # Buckets
    'RAW_BUCKET',
    'ARCHIVE_BUCKET',
    'BACKUP_BUCKET',
    'WAREHOUSE_BUCKET',
    # Raw HTML
    'upload_html',
    'list_html_files',
    'download_html',
    # DWH
    'reset_dwh',
    'download_duckdb',
    'upload_duckdb',
    'backup_duckdb',
    'export_parquet',
    'get_duckdb_connection',
    # Archive
    'get_old_records',
    'export_to_parquet',
    'upload_archive_to_minio',
    'verify_archive',
    'delete_old_records',
    'list_archives',
    'restore_from_archive',
]
