# MinIO Storage Guide

## Overview

MinIO là object storage (S3-compatible) cho JobInsight, lưu trữ HTML backups, archives, và DWH data.

## Access

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Web Console | http://localhost:9001 | minioadmin / minioadmin |
| S3 API | http://localhost:9000 | - |

## Buckets

| Bucket | Mục đích | Retention |
|--------|----------|-----------|
| `jobinsight-raw` | HTML backup từ crawler | 15 ngày |
| `jobinsight-archive` | Parquet archives từ PostgreSQL | Vĩnh viễn |
| `jobinsight-backup` | Database backups (PostgreSQL + DuckDB) | 7 ngày |
| `jobinsight-warehouse` | DWH database + Parquet exports | 12 tháng |

> Chi tiết retention policies: [retention_policies.md](../governance/retention_policies.md)

## Bucket Structure

### jobinsight-raw (HTML Backup)
```
jobinsight-raw/
└── html/
    ├── it_p1_20260118060000.html    # Page 1, crawled at 06:00
    ├── it_p2_20260118060000.html    # Page 2
    ├── it_p3_20260118060000.html    # Page 3
    └── ...
```
- **Naming**: `it_p{page}_{YYYYMMDDHHmmss}.html`
- **Source**: `pipeline_dag.py` → `upload_html_to_minio()`
- **Cleanup**: `maintenance_dag.py` xóa files > 15 ngày

### jobinsight-archive (Parquet Archives)
```
jobinsight-archive/
└── year=2026/
    └── month=01/
        ├── raw_jobs_20260112_020000.parquet
        ├── raw_jobs_20260119_020000.parquet
        └── ...
```
- **Naming**: `year={YYYY}/month={MM}/raw_jobs_{timestamp}.parquet`
- **Source**: `archive_dag.py` archive raw_jobs > 30 ngày
- **Content**: PostgreSQL `raw_jobs` data dạng Parquet

### jobinsight-backup (Database Backups)
```
jobinsight-backup/
├── pg_backups/
│   ├── jobinsight_20260118_030000.dump
│   ├── jobinsight_20260117_030000.dump
│   └── ... (giữ 7 ngày gần nhất)
└── dwh_backups/
    ├── jobinsight_20260118_070000.duckdb
    ├── jobinsight_20260117_070000.duckdb
    └── ... (giữ 7 ngày gần nhất)
```
- **pg_backups**: PostgreSQL dump (`pg_dump -Fc`)
- **dwh_backups**: DuckDB snapshot trước mỗi ETL
- **Source**: `maintenance_dag.py` (PostgreSQL), `dwh_dag.py` (DuckDB)

### jobinsight-warehouse (DWH)
```
jobinsight-warehouse/
├── dwh/
│   └── jobinsight.duckdb           # DuckDB database (latest)
└── parquet/
    └── load_month=2026-01/
        ├── DimJob.parquet          # Dimension: Jobs
        ├── DimCompany.parquet      # Dimension: Companies
        ├── DimLocation.parquet     # Dimension: Locations
        ├── DimDate.parquet         # Dimension: Dates
        ├── FactJobPostingDaily.parquet    # Fact table
        └── FactJobLocationBridge.parquet  # Bridge table
```
- **dwh/**: DuckDB database file, download/upload mỗi ETL run
- **parquet/**: Export từ DuckDB, partitioned by `load_month`
- **Source**: `dwh_dag.py` → `src/etl/warehouse/pipeline.py`

## Configuration

**Environment variables** (`.env`):
```bash
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
RETENTION_HTML_DAYS=15
RETENTION_BACKUP_DAYS=7
```

## Automated Tasks

| DAG | Schedule | MinIO Tasks |
|-----|----------|-------------|
| `jobinsight_pipeline` | Daily 6:00 AM | Upload HTML → `jobinsight-raw` |
| `jobinsight_dwh` | Daily 7:00 AM | Backup DuckDB, upload DWH + Parquet |
| `jobinsight_maintenance` | Daily 3:00 AM | Backup PostgreSQL, cleanup old files |
| `jobinsight_archive` | Sunday 2:00 AM | Archive raw_jobs → Parquet |

## Quick Commands

```bash
# Start MinIO
docker compose up -d minio

# Check health
curl http://localhost:9000/minio/health/live

# List buckets
docker exec jobinsight_minio mc ls /data/

# List objects in bucket
docker exec jobinsight_minio mc ls /data/jobinsight-warehouse/

# Check storage usage
docker exec jobinsight_minio mc du /data/

# View logs
docker logs jobinsight_minio
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | `docker compose restart minio` |
| Bucket not found | `docker compose restart airflow-init` |
| Disk full | `airflow dags trigger jobinsight_maintenance` |

## Related Files

- Config: `src/config/storage_config.py`
- MinIO client: `src/storage/minio.py`, `src/storage/minio_storage.py`
- Archive: `src/storage/archive.py`
- Retention details: `docs/governance/retention_policies.md`
