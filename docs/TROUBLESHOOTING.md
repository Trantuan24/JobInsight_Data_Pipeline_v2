# Troubleshooting Guide

## Common Issues

### Airflow

#### DAG not showing in UI
```bash
# Check for syntax errors
docker exec jobinsight-airflow-webserver-1 python3 -c "from dags.pipeline_dag import dag"

# Check scheduler logs
docker compose logs airflow-scheduler | tail -50

# Force refresh
docker exec jobinsight-airflow-webserver-1 airflow dags reserialize
```

#### Task stuck in "queued"
```bash
# Restart scheduler
docker compose restart airflow-scheduler

# Check executor
docker exec jobinsight-airflow-webserver-1 airflow config get-value core executor
```

#### Import errors in DAG
```bash
# Check PYTHONPATH
docker exec jobinsight-airflow-webserver-1 python3 -c "import sys; print(sys.path)"

# Test import
docker exec jobinsight-airflow-webserver-1 python3 -c "from src.storage import upload_html"
```

---

### PostgreSQL

#### Connection refused
```bash
# Check if running
docker compose ps postgres

# Check health
docker exec jobinsight-airflow-webserver-1 pg_isready -h postgres -U jobinsight

# Restart
docker compose restart postgres
```

#### Database not found
```bash
# Check databases
docker exec -it jobinsight-airflow-webserver-1 psql -h postgres -U airflow -c "\l"

# Re-run init script
docker compose down
docker compose up -d
```

---

### MinIO

#### Connection refused
```bash
# Check health
curl http://localhost:9000/minio/health/live

# Restart
docker compose restart minio
```

#### Bucket not found
```bash
# List buckets
docker exec jobinsight_minio mc ls /data/

# Re-init buckets
docker exec jobinsight-airflow-webserver-1 python3 -c "
from src.storage.minio import init_minio_buckets
init_minio_buckets()
"
```

#### Upload failed
```bash
# Check credentials in .env
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Check endpoint
MINIO_ENDPOINT=minio:9000  # Inside container
# NOT localhost:9000
```

---

### DuckDB

#### File not found
```bash
# Check if exists
docker exec jobinsight-airflow-webserver-1 ls -la /opt/airflow/data/

# Download from MinIO
docker exec jobinsight-airflow-webserver-1 python3 -c "
from src.storage.minio import download_duckdb
path = download_duckdb()
print(f'Downloaded to: {path}')
"
```

#### Read-only error (Superset)
- Check `docker-compose.yml`: volume mount should NOT have `:ro`
```yaml
volumes:
  - ./data:/app/data  # NOT ./data:/app/data:ro
```

#### Corrupted database
```bash
# Restore from backup
docker exec jobinsight_minio mc cp /data/jobinsight-backup/dwh_backups/latest.duckdb /tmp/

# Or rebuild from staging
docker exec jobinsight-airflow-webserver-1 airflow dags trigger jobinsight_dwh --conf '{"force_new": true}'
```

---

### Grafana

#### Dashboard empty
1. Check datasource connection: Settings → Data Sources → Test
2. Verify PostgreSQL monitoring schema has data:
```sql
SELECT COUNT(*) FROM monitoring.etl_metrics;
SELECT COUNT(*) FROM monitoring.quality_metrics;
```

#### Datasource error
- Host should be `postgres` (not `localhost`)
- Port: `5432` (internal), NOT `5434` (external)

---

### Superset

#### DuckDB connection failed
1. Install duckdb-engine (already in docker-compose command)
2. SQLAlchemy URI: `duckdb:////app/data/jobinsight.duckdb`
3. Check file permissions

#### Charts not loading
1. Refresh dataset schema: Data → Datasets → Edit → Sync columns
2. Check DuckDB has data

---

### Crawling

#### No jobs crawled
```bash
# Test crawler manually
docker exec jobinsight-airflow-webserver-1 python3 -c "
import asyncio
from src.data_sources.topcv import scrape_pages
results = asyncio.run(scrape_pages(num_pages=1))
print(results)
"
```

#### Playwright errors
```bash
# Check browser installed
docker exec jobinsight-airflow-webserver-1 playwright install chromium

# Check dependencies
docker exec jobinsight-airflow-webserver-1 playwright install-deps
```

---

### ETL Pipeline

#### Staging empty after pipeline
```bash
# Check raw_jobs has data
docker exec jobinsight-airflow-webserver-1 psql -h postgres -U jobinsight -d jobinsight -c "
SELECT COUNT(*) FROM raw_jobs WHERE DATE(crawled_at) = CURRENT_DATE;
"

# Check staging pipeline logs
docker compose logs airflow-scheduler | grep transform_staging
```

#### DWH facts not created
```bash
# Check staging has today's data
docker exec jobinsight-airflow-webserver-1 psql -h postgres -U jobinsight -d jobinsight -c "
SELECT COUNT(*) FROM jobinsight_staging.staging_jobs WHERE DATE(crawled_at) = CURRENT_DATE;
"

# Check DWH ETL logs
docker compose logs airflow-scheduler | grep run_dwh_etl
```

---

## Reset Everything

```bash
# Stop all
docker compose down

# Remove volumes (WARNING: deletes all data)
docker compose down -v

# Rebuild
docker compose build --no-cache

# Start fresh
docker compose up -d
```

## Logs Location

| Component | Location |
|-----------|----------|
| Airflow scheduler | `logs/scheduler/` |
| DAG runs | `logs/dag_id=*/` |
| Docker logs | `docker compose logs <service>` |

## Health Checks

```bash
# All services
docker compose ps

# Airflow
curl http://localhost:8080/health

# MinIO
curl http://localhost:9000/minio/health/live

# PostgreSQL
docker exec jobinsight-airflow-webserver-1 pg_isready -h postgres
```
