# MinIO Operations Guide - JobInsight Data Pipeline

## Giới thiệu

Hướng dẫn vận hành MinIO hàng ngày. Đọc `minio_setup_guide.md` trước nếu chưa setup.

---

## 1. Health Checks

### Check service status

```bash
# Docker status
docker ps | grep minio

# Health endpoint
curl http://localhost:9000/minio/health/live
# Expected: OK hoặc {"status":"ok"}

# Container logs
docker logs jobinsight_minio --tail 50
```

### Check từ Airflow

MinIO là dependency của Airflow services. Nếu MinIO down, Airflow sẽ không start.

```bash
# Restart nếu cần
docker-compose restart minio
```

---

## 2. Monitoring Storage

### Via Console (Web UI)

1. Truy cập http://localhost:9001
2. Login: `minioadmin / minioadmin`
3. Xem **Buckets** → Click vào bucket → Xem objects

### Via CLI

```bash
# List buckets
docker exec jobinsight_minio mc ls /data/

# List objects trong bucket
docker exec jobinsight_minio mc ls /data/jobinsight-raw/

# Disk usage
docker exec jobinsight_minio mc du /data/jobinsight-raw/
```

### Via Python

```python
from src.storage.minio_storage import list_html_files
from src.storage.archive import list_archives

# List HTML files
html_files = list_html_files()
print(f"HTML files: {len(html_files)}")

# List archives
archives = list_archives(year="2025")
print(f"Archives: {len(archives)}")
```

---

## 3. Backup Operations

### 3.1 Archive Flow (Tự động)

`archive_dag.py` chạy weekly (Sunday 2:00 AM):

```
Query old records (>30 days) → Export Parquet → Upload MinIO → Verify → Delete from DB
```

**Manual trigger:**

```bash
docker exec jobinsight-airflow-webserver-1 \
  airflow dags trigger jobinsight_archive
```

### 3.2 Manual Backup

**Backup bucket ra local:**

```bash
# Copy từ MinIO container ra host
docker cp jobinsight_minio:/data/jobinsight-archive ./backup/

# Hoặc dùng mc
docker run --rm -it \
  -v $(pwd)/backup:/backup \
  --network jobinsight_network \
  minio/mc mirror minio/jobinsight-archive /backup/archive/
```

**Backup toàn bộ MinIO data:**

```bash
# Stop MinIO trước
docker-compose stop minio

# Backup volume
docker run --rm \
  -v minio-data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar czf /backup/minio_backup_$(date +%Y%m%d).tar.gz /data

# Start lại
docker-compose start minio
```

### 3.3 DWH Operations

**Automated ETL Flow (Pure Periodic Snapshot):**
```
1. Download dwh.duckdb từ MinIO (hoặc tạo mới)
2. Backup existing database
3. Carry forward: Tạo facts cho jobs còn hạn từ ngày trước
4. Query staging data từ PostgreSQL (chỉ jobs crawl ngày hôm nay)
5. Process dimensions (SCD Type 2)
6. Process facts (UPSERT cho ngày hôm nay)
7. Process bridge tables
8. Export Parquet partitions
9. Upload dwh.duckdb back to MinIO
```

**Manual trigger (when DAG exists):**

```bash
# Trigger DWH ETL DAG
docker exec jobinsight-airflow-webserver-1 \
  airflow dags trigger jobinsight_dwh
```

**Run directly from Python:**

```python
# Execute inside Airflow container
docker exec jobinsight-airflow-webserver-1 python -c "
from src.etl.warehouse.pipeline import run_etl
import os

pg_conn = f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@postgres:5432/{os.getenv("POSTGRES_DB")}'
result = run_etl(pg_conn_string=pg_conn)
print(result)
"
```

**Check DWH data:**

```python
from src.storage.minio import download_duckdb, get_duckdb_connection

db_path = download_duckdb()
with get_duckdb_connection(db_path) as conn:
    # Check fact counts
    facts = conn.execute("SELECT COUNT(*) FROM FactJobPostingDaily").fetchone()[0]
    print(f"Total facts: {facts}")
    
    # Check dimensions
    jobs = conn.execute("SELECT COUNT(*) FROM DimJob WHERE is_current=TRUE").fetchone()[0]
    companies = conn.execute("SELECT COUNT(*) FROM DimCompany WHERE is_current=TRUE").fetchone()[0]
    print(f"Current jobs: {jobs}, companies: {companies}")
    
    # Check facts by date (Pure Periodic Snapshot - 1 record/job/ngày)
    facts_by_date = conn.execute("""
        SELECT date_id, COUNT(*) as count
        FROM FactJobPostingDaily
        WHERE date_id >= CURRENT_DATE - 7
        GROUP BY date_id
        ORDER BY date_id DESC
    """).fetchdf()
    print(f"Facts by date (last 7 days):\n{facts_by_date}")
```

**Download Parquet exports:**

```bash
# List Parquet files
docker exec jobinsight_minio mc ls --recursive /data/jobinsight-warehouse/parquet/

# Download specific month
docker exec jobinsight_minio mc cp --recursive \
  /data/jobinsight-warehouse/parquet/load_month=2025-01/ \
  ./local_parquet/
```

---

## 4. Restore Operations

### Restore từ Archive

```python
from src.storage.archive import restore_from_archive

# Restore specific archive
result = restore_from_archive("year=2025/month=01/raw_jobs_20250102_143022.parquet")
print(f"Restored: {result['restored']} records")
```

### Restore toàn bộ MinIO

```bash
# Stop MinIO
docker-compose stop minio

# Restore volume
docker run --rm \
  -v minio-data:/data \
  -v $(pwd)/backup:/backup \
  alpine sh -c "rm -rf /data/* && tar xzf /backup/minio_backup_20250102.tar.gz -C /"

# Start lại
docker-compose start minio
```

---

## 5. Cleanup Operations

### Automated Cleanup (Maintenance DAG)

`maintenance_dag.py` chạy daily 3:00 AM, tự động:
- Backup PostgreSQL
- Cleanup HTML files > 15 ngày
- Cleanup DWH backups > 7 ngày
- Cleanup PostgreSQL backups > 7 ngày

**Manual trigger:**
```bash
docker exec jobinsight-airflow-webserver-1 \
  airflow dags trigger jobinsight_maintenance
```

### Manual Delete old HTML files

```bash
# List files older than 15 days
docker exec jobinsight_minio mc find /data/jobinsight-raw/ --older-than 15d

# Delete (cẩn thận!)
docker exec jobinsight_minio mc rm --recursive --older-than 15d /data/jobinsight-raw/html/
```

### Delete specific object

```python
from minio import Minio
from src.config import MINIO_CONFIG

client = Minio(
    MINIO_CONFIG["endpoint"],
    access_key=MINIO_CONFIG["access_key"],
    secret_key=MINIO_CONFIG["secret_key"],
    secure=False
)

client.remove_object("jobinsight-raw", "html/it_p1_20250101000000.html")
```

---

## 6. Troubleshooting

### Issue: Connection refused

**Triệu chứng:**
```
S3 error: Could not connect to the endpoint URL
```

**Giải pháp:**

```bash
# Check MinIO running
docker ps | grep minio

# Check network
docker network inspect jobinsight_network | grep minio

# Restart
docker-compose restart minio
```

### Issue: Bucket not found

**Triệu chứng:**
```
NoSuchBucket: The specified bucket does not exist
```

**Giải pháp:**

```bash
# Tạo bucket
docker exec jobinsight_minio mc mb /data/jobinsight-raw

# Hoặc restart airflow-init
docker-compose restart airflow-init
```

### Issue: Disk full

**Triệu chứng:**
```
disk quota exceeded
```

**Giải pháp:**

```bash
# Check disk usage
docker exec jobinsight_minio mc du /data/

# Cleanup old files
docker exec jobinsight_minio mc rm --recursive --older-than 30d /data/jobinsight-raw/

# Hoặc trigger archive DAG
docker exec jobinsight-airflow-webserver-1 airflow dags trigger jobinsight_archive
```

### Issue: Upload failed

**Triệu chứng:**
```
MinIO upload error: ...
```

**Debug:**

```python
# Test connection
from src.storage.minio_storage import get_minio_client

client = get_minio_client()
print(client.bucket_exists("jobinsight-raw"))  # Should be True
```

---

## 7. Maintenance Tasks

### Daily

- [ ] Check MinIO health: `curl http://localhost:9000/minio/health/live`
- [ ] Verify maintenance DAG chạy thành công (backup + cleanup)
- [ ] Verify pipeline DAG chạy thành công (check Airflow UI)

### Weekly

- [ ] Verify archive DAG chạy thành công
- [ ] Check storage usage qua Console
- [ ] Review logs: `docker logs jobinsight_minio --since 7d | grep -i error`

### Monthly

- [ ] Backup MinIO data ra external storage
- [ ] Review và cleanup files không cần thiết
- [ ] Check disk space của Docker volume

---

## 8. Useful Commands

```bash
# MinIO status
docker ps | grep minio

# MinIO logs
docker logs jobinsight_minio -f

# List all objects
docker exec jobinsight_minio mc ls --recursive /data/

# Disk usage per bucket
docker exec jobinsight_minio mc du /data/jobinsight-raw/
docker exec jobinsight_minio mc du /data/jobinsight-archive/

# Health check
curl -s http://localhost:9000/minio/health/live

# Restart MinIO
docker-compose restart minio

# Access Console
open http://localhost:9001
```

---

## 9. DAGs liên quan

| DAG | Schedule | Mô tả |
|-----|----------|-------|
| `jobinsight_maintenance` | Daily 3:00 AM | Backup PostgreSQL + Cleanup old files |
| `jobinsight_pipeline` | Daily 6:00 AM | Upload HTML to MinIO |
| `jobinsight_dwh` | Daily 7:00 AM | ETL Staging → DWH (DuckDB + Parquet) |
| `jobinsight_archive` | Weekly Sunday 2:00 AM | Archive old data to MinIO |

### Maintenance DAG Tasks

| Task | Mô tả |
|------|-------|
| `backup_postgres` | pg_dump → MinIO (`pg_backups/`) |
| `cleanup_raw_html` | Xóa HTML > `RETENTION_HTML_DAYS` |
| `cleanup_dwh_backups` | Xóa DuckDB backups > `RETENTION_BACKUP_DAYS` |
| `cleanup_pg_backups` | Xóa PostgreSQL backups > `RETENTION_BACKUP_DAYS` |
| `get_storage_stats` | Log storage usage cho monitoring |

---

## References

- Setup guide: `docs/infrastructure/minio_setup_guide.md`
- Retention policies: `docs/governance/retention_policies.md`
- Source code: `src/storage/minio.py`, `src/storage/minio_storage.py`, `src/storage/archive.py`
- DWH ETL pipeline: `src/etl/warehouse/pipeline.py`
- DWH schema: `sql/schemas/dwh_schema.sql`
- Maintenance DAG: `dags/maintenance_dag.py`
- Config: `src/config/storage_config.py`, `.env`
