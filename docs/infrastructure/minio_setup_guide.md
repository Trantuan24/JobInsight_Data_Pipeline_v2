# MinIO Setup Guide - JobInsight Data Pipeline

## Giới thiệu

MinIO là object storage tương thích S3 API, được sử dụng trong JobInsight để:
- Lưu trữ HTML backup từ crawler
- Archive dữ liệu cũ dạng Parquet
- Backup và restore dữ liệu

**Buckets hiện tại:**
| Bucket | Mục đích | Retention |
|--------|----------|-----------|
| `jobinsight-raw` | HTML backup từ crawler | 15 ngày |
| `jobinsight-archive` | Parquet archives từ PostgreSQL | 12 tháng |
| `jobinsight-backup` | Database backups | 7 ngày |
| `jobinsight-warehouse` | Parquet exports cho DWH | 12 tháng |

---

## 1. Cấu hình Docker

MinIO được định nghĩa trong `docker-compose.yml`:

```yaml
minio:
  image: minio/minio:latest
  container_name: jobinsight_minio
  ports:
    - "9000:9000"  # API
    - "9001:9001"  # Console (Web UI)
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
  volumes:
    - minio-data:/data
  command: server /data --console-address ":9001"
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
    interval: 30s
    timeout: 10s
    retries: 3
```

**Ports:**
- `9000` - S3 API endpoint
- `9001` - Web Console

---

## 2. Biến môi trường

File `.env`:

```bash
# MinIO Configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=false

# Buckets
MINIO_RAW_BUCKET=jobinsight-raw

# Các bucket khác (archive, backup, warehouse) được hardcode trong src/storage/minio.py
```

**Lưu ý:** Trong Docker network, sử dụng `minio:9000`. Từ host machine, sử dụng `localhost:9000`.

---

## 3. Khởi động MinIO

```bash
# Khởi động tất cả services
docker-compose up -d

# Hoặc chỉ MinIO
docker-compose up -d minio

# Kiểm tra status
docker ps | grep minio
```

**Buckets được tạo tự động** khi `airflow-init` chạy, gọi `init_minio_buckets()` từ `src/storage/minio.py`.

---

## 4. Truy cập MinIO Console

**URL:** http://localhost:9001

**Credentials:**
- Username: `minioadmin`
- Password: `minioadmin`

**Kiểm tra:**
- Buckets đã tạo
- Objects trong mỗi bucket
- Storage usage

---

## 5. Cấu trúc code Python

### 5.1 Config (`src/config/storage_config.py`)

```python
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "bucket": os.getenv("MINIO_RAW_BUCKET", "jobinsight-raw"),
    "secure": False,
}
```

### 5.2 MinIO Client (`src/storage/minio_storage.py`)

Sử dụng thư viện `minio` (không phải boto3):

```python
from minio import Minio
from src.config import MINIO_CONFIG

def get_minio_client() -> Minio:
    return Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"]
    )
```

### 5.3 Functions có sẵn

| Function | File | Mô tả |
|----------|------|-------|
| `init_minio_buckets()` | `minio.py` | **Tạo 4 buckets khi init** |
| `upload_html_to_minio()` | `minio_storage.py` | Upload HTML từ crawler |
| `list_html_files()` | `minio_storage.py` | List HTML files |
| `download_html_from_minio()` | `minio_storage.py` | Download HTML |
| `upload_archive_to_minio()` | `archive.py` | Upload Parquet archive |
| `verify_archive()` | `archive.py` | Verify archive integrity |
| `list_archives()` | `archive.py` | List archive files |
| `restore_from_archive()` | `archive.py` | Restore từ archive |

### 5.4 DuckDB Operations (`src/storage/minio.py`)

| Function | Mô tả |
|----------|-------|
| `download_duckdb(force_new)` | Download DuckDB từ MinIO hoặc tạo mới |
| `upload_duckdb(local_path)` | Upload DuckDB lên MinIO |
| `backup_duckdb(local_path)` | Backup DuckDB trước khi ETL |
| `export_parquet(conn, load_month)` | Export tables ra Parquet partitions |
| `get_duckdb_connection(db_path)` | Context manager cho DuckDB connection |

**Example:**
```python
from src.storage.minio import download_duckdb, upload_duckdb, get_duckdb_connection

# Download from MinIO
db_path = download_duckdb()

# Work with DuckDB
with get_duckdb_connection(db_path) as conn:
    result = conn.execute("SELECT COUNT(*) FROM FactJobPostingDaily").fetchone()
    print(f"Facts: {result[0]}")

# Upload back
upload_duckdb(db_path)
```

---

### 5.5 Bucket Usage hiện tại

| Bucket | Được sử dụng trong | Status |
|--------|-------------------|--------|
| `jobinsight-raw` | `pipeline_dag.py` → `upload_html_to_minio()` | ✅ Production |
| `jobinsight-archive` | `archive_dag.py` → `upload_archive_to_minio()` | ✅ Production |
| `jobinsight-backup` | *Chưa implement* | 🚧 TODO |
| `jobinsight-warehouse` | DWH ETL (`src/etl/warehouse/pipeline.py`) | ✅ Production |

---

## 6. Object naming conventions

### HTML Backup (jobinsight-raw)
```
html/it_p{page}_{timestamp}.html
# Ví dụ: html/it_p1_20250102143022.html
```

### Archive (jobinsight-archive)
```
year={YYYY}/month={MM}/raw_jobs_{timestamp}.parquet
# Ví dụ: year=2025/month=01/raw_jobs_20250102_143022.parquet
```

### DWH Database (jobinsight-warehouse)
```
dwh.duckdb                                    # Latest DuckDB database
backups/dwh_backup_{timestamp}.duckdb        # Backups before ETL
parquet/load_month=2025-01/DimJob.parquet    # Dimension tables
parquet/load_month=2025-01/FactJobPostingDaily.parquet
# Ví dụ: parquet/load_month=2025-01/DimJob.parquet
```

---

## 7. Tích hợp với Pipeline

### 7.1 Crawler → MinIO

Trong `pipeline_dag.py`, task `upload_minio` gọi:

```python
from src.storage import upload_html_to_minio

result = upload_html_to_minio(html_content, page_number)
# Returns: {"success": True, "bucket": "...", "object": "...", "size": ...}
```

### 7.2 Archive DAG

Trong `archive_dag.py`:

```python
from src.storage import (
    get_old_records, export_to_parquet, 
    upload_archive_to_minio, verify_archive, delete_old_records
)

# Flow: Query old → Export Parquet → Upload MinIO → Verify → Delete from DB
```

---

## 8. Retention Policies

Hiện tại chưa có lifecycle policies tự động. Retention được xử lý bởi:

1. **Archive DAG** - Chạy weekly, archive records > 30 ngày
2. **Maintenance DAG** - Cleanup manual nếu cần

**Để thêm lifecycle policy (tương lai):**

```bash
# Sử dụng mc (MinIO Client)
docker run --rm -it --network jobinsight_network \
  minio/mc alias set minio http://minio:9000 minioadmin minioadmin

# Set expiration 15 days cho raw bucket
docker run --rm -it --network jobinsight_network \
  minio/mc ilm add --expiry-days 15 minio/jobinsight-raw
```

---

## 9. Troubleshooting

### MinIO không start

```bash
# Check logs
docker logs jobinsight_minio

# Check health
curl http://localhost:9000/minio/health/live
```

### Connection refused từ Airflow

Đảm bảo:
- `MINIO_ENDPOINT=minio:9000` (không phải localhost)
- MinIO và Airflow cùng network `jobinsight_network`

### Bucket không tồn tại

```bash
# Tạo manual
docker exec -it jobinsight_minio mc mb /data/jobinsight-raw
```

Hoặc restart `airflow-init`:
```bash
docker-compose restart airflow-init
```

---

## 10. Security Notes

**Development:**
- Credentials mặc định: `minioadmin/minioadmin`
- Không có HTTPS

**Production (TODO):**
- Thay đổi credentials
- Enable HTTPS với SSL cert
- Tạo IAM users riêng cho application
- Bucket policies để restrict access

---

## References

- [MinIO Documentation](https://min.io/docs)
- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/API.html)
- Project files: `docker-compose.yml`, `src/storage/`, `.env`
