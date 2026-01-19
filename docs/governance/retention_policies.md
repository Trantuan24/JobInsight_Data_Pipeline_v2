# Data Retention Policies - JobInsight

## Tổng quan

Retention Policies định nghĩa vòng đời dữ liệu: lưu trữ bao lâu, khi nào archive, khi nào xóa.

**Mục tiêu:**
- Tối ưu chi phí storage
- Duy trì hiệu năng database
- Đảm bảo khả năng recovery
- Quản lý vòng đời dữ liệu rõ ràng

**Nguyên tắc:**
- **HOT**: Dữ liệu truy vấn thường xuyên → PostgreSQL
- **WARM**: Dữ liệu ít truy vấn → MinIO (Parquet)
- **COLD**: Dữ liệu hiếm khi cần → Xóa hoặc external backup

---

## Tóm tắt Retention

| Loại dữ liệu | Vị trí | Retention | Xử lý | Status |
|--------------|--------|-----------|-------|--------|
| HTML Backup | MinIO `jobinsight-raw` | 15 ngày | Auto cleanup | ✅ Production |
| Raw Jobs | PostgreSQL | 30 ngày | Archive → MinIO | ✅ Production |
| Staging Jobs | PostgreSQL | Vĩnh viễn | Không cần archive | ✅ By Design |
| Archive Parquet | MinIO `jobinsight-archive` | Vĩnh viễn | Manual cleanup yearly | ✅ Low priority |
| PostgreSQL Backup | MinIO `jobinsight-backup` | 7 ngày | Auto backup + cleanup | ✅ Production |
| DWH Backup | MinIO `jobinsight-backup` | 7 ngày | Auto backup + cleanup | ✅ Production |
| Warehouse Parquet | MinIO `jobinsight-warehouse` | 12 tháng | DWH ETL | ✅ Production |
| Airflow Logs | Container | 30 ngày | Auto-cleanup | ✅ Airflow native |

---

## Chi tiết từng loại dữ liệu

### 1. HTML Backup

**Vị trí:** MinIO bucket `jobinsight-raw`

**Retention:** 15 ngày (env: `RETENTION_HTML_DAYS`)

**Lý do:**
- HTML chỉ cần khi debug parsing issues
- Dữ liệu đã extract vào database
- File HTML lớn, tốn storage

**Vòng đời:**
```
Ngày 0-15: Active (có thể debug)
Ngày 16+:  Auto cleanup bởi maintenance_dag.py
```

**Status:** ✅ **Production** - `maintenance_dag.py` task `cleanup_raw_html`

**Recovery:** Không thể khôi phục sau khi xóa. Có thể crawl lại nếu cần.

---

### 2. PostgreSQL Raw Jobs

**Vị trí:** PostgreSQL `public.raw_jobs`

**Retention:** 30 ngày HOT, sau đó archive

**Lý do:**
- Raw data cần cho ETL hàng ngày
- Sau 30 ngày, data đã qua ETL nhiều lần
- Archive giữ database nhỏ gọn, query nhanh

**Vòng đời:**
```
Ngày 0-30:  HOT - PostgreSQL (query nhanh)
Ngày 31+:   WARM - Archive sang MinIO Parquet
            Xóa khỏi PostgreSQL
Năm 2+:     COLD - Xóa archive (tùy nhu cầu)
```

**Xử lý:** `archive_dag.py` chạy weekly (Sunday 2:00 AM)

**Recovery:** Restore từ MinIO archive bằng `restore_from_archive()`

---

### 3. PostgreSQL Staging Jobs

**Vị trí:** PostgreSQL `jobinsight_staging.staging_jobs`

**Retention:** Vĩnh viễn

**Status:** ✅ **By Design** - Không cần archive

**Lý do:**
- Staging là intermediate layer (raw → staging → DWH)
- DWH đã lưu full history với SCD Type 2
- Nếu cần historical data → query từ DWH hoặc raw archive
- Staging data nhỏ (~3MB/ngày), không ảnh hưởng performance

---

### 4. Archive Parquet

**Vị trí:** MinIO bucket `jobinsight-archive`

**Retention:** Vĩnh viễn (manual cleanup yearly nếu cần)

**Status:** ✅ **Low Priority** - Parquet nén tốt, không tốn storage

**Cấu trúc:**
```
jobinsight-archive/
└── year=2025/
    └── month=01/
        └── raw_jobs_20250102_143022.parquet
```

**Lý do không cần auto-cleanup:**
- Parquet nén rất tốt (~90% compression)
- 12 tháng data chỉ ~5-10 GB
- Archive hiếm khi cần access
- Manual cleanup yearly đủ rồi

---

### 5. Database Backup

**Vị trí:** MinIO bucket `jobinsight-backup`

**Retention:** 7 ngày (env: `RETENTION_BACKUP_DAYS`)

**Status:** ✅ **Production** - `maintenance_dag.py` task `backup_postgres`

**Lý do:**
- Daily backup cho disaster recovery
- 7 ngày đủ để phát hiện và rollback
- Full dump lớn, không giữ lâu

**Cấu trúc:**
```
jobinsight-backup/
├── pg_backups/
│   ├── jobinsight_20260118_030000.dump
│   ├── jobinsight_20260117_030000.dump
│   └── ... (7 files gần nhất)
└── dwh_backups/
    ├── jobinsight_20260118_070000.duckdb
    └── ... (7 files gần nhất)
```

**Automation:**
- `backup_postgres_task()` - Chạy `pg_dump` và upload lên MinIO
- `cleanup_pg_backups_task()` - Xóa backups cũ hơn `RETENTION_BACKUP_DAYS`
- `cleanup_dwh_backups_task()` - Xóa DuckDB backups cũ

**Recovery:** Restore bằng `pg_restore`

```bash
# Download backup từ MinIO
docker exec jobinsight_minio mc cp /data/jobinsight-backup/pg_backups/jobinsight_20260118_030000.dump /tmp/

# Restore
pg_restore -h postgres -U jobinsight -d jobinsight /tmp/jobinsight_20260118_030000.dump
```

---

### 6. Warehouse Parquet

**Vị trí:** MinIO bucket `jobinsight-warehouse`

**Retention:** 12 tháng (planned)

**Status:** ✅ **Production** - DWH ETL đang chạy

**Cấu trúc:**
```
jobinsight-warehouse/
├── dwh/
│   └── jobinsight.duckdb                # DuckDB database (latest)
├── backups/
│   └── dwh_backup_20260114_080000.duckdb
└── parquet/
    └── load_month=2026-01/
        ├── DimJob.parquet
        ├── DimCompany.parquet
        ├── DimLocation.parquet
        ├── DimDate.parquet
        ├── FactJobPostingDaily.parquet
        └── FactJobLocationBridge.parquet
```

**Lý do:**
- Business queries thường focus 1 năm gần nhất
- Parquet optimized, không tốn nhiều storage
- DuckDB database cho ad-hoc queries
- Auto-backup trước mỗi ETL run

**ETL Logic (Pure Periodic Snapshot):**
```
1. Carry forward: Tạo facts cho jobs còn hạn từ ngày trước
2. Process staging: Chỉ lấy jobs crawl ngày hôm nay (DATE(crawled_at) = today)
3. SCD2: Apply cho dimensions khi jobs được update
```

**Flow:**
```
Staging (PostgreSQL) → DWH ETL → DuckDB (MinIO) → Parquet Export (MinIO)
```

**Recovery:**
- DuckDB: Restore từ backup hoặc rebuild từ staging
- Parquet: Immutable exports, không cần recovery

---

### 7. Airflow Logs

**Vị trí:** Container `/opt/airflow/logs/`

**Retention:** 30 ngày

**Cấu hình:** Airflow tự động cleanup

---

## Automation

### Archive DAG

**File:** `dags/archive_dag.py`

**Schedule:** Weekly, Sunday 2:00 AM

**Flow:**
```
Check old data → Export Parquet → Upload MinIO → Verify → Delete from DB
```

**Đặc điểm:**
- Chỉ xóa PostgreSQL sau khi verify archive thành công
- Nếu verify fail → giữ nguyên data, alert

### Maintenance DAG

**File:** `dags/maintenance_dag.py`

**Schedule:** Daily 3:00 AM

**Tasks:**
| Task | Mô tả | Status |
|------|-------|--------|
| `backup_postgres` | pg_dump → MinIO | ✅ Production |
| `cleanup_raw_html` | Xóa HTML > 15 ngày | ✅ Production |
| `cleanup_dwh_backups` | Xóa DuckDB backups > 7 ngày | ✅ Production |
| `cleanup_pg_backups` | Xóa PostgreSQL backups > 7 ngày | ✅ Production |
| `get_storage_stats` | Log storage usage | ✅ Production |

**Flow:**
```
start → backup_postgres → [cleanup_html, cleanup_dwh_backups, cleanup_pg_backups] → storage_stats → end
```

---

## Ước tính Storage

### Dữ liệu hàng ngày

| Loại | Daily | Monthly | Yearly |
|------|-------|---------|--------|
| HTML Backup | ~50 MB | 1.5 GB | 18 GB |
| Raw Jobs (PG) | ~2 MB | 60 MB | 720 MB |
| Staging Jobs (PG) | ~3 MB | 90 MB | 1 GB |
| Warehouse Parquet | ~10 MB | 300 MB | 3.6 GB |
| Database Backup | ~50 MB | 1.5 GB | 18 GB |

### Với Retention Policies

**PostgreSQL (HOT):**
- Raw + Staging (30 ngày): ~150 MB
- Query performance: Tốt

**MinIO:**
- HTML (15 ngày): ~750 MB
- Warehouse (12 tháng): ~3.6 GB
- Backups (7 ngày): ~350 MB
- Archives: ~5-10 GB/năm
- **Tổng:** ~10-15 GB

**Tổng active storage:** ~15-20 GB

---

## Disaster Recovery

### RTO/RPO

| Metric | Giá trị |
|--------|---------|
| RTO (Recovery Time) | 4 giờ |
| RPO (Recovery Point) | 24 giờ (last backup) |

### Scenarios

**1. Xóa nhầm dữ liệu:**
- Stop pipelines
- Restore từ backup gần nhất
- Backfill dữ liệu thiếu

**2. Database corruption:**
- Restore từ MinIO backup
- Verify integrity
- Resume pipelines

**3. MinIO data loss:**
- HTML: Crawl lại
- Archive: Không thể khôi phục (cần external backup)
- Warehouse: Rebuild từ PostgreSQL staging

**4. Full system failure:**
- Rebuild infrastructure
- Restore PostgreSQL từ backup
- Restore MinIO critical buckets
- Rebuild warehouse

### External Backup (Khuyến nghị)

Weekly backup MinIO ra external storage:
```bash
mc mirror minio/jobinsight-backup /external/backup/
mc mirror minio/jobinsight-archive /external/backup/
```

---

## Schedule tổng hợp

### Daily (Hiện tại)

| Thời gian | Task | Status |
|-----------|------|--------|
| 03:00 | Maintenance DAG (backup + cleanup) | ✅ Production |
| 06:00 | Pipeline DAG (crawl → staging) | ✅ Production |
| 07:00 | DWH DAG (staging → DWH) | ✅ Production |

### Weekly (Hiện tại)

| Ngày | Thời gian | Task | Status |
|------|-----------|------|--------|
| Sunday | 02:00 | Archive old data (raw_jobs) | ✅ Production |

### Monthly (Optional)

| Ngày | Task | Status |
|------|------|--------|
| Yearly | Cleanup old archive Parquet (nếu cần) | ✅ Manual |
| Yearly | Storage capacity review | ✅ Manual |

> **Note:** Monthly tasks không cần automation vì daily maintenance đã đủ. Storage stats được log daily.

---

## Checklist Implementation

### Đã hoàn thành

- [x] Archive DAG (PostgreSQL raw_jobs → MinIO)
- [x] Archive functions (`src/storage/archive.py`)
- [x] MinIO buckets setup
- [x] HTML cleanup automation (`maintenance_dag.py`)
- [x] PostgreSQL backup automation (`maintenance_dag.py`)
- [x] DWH backup automation (`maintenance_dag.py`)
- [x] Storage stats logging (`maintenance_dag.py`)

### Optional (Low Priority)

- [ ] MinIO lifecycle policies (native) - Không cần, đã có maintenance DAG
- [ ] External backup script - Nice-to-have cho disaster recovery
- [ ] Storage alerting (Telegram/Discord) - Nice-to-have

---

## References

- Archive DAG: `dags/archive_dag.py`
- Maintenance DAG: `dags/maintenance_dag.py`
- Archive functions: `src/storage/archive.py`
- DWH ETL pipeline: `src/etl/warehouse/pipeline.py`
- DWH schema: `sql/schemas/dwh_schema.sql`
- DuckDB operations: `src/storage/minio.py`
- MinIO guide: `docs/infrastructure/minio_guide.md`
