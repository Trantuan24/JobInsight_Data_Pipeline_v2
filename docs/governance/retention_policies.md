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
| HTML Backup | MinIO `jobinsight-raw` | 15 ngày | Manual cleanup | 🚧 TODO |
| Raw Jobs | PostgreSQL | 30 ngày | Archive → MinIO | ✅ Production |
| Staging Jobs | PostgreSQL | Vĩnh viễn | Manual | 🚧 TODO |
| Archive Parquet | MinIO `jobinsight-archive` | 12 tháng | Manual cleanup | 🚧 TODO |
| Database Backup | MinIO `jobinsight-backup` | 7 ngày | Manual | 🚧 TODO |
| Warehouse Parquet | MinIO `jobinsight-warehouse` | 12 tháng | Manual | 🚧 TODO |
| Airflow Logs | Container | 30 ngày | Auto-cleanup | ✅ Airflow native |

---

## Chi tiết từng loại dữ liệu

### 1. HTML Backup

**Vị trí:** MinIO bucket `jobinsight-raw`

**Retention:** 15 ngày

**Lý do:**
- HTML chỉ cần khi debug parsing issues
- Dữ liệu đã extract vào database
- File HTML lớn, tốn storage

**Vòng đời:**
```
Ngày 0-15: Active (có thể debug)
Ngày 16+:  Nên xóa manual hoặc setup lifecycle policy
```

**Status:** 🚧 **TODO** - Cần implement cleanup automation

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

**Retention:** Vĩnh viễn (hiện tại)

**Status:** 🚧 **TODO** - Chưa có archive automation

**Xử lý planned:** Mở rộng `archive_dag.py` để archive staging tương tự raw

---

### 4. Archive Parquet

**Vị trí:** MinIO bucket `jobinsight-archive`

**Retention:** 12 tháng (planned)

**Status:** 🚧 **TODO** - Chưa có lifecycle cleanup automation

**Cấu trúc:**
```
jobinsight-archive/
└── year=2025/
    └── month=01/
        └── raw_jobs_20250102_143022.parquet
```

**Lý do:**
- Parquet nén tốt, không tốn nhiều storage
- 12 tháng đủ cho phân tích historical
- Có thể restore về PostgreSQL khi cần

---

### 5. Database Backup

**Vị trí:** MinIO bucket `jobinsight-backup`

**Retention:** 7 ngày (planned)

**Status:** 🚧 **TODO** - Chưa có backup automation

**Lý do:**
- Daily backup cho disaster recovery
- 7 ngày đủ để phát hiện và rollback
- Full dump lớn, không giữ lâu

**Cấu trúc:**
```
jobinsight-backup/
├── jobinsight_20250106.dump.gz
├── jobinsight_20250105.dump.gz
└── ... (7 files gần nhất)
```

**Recovery:** Restore bằng `pg_restore`

---

### 6. Warehouse Parquet

**Vị trí:** MinIO bucket `jobinsight-warehouse`

**Retention:** 12 tháng (planned)

**Status:** 🚧 **TODO** - DWH ETL chưa implement

**Lý do:**
- Business queries thường focus 1 năm gần nhất
- Parquet optimized, không tốn nhiều storage

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

### Maintenance DAG (Planned)

**File:** `dags/maintenance_dag.py` 🚧 **CHƯA TỒN TẠI**

**Tasks cần implement:**
- Cleanup HTML backups (>15 ngày)
- Cleanup old Parquet partitions (>12 tháng)
- Database backup daily
- Storage usage report
- Lifecycle policy enforcement

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
| 06:00 | Pipeline DAG (crawl → staging) | ✅ Production |

### Daily (Planned)

| Thời gian | Task | Status |
|-----------|------|--------|
| 02:00 | Database backup | 🚧 TODO |
| 03:00 | Cleanup HTML cũ | 🚧 TODO |

### Weekly (Hiện tại)

| Ngày | Thời gian | Task | Status |
|------|-----------|------|--------|
| Sunday | 02:00 | Archive old data (raw_jobs) | ✅ Production |

### Weekly (Planned)

| Ngày | Thời gian | Task | Status |
|------|-----------|------|--------|
| Sunday | 03:00 | Cleanup old archives | 🚧 TODO |
| Sunday | 04:00 | Cleanup old backups | 🚧 TODO |

### Monthly (Planned)

| Ngày | Task | Status |
|------|------|--------|
| 1st | Cleanup old Parquet partitions | 🚧 TODO |
| 1st | Storage usage report | 🚧 TODO |
| 1st | Capacity planning review | 🚧 TODO |

---

## Checklist Implementation

### Đã hoàn thành

- [x] Archive DAG (PostgreSQL → MinIO)
- [x] Archive functions (`src/storage/archive.py`)
- [x] MinIO buckets setup

### Cần làm

- [ ] HTML cleanup automation
- [ ] Database backup automation
- [ ] MinIO lifecycle policies
- [ ] External backup script
- [ ] Storage alerting
- [ ] Retention audit log

---

## References

- Archive DAG: `dags/archive_dag.py`
- Archive functions: `src/storage/archive.py`
- MinIO setup: `docs/infrastructure/minio_setup_guide.md`
- MinIO operations: `docs/infrastructure/minio_operations.md`
