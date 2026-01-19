# JobInsight Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              JOBINSIGHT PIPELINE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐     ┌──────────────────────────────────────────────────┐     │
│   │ TopCV.vn │────▶│                  AIRFLOW                         │     │
│   └──────────┘     │  ┌─────────┐  ┌─────────┐  ┌─────────┐          │     │
│                    │  │pipeline │  │  dwh    │  │maintain │          │     │
│                    │  │  dag    │  │  dag    │  │  dag    │          │     │
│                    │  └────┬────┘  └────┬────┘  └────┬────┘          │     │
│                    └───────┼────────────┼───────────┼────────────────┘     │
│                            │            │           │                       │
│   ┌────────────────────────▼────────────┼───────────┼────────────────┐     │
│   │                    POSTGRESQL       │           │                │     │
│   │  ┌──────────┐    ┌──────────┐      │           │                │     │
│   │  │ raw_jobs │───▶│ staging  │──────┼───────────┘                │     │
│   │  └──────────┘    │  _jobs   │      │                            │     │
│   │                  └──────────┘      │                            │     │
│   └────────────────────────────────────┼────────────────────────────┘     │
│                                        │                                   │
│   ┌────────────────────────────────────▼────────────────────────────┐     │
│   │                         MINIO                                    │     │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │     │
│   │  │jobinsight-  │  │jobinsight-  │  │jobinsight-  │              │     │
│   │  │  raw (HTML) │  │  warehouse  │  │   backup    │              │     │
│   │  └─────────────┘  │  (DuckDB)   │  └─────────────┘              │     │
│   │                   └──────┬──────┘                                │     │
│   └──────────────────────────┼──────────────────────────────────────┘     │
│                              │                                             │
│   ┌──────────────────────────▼──────────────────────────────────────┐     │
│   │                    VISUALIZATION                                 │     │
│   │  ┌─────────────┐              ┌─────────────┐                   │     │
│   │  │   GRAFANA   │              │  SUPERSET   │                   │     │
│   │  │ (Pipeline   │              │ (Business   │                   │     │
│   │  │  Metrics)   │              │  Analytics) │                   │     │
│   │  └─────────────┘              └─────────────┘                   │     │
│   └─────────────────────────────────────────────────────────────────┘     │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Data Sources
- **TopCV.vn**: Job listing website (IT category)
- **Crawler**: Playwright-based async scraper (`src/data_sources/topcv/`)

### 2. Orchestration (Airflow)

| DAG | Schedule | Flow |
|-----|----------|------|
| `jobinsight_pipeline` | 6:00 AM | Crawl → Parse → Validate → Raw → Staging |
| `jobinsight_dwh` | 7:00 AM | Staging → Dimensions → Facts → Export |
| `jobinsight_maintenance` | 3:00 AM | Backup PostgreSQL, cleanup old files |
| `jobinsight_archive` | Sunday 2:00 AM | Archive raw_jobs > 30 days |

### 3. Storage

| Storage | Technology | Data |
|---------|------------|------|
| OLTP | PostgreSQL | raw_jobs, staging_jobs, monitoring |
| OLAP | DuckDB | Star Schema (Dims + Facts) |
| Object | MinIO | HTML backup, archives, DWH file |

### 4. Visualization

| Tool | Purpose | Data Source |
|------|---------|-------------|
| Grafana | Pipeline monitoring | PostgreSQL (monitoring schema) |
| Superset | Business analytics | DuckDB (Star Schema) |

---

## Data Flow

### Pipeline DAG (Daily 6:00 AM)

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│  Crawl  │───▶│  Parse  │───▶│Validate │───▶│  Raw DB │
│ TopCV   │    │  HTML   │    │ Quality │    │(Postgres)│
└─────────┘    └─────────┘    └─────────┘    └────┬────┘
     │                                            │
     ▼                                            ▼
┌─────────┐                               ┌─────────────┐
│ Upload  │                               │  Transform  │
│ MinIO   │                               │  Staging    │
└─────────┘                               └─────────────┘
```

### DWH DAG (Daily 7:00 AM)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Download   │───▶│  Process    │───▶│  Process    │
│  DuckDB     │    │ Dimensions  │    │   Facts     │
│ from MinIO  │    │  (SCD2)     │    │ (Snapshot)  │
└─────────────┘    └─────────────┘    └──────┬──────┘
                                             │
                   ┌─────────────┐    ┌──────▼──────┐
                   │   Export    │◀───│   Upload    │
                   │  Parquet    │    │   DuckDB    │
                   └─────────────┘    └─────────────┘
```

### Fact Processing: Carry Forward Logic

DWH sử dụng **Pure Periodic Snapshot** với carry forward:

```
Day N-1 Facts                    Day N Facts
┌─────────────┐                  ┌─────────────┐
│ Job A (active)│──carry forward──▶│ Job A       │
│ Job B (active)│──carry forward──▶│ Job B       │
│ Job C (expired)│       ✗        │             │
└─────────────┘                  │ Job D (new) │ ◀── từ staging
                                 └─────────────┘
```

**Logic trong `daily.py`:**
1. **Carry forward**: Lấy tất cả jobs từ ngày hôm qua có `due_date >= today` (còn hạn), tạo fact mới cho today
2. **Process staging**: Xử lý jobs mới crawl hôm nay từ `staging_jobs`
3. **Skip expired**: Jobs có `due_date < today` không được carry forward

**Kết quả**: Mỗi ngày có snapshot đầy đủ của TẤT CẢ jobs đang active, không chỉ jobs mới crawl.

---

## Star Schema (DuckDB)

```
                    ┌─────────────┐
                    │   DimDate   │
                    │─────────────│
                    │ date_id (PK)│
                    │ day, month  │
                    │ quarter,year│
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌─────────────┐
│  DimCompany │    │FactJobPosting │    │   DimJob    │
│─────────────│    │    Daily      │    │─────────────│
│company_sk(PK)◀───│───────────────│───▶│ job_sk (PK) │
│company_name │    │ fact_id (PK)  │    │ job_id (BK) │
│company_url  │    │ job_sk (FK)   │    │ title       │
│verified     │    │ company_sk(FK)│    │ skills      │
│(SCD Type 2) │    │ date_id (FK)  │    │ (SCD Type 2)│
└─────────────┘    │ salary_min/max│    └─────────────┘
                   │ posted_date   │
                   │ due_date      │
                   │ load_month    │
                   └───────┬───────┘
                           │
                   ┌───────┴───────┐
                   │FactJobLocation│
                   │    Bridge     │
                   │───────────────│
                   │ bridge_id (PK)│
                   │ fact_id (FK)  │
                   │location_sk(FK)│───▶┌─────────────┐
                   └───────────────┘    │ DimLocation │
                                        │─────────────│
                                        │location_sk  │
                                        │ city        │
                                        │ country     │
                                        └─────────────┘
```

### Design Decisions

**Fact Table: Pure Periodic Snapshot**
- Grain: 1 job × 1 ngày crawl
- Mỗi ngày tạo snapshot của tất cả jobs đang active
- Cho phép track job lifecycle theo thời gian

**Dimensions: SCD Type 2**
- DimJob, DimCompany: Track changes với effective_date/expiry_date
- DimLocation: Type 1 (city không thay đổi)
- DimDate: Data-driven (tạo khi cần)

**Bridge Table**
- FactJobLocationBridge: Handle multi-location jobs
- 1 job có thể có nhiều locations

---

## MinIO Bucket Structure

```
MinIO
├── jobinsight-raw/           # HTML backup (15 days)
│   └── html/
│       └── it_p{page}_{timestamp}.html
│
├── jobinsight-archive/       # Parquet archives (permanent)
│   └── year=YYYY/month=MM/
│       └── raw_jobs_{timestamp}.parquet
│
├── jobinsight-backup/        # Database backups (7 days)
│   ├── pg_backups/
│   └── dwh_backups/
│
└── jobinsight-warehouse/     # DWH storage
    ├── dwh/
    │   └── jobinsight.duckdb
    └── parquet/
        └── load_month=YYYY-MM/
            ├── DimJob.parquet
            ├── DimCompany.parquet
            └── FactJobPostingDaily.parquet
```

> Chi tiết: [MinIO Guide](./infrastructure/minio_guide.md)

---

## Data Quality

### Validation Layers

| Layer | Validator | Thresholds |
|-------|-----------|------------|
| Crawl | CrawlValidator | valid_rate ≥ 80% |
| Business | BusinessRuleValidator | violation_rate < 10% |
| Staging | StagingValidator | valid_rate ≥ 95% |

### Quality Gates

```
Crawl Data → CrawlValidator → QualityGate → Pass/Fail
                                   │
                                   ▼
                            BusinessRuleValidator
                                   │
                                   ▼
                         Log to monitoring.quality_metrics
```

> Chi tiết: [Data Quality Rules](./governance/data_quality_rules.md)

---

## Monitoring

### PostgreSQL Schema: `monitoring`

| Table | Purpose |
|-------|---------|
| `etl_metrics` | Pipeline execution metrics |
| `quality_metrics` | Data quality scores |

### Dashboards

| Dashboard | Tool | Metrics |
|-----------|------|---------|
| Pipeline Health | Grafana | ETL success rate, duration, rows processed |
| Data Quality | Grafana | Valid rate, violations by type |
| Market Overview | Superset | Job trends, top companies, salary |

---

## Deployment

### Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| postgres | 5434 | OLTP database |
| minio | 9000/9001 | Object storage |
| airflow-webserver | 8080 | DAG UI |
| airflow-scheduler | - | Task execution |
| grafana | 3000 | Pipeline monitoring |
| superset | 8088 | Business analytics |

### Volumes

| Volume | Data |
|--------|------|
| postgres-db-volume | PostgreSQL data |
| minio-data | MinIO objects |
| grafana-data | Grafana configs |
| superset-data | Superset metadata |

---

## References

- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Apache Airflow](https://airflow.apache.org/docs/)
