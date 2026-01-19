# JobInsight Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.7-green.svg)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An end-to-end Data Pipeline that crawls IT job postings from TopCV.vn, builds a Star Schema Data Warehouse, and provides analytics dashboards.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           JOBINSIGHT PIPELINE                            │
│                                                                          │
│  ┌─────────┐     ┌─────────────────────────────────────────────────┐    │
│  │TopCV.vn │────▶│                   AIRFLOW                       │    │
│  └─────────┘     │   pipeline_dag → dwh_dag → maintenance_dag      │    │
│                  └──────────────────────┬──────────────────────────┘    │
│                                         │                                │
│  ┌──────────────────────────────────────▼───────────────────────────┐   │
│  │                        POSTGRESQL                                 │   │
│  │              raw_jobs → staging_jobs → monitoring                 │   │
│  └──────────────────────────────────────┬───────────────────────────┘   │
│                                         │                                │
│  ┌──────────────────────────────────────▼───────────────────────────┐   │
│  │                          MINIO                                    │   │
│  │   jobinsight-raw │ jobinsight-warehouse │ jobinsight-backup      │   │
│  │      (HTML)      │   (DuckDB + Parquet) │   (pg_dump)            │   │
│  └──────────────────────────────────────┬───────────────────────────┘   │
│                                         │                                │
│  ┌──────────────────────────────────────▼───────────────────────────┐   │
│  │                      VISUALIZATION                                │   │
│  │            GRAFANA (Monitoring)  │  SUPERSET (Analytics)         │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
TopCV.vn ──crawl──▶ PostgreSQL (raw) ──transform──▶ PostgreSQL (staging)
                          │                                │
                          ▼                                ▼
                    MinIO (HTML)                    DuckDB (Star Schema)
                                                          │
                                                          ▼
                                              Grafana + Superset (Dashboards)
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Orchestration | Apache Airflow 2.7 | DAG scheduling, monitoring |
| OLTP | PostgreSQL 13 | Raw & staging data |
| OLAP | DuckDB | Data Warehouse (Star Schema) |
| Object Storage | MinIO | HTML backup, archives, DWH |
| Visualization | Grafana + Superset | Dashboards & BI |
| Infrastructure | Docker Compose | Containerization |

## Star Schema Design

```
                    ┌─────────────┐
                    │   DimDate   │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌─────────────┐
│ DimCompany  │◀───│FactJobPosting │───▶│   DimJob    │
│  (SCD2)     │    │    Daily      │    │   (SCD2)    │
└─────────────┘    └───────┬───────┘    └─────────────┘
                           │
                   ┌───────┴───────┐
                   │   Bridge      │───▶┌─────────────┐
                   │ (Job×Location)│    │ DimLocation │
                   └───────────────┘    └─────────────┘
```

**Design Decisions:**
- **Fact Table**: Pure Periodic Snapshot (1 job × 1 day)
- **Dimensions**: SCD Type 2 for DimJob, DimCompany
- **Bridge Table**: Handle multi-location jobs

## Quick Start

```bash
# Clone repository
git clone https://github.com/yourusername/JobInsight_Data_Pipeline_v2.git
cd JobInsight_Data_Pipeline_v2

# Create environment file
cp .env.example .env

# Start all services
docker compose up -d

# Access services
# Airflow:  http://localhost:8080  (admin/admin)
# Grafana:  http://localhost:3000  (admin/admin)
# Superset: http://localhost:8088  (admin/admin)
# MinIO:    http://localhost:9001  (minioadmin/minioadmin)
```

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `jobinsight_pipeline` | Daily 6:00 AM | Crawl → Validate → Raw → Staging |
| `jobinsight_dwh` | Daily 7:00 AM | Staging → Dimensions → Facts |
| `jobinsight_maintenance` | Daily 3:00 AM | Backup PostgreSQL, cleanup |
| `jobinsight_archive` | Sunday 2:00 AM | Archive old data to Parquet |

## Data Quality

Pipeline includes multi-layer validation:

| Layer | Validator | Threshold |
|-------|-----------|-----------|
| Crawl | CrawlValidator | valid_rate ≥ 80% |
| Business | BusinessRuleValidator | violation_rate < 10% |
| Staging | StagingValidator | valid_rate ≥ 95% |

## Project Structure

```
JobInsight_Data_Pipeline_v2/
├── dags/                    # Airflow DAGs
├── src/
│   ├── data_sources/        # Crawlers (TopCV)
│   ├── etl/                 # ETL pipelines
│   │   ├── staging/         # Raw → Staging
│   │   └── warehouse/       # Staging → DWH
│   ├── quality/             # Data validation
│   ├── storage/             # PostgreSQL, MinIO, DuckDB
│   └── monitoring/          # Metrics & alerts
├── sql/                     # SQL schemas
├── docker/                  # Docker configs
├── docs/                    # Documentation
└── tests/                   # Unit & integration tests
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - System design, data flow
- [Development Guide](docs/DEVELOPMENT.md) - Setup, workflow
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues
- [Data Contracts](docs/data_contracts/) - Schema definitions
- [Governance](docs/governance/) - Quality rules, retention policies

## Lessons Learned

### What Went Well
- **Kimball methodology** works great for job market analytics
- **DuckDB on MinIO** provides cost-effective OLAP without cloud warehouse
- **Quality gates** catch data issues early, prevent bad data in DWH
- **Docker Compose** makes local development and testing easy

### Challenges Overcome
- **Multi-location jobs**: Solved with Bridge table pattern
- **SCD Type 2**: Implemented for tracking job/company changes over time
- **DuckDB + Superset**: Required custom setup for read-write access

### Future Improvements
- Add more data sources (LinkedIn, Indeed)
- Implement real-time streaming with Kafka
- Add ML models for salary prediction
- Deploy to cloud (AWS/GCP)

## Technologies Used

- **Python 3.9+**: Core language
- **Apache Airflow**: Orchestration
- **PostgreSQL**: OLTP database
- **DuckDB**: OLAP warehouse
- **MinIO**: S3-compatible storage
- **Playwright**: Web scraping
- **Grafana**: Pipeline monitoring
- **Apache Superset**: Business analytics
- **Docker**: Containerization

## Contact

**Author**: [Your Name]

- LinkedIn: [your-linkedin]
- Email: [your-email]
- GitHub: [your-github]

Feel free to reach out if you have questions or want to discuss data engineering!

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

⭐ If you found this project helpful, please give it a star!
