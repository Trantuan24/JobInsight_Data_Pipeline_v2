# Development Guide

## Prerequisites

- Docker & Docker Compose
- Git
- Python 3.9+ (optional, for local development)

## Setup

### 1. Clone & Configure

```bash
git clone <repo-url>
cd JobInsight_Data_Pipeline_v2

# Copy environment file
cp .env.example .env
```

### 2. Environment Variables

Edit `.env`:
```bash
# PostgreSQL
DB_HOST=postgres
DB_PORT=5432
DB_USER=jobinsight
DB_PASSWORD=jobinsight
DB_NAME=jobinsight

# DuckDB
DUCKDB_PATH=/opt/airflow/data/jobinsight.duckdb

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# Telegram (optional)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

### 3. Start Services

```bash
# Start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f airflow-scheduler
```

## Development Workflow

### Running DAGs Manually

```bash
# Trigger pipeline DAG
docker exec jobinsight-airflow-webserver-1 airflow dags trigger jobinsight_pipeline

# Trigger DWH DAG
docker exec jobinsight-airflow-webserver-1 airflow dags trigger jobinsight_dwh

# Check DAG status
docker exec jobinsight-airflow-webserver-1 airflow dags list-runs -d jobinsight_pipeline
```

### Testing Code Changes

```bash
# Run tests in container
docker exec jobinsight-airflow-webserver-1 pytest tests/ -v

# Run specific test
docker exec jobinsight-airflow-webserver-1 pytest tests/unit/test_cleaners.py -v
```

### Database Access

```bash
# PostgreSQL
docker exec -it jobinsight-airflow-webserver-1 psql -h postgres -U jobinsight -d jobinsight

# DuckDB (in container)
docker exec -it jobinsight-airflow-webserver-1 python3 -c "
import duckdb
conn = duckdb.connect('/opt/airflow/data/jobinsight.duckdb')
print(conn.execute('SELECT COUNT(*) FROM FactJobPostingDaily').fetchone())
"
```

### MinIO Access

```bash
# List buckets
docker exec jobinsight_minio mc ls /data/

# List objects
docker exec jobinsight_minio mc ls /data/jobinsight-warehouse/
```

## Project Structure

```
src/
├── config/              # Configuration classes
│   ├── database_config.py
│   ├── storage_config.py
│   └── quality_config.py
│
├── data_sources/        # Crawlers
│   └── topcv/
│       ├── scraper.py   # Playwright crawler
│       └── parser.py    # HTML parser
│
├── etl/
│   ├── staging/         # Raw → Staging ETL
│   │   ├── cleaners.py  # Data cleaning functions
│   │   └── pipeline.py  # Staging pipeline
│   │
│   └── warehouse/       # Staging → DWH ETL
│       ├── dimensions/  # Dimension processors
│       ├── facts/       # Fact processors
│       └── pipeline.py  # DWH pipeline
│
├── quality/             # Data validation
│   ├── validators.py    # Validation classes
│   ├── gates.py         # Quality gates
│   └── metrics_logger.py
│
├── storage/             # Storage clients
│   ├── postgres.py      # PostgreSQL operations
│   ├── minio.py         # MinIO + DuckDB operations
│   └── archive.py       # Archive functions
│
└── monitoring/          # Monitoring
    ├── etl_metrics.py   # ETL metrics logger
    └── alerts.py        # Alert functions
```

## Adding New Features

### New Dimension

1. Create processor: `src/etl/warehouse/dimensions/new_dim.py`
2. Add to pipeline: `src/etl/warehouse/pipeline.py`
3. Update schema: `sql/schemas/dwh_schema.sql`
4. Add data contract: `docs/data_contracts/dim_new.yaml`

### New Validation Rule

1. Add rule to: `src/quality/validators.py`
2. Update config: `src/config/quality_config.py`
3. Document: `docs/governance/data_quality_rules.md`

### New DAG

1. Create DAG file: `dags/new_dag.py`
2. Add to documentation
3. Test: `airflow dags test new_dag 2024-01-01`

## Code Style

- Python: PEP 8
- SQL: Uppercase keywords, lowercase identifiers
- Docstrings: Google style

## Useful Commands

```bash
# Rebuild containers
docker compose build --no-cache

# Reset database
docker compose down -v
docker compose up -d

# View Airflow logs
docker compose logs -f airflow-scheduler

# Check container resources
docker stats
```

## Troubleshooting

See [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)
