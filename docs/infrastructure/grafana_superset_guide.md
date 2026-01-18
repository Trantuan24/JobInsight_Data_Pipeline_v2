# Grafana + Superset Setup Guide

## Overview

| Tool | Port | Purpose | Data Source |
|------|------|---------|-------------|
| Grafana | 3000 | Pipeline Monitoring | PostgreSQL (monitoring schema) |
| Superset | 8088 | Business Intelligence | DuckDB (DWH) |

## Grafana - Pipeline Monitoring

### Access
- URL: http://localhost:3000
- Login: `admin` / `admin`

### Pre-built Dashboards (Auto-provisioned)

**Pipeline Health** (`/d/jobinsight-pipeline`)
- Success rate gauge, total runs, avg duration
- Task duration over time, success/failure by task
- Recent pipeline runs table

**Data Quality** (`/d/jobinsight-quality`)
- Valid rate gauge, duplicate rate
- Quality trend by validation type
- Quality gate status distribution

### Datasource
- Auto-configured: `PostgreSQL-Monitoring`
- Database: `jobinsight`, Schema: `monitoring`

## Superset - Business Intelligence

### Access
- URL: http://localhost:8088
- Login: `admin` / `admin`

### Setup DuckDB Connection (First-time)

1. **Settings** → **Database Connections** → **+ Database**
2. Click **"Connect this database with a SQLAlchemy URI string instead"**
3. Enter:
   - SQLALCHEMY URI: `duckdb:////app/data/jobinsight.duckdb`
   - DISPLAY NAME: `JobInsight DWH`
4. Click **TEST CONNECTION** → **CONNECT**

### Create Datasets

**Data** → **Datasets** → **+ Dataset**

| Dataset | Schema | Table |
|---------|--------|-------|
| Facts | main | FactJobPostingDaily |
| Jobs | main | DimJob |
| Companies | main | DimCompany |
| Locations | main | DimLocation |

### Suggested Charts

| Chart | Type | Dataset | Config |
|-------|------|---------|--------|
| Daily Job Trends | Line | FactJobPostingDaily | X: date_id, Metric: COUNT(*) |
| Top 10 Companies | Bar | DimCompany | X: company_name, Metric: COUNT(*), Limit: 10 |
| Jobs by Location | Pie | DimLocation | Dimension: city, Metric: COUNT(*) |
| Total Jobs | Big Number | FactJobPostingDaily | Metric: COUNT(*) |

## Troubleshooting

**Grafana "No data"**
```sql
-- Check monitoring data
SELECT COUNT(*) FROM monitoring.etl_metrics;
```

**Superset DuckDB "Read-only" error**
- Ensure volume mount is NOT read-only in docker-compose.yml
- Correct: `./data:/app/data`
- Wrong: `./data:/app/data:ro`

**Restart services**
```bash
docker compose restart grafana superset
```

## Config Files

```
docker/
├── grafana/provisioning/
│   ├── dashboards/
│   │   ├── dashboards.yml
│   │   └── json/
│   │       ├── pipeline-health.json
│   │       └── data-quality.json
│   └── datasources/
│       └── datasources.yml
└── superset/
    ├── superset_config.py
    └── superset-init.sh
```

## Quick Commands

```bash
# Start
docker compose up -d grafana superset

# Logs
docker logs jobinsight_grafana
docker logs jobinsight_superset

# Status
docker compose ps
```
