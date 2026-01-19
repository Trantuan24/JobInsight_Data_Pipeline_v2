# Visualization Tools Guide

## Overview

| Tool | Port | Purpose | Data Source |
|------|------|---------|-------------|
| Grafana | 3000 | Pipeline Monitoring | PostgreSQL (monitoring) |
| Superset | 8088 | Business Intelligence | DuckDB (DWH) |

## Grafana

### Access
- URL: http://localhost:3000
- Login: `admin` / `admin`

### Pre-built Dashboards

**Pipeline Health** - Success rate, duration, task status

**Data Quality** - Valid rate, duplicate rate, quality gates

### Datasource
Auto-configured: `PostgreSQL-Monitoring` → `jobinsight.monitoring`

## Superset

### Access
- URL: http://localhost:8088
- Login: `admin` / `admin`

### Setup DuckDB Connection (First-time)

1. **Settings** → **Database Connections** → **+ Database**
2. Click **"Connect with SQLAlchemy URI"**
3. Enter:
   - URI: `duckdb:////app/data/jobinsight.duckdb`
   - Name: `JobInsight DWH`
4. **TEST CONNECTION** → **CONNECT**

### Create Datasets

**Data** → **Datasets** → **+ Dataset** → Schema: `main`

Tables: `FactJobPostingDaily`, `DimJob`, `DimCompany`, `DimLocation`

### Suggested Charts

| Chart | Type | Config |
|-------|------|--------|
| Daily Job Trends | Line | X: date_id, Metric: COUNT(*) |
| Top Companies | Bar | X: company_name, Limit: 10 |
| Jobs by Location | Pie | Dimension: city |

## Quick Commands

```bash
# Start
docker compose up -d grafana superset

# Restart
docker compose restart grafana superset

# Logs
docker logs jobinsight_grafana
docker logs jobinsight_superset
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Grafana "No data" | Check `monitoring.etl_metrics` has data |
| Superset "Read-only" | Ensure volume mount is NOT `:ro` |
| Connection failed | Verify containers on same network |

## Config Files

```
docker/
├── grafana/provisioning/
│   ├── dashboards/*.json
│   └── datasources/datasources.yml
└── superset/
    ├── superset_config.py
    └── superset-init.sh
```
