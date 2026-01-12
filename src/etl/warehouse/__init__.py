"""
DWH ETL Module.

Handles ETL from PostgreSQL staging to DuckDB Star Schema.
Data stored on MinIO (S3-compatible object storage).

Structure:
├── pipeline.py          - Main ETL orchestrator
├── cache.py             - Dimension caches
├── dimensions/          - Dimension processors
│   ├── job.py          - DimJob (SCD2)
│   ├── company.py      - DimCompany (SCD2)
│   ├── location.py     - DimLocation
│   └── date.py         - DimDate
└── facts/              - Fact processors
    ├── daily.py        - FactJobPostingDaily
    └── bridge.py       - FactJobLocationBridge

Storage: src/storage/minio.py
"""

from .pipeline import run_etl
from .cache import init_dimension_caches
from .dimensions import (
    process_dim_job,
    process_dim_company,
    process_dim_location,
    process_dim_date,
    compute_company_hash
)
from .facts import (
    process_facts,
    process_bridges,
    cleanup_duplicate_facts
)

__all__ = [
    'run_etl',
    'init_dimension_caches',
    'process_dim_job',
    'process_dim_company',
    'process_dim_location',
    'process_dim_date',
    'compute_company_hash',
    'process_facts',
    'process_bridges',
    'cleanup_duplicate_facts',
]
