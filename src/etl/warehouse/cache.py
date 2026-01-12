"""
Dimension cache utilities.
"""

import logging
from typing import Dict

import duckdb

logger = logging.getLogger(__name__)


def init_dimension_caches(conn: duckdb.DuckDBPyConnection) -> Dict[str, Dict]:
    """
    Initialize caches for dimension lookups.
    
    Returns dict with:
    - job: job_id -> job_sk
    - company: company_bk_hash -> company_sk
    - location: (city, country) -> location_sk
    """
    caches = {}
    
    # Job cache: job_id -> job_sk
    jobs = conn.execute("""
        SELECT job_id, job_sk FROM DimJob WHERE is_current = TRUE
    """).fetchdf()
    caches['job'] = dict(zip(jobs['job_id'].astype(str), jobs['job_sk']))
    
    # Company cache: company_bk_hash -> company_sk
    companies = conn.execute("""
        SELECT company_bk_hash, company_sk FROM DimCompany WHERE is_current = TRUE
    """).fetchdf()
    caches['company'] = dict(zip(companies['company_bk_hash'], companies['company_sk']))
    
    # Location cache: (city, country) -> location_sk
    locations = conn.execute("""
        SELECT city, country, location_sk FROM DimLocation
    """).fetchdf()
    caches['location'] = {(row['city'], row['country']): row['location_sk'] for _, row in locations.iterrows()}
    
    logger.info(f"Caches initialized: jobs={len(caches['job'])}, companies={len(caches['company'])}, locations={len(caches['location'])}")
    return caches
