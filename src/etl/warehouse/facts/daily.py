"""
FactJobPostingDaily fact processor.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, Optional

import duckdb
import pandas as pd

from ..dimensions import compute_company_hash

logger = logging.getLogger(__name__)


def process_facts(
    conn: duckdb.DuckDBPyConnection,
    staging_df: pd.DataFrame,
    caches: Dict[str, Dict]
) -> Dict[str, int]:
    """
    Process FactJobPostingDaily with 5-day grain (1 observed + 4 projected).
    """
    stats = {'facts_created': 0, 'facts_updated': 0, 'skipped': 0}
    
    if staging_df.empty:
        return stats
    
    today = date.today()
    crawled_at = datetime.now()
    load_month = today.strftime('%Y-%m')
    
    dates_to_create = [today + timedelta(days=i) for i in range(5)]
    
    job_cache = caches.get('job', {})
    company_cache = caches.get('company', {})
    
    for _, job in staging_df.iterrows():
        job_id = str(job.get('job_id', ''))
        company_name = job.get('company_name_standardized', '')
        
        job_sk = job_cache.get(job_id)
        company_hash = compute_company_hash(company_name)
        company_sk = company_cache.get(company_hash)
        
        if not job_sk or not company_sk:
            stats['skipped'] += 1
            continue
        
        # Parse dates
        posted_time = None
        posted_date_id = None
        if pd.notna(job.get('posted_time')):
            try:
                posted_time = pd.to_datetime(job['posted_time'])
                posted_date_id = posted_time.date()
            except:
                pass
        
        due_date = None
        due_date_id = None
        if pd.notna(job.get('due_date')):
            try:
                due_date = pd.to_datetime(job['due_date'])
                due_date_id = due_date.date()
            except:
                pass
        
        for i, fact_date in enumerate(dates_to_create):
            is_observed = (i == 0)
            
            result = _upsert_fact(
                conn=conn,
                job_sk=job_sk,
                company_sk=company_sk,
                date_id=fact_date,
                is_observed=is_observed,
                posted_date_id=posted_date_id,
                due_date_id=due_date_id,
                salary_min=job.get('salary_min'),
                salary_max=job.get('salary_max'),
                salary_type=job.get('salary_type'),
                time_remaining=job.get('time_remaining'),
                posted_time=posted_time,
                due_date=due_date,
                crawled_at=crawled_at,
                load_month=load_month
            )
            
            if result == 'created':
                stats['facts_created'] += 1
            elif result == 'updated':
                stats['facts_updated'] += 1
    
    logger.info(f"Facts: created={stats['facts_created']}, updated={stats['facts_updated']}, skipped={stats['skipped']}")
    return stats


def _upsert_fact(
    conn: duckdb.DuckDBPyConnection,
    job_sk: int,
    company_sk: int,
    date_id: date,
    is_observed: bool,
    posted_date_id: Optional[date],
    due_date_id: Optional[date],
    salary_min: Optional[float],
    salary_max: Optional[float],
    salary_type: Optional[str],
    time_remaining: Optional[str],
    posted_time: Optional[datetime],
    due_date: Optional[datetime],
    crawled_at: datetime,
    load_month: str
) -> str:
    """
    UPSERT a single fact record using DELETE + INSERT pattern.
    
    Note: DuckDB has a bug with UPDATE on tables with UNIQUE constraints.
    Workaround: DELETE existing record then INSERT new one.
    """
    salary_min = float(salary_min) if pd.notna(salary_min) else None
    salary_max = float(salary_max) if pd.notna(salary_max) else None
    salary_type = str(salary_type) if pd.notna(salary_type) else None
    time_remaining = str(time_remaining) if pd.notna(time_remaining) else None
    
    existing = conn.execute("""
        SELECT fact_id FROM FactJobPostingDaily
        WHERE job_sk = ? AND date_id = ?
    """, [job_sk, date_id]).fetchone()
    
    if existing:
        conn.execute("""
            DELETE FROM FactJobPostingDaily
            WHERE job_sk = ? AND date_id = ?
        """, [job_sk, date_id])
        
        conn.execute("""
            INSERT INTO FactJobPostingDaily (
                fact_id, job_sk, company_sk, date_id, is_observed,
                posted_date_id, due_date_id,
                salary_min, salary_max, salary_type, time_remaining,
                posted_time, due_date, crawled_at, load_month
            ) VALUES (NEXTVAL('seq_fact_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            job_sk, company_sk, date_id, is_observed,
            posted_date_id, due_date_id,
            salary_min, salary_max, salary_type, time_remaining,
            posted_time, due_date, crawled_at, load_month
        ])
        return 'updated'
    else:
        conn.execute("""
            INSERT INTO FactJobPostingDaily (
                fact_id, job_sk, company_sk, date_id, is_observed,
                posted_date_id, due_date_id,
                salary_min, salary_max, salary_type, time_remaining,
                posted_time, due_date, crawled_at, load_month
            ) VALUES (NEXTVAL('seq_fact_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            job_sk, company_sk, date_id, is_observed,
            posted_date_id, due_date_id,
            salary_min, salary_max, salary_type, time_remaining,
            posted_time, due_date, crawled_at, load_month
        ])
        return 'created'


def cleanup_duplicate_facts(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """
    Cleanup duplicate fact records (same job_sk + date_id).
    Keeps the record with smallest fact_id.
    """
    stats = {'duplicates_removed': 0}
    
    duplicates = conn.execute("""
        WITH dups AS (
            SELECT job_sk, date_id, MIN(fact_id) as keep_id
            FROM FactJobPostingDaily
            GROUP BY job_sk, date_id
            HAVING COUNT(*) > 1
        )
        SELECT f.fact_id
        FROM FactJobPostingDaily f
        JOIN dups d ON f.job_sk = d.job_sk AND f.date_id = d.date_id
        WHERE f.fact_id != d.keep_id
    """).fetchall()
    
    if duplicates:
        fact_ids = [d[0] for d in duplicates]
        
        conn.execute(f"""
            DELETE FROM FactJobLocationBridge
            WHERE fact_id IN ({','.join(map(str, fact_ids))})
        """)
        
        conn.execute(f"""
            DELETE FROM FactJobPostingDaily
            WHERE fact_id IN ({','.join(map(str, fact_ids))})
        """)
        
        stats['duplicates_removed'] = len(fact_ids)
        logger.info(f"Removed {stats['duplicates_removed']} duplicate fact records")
    
    return stats
