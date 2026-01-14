"""
FactJobPostingDaily fact processor.

Design: Pure Periodic Snapshot (Kimball methodology)
- Grain: 1 job × 1 ngày
- Mỗi ngày mới:
  1. Tạo facts cho TẤT CẢ jobs còn hạn (carry forward từ ngày trước)
  2. Xử lý jobs mới từ staging (INSERT)
  3. Xử lý jobs update từ staging (SCD2 cho dimensions)
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, Optional, Set

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
    Process FactJobPostingDaily as Pure Periodic Snapshot.
    
    Logic:
    1. Carry forward: Tạo facts cho jobs còn hạn từ ngày trước
    2. Process staging: INSERT/UPDATE facts từ staging mới
    """
    stats = {
        'carried_forward': 0,
        'facts_created': 0, 
        'facts_updated': 0, 
        'skipped': 0,
        'expired_skipped': 0
    }
    
    today = date.today()
    yesterday = today - timedelta(days=1)
    crawled_at = datetime.now()
    load_month = today.strftime('%Y-%m')
    
    # Step 1: Carry forward - tạo facts cho jobs còn hạn
    stats['carried_forward'] = _carry_forward_facts(conn, today, yesterday, crawled_at, load_month)
    
    # Step 2: Process staging data
    if staging_df.empty:
        logger.info(f"Facts: carried_forward={stats['carried_forward']}, no staging data")
        return stats
    
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
        
        # Check if job is expired
        if due_date_id and due_date_id < today:
            stats['expired_skipped'] += 1
            continue
        
        # UPSERT fact for today
        result = _upsert_fact(
            conn=conn,
            job_sk=job_sk,
            company_sk=company_sk,
            date_id=today,
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
    
    logger.info(f"Facts: carried_forward={stats['carried_forward']}, created={stats['facts_created']}, updated={stats['facts_updated']}, skipped={stats['skipped']}, expired={stats['expired_skipped']}")
    return stats


def _carry_forward_facts(
    conn: duckdb.DuckDBPyConnection,
    today: date,
    yesterday: date,
    crawled_at: datetime,
    load_month: str
) -> int:
    """
    Carry forward facts từ ngày trước cho jobs còn hạn.
    
    Logic:
    - Lấy tất cả jobs có fact ngày hôm qua
    - Với mỗi job còn hạn (due_date >= today hoặc NULL) → tạo fact cho today
    - Skip jobs đã hết hạn
    """
    # Get jobs from yesterday that are still valid
    yesterday_jobs = conn.execute("""
        SELECT 
            f.job_sk, f.company_sk, f.posted_date_id, f.due_date_id,
            f.salary_min, f.salary_max, f.salary_type, f.time_remaining,
            f.posted_time, f.due_date
        FROM FactJobPostingDaily f
        WHERE f.date_id = ?
        AND (f.due_date_id IS NULL OR f.due_date_id >= ?)
    """, [yesterday, today]).fetchall()
    
    if not yesterday_jobs:
        logger.info("No jobs to carry forward from yesterday")
        return 0
    
    # Check which jobs already have fact for today
    existing_today = set(r[0] for r in conn.execute("""
        SELECT job_sk FROM FactJobPostingDaily WHERE date_id = ?
    """, [today]).fetchall())
    
    carried = 0
    for job in yesterday_jobs:
        job_sk = job[0]
        
        # Skip if already has fact for today
        if job_sk in existing_today:
            continue
        
        conn.execute("""
            INSERT INTO FactJobPostingDaily (
                fact_id, job_sk, company_sk, date_id,
                posted_date_id, due_date_id,
                salary_min, salary_max, salary_type, time_remaining,
                posted_time, due_date, crawled_at, load_month
            ) VALUES (NEXTVAL('seq_fact_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            job_sk, job[1], today,  # job_sk, company_sk, date_id
            job[2], job[3],  # posted_date_id, due_date_id
            job[4], job[5], job[6], job[7],  # salary_min, salary_max, salary_type, time_remaining
            job[8], job[9], crawled_at, load_month  # posted_time, due_date, crawled_at, load_month
        ])
        carried += 1
    
    logger.info(f"Carried forward {carried} facts from {yesterday} to {today}")
    return carried


def _upsert_fact(
    conn: duckdb.DuckDBPyConnection,
    job_sk: int,
    company_sk: int,
    date_id: date,
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
                fact_id, job_sk, company_sk, date_id,
                posted_date_id, due_date_id,
                salary_min, salary_max, salary_type, time_remaining,
                posted_time, due_date, crawled_at, load_month
            ) VALUES (NEXTVAL('seq_fact_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            job_sk, company_sk, date_id,
            posted_date_id, due_date_id,
            salary_min, salary_max, salary_type, time_remaining,
            posted_time, due_date, crawled_at, load_month
        ])
        return 'updated'
    else:
        conn.execute("""
            INSERT INTO FactJobPostingDaily (
                fact_id, job_sk, company_sk, date_id,
                posted_date_id, due_date_id,
                salary_min, salary_max, salary_type, time_remaining,
                posted_time, due_date, crawled_at, load_month
            ) VALUES (NEXTVAL('seq_fact_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            job_sk, company_sk, date_id,
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
