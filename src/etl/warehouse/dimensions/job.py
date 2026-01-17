"""
DimJob dimension processor with SCD Type 2.
"""

import json
import logging
from datetime import date
from typing import Dict

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def process_dim_job(
    conn: duckdb.DuckDBPyConnection,
    staging_df: pd.DataFrame
) -> Dict[str, int]:
    """
    Process DimJob with SCD Type 2 (batch processing).
    
    Compare columns: title, skills, job_url
    """
    stats = {'inserted': 0, 'updated': 0, 'unchanged': 0}
    today = date.today()
    
    if staging_df.empty:
        return stats
    
    jobs = staging_df.drop_duplicates(subset=['job_id']).copy()
    job_ids = [str(j) for j in jobs['job_id'].dropna().tolist()]
    
    if not job_ids:
        return stats
    
    # Batch fetch all existing records
    placeholders = ','.join(['?'] * len(job_ids))
    existing_rows = conn.execute(f"""
        SELECT job_id, job_sk, title, skills, job_url
        FROM DimJob WHERE job_id IN ({placeholders}) AND is_current = TRUE
    """, job_ids).fetchall()
    existing_map = {row[0]: row for row in existing_rows}
    
    for _, job in jobs.iterrows():
        job_id = str(job.get('job_id', ''))
        if not job_id:
            continue
        
        new_title = job.get('title_clean', '')
        new_skills = job.get('skills')
        new_url = job.get('job_url', '')
        
        # Handle skills JSON
        if isinstance(new_skills, (dict, list)):
            new_skills_str = json.dumps(new_skills, ensure_ascii=False)
        else:
            new_skills_str = str(new_skills) if new_skills else None
        
        existing = existing_map.get(job_id)
        
        if not existing:
            conn.execute("""
                INSERT INTO DimJob (job_sk, job_id, title, job_url, skills, effective_date, is_current)
                VALUES (NEXTVAL('seq_dim_job_sk'), ?, ?, ?, ?, ?, TRUE)
            """, [job_id, new_title, new_url, new_skills_str, today])
            stats['inserted'] += 1
        else:
            _, old_sk, old_title, old_skills, old_url = existing
            
            has_changes = (
                str(old_title or '') != str(new_title or '') or
                str(old_url or '') != str(new_url or '') or
                str(old_skills or '') != str(new_skills_str or '')
            )
            
            if has_changes:
                conn.execute("BEGIN TRANSACTION")
                try:
                    conn.execute("""
                        UPDATE DimJob SET expiry_date = ?, is_current = FALSE WHERE job_sk = ?
                    """, [today, old_sk])
                    conn.execute("""
                        INSERT INTO DimJob (job_sk, job_id, title, job_url, skills, effective_date, is_current)
                        VALUES (NEXTVAL('seq_dim_job_sk'), ?, ?, ?, ?, ?, TRUE)
                    """, [job_id, new_title, new_url, new_skills_str, today])
                    conn.execute("COMMIT")
                    stats['updated'] += 1
                except Exception as e:
                    conn.execute("ROLLBACK")
                    logger.warning(f"SCD2 update failed for job {job_id}: {e}")
                    stats['unchanged'] += 1
            else:
                stats['unchanged'] += 1
    
    logger.info(f"DimJob: inserted={stats['inserted']}, updated={stats['updated']}, unchanged={stats['unchanged']}")
    return stats
