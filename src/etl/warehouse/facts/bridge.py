"""
FactJobLocationBridge processor.

Design: Pure Periodic Snapshot
- Chỉ tạo bridges cho facts của ngày hôm nay
- KHÔNG tạo bridges cho projected records (vì không còn projected)
"""

import logging
from datetime import date, timedelta
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

from ..dimensions import normalize_city_name

logger = logging.getLogger(__name__)

UNKNOWN_LOCATION_SK = -1

FOREIGN_COUNTRIES = [
    'Nước Ngoài', 'Nhật Bản', 'Hàn Quốc', 'Đài Loan', 'Singapore', 
    'Malaysia', 'Thái Lan', 'Trung Quốc', 'Mỹ', 'Úc', 'Đức', 'Pháp',
    'Anh', 'Canada', 'Japan', 'Korea', 'Taiwan', 'USA', 'Australia'
]


def process_bridges(
    conn: duckdb.DuckDBPyConnection,
    staging_df: pd.DataFrame,
    caches: Dict[str, Dict]
) -> Dict[str, int]:
    """
    Process FactJobLocationBridge.
    
    Maps each fact to its locations. NULL location -> Unknown (SK=-1).
    
    Logic:
    1. Carry forward bridges cho facts được carry forward
    2. Tạo bridges cho facts mới từ staging
    """
    stats = {'carried_forward': 0, 'created': 0, 'skipped': 0, 'orphans_cleaned': 0}
    
    today = date.today()
    
    # Clean up orphan bridges
    orphan_count = conn.execute("""
        SELECT COUNT(*) FROM FactJobLocationBridge
        WHERE fact_id NOT IN (SELECT fact_id FROM FactJobPostingDaily)
    """).fetchone()[0]
    
    if orphan_count > 0:
        conn.execute("""
            DELETE FROM FactJobLocationBridge
            WHERE fact_id NOT IN (SELECT fact_id FROM FactJobPostingDaily)
        """)
        stats['orphans_cleaned'] = orphan_count
        logger.info(f"Cleaned up {orphan_count} orphan bridge records")
    
    # Step 1: Carry forward bridges for carried forward facts
    stats['carried_forward'] = _carry_forward_bridges(conn, today)
    
    # Step 2: Create bridges for staging jobs
    if staging_df.empty:
        logger.info(f"Bridges: carried_forward={stats['carried_forward']}, no staging data")
        return stats
    
    job_cache = caches.get('job', {})
    location_cache = caches.get('location', {})
    
    for _, job in staging_df.iterrows():
        job_id = str(job.get('job_id', ''))
        job_sk = job_cache.get(job_id)
        
        if not job_sk:
            stats['skipped'] += 1
            continue
        
        location_str = job.get('location', '')
        location_sks = _parse_location_sks(location_str, location_cache)
        
        # Get fact for today
        fact_result = conn.execute("""
            SELECT fact_id FROM FactJobPostingDaily
            WHERE job_sk = ? AND date_id = ?
        """, [job_sk, today]).fetchone()
        
        if not fact_result:
            continue
        
        fact_id = fact_result[0]
        
        for location_sk in location_sks:
            existing = conn.execute("""
                SELECT 1 FROM FactJobLocationBridge
                WHERE fact_id = ? AND location_sk = ?
            """, [fact_id, location_sk]).fetchone()
            
            if not existing:
                conn.execute("""
                    INSERT INTO FactJobLocationBridge (bridge_id, fact_id, location_sk)
                    VALUES (NEXTVAL('seq_bridge_id'), ?, ?)
                """, [fact_id, location_sk])
                stats['created'] += 1
    
    logger.info(f"Bridges: carried_forward={stats['carried_forward']}, created={stats['created']}, skipped={stats['skipped']}")
    return stats


def _carry_forward_bridges(conn: duckdb.DuckDBPyConnection, today: date) -> int:
    """
    Carry forward bridges cho facts được carry forward.
    
    Logic: Facts ngày hôm nay chưa có bridge → copy từ fact cùng job_sk ngày hôm qua
    """
    yesterday = today - timedelta(days=1)
    
    # Find facts today without bridges
    facts_without_bridges = conn.execute("""
        SELECT f.fact_id, f.job_sk
        FROM FactJobPostingDaily f
        WHERE f.date_id = ?
        AND f.fact_id NOT IN (SELECT DISTINCT fact_id FROM FactJobLocationBridge)
    """, [today]).fetchall()
    
    if not facts_without_bridges:
        return 0
    
    carried = 0
    for fact_id, job_sk in facts_without_bridges:
        # Get bridges from yesterday's fact for same job
        yesterday_bridges = conn.execute("""
            SELECT b.location_sk
            FROM FactJobLocationBridge b
            JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
            WHERE f.job_sk = ? AND f.date_id = ?
        """, [job_sk, yesterday]).fetchall()
        
        for (location_sk,) in yesterday_bridges:
            conn.execute("""
                INSERT INTO FactJobLocationBridge (bridge_id, fact_id, location_sk)
                VALUES (NEXTVAL('seq_bridge_id'), ?, ?)
            """, [fact_id, location_sk])
            carried += 1
    
    logger.info(f"Carried forward {carried} bridges for {len(facts_without_bridges)} facts")
    return carried


def _parse_location_sks(location_str: str, location_cache: Dict[Tuple[str, str], int]) -> List[int]:
    """
    Parse location string and return list of location_sk.
    """
    if not location_str or pd.isna(location_str):
        return [UNKNOWN_LOCATION_SK]
    
    location_str = str(location_str).strip()
    
    if location_str in FOREIGN_COUNTRIES:
        sk = location_cache.get(('Unknown', location_str))
        return [sk] if sk else [UNKNOWN_LOCATION_SK]
    
    location_sks = []
    parts = location_str.split(' & ')
    
    for part in parts:
        part = part.strip()
        
        if 'nơi khác' in part.lower():
            continue
        
        if not part or part.lower() in ['', 'nan', 'none']:
            continue
        
        if part in FOREIGN_COUNTRIES:
            sk = location_cache.get(('Unknown', part))
        else:
            normalized_city = normalize_city_name(part)
            sk = location_cache.get((normalized_city, 'Vietnam'))
        
        if sk:
            location_sks.append(sk)
    
    return location_sks if location_sks else [UNKNOWN_LOCATION_SK]
