"""
DimDate dimension processor.
"""

import logging
from datetime import date, timedelta
from typing import Dict

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def process_dim_date(
    conn: duckdb.DuckDBPyConnection,
    staging_df: pd.DataFrame = None,
    projection_days: int = 5
) -> Dict[str, int]:
    """
    Process DimDate based on actual data dates.
    
    Logic:
    - min_date = MIN(posted_time, due_date, crawled_at) from staging
    - max_date = MAX(..., today + projection_days)
    - Data-driven and auto-expands when new data arrives
    """
    stats = {'inserted': 0, 'unchanged': 0}
    
    today = date.today()
    
    min_date = today - timedelta(days=30)
    max_date = today + timedelta(days=projection_days)
    
    if staging_df is not None and not staging_df.empty:
        all_dates = []
        
        for col in ['posted_time', 'due_date', 'crawled_at']:
            if col in staging_df.columns:
                dates = pd.to_datetime(staging_df[col], errors='coerce').dropna()
                all_dates.extend(dates.dt.date.tolist())
        
        if all_dates:
            min_date = min(all_dates)
            max_date = max(max(all_dates), today + timedelta(days=projection_days))
    
    logger.debug(f"DimDate range: {min_date} to {max_date}")
    
    current = min_date
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    while current <= max_date:
        existing = conn.execute("""
            SELECT 1 FROM DimDate WHERE date_id = ?
        """, [current]).fetchone()
        
        if not existing:
            quarter = (current.month - 1) // 3 + 1
            day_of_week = current.isoweekday()
            
            conn.execute("""
                INSERT INTO DimDate (date_id, day, month, quarter, year, week_of_year, day_of_week, weekday_name, is_weekend, year_month, quarter_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                current,
                current.day,
                current.month,
                quarter,
                current.year,
                current.isocalendar()[1],
                day_of_week,
                weekday_names[day_of_week - 1],
                day_of_week >= 6,
                current.strftime('%Y-%m'),
                f'Q{quarter}'
            ])
            stats['inserted'] += 1
        else:
            stats['unchanged'] += 1
        
        current += timedelta(days=1)
    
    logger.info(f"DimDate: inserted={stats['inserted']}, unchanged={stats['unchanged']}")
    return stats
