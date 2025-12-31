"""PostgreSQL storage operations"""
import logging
from typing import Dict, Any

import psycopg2
import pandas as pd

from src.config import DB_CONFIG

logger = logging.getLogger(__name__)

# Các trường quan trọng sẽ được UPDATE khi có thay đổi
UPDATE_COLUMNS = [
    'title',              # Job title có thể thay đổi
    'salary',             # Lương có thể thay đổi
    'location',           # Location có thể thay đổi
    'skills',             # Skills requirements có thể thay đổi
    'deadline',           # Deadline có thể kéo dài
    'verified_employer',  # Verification status có thể thay đổi
]


def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        dbname=DB_CONFIG["database"]
    )


def bulk_upsert(df: pd.DataFrame, table: str = "raw_jobs") -> Dict[str, Any]:
    """
    Bulk upsert DataFrame to PostgreSQL
    Chỉ UPDATE các trường quan trọng khi có thay đổi thực sự
    """
    if df.empty:
        return {"inserted": 0, "updated": 0, "unchanged": 0}
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        columns = list(df.columns)
        col_str = ", ".join(['"{}"'.format(c) for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        
        update_cols = [c for c in UPDATE_COLUMNS if c in columns]
        
        update_parts = []
        for col in update_cols:
            update_parts.append('"{col}" = EXCLUDED."{col}"'.format(col=col))
        
        where_conditions = []
        for col in update_cols:
            where_conditions.append(
                '({tbl}."{col}" IS DISTINCT FROM EXCLUDED."{col}")'.format(tbl=table, col=col)
            )
        where_clause = " OR ".join(where_conditions)
        
        update_parts.append('"updated_at" = NOW()')
        update_str = ", ".join(update_parts)
        
        query = """
            INSERT INTO {table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT (job_id)
            DO UPDATE SET {updates}
            WHERE {where}
            RETURNING (xmax = 0) AS is_inserted
        """.format(
            table=table,
            cols=col_str,
            placeholders=placeholders,
            updates=update_str,
            where=where_clause
        )
        
        inserted = 0
        updated = 0
        unchanged = 0
        
        for _, row in df.iterrows():
            values = tuple(None if pd.isna(v) else v for v in row.values)
            cur.execute(query, values)
            result = cur.fetchone()
            
            if result is None:
                unchanged += 1
            elif result[0]:
                inserted += 1
            else:
                updated += 1
        
        conn.commit()
        
        logger.info("Upserted to {}: {} inserted, {} updated, {} unchanged".format(
            table, inserted, updated, unchanged
        ))
        
        return {"inserted": inserted, "updated": updated, "unchanged": unchanged}
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Database error: {}".format(e))
        raise
    finally:
        if conn:
            conn.close()
