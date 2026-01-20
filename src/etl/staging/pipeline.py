"""Staging ETL Pipeline - Transform raw_jobs to staging_jobs using Pandas + SQL procedures"""
import json
import logging
import pandas as pd
from typing import Dict, Any

from src.storage.postgres import get_db_connection
from src.etl.staging.cleaners import clean_title, clean_company_name

logger = logging.getLogger(__name__)


def run_staging_pipeline() -> Dict[str, Any]:
    """ Run staging ETL pipeline """
    logger.info("Starting staging ETL pipeline...")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get count before
        cur.execute("SELECT COUNT(*) FROM jobinsight_staging.staging_jobs")
        before_count = cur.fetchone()[0]
        
        # Step 1: Load raw data chưa có trong staging
        df = pd.read_sql("""
            SELECT r.* FROM public.raw_jobs r
            LEFT JOIN jobinsight_staging.staging_jobs s ON r.job_id = s.job_id
            WHERE s.job_id IS NULL
        """, conn)
        
        if df.empty:
            logger.info("No new records to process")
            return {"status": "success", "total": before_count, "new_records": 0}
        
        logger.info(f"Processing {len(df)} new records with Pandas...")
        
        # Step 2: Clean data với Pandas
        df['title_clean'] = df['title'].apply(clean_title)
        df['company_name_standardized'] = df['company_name'].apply(clean_company_name)
        
        # Step 3: Insert vào staging
        for _, row in df.iterrows():
            # Convert skills to JSON string for JSONB column
            skills_val = row.get('skills')
            if skills_val is not None and not isinstance(skills_val, str):
                skills_val = json.dumps(skills_val)
            
            cur.execute("""
                INSERT INTO jobinsight_staging.staging_jobs (
                    job_id, title, title_clean, job_url, company_name, company_name_standardized,
                    company_url, verified_employer, logo_url, salary, location, deadline,
                    skills, last_update, posted_time, crawled_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
            """, (
                row['job_id'], row['title'], row['title_clean'], row.get('job_url'),
                row['company_name'], row['company_name_standardized'], row.get('company_url'),
                row.get('verified_employer', False), row.get('logo_url'), row.get('salary'),
                row.get('location'), row.get('deadline'), skills_val,
                row.get('last_update'), row.get('posted_time'), row.get('crawled_at')
            ))
        
        conn.commit()
        logger.info("Pandas cleaning done, calling SQL procedures...")
        
        # Step 4: Gọi SQL procedures để parse salary và deadline
        cur.execute("CALL jobinsight_staging.transform_raw_to_staging()")
        conn.commit()
        
        # Get count after
        cur.execute("SELECT COUNT(*) FROM jobinsight_staging.staging_jobs")
        after_count = cur.fetchone()[0]
        
        new_records = after_count - before_count
        
        logger.info(f"Staging pipeline completed: {after_count} total, {new_records} new")
        
        return {
            "status": "success",
            "total": after_count,
            "new_records": new_records
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Staging pipeline error: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        if conn:
            conn.close()


def get_staging_stats() -> Dict[str, Any]:
    """Get statistics about staging_jobs table"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) as with_salary,
                COUNT(CASE WHEN due_date IS NOT NULL THEN 1 END) as with_deadline,
                COUNT(CASE WHEN processed_to_dwh = TRUE THEN 1 END) as processed
            FROM jobinsight_staging.staging_jobs
        """)
        
        row = cur.fetchone()
        
        return {
            "total": row[0],
            "with_salary": row[1],
            "with_deadline": row[2],
            "processed_to_dwh": row[3]
        }
        
    except Exception as e:
        logger.error(f"Error getting staging stats: {e}")
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()
