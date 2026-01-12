"""
DimCompany dimension processor with SCD Type 2.
"""

import hashlib
import logging
from datetime import date
from typing import Dict

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def compute_company_hash(company_name: str) -> str:
    """
    Compute business key hash for company.
    Hash name-only because TopCV creates multiple URLs for same company.
    """
    if not company_name:
        return hashlib.md5(b'unknown').hexdigest()
    return hashlib.md5(company_name.lower().strip().encode('utf-8')).hexdigest()


def process_dim_company(
    conn: duckdb.DuckDBPyConnection,
    staging_df: pd.DataFrame
) -> Dict[str, int]:
    """
    Process DimCompany with SCD Type 2.
    
    Business key: company_bk_hash = MD5(LOWER(company_name))
    Compare columns: company_url, logo_url, verified_employer
    """
    stats = {'inserted': 0, 'updated': 0, 'unchanged': 0}
    today = date.today()
    
    if staging_df.empty:
        return stats
    
    companies = staging_df.drop_duplicates(subset=['company_name_standardized']).copy()
    
    for _, company in companies.iterrows():
        company_name = company.get('company_name_standardized', '')
        if not company_name:
            continue
        
        bk_hash = compute_company_hash(company_name)
        
        existing = conn.execute("""
            SELECT company_sk, company_url, logo_url, verified_employer
            FROM DimCompany
            WHERE company_bk_hash = ? AND is_current = TRUE
        """, [bk_hash]).fetchone()
        
        new_url = company.get('company_url', '')
        new_logo = company.get('logo_url', '')
        new_verified = bool(company.get('verified_employer', False))
        
        if not existing:
            conn.execute("""
                INSERT INTO DimCompany (company_sk, company_bk_hash, company_name, company_url, logo_url, verified_employer, effective_date, is_current)
                VALUES (NEXTVAL('seq_dim_company_sk'), ?, ?, ?, ?, ?, ?, TRUE)
            """, [bk_hash, company_name, new_url, new_logo, new_verified, today])
            stats['inserted'] += 1
        else:
            old_sk, old_url, old_logo, old_verified = existing
            
            has_changes = (
                str(old_url or '') != str(new_url or '') or
                str(old_logo or '') != str(new_logo or '') or
                bool(old_verified) != new_verified
            )
            
            if has_changes:
                # SCD2: Close old record
                conn.execute("""
                    UPDATE DimCompany
                    SET expiry_date = ?, is_current = FALSE
                    WHERE company_sk = ?
                """, [today, old_sk])
                
                # Insert new version
                conn.execute("""
                    INSERT INTO DimCompany (company_sk, company_bk_hash, company_name, company_url, logo_url, verified_employer, effective_date, is_current)
                    VALUES (NEXTVAL('seq_dim_company_sk'), ?, ?, ?, ?, ?, ?, TRUE)
                """, [bk_hash, company_name, new_url, new_logo, new_verified, today])
                stats['updated'] += 1
            else:
                stats['unchanged'] += 1
    
    logger.info(f"DimCompany: inserted={stats['inserted']}, updated={stats['updated']}, unchanged={stats['unchanged']}")
    return stats
