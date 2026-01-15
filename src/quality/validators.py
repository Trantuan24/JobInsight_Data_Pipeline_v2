"""Data Quality Validators for crawl and staging pipelines."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationConfig:
    """Validation thresholds."""
    min_job_count: int = 50
    hard_fail_duplicate_rate: float = 0.20  # 20%
    success_threshold: float = 0.90  # 90%
    warning_threshold: float = 0.70  # 70%


@dataclass
class ValidationResult:
    """Validation result."""
    validation_type: str  # 'crawl' or 'staging'
    timestamp: datetime
    total_jobs: int
    unique_jobs: int
    duplicate_rate: float
    valid_jobs: int
    valid_rate: float
    field_missing_rates: Dict[str, float] = field(default_factory=dict)
    # Staging-specific
    raw_count: Optional[int] = None
    data_loss_rate: Optional[float] = None


class CrawlValidator:
    """Validator for crawl data quality."""
    
    def __init__(self, config: ValidationConfig = None):
        self.config = config or ValidationConfig()
    
    def validate(self, jobs: List[Dict]) -> ValidationResult:
        """Run all validations on parsed jobs."""
        if not jobs:
            return ValidationResult(
                validation_type='crawl', timestamp=datetime.now(),
                total_jobs=0, unique_jobs=0, duplicate_rate=0.0,
                valid_jobs=0, valid_rate=0.0
            )
        
        total = len(jobs)
        unique = len(set(j.get('job_id') for j in jobs if j.get('job_id')))
        dup_rate = (total - unique) / total
        
        # Field validation
        valid_count = 0
        missing = {'job_id': 0, 'title': 0, 'company_name': 0}
        
        for job in jobs:
            is_valid = True
            if not job.get('job_id') or not str(job['job_id']).isdigit():
                missing['job_id'] += 1
                is_valid = False
            if not job.get('title') or not str(job['title']).strip():
                missing['title'] += 1
                is_valid = False
            if not job.get('company_name'):
                missing['company_name'] += 1
                is_valid = False
            if is_valid:
                valid_count += 1
        
        result = ValidationResult(
            validation_type='crawl',
            timestamp=datetime.now(),
            total_jobs=total, unique_jobs=unique, duplicate_rate=dup_rate,
            valid_jobs=valid_count, valid_rate=valid_count / total,
            field_missing_rates={k: v/total for k, v in missing.items()}
        )
        logger.info(f"Crawl validation: {total} jobs, {result.valid_rate:.1%} valid")
        return result


class StagingValidator:
    """Validator for staging data quality after ETL."""
    
    def __init__(self, config: ValidationConfig = None):
        # Create new config with staging thresholds (don't mutate input)
        self.config = ValidationConfig(
            min_job_count=config.min_job_count if config else 50,
            hard_fail_duplicate_rate=config.hard_fail_duplicate_rate if config else 0.20,
            success_threshold=0.95,  # Stricter for staging
            warning_threshold=0.90
        )
    
    def validate(self, pg_conn_string: str, new_staging_count: int = 0) -> ValidationResult:
        """Validate staging data quality."""
        import psycopg2
        
        try:
            with psycopg2.connect(pg_conn_string) as conn:
                with conn.cursor() as cur:
                    # Get staging stats for today's data
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(DISTINCT job_id) as unique_jobs,
                            COUNT(CASE WHEN title_clean IS NOT NULL AND title_clean != '' THEN 1 END) as valid_title,
                            COUNT(CASE WHEN company_name_standardized IS NOT NULL THEN 1 END) as valid_company,
                            COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) as with_salary,
                            COUNT(CASE WHEN due_date IS NOT NULL THEN 1 END) as with_deadline
                        FROM jobinsight_staging.staging_jobs
                        WHERE DATE(crawled_at) = CURRENT_DATE
                    """)
                    row = cur.fetchone()
                    
                    total = row[0] or 0
                    unique = row[1] or 0
                    valid_title = row[2] or 0
                    valid_company = row[3] or 0
                    
                    if total == 0:
                        return ValidationResult(
                            validation_type='staging', timestamp=datetime.now(),
                            total_jobs=0, unique_jobs=0, duplicate_rate=0.0,
                            valid_jobs=0, valid_rate=0.0, raw_count=new_staging_count,
                            data_loss_rate=0.0
                        )
                    
                    # Calculate metrics
                    dup_rate = (total - unique) / total if total > 0 else 0.0
                    valid_jobs = min(valid_title, valid_company)
                    valid_rate = valid_jobs / total
                    
                    result = ValidationResult(
                        validation_type='staging',
                        timestamp=datetime.now(),
                        total_jobs=total, unique_jobs=unique, duplicate_rate=dup_rate,
                        valid_jobs=valid_jobs, valid_rate=valid_rate,
                        field_missing_rates={
                            'title_clean': (total - valid_title) / total,
                            'company_name_standardized': (total - valid_company) / total,
                            'salary': (total - (row[4] or 0)) / total,
                            'due_date': (total - (row[5] or 0)) / total
                        },
                        raw_count=new_staging_count,
                        data_loss_rate=0.0  # Not comparing raw vs staging anymore
                    )
                    
                    logger.info(f"Staging validation: {total} jobs today, {valid_rate:.1%} valid")
                    return result
                    
        except Exception as e:
            logger.error(f"Staging validation error: {e}")
            return ValidationResult(
                validation_type='staging', timestamp=datetime.now(),
                total_jobs=0, unique_jobs=0, duplicate_rate=0.0,
                valid_jobs=0, valid_rate=0.0
            )
