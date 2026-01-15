"""Data Quality Validators for crawl and staging pipelines."""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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


@dataclass
class BusinessRuleResult:
    """Business rule validation result."""
    timestamp: datetime
    total_jobs: int
    violations: Dict[str, int]  # rule_name -> count
    violation_rate: float
    status: str  # 'healthy', 'degraded', 'unhealthy'
    details: List[str] = field(default_factory=list)


class BusinessRuleValidator:
    """Validate business rules on job data."""
    
    # Thresholds based on VN market
    SALARY_HARD_CAP = 200_000_000      # 200M - hard fail
    SALARY_WARNING_CAP = 500_000_000   # 500M - warning only
    DEADLINE_HARD_DAYS = 180           # 6 months
    DEADLINE_WARNING_DAYS = 90         # 3 months
    MIN_TITLE_LENGTH = 5
    MIN_COMPANY_LENGTH = 3
    INVALID_LOCATIONS = {'', 'n/a', 'na', 'none'}  # lowercase for comparison
    
    def validate(self, jobs: List[Dict[str, Any]]) -> BusinessRuleResult:
        """Validate business rules on parsed jobs."""
        if not jobs:
            return BusinessRuleResult(
                timestamp=datetime.now(), total_jobs=0,
                violations={}, violation_rate=0.0, status='healthy'
            )
        
        violations: Dict[str, int] = {
            'salary_invalid': 0,      # min < 0 or max < min
            'salary_too_high': 0,     # > 200M (hard)
            'salary_suspicious': 0,   # > 500M (warning)
            'deadline_past': 0,       # deadline < today
            'deadline_too_far': 0,    # > 180 days (hard)
            'deadline_suspicious': 0, # > 90 days (warning)
            'title_too_short': 0,     # < 5 chars
            'company_too_short': 0,   # < 3 chars
            'location_invalid': 0,    # empty or N/A
        }
        details = []
        today = datetime.now().date()
        
        for job in jobs:
            job_id = job.get('job_id', 'unknown')
            
            # Salary checks
            salary_min = job.get('salary_min')
            salary_max = job.get('salary_max')
            if salary_min is not None and salary_max is not None:
                if salary_min < 0 or salary_max < salary_min:
                    violations['salary_invalid'] += 1
                elif salary_max > self.SALARY_HARD_CAP:
                    if salary_max > self.SALARY_WARNING_CAP:
                        violations['salary_suspicious'] += 1
                    else:
                        violations['salary_too_high'] += 1
            
            # Deadline checks (parsed jobs use 'deadline' field)
            deadline = job.get('deadline')
            if deadline:
                try:
                    if isinstance(deadline, str):
                        deadline = datetime.strptime(deadline, '%Y-%m-%d').date()
                    elif isinstance(deadline, datetime):
                        deadline = deadline.date()
                    
                    days_until = (deadline - today).days
                    if days_until < 0:
                        violations['deadline_past'] += 1
                    elif days_until > self.DEADLINE_HARD_DAYS:
                        violations['deadline_too_far'] += 1
                    elif days_until > self.DEADLINE_WARNING_DAYS:
                        violations['deadline_suspicious'] += 1
                except (ValueError, TypeError):
                    pass  # Invalid date format, skip
            
            # Title length
            title = job.get('title', '')
            if len(str(title).strip()) < self.MIN_TITLE_LENGTH:
                violations['title_too_short'] += 1
            
            # Company name length
            company = job.get('company_name', '')
            if len(str(company).strip()) < self.MIN_COMPANY_LENGTH:
                violations['company_too_short'] += 1
            
            # Location check (normalize to lowercase)
            location = job.get('location')
            location_str = str(location).strip().lower() if location else ''
            if not location_str or location_str in self.INVALID_LOCATIONS:
                violations['location_invalid'] += 1
        
        # Calculate violation rate (hard violations only)
        hard_violations = (
            violations['salary_invalid'] + violations['salary_too_high'] +
            violations['deadline_past'] + violations['deadline_too_far'] +
            violations['title_too_short'] + violations['company_too_short'] +
            violations['location_invalid']
        )
        warning_violations = violations['salary_suspicious'] + violations['deadline_suspicious']
        
        total = len(jobs)
        violation_rate = hard_violations / total
        
        # Determine status
        if violation_rate > 0.10:  # >10% hard violations = unhealthy
            status = 'unhealthy'
        elif violation_rate > 0.05 or warning_violations > total * 0.10:
            status = 'degraded'
        else:
            status = 'healthy'
        
        # Log summary
        if hard_violations > 0:
            details.append(f"Hard violations: {hard_violations}/{total}")
        if warning_violations > 0:
            details.append(f"Warnings: {warning_violations}/{total}")
        
        logger.info(f"Business rules: {total} jobs, {violation_rate:.1%} violations, status={status}")
        
        return BusinessRuleResult(
            timestamp=datetime.now(),
            total_jobs=total,
            violations={k: v for k, v in violations.items() if v > 0},
            violation_rate=violation_rate,
            status=status,
            details=details
        )
