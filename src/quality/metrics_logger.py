"""Metrics Logger - Log validation metrics to PostgreSQL."""

import json
import logging
import psycopg2

from .validators import ValidationResult, BusinessRuleResult
from .gates import GateResult

logger = logging.getLogger(__name__)


class MetricsLogger:
    """Logger for validation metrics to monitoring.quality_metrics table."""
    
    def __init__(self, pg_conn_string: str):
        self.conn_string = pg_conn_string
    
    def log(self, result: ValidationResult, gate: GateResult, dag_run_id: str = None) -> bool:
        """Log metrics to quality_metrics table. Returns True on success."""
        try:
            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO monitoring.quality_metrics (
                            validation_type, run_timestamp, dag_run_id, total_jobs, unique_jobs,
                            duplicate_count, duplicate_rate, valid_jobs, invalid_jobs,
                            valid_rate, field_missing_rates, raw_count, data_loss_rate,
                            gate_status, gate_message
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        result.validation_type, result.timestamp, dag_run_id,
                        result.total_jobs, result.unique_jobs,
                        result.total_jobs - result.unique_jobs, result.duplicate_rate,
                        result.valid_jobs, result.total_jobs - result.valid_jobs,
                        result.valid_rate, json.dumps(result.field_missing_rates),
                        result.raw_count, result.data_loss_rate,
                        gate.status, gate.message
                    ))
                conn.commit()
            return True
        except Exception as e:
            logger.warning(f"Failed to log metrics: {e}")
            return False
    
    def log_business_rules(self, result: BusinessRuleResult, dag_run_id: str = None) -> bool:
        """Log business rule validation to quality_metrics table."""
        try:
            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO monitoring.quality_metrics (
                            validation_type, run_timestamp, dag_run_id, total_jobs, unique_jobs,
                            duplicate_count, duplicate_rate, valid_jobs, invalid_jobs,
                            valid_rate, field_missing_rates, raw_count, data_loss_rate,
                            gate_status, gate_message
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        'business_rules', result.timestamp, dag_run_id,
                        result.total_jobs, result.total_jobs,  # unique = total (not applicable)
                        0, 0.0,  # duplicate not applicable
                        int(result.total_jobs * (1 - result.violation_rate)),
                        int(result.total_jobs * result.violation_rate),
                        1 - result.violation_rate,
                        json.dumps(result.violations),  # Store violations in field_missing_rates
                        None, None,
                        result.status, '; '.join(result.details) if result.details else None
                    ))
                conn.commit()
            return True
        except Exception as e:
            logger.warning(f"Failed to log business rules: {e}")
            return False
