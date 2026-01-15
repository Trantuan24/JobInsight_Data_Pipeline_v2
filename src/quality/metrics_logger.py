"""Metrics Logger - Log validation metrics to PostgreSQL."""

import json
import logging
import psycopg2

from .validators import ValidationResult
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
