"""ETL Metrics Logger - Track pipeline performance."""

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2

logger = logging.getLogger(__name__)


@dataclass
class ETLMetrics:
    """ETL task metrics."""
    dag_id: str
    task_id: str
    dag_run_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    status: str = 'running'
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def throughput(self) -> float:
        """Rows per second."""
        if self.duration_seconds > 0:
            return self.rows_out / self.duration_seconds
        return 0.0


class ETLMetricsLogger:
    """Logger for ETL metrics to monitoring.pipeline_runs table."""
    
    def __init__(self, pg_conn_string: str):
        self.conn_string = pg_conn_string
    
    def log(self, metrics: ETLMetrics) -> bool:
        """Log metrics to etl_metrics table."""
        import json
        try:
            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO monitoring.etl_metrics (
                            dag_id, task_id, dag_run_id, status,
                            duration_seconds, rows_in, rows_out, rows_inserted,
                            rows_updated, rows_failed, throughput, error_message,
                            metadata, started_at, completed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        metrics.dag_id, metrics.task_id, metrics.dag_run_id,
                        metrics.status, metrics.duration_seconds,
                        metrics.rows_in, metrics.rows_out, metrics.rows_inserted,
                        metrics.rows_updated, metrics.rows_failed, metrics.throughput,
                        metrics.error_message,
                        json.dumps(metrics.metadata) if metrics.metadata else None,
                        metrics.start_time, metrics.end_time
                    ))
                conn.commit()
            logger.info(f"ETL metrics logged: {metrics.task_id} - {metrics.rows_out} rows in {metrics.duration_seconds:.2f}s")
            return True
        except Exception as e:
            logger.warning(f"Failed to log ETL metrics: {e}")
            return False
    
    @contextmanager
    def track(self, dag_id: str, task_id: str, dag_run_id: str = None):
        """Context manager to track task duration and metrics."""
        metrics = ETLMetrics(
            dag_id=dag_id,
            task_id=task_id,
            dag_run_id=dag_run_id,
            start_time=datetime.now()
        )
        start = time.time()
        
        try:
            yield metrics
            metrics.status = 'success'
        except Exception as e:
            metrics.status = 'failed'
            metrics.error_message = str(e)
            raise
        finally:
            metrics.end_time = datetime.now()
            metrics.duration_seconds = time.time() - start
            self.log(metrics)
