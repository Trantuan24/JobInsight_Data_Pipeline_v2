"""Health Check - Check PostgreSQL, MinIO, DuckDB status."""

import os
import logging
import signal
from datetime import datetime
from typing import Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 10


@contextmanager
def timeout(seconds: int):
    """Context manager for timeout protection."""
    def handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds}s")
    
    old_handler = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


class HealthChecker:
    """Check health of all services."""
    
    def __init__(self):
        self.pg_conn = os.environ.get(
            'PG_CONN_STRING',
            'postgresql://jobinsight:jobinsight@postgres:5432/jobinsight'
        )
        self.minio_endpoint = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
        self.minio_access = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
    
    def check_postgres(self) -> Dict[str, Any]:
        """Check PostgreSQL connection and basic stats."""
        try:
            with timeout(TIMEOUT_SECONDS):
                import psycopg2
                with psycopg2.connect(self.pg_conn, connect_timeout=5) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT version()")
                        version = cur.fetchone()[0].split(',')[0]
                        
                        cur.execute("SELECT COUNT(*) FROM public.raw_jobs")
                        raw_count = cur.fetchone()[0]
                        
                        cur.execute("SELECT COUNT(*) FROM jobinsight_staging.staging_jobs")
                        staging_count = cur.fetchone()[0]
                
                # Degraded if no data
                status = 'healthy'
                if raw_count == 0:
                    status = 'degraded'
                
                return {
                    'status': status,
                    'version': version,
                    'raw_jobs': raw_count,
                    'staging_jobs': staging_count
                }
        except TimeoutError:
            logger.error("PostgreSQL health check timed out")
            return {'status': 'unhealthy', 'error': 'Connection timeout'}
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_minio(self) -> Dict[str, Any]:
        """Check MinIO connection and bucket status."""
        try:
            with timeout(TIMEOUT_SECONDS):
                from minio import Minio
                from urllib3 import Timeout
                
                client = Minio(
                    self.minio_endpoint,
                    access_key=self.minio_access,
                    secret_key=self.minio_secret,
                    secure=False
                )
                
                buckets = [b.name for b in client.list_buckets()]
                dwh_exists = 'jobinsight-warehouse' in buckets
                
                # Get warehouse size (lightweight - just stat objects)
                dwh_size = 0
                if dwh_exists:
                    for obj in client.list_objects('jobinsight-warehouse', recursive=True):
                        dwh_size += obj.size
                
                status = 'healthy'
                if not dwh_exists:
                    status = 'degraded'
                
                return {
                    'status': status,
                    'buckets': buckets,
                    'warehouse_exists': dwh_exists,
                    'warehouse_size_mb': round(dwh_size / 1024 / 1024, 2)
                }
        except TimeoutError:
            logger.error("MinIO health check timed out")
            return {'status': 'unhealthy', 'error': 'Connection timeout'}
        except Exception as e:
            logger.error(f"MinIO health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_duckdb(self) -> Dict[str, Any]:
        """Check DuckDB file exists on MinIO (lightweight - no download)."""
        try:
            with timeout(TIMEOUT_SECONDS):
                from minio import Minio
                
                client = Minio(
                    self.minio_endpoint,
                    access_key=self.minio_access,
                    secret_key=self.minio_secret,
                    secure=False
                )
                
                # Just check file exists and get metadata (no download)
                stat = client.stat_object('jobinsight-warehouse', 'dwh/jobinsight.duckdb')
                
                return {
                    'status': 'healthy',
                    'file_exists': True,
                    'size_mb': round(stat.size / 1024 / 1024, 2),
                    'last_modified': stat.last_modified.isoformat()
                }
        except TimeoutError:
            logger.error("DuckDB health check timed out")
            return {'status': 'unhealthy', 'error': 'Connection timeout'}
        except Exception as e:
            logger.error(f"DuckDB health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}
    
    def check_grafana(self) -> Dict[str, Any]:
        """Check Grafana health endpoint."""
        try:
            with timeout(TIMEOUT_SECONDS):
                import urllib.request
                
                url = 'http://grafana:3000/api/health'
                req = urllib.request.Request(url, method='GET')
                
                with urllib.request.urlopen(req, timeout=5) as response:
                    if response.status == 200:
                        return {
                            'status': 'healthy',
                            'url': 'http://localhost:3000'
                        }
                    else:
                        return {
                            'status': 'degraded',
                            'http_status': response.status
                        }
        except TimeoutError:
            logger.error("Grafana health check timed out")
            return {'status': 'unhealthy', 'error': 'Connection timeout'}
        except Exception as e:
            logger.warning(f"Grafana health check failed: {e}")
            # Grafana is optional, so degraded not unhealthy
            return {'status': 'degraded', 'error': str(e)}
    
    def check_superset(self) -> Dict[str, Any]:
        """Check Superset health endpoint."""
        try:
            with timeout(TIMEOUT_SECONDS):
                import urllib.request
                
                url = 'http://superset:8088/health'
                req = urllib.request.Request(url, method='GET')
                
                with urllib.request.urlopen(req, timeout=5) as response:
                    if response.status == 200:
                        return {
                            'status': 'healthy',
                            'url': 'http://localhost:8088'
                        }
                    else:
                        return {
                            'status': 'degraded',
                            'http_status': response.status
                        }
        except TimeoutError:
            logger.error("Superset health check timed out")
            return {'status': 'unhealthy', 'error': 'Connection timeout'}
        except Exception as e:
            logger.warning(f"Superset health check failed: {e}")
            # Superset is optional, so degraded not unhealthy
            return {'status': 'degraded', 'error': str(e)}
    
    def check_all(self) -> Dict[str, Any]:
        """Run all health checks."""
        result = {
            'timestamp': datetime.now().isoformat(),
            'services': {
                'postgres': self.check_postgres(),
                'minio': self.check_minio(),
                'duckdb': self.check_duckdb(),
                'grafana': self.check_grafana(),
                'superset': self.check_superset()
            }
        }
        
        # Overall status: unhealthy > degraded > healthy
        # Note: Grafana/Superset are optional (degraded won't fail pipeline)
        statuses = [s.get('status') for s in result['services'].values()]
        if 'unhealthy' in statuses:
            result['overall'] = 'unhealthy'
        elif 'degraded' in statuses:
            result['overall'] = 'degraded'
        else:
            result['overall'] = 'healthy'
        
        return result


def check_all_services() -> Dict[str, Any]:
    """Convenience function to check all services."""
    return HealthChecker().check_all()
