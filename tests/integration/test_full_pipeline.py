"""Integration tests for full pipeline.

NOTE: These tests require running services (PostgreSQL, MinIO).
Run with: pytest tests/integration/ -v --ignore-glob="*" (skip by default)
Or in Docker: docker exec jobinsight-airflow-webserver-1 pytest tests/integration/ -v
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestStagingPipeline:
    """Integration tests for staging pipeline."""
    
    @pytest.mark.skip(reason="Requires PostgreSQL connection")
    def test_staging_pipeline_runs(self):
        """Test that staging pipeline can run without errors."""
        from src.etl.staging.pipeline import run_staging_pipeline
        
        # This would require actual database connection
        # result = run_staging_pipeline()
        # assert result is not None
        pass


class TestMinIOConnection:
    """Integration tests for MinIO connection."""
    
    @pytest.mark.skip(reason="Requires MinIO connection")
    def test_minio_buckets_exist(self):
        """Test that required MinIO buckets exist."""
        from src.storage.minio import get_minio_client
        
        # client = get_minio_client()
        # buckets = [b.name for b in client.list_buckets()]
        # assert 'jobinsight-warehouse' in buckets
        pass


class TestDWHPipeline:
    """Integration tests for DWH pipeline."""
    
    @pytest.mark.skip(reason="Requires full infrastructure")
    def test_dwh_etl_runs(self):
        """Test that DWH ETL can run without errors."""
        # This would require actual database + MinIO connection
        pass
