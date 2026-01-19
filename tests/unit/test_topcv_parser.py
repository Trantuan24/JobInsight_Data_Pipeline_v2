"""Unit tests for TopCV parser."""
import pytest
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.data_sources.topcv.parser import jobs_to_dataframe


class TestJobsToDataframe:
    """Tests for jobs_to_dataframe function."""
    
    def test_converts_list_to_dataframe(self):
        """Should convert jobs list to DataFrame."""
        jobs = [
            {"job_id": "123", "title": "Dev", "company_name": "FPT"},
            {"job_id": "456", "title": "QA", "company_name": "VNG"},
        ]
        df = jobs_to_dataframe(jobs)
        assert len(df) == 2
        assert "job_id" in df.columns
        assert "title" in df.columns
    
    def test_removes_duplicates(self):
        """Should remove duplicate job_ids."""
        jobs = [
            {"job_id": "123", "title": "Dev", "company_name": "FPT"},
            {"job_id": "123", "title": "Dev", "company_name": "FPT"},  # Duplicate
        ]
        df = jobs_to_dataframe(jobs)
        assert len(df) == 1
    
    def test_converts_skills_to_json(self):
        """Should convert skills list to JSON string."""
        jobs = [
            {"job_id": "123", "title": "Dev", "skills": ["Python", "SQL"]},
        ]
        df = jobs_to_dataframe(jobs)
        # Skills should be JSON string
        skills = df.iloc[0]["skills"]
        assert isinstance(skills, str)
        assert json.loads(skills) == ["Python", "SQL"]
    
    def test_handles_empty_list(self):
        """Should return empty DataFrame for empty list."""
        df = jobs_to_dataframe([])
        assert len(df) == 0
    
    def test_replaces_empty_strings_with_none(self):
        """Should replace empty strings with None."""
        jobs = [
            {"job_id": "123", "title": "", "company_name": "FPT"},
        ]
        df = jobs_to_dataframe(jobs)
        assert df.iloc[0]["title"] is None
