"""Unit tests for data quality validators."""
import pytest
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.quality.validators import CrawlValidator, BusinessRuleValidator


class TestCrawlValidator:
    """Tests for CrawlValidator."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.validator = CrawlValidator()
        self.valid_jobs = [
            {"job_id": "2008076", "title": "Python Developer", "company_name": "FPT"},
            {"job_id": "2008077", "title": "Java Developer", "company_name": "VNG"},
            {"job_id": "2008078", "title": "Data Engineer", "company_name": "Grab"},
        ]
    
    def test_validate_returns_correct_total(self):
        """Should return correct total job count."""
        result = self.validator.validate(self.valid_jobs)
        assert result.total_jobs == 3
    
    def test_validate_returns_correct_valid_rate(self):
        """Should return 100% valid rate for valid jobs."""
        result = self.validator.validate(self.valid_jobs)
        assert result.valid_rate == 1.0
    
    def test_validate_detects_missing_job_id(self):
        """Should detect missing job_id."""
        jobs = [{"job_id": None, "title": "Test", "company_name": "Test"}]
        result = self.validator.validate(jobs)
        assert result.valid_rate == 0.0
        assert result.field_missing_rates['job_id'] == 1.0
    
    def test_validate_detects_invalid_job_id(self):
        """Should detect non-numeric job_id."""
        jobs = [{"job_id": "abc", "title": "Test", "company_name": "Test"}]
        result = self.validator.validate(jobs)
        assert result.valid_rate == 0.0
    
    def test_validate_detects_missing_title(self):
        """Should detect missing title."""
        jobs = [{"job_id": "123", "title": "", "company_name": "Test"}]
        result = self.validator.validate(jobs)
        assert result.valid_rate == 0.0
        assert result.field_missing_rates['title'] == 1.0
    
    def test_validate_detects_missing_company(self):
        """Should detect missing company_name."""
        jobs = [{"job_id": "123", "title": "Test", "company_name": None}]
        result = self.validator.validate(jobs)
        assert result.valid_rate == 0.0
    
    def test_validate_calculates_duplicate_rate(self):
        """Should calculate duplicate rate correctly."""
        jobs = [
            {"job_id": "123", "title": "Test", "company_name": "A"},
            {"job_id": "123", "title": "Test", "company_name": "A"},  # duplicate
            {"job_id": "456", "title": "Test", "company_name": "B"},
        ]
        result = self.validator.validate(jobs)
        assert result.duplicate_rate == pytest.approx(1/3, rel=0.01)
    
    def test_validate_empty_list(self):
        """Should handle empty job list."""
        result = self.validator.validate([])
        assert result.total_jobs == 0
        assert result.valid_rate == 0.0


class TestBusinessRuleValidator:
    """Tests for BusinessRuleValidator."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.validator = BusinessRuleValidator()
    
    def test_validate_healthy_jobs(self):
        """Should return healthy status for valid jobs."""
        jobs = [
            {
                "job_id": "123",
                "title": "Python Developer",
                "company_name": "FPT Software",
                "location": "Hà Nội",
                "salary_min": 15_000_000,
                "salary_max": 25_000_000,
            }
        ]
        result = self.validator.validate(jobs)
        assert result.status == 'healthy'
        assert result.violation_rate == 0.0
    
    def test_validate_detects_invalid_salary(self):
        """Should detect invalid salary (min > max)."""
        jobs = [
            {
                "job_id": "123",
                "title": "Python Developer",
                "company_name": "FPT",
                "location": "HN",
                "salary_min": 30_000_000,
                "salary_max": 20_000_000,  # Invalid: min > max
            }
        ]
        result = self.validator.validate(jobs)
        assert result.violations.get('salary_invalid', 0) == 1
    
    def test_validate_detects_salary_too_high(self):
        """Should detect salary exceeding 200M cap."""
        jobs = [
            {
                "job_id": "123",
                "title": "Python Developer",
                "company_name": "FPT",
                "location": "HN",
                "salary_min": 100_000_000,
                "salary_max": 250_000_000,  # > 200M
            }
        ]
        result = self.validator.validate(jobs)
        assert result.violations.get('salary_too_high', 0) == 1
    
    def test_validate_detects_short_title(self):
        """Should detect title shorter than 5 chars."""
        jobs = [
            {
                "job_id": "123",
                "title": "Dev",  # < 5 chars
                "company_name": "FPT Software",
                "location": "HN",
            }
        ]
        result = self.validator.validate(jobs)
        assert result.violations.get('title_too_short', 0) == 1
    
    def test_validate_detects_short_company(self):
        """Should detect company name shorter than 3 chars."""
        jobs = [
            {
                "job_id": "123",
                "title": "Python Developer",
                "company_name": "AB",  # < 3 chars
                "location": "HN",
            }
        ]
        result = self.validator.validate(jobs)
        assert result.violations.get('company_too_short', 0) == 1
    
    def test_validate_detects_invalid_location(self):
        """Should detect empty or N/A location."""
        jobs = [
            {
                "job_id": "123",
                "title": "Python Developer",
                "company_name": "FPT Software",
                "location": "",  # Empty
            }
        ]
        result = self.validator.validate(jobs)
        assert result.violations.get('location_invalid', 0) == 1
    
    def test_validate_unhealthy_status(self):
        """Should return unhealthy when >10% violations."""
        # Create 10 jobs, all with violations
        jobs = [
            {
                "job_id": str(i),
                "title": "Dev",  # Too short
                "company_name": "AB",  # Too short
                "location": "",  # Invalid
            }
            for i in range(10)
        ]
        result = self.validator.validate(jobs)
        assert result.status == 'unhealthy'
    
    def test_validate_empty_list(self):
        """Should handle empty job list."""
        result = self.validator.validate([])
        assert result.total_jobs == 0
        assert result.status == 'healthy'
