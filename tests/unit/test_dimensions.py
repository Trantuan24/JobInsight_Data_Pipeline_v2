"""Unit tests for dimension processing."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.etl.warehouse.dimensions import compute_company_hash


class TestComputeCompanyHash:
    """Tests for compute_company_hash function."""
    
    def test_returns_md5_hash(self):
        """Should return MD5 hash of lowercase company name."""
        result = compute_company_hash("FPT Software")
        assert len(result) == 32  # MD5 hash length
        assert result.isalnum()  # Only alphanumeric
    
    def test_same_name_same_hash(self):
        """Should return same hash for same company name."""
        hash1 = compute_company_hash("FPT Software")
        hash2 = compute_company_hash("FPT Software")
        assert hash1 == hash2
    
    def test_case_insensitive(self):
        """Should be case insensitive."""
        hash1 = compute_company_hash("FPT Software")
        hash2 = compute_company_hash("fpt software")
        hash3 = compute_company_hash("FPT SOFTWARE")
        assert hash1 == hash2 == hash3
    
    def test_different_names_different_hash(self):
        """Should return different hash for different names."""
        hash1 = compute_company_hash("FPT Software")
        hash2 = compute_company_hash("VNG Corporation")
        assert hash1 != hash2
    
    def test_handles_empty_string(self):
        """Should handle empty string."""
        result = compute_company_hash("")
        assert len(result) == 32
    
    def test_handles_special_characters(self):
        """Should handle special characters."""
        result = compute_company_hash("Công Ty TNHH ABC & XYZ")
        assert len(result) == 32
