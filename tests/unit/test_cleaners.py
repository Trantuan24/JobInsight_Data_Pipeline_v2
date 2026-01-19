"""Unit tests for data cleaning functions."""
import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.etl.staging.cleaners import clean_title, clean_company_name


class TestCleanTitle:
    """Tests for clean_title function."""
    
    def test_clean_title_removes_salary_info(self):
        """Should remove salary information from title."""
        title = "Senior Python Developer - Thu Nhập Upto 40 Triệu"
        result = clean_title(title)
        assert "Thu Nhập" not in result
        assert "40 Triệu" not in result
        assert "Senior Python Developer" in result
    
    def test_clean_title_removes_location_suffix(self):
        """Should remove location suffix from title."""
        title = "Backend Developer - Tại Hà Nội"
        result = clean_title(title)
        assert "Tại Hà Nội" not in result
        assert "Backend Developer" in result
    
    def test_clean_title_adds_space_before_parenthesis(self):
        """Should add space before parenthesis if missing."""
        title = "Backend Developer(Java, Spring Boot)"
        result = clean_title(title)
        assert "Developer (" in result or "Developer(" not in result
    
    def test_clean_title_preserves_cpp(self):
        """Should preserve C++ in title."""
        title = "C++ Developer"
        result = clean_title(title)
        assert "C++" in result
    
    def test_clean_title_preserves_csharp(self):
        """Should preserve C# in title."""
        title = "C# Developer"
        result = clean_title(title)
        assert "C#" in result
    
    def test_clean_title_preserves_dotnet(self):
        """Should preserve .NET in title."""
        title = ".NET Developer"
        result = clean_title(title)
        assert ".NET" in result
    
    def test_clean_title_handles_empty(self):
        """Should return empty string for empty input."""
        assert clean_title("") == ""
        assert clean_title(None) == ""
    
    def test_clean_title_removes_pipe_salary(self):
        """Should remove salary after pipe."""
        title = ".NET Developer | Lương 30 Triệu"
        result = clean_title(title)
        assert "Lương" not in result
        assert "30 Triệu" not in result


class TestCleanCompanyName:
    """Tests for clean_company_name function."""
    
    def test_clean_company_capitalizes_words(self):
        """Should capitalize words properly."""
        name = "công ty abc"
        result = clean_company_name(name)
        assert result[0].isupper()  # First letter capitalized
    
    def test_clean_company_preserves_abbreviations(self):
        """Should preserve company abbreviations like TNHH, CP."""
        name = "công ty tnhh abc"
        result = clean_company_name(name)
        assert "TNHH" in result
    
    def test_clean_company_preserves_tech_words(self):
        """Should preserve tech words like IT, AI."""
        name = "công ty it solutions"
        result = clean_company_name(name)
        assert "IT" in result
    
    def test_clean_company_removes_recruitment_keywords(self):
        """Should remove recruitment keywords."""
        name = "ABC Company tuyển dụng"
        result = clean_company_name(name)
        assert "tuyển dụng" not in result.lower()
    
    def test_clean_company_handles_empty(self):
        """Should return empty string for empty input."""
        assert clean_company_name("") == ""
        assert clean_company_name(None) == ""
    
    def test_clean_company_removes_extra_spaces(self):
        """Should remove extra spaces."""
        name = "Công  Ty   ABC"
        result = clean_company_name(name)
        assert "  " not in result
