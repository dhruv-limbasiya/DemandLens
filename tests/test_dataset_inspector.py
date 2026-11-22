"""Tests for DemandLens Dataset Inspector."""

import tempfile
from pathlib import Path
import pytest
import pandas as pd

from src.demandlens.ingestion.dataset_inspector import (
    DatasetInspector,
    inspect_dataset_directory,
)


@pytest.fixture
def sample_linkedin_csv(tmp_path: Path) -> Path:
    """Generate a representative sample CSV matching Source A (LinkedIn 2024)."""
    csv_path = tmp_path / "linkedin_job_postings_sample.csv"
    data = {
        "job_link": [
            "https://www.linkedin.com/jobs/view/101",
            "https://www.linkedin.com/jobs/view/102",
            "https://www.linkedin.com/jobs/view/103",
        ],
        "last_processed_time": ["2024-01-10 12:00:00", "2024-01-11 14:30:00", None],
        "last_status": ["PROCESSED", "PROCESSED", "PROCESSED"],
        "got_summary": ["t", "t", "f"],
        "got_ner": ["t", "t", "f"],
        "job_title": ["Senior Data Engineer", "Machine Learning Specialist", "Analytics Lead"],
        "company": ["TechCorp", "DataAI Inc", None],
        "job_location": ["Austin, TX", "Bengaluru, Karnataka, India", "London, UK"],
        "first_seen": ["2024-01-05", "2024-01-06", "2024-01-07"],
        "search_city": ["Austin", "Bengaluru", "London"],
        "search_country": ["United States", "India", "United Kingdom"],
        "search_position": ["Data Engineer", "Machine Learning", "Analytics"],
        "job_level": ["Mid-Senior level", "Associate", "Director"],
        "job_type": ["Full-time", "Remote", "Hybrid"],
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def sample_india_jobs_csv(tmp_path: Path) -> Path:
    """Generate a representative sample CSV matching Source B (Indian Job Market)."""
    csv_path = tmp_path / "indian_job_market_sample.csv"
    data = {
        "jobId": ["IND_101", "IND_102", "IND_103"],
        "title": ["Data Engineer - PySpark/SQL", "Full Stack Java Developer", "AI/ML Engineer"],
        "companyName": ["Infosys", "Wipro", "TCS"],
        "location": ["Bengaluru", "Hyderabad", "Pune"],
        "tagsAndSkills": ["Python, PySpark, AWS, SQL", "Java, Spring Boot, React", "Python, PyTorch, LLM"],
        "experience": ["3-6 Yrs", "1-3 Yrs", "5-8 Yrs"],
        "minimumExperience": [3, 1, 5],
        "maximumExperience": [6, 3, 8],
        "salary": ["8-15 Lacs PA", "Not Disclosed", "18-28 Lacs PA"],
        "minimumSalary": [800000.0, None, 1800000.0],
        "maximumSalary": [1500000.0, None, 2800000.0],
        "currency": ["INR", None, "INR"],
        "jobUploaded": ["3 Days Ago", "Just Now", "15 Days Ago"],
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)
    return csv_path


def test_inspect_source_a_with_pandas(sample_linkedin_csv: Path):
    """Verify inspection on Source A (LinkedIn 2024) format."""
    inspector = DatasetInspector()
    report = inspector.inspect_with_pandas(sample_linkedin_csv, sample_rows=3)

    assert report.total_records == 3
    assert report.columns_count == 14
    assert "job_link" in report.detected_potential_keys
    assert len(report.detected_salary_fields) == 0  # Confirms no salary field
    assert report.file_name == "linkedin_job_postings_sample.csv"

    # Verify column profile for job_title
    title_profile = next(c for c in report.column_profiles if c.name == "job_title")
    assert title_profile.null_count == 0
    assert title_profile.null_percentage == 0.0

    # Verify markdown report generation
    md = report.summary_markdown()
    assert "### Dataset Inspection:" in md
    assert "`job_link`" in md


def test_inspect_source_b_with_pandas(sample_india_jobs_csv: Path):
    """Verify inspection on Source B (Indian Job Market) format."""
    inspector = DatasetInspector()
    report = inspector.inspect_with_pandas(sample_india_jobs_csv, sample_rows=3)

    assert report.total_records == 3
    assert report.columns_count == 13
    assert "jobId" in report.detected_potential_keys
    assert any("skill" in s.lower() or "tag" in s.lower() for s in report.detected_skills_fields)
    assert any("salary" in s.lower() for s in report.detected_salary_fields)

    # Check date coverage notes relative strings
    assert "jobUploaded" in report.date_coverage
    assert report.date_coverage["jobUploaded"]["apparent_precision"] == "RELATIVE_STRING"


def test_inspect_missing_file_raises_error():
    """Verify appropriate error on missing files."""
    inspector = DatasetInspector()
    with pytest.raises(FileNotFoundError):
        inspector.inspect_with_pandas(Path("non_existent_file.csv"))


def test_inspect_directory_utility(tmp_path: Path, sample_linkedin_csv: Path, sample_india_jobs_csv: Path):
    """Verify directory scanner utility."""
    reports = inspect_dataset_directory(tmp_path, engine="pandas", sample_rows=2)
    assert len(reports) == 2

