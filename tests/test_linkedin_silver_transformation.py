"""DemandLens - Unit Tests for LinkedIn Job Postings Silver Transformation.

Tests cover:
  1. Workplace classification (Remote vs Hybrid vs Onsite distinction).
  2. Invariant verification: Hybrid is NEVER classified as fully remote.
  3. Country ISO alpha-2 code mapping.
  4. Seniority tier harmonization.
  5. Text & Location cleaning with genuine NULL preservation.
  6. Date & Timestamp parsing.
  7. Full Spark DataFrame transformation flow with synthetic Bronze data.
  8. Schema preservation: 14 original columns + 4 Bronze provenance + standardized fields.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

from src.demandlens.silver.linkedin_postings_transformer import (
    _py_classify_workplace,
    _py_clean_company,
    _py_clean_location,
    _py_clean_title,
    _py_harmonize_seniority,
    _py_map_country_code,
    transform_linkedin_postings,
)


# ==============================================================================
# 1. Pure Python Helper Function Tests
# ==============================================================================

class TestLinkedInTransformationHelpers:
    """Tests unit rules for Python helper functions."""

    def test_workplace_classification_remote(self):
        wp, is_rem, is_hyb = _py_classify_workplace("Remote")
        assert wp == "REMOTE"
        assert is_rem is True
        assert is_hyb is False

    def test_workplace_classification_hybrid(self):
        wp, is_rem, is_hyb = _py_classify_workplace("Hybrid")
        assert wp == "HYBRID"
        assert is_rem is False  # CRITICAL INVARIANT: Hybrid is NOT fully remote!
        assert is_hyb is True

    def test_workplace_classification_onsite(self):
        for val in ["Onsite", "on-site", "On Site"]:
            wp, is_rem, is_hyb = _py_classify_workplace(val)
            assert wp == "ON_SITE"
            assert is_rem is False
            assert is_hyb is False

    def test_workplace_classification_unknown_and_null(self):
        for val in [None, "", "   ", "Flexible", "123"]:
            wp, is_rem, is_hyb = _py_classify_workplace(val)
            assert wp == "UNSPECIFIED"
            assert is_rem is False
            assert is_hyb is False

    def test_country_code_mapping(self):
        assert _py_map_country_code("United States") == "US"
        assert _py_map_country_code("united kingdom") == "GB"
        assert _py_map_country_code("Canada") == "CA"
        assert _py_map_country_code("AUSTRALIA") == "AU"
        assert _py_map_country_code("Germany") == "OTHER"
        assert _py_map_country_code(None) is None
        assert _py_map_country_code("") is None

    def test_seniority_harmonization(self):
        assert _py_harmonize_seniority("Mid senior") == "MID_SENIOR"
        assert _py_harmonize_seniority("Associate") == "ASSOCIATE"
        assert _py_harmonize_seniority("Entry level") == "ENTRY"
        assert _py_harmonize_seniority("Director") == "DIRECTOR"
        assert _py_harmonize_seniority("Unknown") == "UNSPECIFIED"
        assert _py_harmonize_seniority(None) == "UNSPECIFIED"

    def test_null_preservation_in_cleaning(self):
        assert _py_clean_title(None) is None
        assert _py_clean_company(None) is None
        assert _py_clean_location(None) is None

    def test_text_cleaning_behavior(self):
        assert _py_clean_title("  Senior  Software   Engineer  ") == "Senior Software Engineer"
        assert _py_clean_company("  Acme   Corp  ") == "Acme Corp"
        assert _py_clean_location("  Dallas,   TX  ,;-  ") == "Dallas, TX"


# ==============================================================================
# 2. PySpark DataFrame Transformation Integration Tests
# ==============================================================================

@pytest.fixture(scope="module")
def spark_session():
    """Create a lightweight local SparkSession for testing."""
    configure_windows_environment()
    project_root = Path(__file__).resolve().parent.parent
    spark_tmp = (project_root / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Test-LinkedIn-Silver")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(spark_tmp))
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.io.tmpdir={spark_tmp_str} --enable-native-access=ALL-UNNAMED",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_bronze_df(spark_session):
    """Generate synthetic DataFrame matching LinkedIn Bronze schema."""
    schema = StructType([
        StructField("job_link", StringType(), False),
        StructField("last_processed_time", StringType(), True),
        StructField("got_summary", StringType(), True),
        StructField("got_ner", StringType(), True),
        StructField("is_being_worked", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("company", StringType(), True),
        StructField("job_location", StringType(), True),
        StructField("first_seen", StringType(), True),
        StructField("search_city", StringType(), True),
        StructField("search_country", StringType(), True),
        StructField("search_position", StringType(), True),
        StructField("job_level", StringType(), True),
        StructField("job_type", StringType(), True),
        StructField("_source_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_timestamp", TimestampType(), True),
        StructField("_ingestion_run_id", StringType(), True),
    ])

    now = datetime.now(timezone.utc)
    data = [
        # 1. Onsite US role
        (
            "https://www.linkedin.com/jobs/view/101",
            "2024-01-21 07:12:29.000000+00", "t", "t", "f",
            "Data Engineer", "Acme Corp", "San Francisco, CA",
            "2024-01-15", "San Francisco", "United States", "Data Engineer",
            "Mid senior", "Onsite",
            "linkedin_2024", "linkedin_job_postings.csv", now, "run_01"
        ),
        # 2. Hybrid UK role
        (
            "https://www.linkedin.com/jobs/view/102",
            "2024-01-21 07:30:00.000000+00", "t", "t", "f",
            "Senior Backend Engineer", "Global Tech", "London, UK",
            "2024-01-16", "London", "United Kingdom", "Backend Engineer",
            "Mid senior", "Hybrid",
            "linkedin_2024", "linkedin_job_postings.csv", now, "run_01"
        ),
        # 3. Remote CA role with NULL company and NULL location
        (
            "https://www.linkedin.com/jobs/view/103",
            "2024-01-21 08:00:00.000000+00", "f", "f", "f",
            "Remote DevOps Lead", None, None,
            "2024-01-17", "Toronto", "Canada", "DevOps",
            "Associate", "Remote",
            "linkedin_2024", "linkedin_job_postings.csv", now, "run_01"
        ),
        # 4. Australia role with whitespace padding
        (
            "https://www.linkedin.com/jobs/view/104",
            "2024-01-21 09:15:00.000000+00", "t", "t", "f",
            "  ML   Engineer  ", "  AI   Works  ", "  Sydney, NSW  ",
            "2024-01-18", "Sydney", "Australia", "ML",
            "Mid senior", "Onsite",
            "linkedin_2024", "linkedin_job_postings.csv", now, "run_01"
        ),
    ]

    return spark_session.createDataFrame(data=data, schema=schema)


def test_transform_linkedin_postings_fidelity(sample_bronze_df):
    """Test full LinkedIn Silver transformation pipeline."""
    df_silver = transform_linkedin_postings(sample_bronze_df, silver_run_id="test_run_01")

    # 1. Row count preservation
    assert df_silver.count() == 4

    rows = {r["job_link"]: r for r in df_silver.collect()}

    # 2. Onsite US role verification
    r1 = rows["https://www.linkedin.com/jobs/view/101"]
    assert r1["title_clean"] == "Data Engineer"
    assert r1["company_clean"] == "Acme Corp"
    assert r1["location_clean"] == "San Francisco, CA"
    assert r1["country_code"] == "US"
    assert r1["workplace_type"] == "ON_SITE"
    assert r1["is_remote"] is False
    assert r1["is_hybrid"] is False
    assert r1["seniority_tier"] == "MID_SENIOR"
    assert r1["first_seen_date"] == date(2024, 1, 15)

    # 3. Hybrid UK role verification (CRITICAL HYBRID INVARIANT)
    r2 = rows["https://www.linkedin.com/jobs/view/102"]
    assert r2["country_code"] == "GB"
    assert r2["workplace_type"] == "HYBRID"
    assert r2["is_remote"] is False  # MUST BE FALSE
    assert r2["is_hybrid"] is True   # MUST BE TRUE
    assert r2["seniority_tier"] == "MID_SENIOR"

    # 4. Remote CA role with genuine NULL preservation
    r3 = rows["https://www.linkedin.com/jobs/view/103"]
    assert r3["company_clean"] is None   # Preserved NULL!
    assert r3["location_clean"] is None  # Preserved NULL!
    assert r3["country_code"] == "CA"
    assert r3["workplace_type"] == "REMOTE"
    assert r3["is_remote"] is True
    assert r3["is_hybrid"] is False
    assert r3["seniority_tier"] == "ASSOCIATE"

    # 5. Whitespace trimming on AU role
    r4 = rows["https://www.linkedin.com/jobs/view/104"]
    assert r4["title_clean"] == "ML Engineer"
    assert r4["company_clean"] == "AI Works"
    assert r4["location_clean"] == "Sydney, NSW"
    assert r4["country_code"] == "AU"

    # 6. Original columns preserved
    expected_orig = [
        "job_title", "company", "job_location", "first_seen", "last_processed_time",
        "search_city", "search_country", "search_position", "job_level", "job_type",
        "got_summary", "got_ner", "is_being_worked",
        "_source_id", "_source_file", "_ingestion_timestamp", "_ingestion_run_id",
        "_silver_run_id", "_silver_timestamp",
    ]
    for col in expected_orig:
        assert col in df_silver.columns, f"Missing expected column: {col}"

    # 7. Zero salary columns added
    salary_cols = [c for c in df_silver.columns if "salary" in c.lower()]
    assert len(salary_cols) == 0, f"Found unintended salary columns: {salary_cols}"
