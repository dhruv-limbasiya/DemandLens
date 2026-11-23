"""DemandLens - Unit Tests for Indian Job Market Silver Transformation.

Tests cover:
  - Spark DataFrame transformation logic with synthetic test data.
  - Primary key deduplication and completeness preference.
  - Salary standardization, range sanity, and null preservation.
  - Experience parsing and seniority tier assignment.
  - Skill normalization and bridge table creation.
  - Referential integrity between jobs and job_skills_bridge.
  - Deterministic posting freshness classification.
"""

from __future__ import annotations

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from datetime import datetime, timezone

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

from src.demandlens.silver.indian_jobs_transformer import (
    deduplicate_indian_jobs,
    transform_indian_jobs,
)


@pytest.fixture(scope="module")
def spark_session():
    """Create a lightweight local SparkSession for unit testing."""
    configure_windows_environment()
    project_root = Path(__file__).resolve().parent.parent
    spark_tmp = (project_root / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Test-Indian-Silver")
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
def sample_synthetic_bronze_df(spark_session):
    """Generate synthetic test DataFrame mimicking Bronze Indian dataset schema."""
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("jobId", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("jobUploaded", StringType(), True),
        StructField("companyName", StringType(), True),
        StructField("tagsAndSkills", StringType(), True),
        StructField("experience", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("location", StringType(), True),
        StructField("companyId", StringType(), True),
        StructField("ReviewsCount", StringType(), True),
        StructField("AggregateRating", StringType(), True),
        StructField("jobDescription", StringType(), True),
        StructField("minimumSalary", StringType(), True),
        StructField("maximumSalary", StringType(), True),
        StructField("minimumExperience", StringType(), True),
        StructField("maximumExperience", StringType(), True),
        StructField("_source_id", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_ingestion_timestamp", TimestampType(), True),
        StructField("_ingestion_run_id", StringType(), True),
    ])

    now = datetime.now(timezone.utc)
    data = [
        # 1. Standard full record
        (
            "Senior Data Engineer", "ID_001", "INR", "1 Day Ago", "Acme Tech",
            "Python, PySpark, AWS, python", "3-6 Yrs", "12-18 Lacs PA",
            "Bengaluru, Karnataka", "COMP_1", "120", "4.2", "Great data job",
            "1200000", "1800000", "3.0", "6.0",
            "indian_job_market_2025_2026", "source.csv", now, "run_01"
        ),
        # 2. Duplicate of ID_001 with fewer details (should be dropped in dedup)
        (
            "Data Engineer", "ID_001", "INR", "1 Day Ago", "Acme Tech",
            "Python", "3-6 Yrs", "Not disclosed",
            "Bengaluru", None, None, None, "Short desc",
            None, None, "3.0", "6.0",
            "indian_job_market_2025_2026", "source.csv", now, "run_01"
        ),
        # 3. Entry level record with undisclosed salary
        (
            "Junior Analyst", "ID_002", "INR", "Few Hours Ago", "Beta Corp",
            "SQL, Excel, Tableau", "0-1 Yrs", "Not disclosed",
            "Mumbai (Remote)", "COMP_2", "45", "3.8", "Entry analytics",
            None, None, "0.0", "1.0",
            "indian_job_market_2025_2026", "source.csv", now, "run_01"
        ),
        # 4. Lead level with text experience and no skills
        (
            "Director of Engineering", "ID_003", "INR", "30+ Days Ago", "Gamma Inc",
            None, "10+ Yrs", "30-50 Lacs PA",
            "Delhi / NCR", "COMP_3", "300", "4.6", "Leadership role",
            "3000000", "5000000", None, None,
            "indian_job_market_2025_2026", "source.csv", now, "run_01"
        ),
        # 5. Invalid salary bounds (min > max) -> should be flagged invalid
        (
            "QA Engineer", "ID_004", "INR", "2 Weeks Ago", "Delta Ltd",
            "Selenium, Java", "2-4 Yrs", "Inverted Salary",
            "Pune", "COMP_4", "10", "3.0", "QA testing",
            "800000", "400000", "2.0", "4.0",
            "indian_job_market_2025_2026", "source.csv", now, "run_01"
        ),
    ]

    return spark_session.createDataFrame(data, schema)


def test_deduplication_removes_duplicates(sample_synthetic_bronze_df):
    """Ensure deduplication reduces 5 rows to 4 unique jobIds and prefers richer record."""
    df_dedup, dupes_removed = deduplicate_indian_jobs(sample_synthetic_bronze_df)
    assert dupes_removed == 1
    assert df_dedup.count() == 4

    # Verify ID_001 preserved the richer record (with companyId 'COMP_1')
    retained_001 = df_dedup.filter("jobId = 'ID_001'").collect()[0]
    assert retained_001["companyId"] == "COMP_1"
    assert retained_001["minimumSalary"] == "1200000"


def test_transform_indian_jobs_schema_and_values(sample_synthetic_bronze_df):
    """Verify full Silver transformation output columns and data types."""
    df_jobs, df_bridge = transform_indian_jobs(sample_synthetic_bronze_df)

    assert df_jobs.count() == 4

    # Test Record 1: Senior Data Engineer
    r1 = df_jobs.filter("job_id = 'ID_001'").collect()[0]
    assert r1["title_clean"] == "Senior Data Engineer"
    assert r1["salary_min"] == 1200000.0
    assert r1["salary_max"] == 1800000.0
    assert r1["salary_midpoint"] == 1500000.0
    assert r1["salary_currency"] == "INR"
    assert r1["salary_period"] == "ANNUAL"
    assert r1["salary_is_valid"] is True
    assert r1["experience_min_years"] == 3.0
    assert r1["experience_max_years"] == 6.0
    assert r1["seniority_tier"] == "MID"
    assert r1["freshness_tier"] == "FRESH_24H"
    assert r1["is_remote"] is False
    assert r1["has_skills"] is True
    assert set(r1["skills_normalized"]) == {"python", "pyspark", "aws"}

    # Test Record 2: Junior Analyst (Undisclosed salary, remote)
    r2 = df_jobs.filter("job_id = 'ID_002'").collect()[0]
    assert r2["salary_min"] is None
    assert r2["salary_max"] is None
    assert r2["salary_midpoint"] is None
    assert r2["salary_currency"] is None
    assert r2["salary_is_valid"] is True
    assert r2["seniority_tier"] == "ENTRY"
    assert r2["freshness_tier"] == "FRESH_24H"
    assert r2["is_remote"] is True

    # Test Record 3: Director (10+ yrs text exp, 30+ days ago)
    r3 = df_jobs.filter("job_id = 'ID_003'").collect()[0]
    assert r3["seniority_tier"] == "LEAD_EXECUTIVE"
    assert r3["freshness_tier"] == "STALE_30D_PLUS"
    assert r3["has_skills"] is False
    assert r3["skills_normalized"] == []

    # Test Record 4: Inverted salary (min > max) -> flagged invalid
    r4 = df_jobs.filter("job_id = 'ID_004'").collect()[0]
    assert r4["salary_is_valid"] is False
    assert r4["salary_min"] is None
    assert r4["freshness_tier"] == "MODERATE_30D"


def test_job_skills_bridge_integrity(sample_synthetic_bronze_df):
    """Verify bridge table contains distinct (job_id, skill) pairs and referential integrity."""
    df_jobs, df_bridge = transform_indian_jobs(sample_synthetic_bronze_df)

    bridge_rows = df_bridge.collect()
    assert len(bridge_rows) > 0

    # ID_001 had skills: Python, PySpark, AWS, python -> 3 distinct skills
    id_001_skills = [r["skill"] for r in bridge_rows if r["job_id"] == "ID_001"]
    assert set(id_001_skills) == {"python", "pyspark", "aws"}
    assert len(id_001_skills) == 3  # Deduplicated within job

    # ID_003 had None skills -> 0 rows in bridge
    id_003_skills = [r["skill"] for r in bridge_rows if r["job_id"] == "ID_003"]
    assert len(id_003_skills) == 0

    # Referential integrity: all job_ids in bridge must exist in jobs table
    job_ids = set(r["job_id"] for r in df_jobs.select("job_id").collect())
    bridge_job_ids = set(r["job_id"] for r in bridge_rows)
    assert bridge_job_ids.issubset(job_ids)
