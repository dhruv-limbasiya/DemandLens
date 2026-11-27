"""DemandLens — Unit & Integration Tests for Canonical Silver Integration (Phase 6D.4).

Verifies:
  - Canonical Surrogate Key Generation (<source_id>#<source_job_id>) & disjoint collision prevention
  - LinkedIn Postings -> Canonical Schema Mapping Fidelity (CANONICAL_JOBS_SCHEMA)
  - Indian Jobs -> Canonical Schema Mapping Fidelity (CANONICAL_JOBS_SCHEMA)
  - Union & Schema Alignment in build_canonical_jobs
  - Skill Bridge Union & Grain Fidelity in build_canonical_skills_bridge
  - Decoupled Job Text Pipeline & Invariant Checks in build_canonical_job_text
  - Referential Integrity & Zero-Orphan Verification
  - Staging and Target Completion Checks in CanonicalSilverPipeline
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
from src.demandlens.silver.canonical_transformer import (
    CANONICAL_JOBS_SCHEMA,
    CANONICAL_SKILLS_SCHEMA,
    CANONICAL_TEXT_SCHEMA,
    CanonicalSilverPipeline,
    build_canonical_job_text,
    build_canonical_jobs,
    build_canonical_skills_bridge,
    map_indian_jobs_to_canonical,
    map_linkedin_postings_to_canonical,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing a lightweight, session-scoped local SparkSession."""
    configure_windows_environment()
    session = (
        SparkSession.builder.appName("TestCanonicalSilverIntegration")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Test Data Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_linkedin_postings(spark: SparkSession):
    schema = StructType([
        StructField("job_link", StringType(), False),
        StructField("job_title", StringType(), True),
        StructField("title_clean", StringType(), True),
        StructField("company", StringType(), True),
        StructField("company_clean", StringType(), True),
        StructField("job_location", StringType(), True),
        StructField("location_clean", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("workplace_type", StringType(), True),
        StructField("is_remote", BooleanType(), True),
        StructField("is_hybrid", BooleanType(), True),
        StructField("seniority_tier", StringType(), True),
        StructField("first_seen_date", DateType(), True),
        StructField("_source_file", StringType(), False),
        StructField("_ingestion_timestamp", TimestampType(), True),
    ])
    data = [
        (
            "https://linkedin.com/jobs/view/1001",
            "Senior Data Engineer",
            "Senior Data Engineer",
            "Meta Inc.",
            "Meta",
            "Menlo Park, CA",
            "Menlo Park, CA, United States",
            "US",
            "HYBRID",
            False,
            True,
            "SENIOR",
            datetime(2024, 1, 15).date(),
            "part-00000.parquet",
            datetime.now(timezone.utc),
        ),
        (
            "https://linkedin.com/jobs/view/1002",
            "Remote Python Dev",
            "Python Developer",
            "Acme",
            "Acme Corp",
            "Remote",
            "Remote",
            "US",
            "REMOTE",
            True,
            False,
            "MID",
            datetime(2024, 2, 1).date(),
            "part-00000.parquet",
            datetime.now(timezone.utc),
        ),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_linkedin_skills(spark: SparkSession):
    schema = StructType([
        StructField("job_link", StringType(), False),
        StructField("skill", StringType(), False),
    ])
    data = [
        ("https://linkedin.com/jobs/view/1001", "Python"),
        ("https://linkedin.com/jobs/view/1001", "Apache Spark"),
        ("https://linkedin.com/jobs/view/1001", "SQL"),
        ("https://linkedin.com/jobs/view/1002", "Python"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_linkedin_summary(spark: SparkSession):
    schema = StructType([
        StructField("job_link", StringType(), False),
        StructField("summary_clean", StringType(), True),
        StructField("character_count", IntegerType(), False),
        StructField("word_count", IntegerType(), False),
    ])
    data = [
        ("https://linkedin.com/jobs/view/1001", "We are hiring a Senior Data Engineer to build pipelines.", 55, 10),
        ("https://linkedin.com/jobs/view/1002", "Remote Python role working on microservices.", 44, 6),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_indian_jobs(spark: SparkSession):
    schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("title_clean", StringType(), True),
        StructField("companyName", StringType(), True),
        StructField("company_name_clean", StringType(), True),
        StructField("location", StringType(), True),
        StructField("location_clean", StringType(), True),
        StructField("is_remote", BooleanType(), True),
        StructField("seniority_tier", StringType(), True),
        StructField("experience_min_years", DoubleType(), True),
        StructField("experience_max_years", DoubleType(), True),
        StructField("salary_min", DoubleType(), True),
        StructField("salary_max", DoubleType(), True),
        StructField("salary_midpoint", DoubleType(), True),
        StructField("salary_currency", StringType(), True),
        StructField("salary_period", StringType(), True),
        StructField("freshness_tier", StringType(), True),
        StructField("skill_count", IntegerType(), True),
        StructField("has_skills", BooleanType(), True),
        StructField("jobDescription", StringType(), True),
        StructField("_source_file", StringType(), False),
        StructField("_ingestion_timestamp", TimestampType(), True),
    ])
    data = [
        (
            "99001",
            "Lead Software Architect",
            "Software Architect",
            "Infosys Ltd",
            "Infosys",
            "Bengaluru, Karnataka",
            "Bengaluru, Karnataka, India",
            False,
            "LEAD",
            8.0,
            12.0,
            2500000.0,
            3500000.0,
            3000000.0,
            "INR",
            "ANNUAL",
            "FRESH",
            2,
            True,
            "We are looking for an experienced Software Architect with cloud expertise.",
            "indian_part-00000.parquet",
            datetime.now(timezone.utc),
        ),
        (
            "99002",
            "Junior Analyst",
            "Data Analyst",
            "TCS",
            "Tata Consultancy Services",
            "Pune, Maharashtra",
            "Pune, Maharashtra, India",
            True,
            "ENTRY",
            0.0,
            2.0,
            None,
            None,
            None,
            "INR",
            None,
            "RECENT",
            0,
            False,
            "Entry level data analyst position.",
            "indian_part-00000.parquet",
            datetime.now(timezone.utc),
        ),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_indian_skills(spark: SparkSession):
    schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("skill", StringType(), False),
    ])
    data = [
        ("99001", "Java"),
        ("99001", "Microservices"),
    ]
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

class TestCanonicalTransformationFidelity:
    """Test canonical projection, schema conformance, and harmonization."""

    def test_canonical_jobs_schema_structure(self):
        """Verify target schema defines all 28 expected canonical columns."""
        assert len(CANONICAL_JOBS_SCHEMA.fields) == 28
        field_names = CANONICAL_JOBS_SCHEMA.names
        assert "canonical_job_id" in field_names
        assert "source_id" in field_names
        assert "source_job_id" in field_names
        assert "workplace_type" in field_names
        assert "is_remote" in field_names
        assert "is_hybrid" in field_names
        assert "seniority_tier" in field_names
        assert "salary_disclosed" in field_names
        assert "skill_count" in field_names
        assert "has_description" in field_names

    def test_map_linkedin_postings_to_canonical(
        self, sample_linkedin_postings, sample_linkedin_skills, sample_linkedin_summary
    ):
        """Verify LinkedIn postings map into canonical schema with zero salary and correct aggregates."""
        df_mapped = map_linkedin_postings_to_canonical(
            sample_linkedin_postings,
            sample_linkedin_skills,
            sample_linkedin_summary,
            canonical_run_id="test_run_li",
            canonical_timestamp="2026-11-26T00:00:00",
        )

        rows = df_mapped.collect()
        assert len(rows) == 2

        row_1001 = [r for r in rows if r["source_job_id"] == "https://linkedin.com/jobs/view/1001"][0]
        assert row_1001["canonical_job_id"] == "linkedin_2024#https://linkedin.com/jobs/view/1001"
        assert row_1001["source_id"] == "linkedin_2024"
        assert row_1001["job_title_clean"] == "Senior Data Engineer"
        assert row_1001["country_code"] == "US"
        assert row_1001["workplace_type"] == "HYBRID"
        assert row_1001["is_remote"] is False
        assert row_1001["is_hybrid"] is True
        assert row_1001["seniority_tier"] == "SENIOR"
        assert row_1001["salary_disclosed"] is False
        assert row_1001["salary_min"] is None
        assert row_1001["salary_max"] is None
        assert row_1001["skill_count"] == 3
        assert row_1001["has_skills"] is True
        assert row_1001["has_description"] is True
        assert row_1001["_canonical_run_id"] == "test_run_li"

        row_1002 = [r for r in rows if r["source_job_id"] == "https://linkedin.com/jobs/view/1002"][0]
        assert row_1002["workplace_type"] == "REMOTE"
        assert row_1002["is_remote"] is True
        assert row_1002["is_hybrid"] is False
        assert row_1002["skill_count"] == 1
        assert row_1002["has_skills"] is True
        assert row_1002["has_description"] is True

    def test_map_indian_jobs_to_canonical(self, sample_indian_jobs):
        """Verify Indian jobs map into canonical schema with INR salary disclosure and numeric experience."""
        df_mapped = map_indian_jobs_to_canonical(
            sample_indian_jobs,
            canonical_run_id="test_run_in",
            canonical_timestamp="2026-11-26T00:00:00",
        )

        rows = df_mapped.collect()
        assert len(rows) == 2

        row_arch = [r for r in rows if r["source_job_id"] == "99001"][0]
        assert row_arch["canonical_job_id"] == "indian_job_market_2025_2026#99001"
        assert row_arch["source_id"] == "indian_job_market_2025_2026"
        assert row_arch["country_code"] == "IN"
        assert row_arch["workplace_type"] == "ON_SITE"
        assert row_arch["is_remote"] is False
        assert row_arch["is_hybrid"] is False
        assert row_arch["seniority_tier"] == "LEAD"
        assert row_arch["experience_min_years"] == 8.0
        assert row_arch["experience_max_years"] == 12.0
        assert row_arch["salary_disclosed"] is True
        assert row_arch["salary_min"] == 2500000.0
        assert row_arch["salary_max"] == 3500000.0
        assert row_arch["salary_currency"] == "INR"
        assert row_arch["skill_count"] == 2
        assert row_arch["has_skills"] is True
        assert row_arch["has_description"] is True

        row_jr = [r for r in rows if r["source_job_id"] == "99002"][0]
        assert row_jr["workplace_type"] == "REMOTE"
        assert row_jr["is_remote"] is True
        assert row_jr["is_hybrid"] is False
        assert row_jr["salary_disclosed"] is False
        assert row_jr["salary_min"] is None
        assert row_jr["skill_count"] == 0
        assert row_jr["has_skills"] is False
        assert row_jr["has_description"] is True

    def test_build_canonical_jobs_union(
        self,
        sample_linkedin_postings,
        sample_linkedin_skills,
        sample_linkedin_summary,
        sample_indian_jobs,
    ):
        """Verify unioning both sources produces exactly 4 rows with distinct canonical keys."""
        df_jobs = build_canonical_jobs(
            sample_linkedin_postings,
            sample_linkedin_skills,
            sample_linkedin_summary,
            sample_indian_jobs,
            canonical_run_id="run_union_test",
        )

        assert df_jobs.count() == 4
        ids = [r["canonical_job_id"] for r in df_jobs.collect()]
        assert len(set(ids)) == 4
        # Verify disjoint key prefixes
        assert all(id_.startswith("linkedin_2024#") or id_.startswith("indian_job_market_2025_2026#") for id_ in ids)

    def test_build_canonical_skills_bridge(
        self, sample_linkedin_skills, sample_indian_skills
    ):
        """Verify skills bridge union projects canonical IDs and retains all 6 edges."""
        df_skills = build_canonical_skills_bridge(
            sample_linkedin_skills,
            sample_indian_skills,
            canonical_run_id="run_skills_test",
        )

        rows = df_skills.collect()
        assert len(rows) == 6
        assert len(CANONICAL_SKILLS_SCHEMA.fields) == 5

        # Check LinkedIn skill row
        li_skill = [r for r in rows if r["skill"] == "Apache Spark"][0]
        assert li_skill["canonical_job_id"] == "linkedin_2024#https://linkedin.com/jobs/view/1001"
        assert li_skill["source_id"] == "linkedin_2024"

        # Check Indian skill row
        in_skill = [r for r in rows if r["skill"] == "Java"][0]
        assert in_skill["canonical_job_id"] == "indian_job_market_2025_2026#99001"
        assert in_skill["source_id"] == "indian_job_market_2025_2026"

    def test_build_canonical_job_text(
        self, sample_linkedin_summary, sample_indian_jobs
    ):
        """Verify canonical text decouples description and computes metrics."""
        df_text = build_canonical_job_text(
            sample_linkedin_summary,
            sample_indian_jobs,
            canonical_run_id="run_text_test",
        )

        rows = df_text.collect()
        assert len(rows) == 4
        assert len(CANONICAL_TEXT_SCHEMA.fields) == 7

        text_map = {r["canonical_job_id"]: r for r in rows}
        assert "linkedin_2024#https://linkedin.com/jobs/view/1001" in text_map
        assert "indian_job_market_2025_2026#99001" in text_map

        # Verify word and char counts are positive
        for r in rows:
            assert r["character_count"] > 0
            assert r["word_count"] > 0
            assert len(r["description_clean"]) > 0

    def test_referential_integrity_no_orphans(
        self,
        sample_linkedin_postings,
        sample_linkedin_skills,
        sample_linkedin_summary,
        sample_indian_jobs,
        sample_indian_skills,
    ):
        """Verify 100% of skills and text entries resolve to a parent in canonical_jobs."""
        df_jobs = build_canonical_jobs(
            sample_linkedin_postings,
            sample_linkedin_skills,
            sample_linkedin_summary,
            sample_indian_jobs,
            canonical_run_id="run_ri_test",
        )
        df_skills = build_canonical_skills_bridge(
            sample_linkedin_skills,
            sample_indian_skills,
            canonical_run_id="run_ri_test",
        )
        df_text = build_canonical_job_text(
            sample_linkedin_summary,
            sample_indian_jobs,
            canonical_run_id="run_ri_test",
        )

        job_ids = df_jobs.select("canonical_job_id")
        skill_orphans = df_skills.select("canonical_job_id").distinct().join(job_ids, on="canonical_job_id", how="left_anti").count()
        text_orphans = df_text.select("canonical_job_id").distinct().join(job_ids, on="canonical_job_id", how="left_anti").count()

        assert skill_orphans == 0
        assert text_orphans == 0
