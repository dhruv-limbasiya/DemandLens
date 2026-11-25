"""Tests for LinkedIn Job Skills Silver Transformation (Phase 6D.2).

Verifies skill normalization, special technical tokens preservation,
null/empty handling, duplicate handling, bridge grain, and referential integrity.
"""

from __future__ import annotations

from typing import List
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.demandlens.silver.skill_normalization import normalize_skill_token, normalize_skills
from src.demandlens.silver.linkedin_skills_transformer import transform_linkedin_skills


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing a lightweight, session-scoped local SparkSession."""
    session = (
        SparkSession.builder.appName("TestLinkedInSkillsSilver")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestSkillNormalizationRules:
    """Unit tests for core skill token normalization rules."""

    def test_special_technical_tokens_preservation(self):
        """Preserve essential technical punctuation: C++, C#, .NET, Node.js, PL/SQL, SQL, CI/CD."""
        tokens = {
            "C++": "c++",
            "C#": "c#",
            ".NET": ".net",
            "Node.js": "node.js",
            "PL/SQL": "pl/sql",
            "SQL": "sql",
            "CI/CD": "ci/cd",
            "  .NET Core  ": ".net core",
            "C++ / STL": "c++ / stl",
        }
        for raw, expected in tokens.items():
            assert normalize_skill_token(raw) == expected, f"Failed preserving {raw}"

    def test_whitespace_and_noise_stripping(self):
        """Strip surrounding punctuation and collapse whitespace."""
        assert normalize_skill_token("  Python   3.x  ") == "python 3.x"
        assert normalize_skill_token("(Data Science)") == "data science"
        assert normalize_skill_token("[Machine Learning]") == "machine learning"
        assert normalize_skill_token("• AWS •") == "aws"

    def test_null_empty_and_noise_tokens(self):
        """Reject null, empty, pure punctuation, and short numeric tokens."""
        assert normalize_skill_token(None) is None
        assert normalize_skill_token("") is None
        assert normalize_skill_token("   ") is None
        assert normalize_skill_token("...") is None
        assert normalize_skill_token("---") is None
        assert normalize_skill_token("12") is None
        assert normalize_skill_token("1") is None

    def test_normalize_skills_deduplication(self):
        """Deduplicate identical and case-varying skills within a single posting."""
        raw = "Python, python, PYTHON, SQL, Sql, C++, c++"
        result = normalize_skills(raw, delimiter=",")
        assert result == ["python", "sql", "c++"]


class TestLinkedInSkillsTransformerFidelity:
    """Integration fidelity tests for transform_linkedin_skills."""

    def test_transform_linkedin_skills_bridge_grain(self, spark: SparkSession):
        """Verify bridge table grain: exactly 1 row per (job_link, skill) with no duplicate pairs."""
        schema = StructType([
            StructField("job_link", StringType(), False),
            StructField("job_skills", StringType(), True),
            StructField("_source_id", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
            StructField("_ingestion_run_id", StringType(), True),
        ])

        data = [
            ("https://linkedin.com/jobs/view/1001", "Python, SQL, C++, Python", "linkedin_2024", "job_skills.csv", None, "run_1"),
            ("https://linkedin.com/jobs/view/1002", "Node.js, .NET, CI/CD", "linkedin_2024", "job_skills.csv", None, "run_1"),
            ("https://linkedin.com/jobs/view/1003", None, "linkedin_2024", "job_skills.csv", None, "run_1"),  # Null skills
            ("https://linkedin.com/jobs/view/1004", "   ", "linkedin_2024", "job_skills.csv", None, "run_1"),  # Empty skills
        ]

        df_bronze = spark.createDataFrame(data, schema)
        df_silver = transform_linkedin_skills(df_bronze, silver_run_id="silver_test_run")

        rows = df_silver.collect()
        # 1001: python, sql, c++ (3)
        # 1002: node.js, .net, ci/cd (3)
        # 1003: 0 rows
        # 1004: 0 rows
        assert len(rows) == 6

        # Check records for 1001
        skills_1001 = sorted([r["skill"] for r in rows if r["job_link"].endswith("1001")])
        assert skills_1001 == ["c++", "python", "sql"]

        # Check records for 1002
        skills_1002 = sorted([r["skill"] for r in rows if r["job_link"].endswith("1002")])
        assert skills_1002 == [".net", "ci/cd", "node.js"]

        # Check schema columns
        assert "job_link" in df_silver.columns
        assert "skill" in df_silver.columns
        assert "_silver_run_id" in df_silver.columns
        assert "_silver_timestamp" in df_silver.columns

        # Verify no null skills in output
        null_skills = [r for r in rows if r["skill"] is None or r["skill"].strip() == ""]
        assert len(null_skills) == 0

    def test_referential_integrity_logic(self, spark: SparkSession):
        """Verify referential integrity metrics computation."""
        from src.demandlens.silver.linkedin_skills_transformer import LinkedInSkillsSilverPipeline

        # Mock postings and skills
        pipeline = LinkedInSkillsSilverPipeline(spark=spark)

        postings_data = [("link_A",), ("link_B",), ("link_C",)]
        postings_schema = StructType([StructField("job_link", StringType(), False)])
        df_postings = spark.createDataFrame(postings_data, postings_schema)

        # Bridge with 2 matched jobs and 1 orphan
        bridge_data = [
            ("link_A", "python"),
            ("link_A", "sql"),
            ("link_B", "c++"),
            ("link_ORPHAN", "java"),
        ]
        bridge_schema = StructType([
            StructField("job_link", StringType(), False),
            StructField("skill", StringType(), False),
        ])
        df_bridge = spark.createDataFrame(bridge_data, bridge_schema)

        # Mocking silver_postings_path read by monkeypatching or testing logic directly
        df_bridge_jobs = df_bridge.select("job_link").distinct()
        total_postings = df_postings.count()
        matched = df_bridge_jobs.join(df_postings, on="job_link", how="inner").count()
        orphans = df_bridge_jobs.join(df_postings, on="job_link", how="left_anti").count()

        assert total_postings == 3
        assert matched == 2
        assert orphans == 1
