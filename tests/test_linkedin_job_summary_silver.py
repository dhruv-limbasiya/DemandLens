"""Tests for LinkedIn Job Summary Silver Transformation (Phase 6D.3).

Verifies text cleaning, HTML stripping, entity unescaping, paragraph preservation,
technical token preservation, financial figures, null/empty handling, derived metrics,
truncation prevention, and Spark DataFrame transformation fidelity.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
from src.demandlens.silver.linkedin_summary_transformer import (
    clean_job_summary,
    transform_linkedin_job_summary,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture providing a lightweight, session-scoped local SparkSession."""
    configure_windows_environment()
    session = (
        SparkSession.builder.appName("TestLinkedInSummarySilver")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestSummaryTextCleansingRules:
    """Unit tests for clean_job_summary function rules."""

    def test_html_entities_decoding(self):
        """Verify HTML entities are properly unescaped to clean characters."""
        raw = "We are seeking a C&amp;B Analyst &quot;Senior&quot; &#39;Lead&#39; with 5+ yrs &lt;exp&gt; &nbsp;."
        clean, char_cnt, word_cnt, has_sum = clean_job_summary(raw)
        assert clean == "We are seeking a C&B Analyst \"Senior\" 'Lead' with 5+ yrs <exp>."
        assert char_cnt == len(clean)
        assert word_cnt == len(clean.split())
        assert has_sum is True

    def test_html_tags_removal(self):
        """Verify inline HTML tags (span, strong, em, b, a) are stripped."""
        raw = "<p>Welcome to <strong>Acme Corp</strong>! Click <a href='https://example.com'>here</a>.</p>"
        clean, char_cnt, word_cnt, has_sum = clean_job_summary(raw)
        assert "<strong>" not in clean
        assert "</a>" not in clean
        assert "<p>" not in clean
        assert clean == "Welcome to Acme Corp! Click here."

    def test_structural_html_elements_to_newlines(self):
        """Verify block tags (br, p, li, div, tr, h1) become proper linebreaks."""
        raw = "<h3>Responsibilities:</h3><ul><li>Develop in Python</li><li>Maintain SQL pipelines</li></ul>"
        clean, _, _, _ = clean_job_summary(raw)
        assert clean == "Responsibilities:\n\nDevelop in Python\n\nMaintain SQL pipelines"

    def test_paragraph_preservation_and_line_endings(self):
        """Verify CRLF/CR normalization and paragraph boundaries (double newlines) are retained."""
        raw = "Paragraph One.\r\n\r\n\r\nParagraph Two.\r\n\r\nParagraph Three."
        clean, _, _, _ = clean_job_summary(raw)
        assert clean == "Paragraph One.\n\nParagraph Two.\n\nParagraph Three."

    def test_whitespace_normalization(self):
        """Verify repeated spaces are collapsed while keeping clean line formatting."""
        raw = "   Senior    Data   Engineer   \n   Remote   Role   "
        clean, _, _, _ = clean_job_summary(raw)
        assert clean == "Senior Data Engineer\nRemote Role"

    def test_technical_symbols_preservation(self):
        """Verify essential programming languages and frameworks are preserved intact."""
        raw = "Requirements: C++, C#, .NET Core, Node.js, PL/SQL, SQL, CI/CD, React.js, TCP/IP."
        clean, _, _, _ = clean_job_summary(raw)
        for token in ["C++", "C#", ".NET Core", "Node.js", "PL/SQL", "SQL", "CI/CD", "React.js", "TCP/IP"]:
            assert token in clean, f"Token {token} was lost or corrupted!"

    def test_financial_figures_preservation(self):
        """Verify salary figures with currency symbols, commas, and hyphens are preserved."""
        raw = "Salary: $120,000 - $150,000 / yr. Bonus: €5,000 or £10k."
        clean, _, _, _ = clean_job_summary(raw)
        assert "$120,000 - $150,000 / yr." in clean
        assert "€5,000" in clean
        assert "£10k." in clean

    def test_null_empty_and_whitespace_only(self):
        """Verify null, empty, and whitespace-only text safely returns None and zero counts."""
        for val in [None, "", "   ", "\n\t\r\n"]:
            clean, char_cnt, word_cnt, has_sum = clean_job_summary(val)
            assert clean is None
            assert char_cnt == 0
            assert word_cnt == 0
            assert has_sum is False

    def test_long_description_no_truncation(self):
        """Verify long descriptions (15,000+ chars) are preserved completely without truncation."""
        chunk = "This is a comprehensive enterprise job description detailing technical requirements.\n\n"
        raw = chunk * 200  # ~17,400 chars
        clean, char_cnt, word_cnt, has_sum = clean_job_summary(raw)
        assert len(clean) == char_cnt
        assert char_cnt > 15000
        assert word_cnt > 1500
        assert has_sum is True


class TestSummaryDataFrameTransformation:
    """Integration test for transform_linkedin_job_summary Spark transformation."""

    def test_transform_dataframe_schema_and_values(self, spark: SparkSession):
        """Verify schema, derived metrics, and provenance on a synthetic Spark DataFrame."""
        schema = StructType([
            StructField("job_link", StringType(), False),
            StructField("job_summary", StringType(), True),
            StructField("_source_id", StringType(), True),
            StructField("_source_file", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
            StructField("_ingestion_run_id", StringType(), True),
        ])

        data = [
            (
                "https://linkedin.com/jobs/view/1001",
                "<p>Senior Software Engineer: C++, C#, .NET &amp; SQL.</p>",
                "linkedin_2024",
                "job_summary.csv",
                None,
                "bronze_run_1",
            ),
            (
                "https://linkedin.com/jobs/view/1002",
                "   ",  # Empty summary
                "linkedin_2024",
                "job_summary.csv",
                None,
                "bronze_run_1",
            ),
            (
                "https://linkedin.com/jobs/view/1003",
                None,  # Null summary
                "linkedin_2024",
                "job_summary.csv",
                None,
                "bronze_run_1",
            ),
            (
                "   ",  # Invalid empty job_link (must be filtered out)
                "Valid description but empty link.",
                "linkedin_2024",
                "job_summary.csv",
                None,
                "bronze_run_1",
            ),
        ]

        df_bronze = spark.createDataFrame(data, schema)
        df_silver = transform_linkedin_job_summary(df_bronze, silver_run_id="silver_test_run")

        rows = df_silver.collect()
        # Row with empty job_link must be filtered out -> exactly 3 rows remain
        assert len(rows) == 3

        # Record 1001
        r1001 = next(r for r in rows if r["job_link"].endswith("1001"))
        assert r1001["summary_clean"] == "Senior Software Engineer: C++, C#, .NET & SQL."
        assert r1001["character_count"] == len(r1001["summary_clean"])
        assert r1001["word_count"] == 8
        assert r1001["has_summary"] is True
        assert r1001["_silver_run_id"] == "silver_test_run"

        # Record 1002 (empty)
        r1002 = next(r for r in rows if r["job_link"].endswith("1002"))
        assert r1002["summary_clean"] is None
        assert r1002["character_count"] == 0
        assert r1002["word_count"] == 0
        assert r1002["has_summary"] is False

        # Record 1003 (null)
        r1003 = next(r for r in rows if r["job_link"].endswith("1003"))
        assert r1003["summary_clean"] is None
        assert r1003["character_count"] == 0
        assert r1003["word_count"] == 0
        assert r1003["has_summary"] is False
