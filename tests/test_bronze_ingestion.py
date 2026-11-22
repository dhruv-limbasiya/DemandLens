"""DemandLens - Unit Tests for Bronze Ingestion Engine."""

import json
import shutil
import tempfile
from pathlib import Path
import pytest

from pyspark.sql import SparkSession
from src.demandlens.ingestion.bronze_ingestor import (
    BronzeIngestor,
    SourceConfig,
    TableConfig,
    load_all_source_configs,
)
from src.demandlens.ingestion.dataset_inspector import configure_windows_environment


@pytest.fixture(scope="session")
def spark_session():
    """Create a lightweight PySpark session for testing."""
    configure_windows_environment()
    spark = (
        SparkSession.builder.appName("DemandLens-TestBronze")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_project_dir():
    """Create an isolated temporary project workspace on D: drive."""
    tmp = Path(__file__).resolve().parent / ".tmp_test_workspace"
    if tmp.exists():
        shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


def test_source_configs_loadable():
    """Verify that existing source configurations load and validate cleanly."""
    configs = load_all_source_configs()
    assert len(configs) >= 2, "Expected at least linkedin_2024 and indian_job_market_2025_2026 configs"

    ids = [c.source_id for c in configs]
    assert "linkedin_2024" in ids
    assert "indian_job_market_2025_2026" in ids

    linkedin = next(c for c in configs if c.source_id == "linkedin_2024")
    assert len(linkedin.tables) == 3
    table_names = [t.table_name for t in linkedin.tables]
    assert "job_postings" in table_names
    assert "job_skills" in table_names
    assert "job_summary" in table_names

    india = next(c for c in configs if c.source_id == "indian_job_market_2025_2026")
    assert len(india.tables) == 1
    assert india.tables[0].table_name == "jobs"


def test_bronze_ingest_synthetic_csv(spark_session, temp_project_dir):
    """Test full ingestion flow with synthetic data verifying schema, metadata and Parquet output."""
    extracted_dir = temp_project_dir / "data" / "source_csv" / "test_source" / "extracted"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    test_csv = extracted_dir / "sample_jobs.csv"

    csv_content = (
        "job_id,title,company,salary\n"
        "101,Data Engineer,Acme Corp,500000\n"
        "102,Software Engineer,Beta Tech,700000\n"
        "103,ML Engineer,Gamma AI,900000\n"
        "104,Cloud Architect,Delta Cloud,1200000\n"
    )
    test_csv.write_text(csv_content, encoding="utf-8")

    source_cfg = SourceConfig(
        source_id="test_source",
        source_name="Test Source Dataset",
        platform="TestPlatform",
        kaggle_identifier=None,
        dataset_url=None,
        license="MIT",
        geographic_focus="Global",
        snapshot_period="2026",
        primary_key="job_id",
        paths={
            "extracted_dir": "data/source_csv/test_source/extracted",
            "bronze_dir": "data/bronze/test_source",
        },
        tables=[
            TableConfig(
                table_name="jobs",
                input_file="sample_jobs.csv",
                output_subdir="jobs",
                format="csv",
                read_options={"header": "true"},
                target_partitions=1,
            )
        ],
    )

    ingestor = BronzeIngestor(spark_session, project_root=temp_project_dir)
    res = ingestor.ingest_table(source_cfg, source_cfg.tables[0], overwrite=False)

    assert res.ingestion_status == "SUCCESS"
    assert res.validation_status == "PASSED"
    assert res.source_row_count == 4
    assert res.output_row_count == 4

    # Verify original source columns preserved
    expected_orig_cols = ["job_id", "title", "company", "salary"]
    for c in expected_orig_cols:
        assert c in res.output_column_names

    # Verify ingestion metadata columns added
    for m in BronzeIngestor.METADATA_COLUMNS:
        assert m in res.output_column_names

    # Verify Parquet on disk
    bronze_jobs_dir = temp_project_dir / "data" / "bronze" / "test_source" / "jobs"
    assert bronze_jobs_dir.is_dir()
    parquet_files = list(bronze_jobs_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # Verify source file was untouched
    assert test_csv.read_text(encoding="utf-8") == csv_content

    # Verify run metadata file was written
    meta_dir = temp_project_dir / "data" / "bronze" / "test_source" / "_metadata"
    assert meta_dir.is_dir()
    meta_files = list(meta_dir.glob("*.json"))
    assert len(meta_files) == 1


def test_bronze_idempotency_and_overwrite(spark_session, temp_project_dir):
    """Test idempotent re-run (skipping when existing) and safe overwrite via staging."""
    extracted_dir = temp_project_dir / "data" / "source_csv" / "test_source" / "extracted"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    test_csv = extracted_dir / "items.csv"
    test_csv.write_text("item_id,name\n1,Alpha\n2,Beta\n", encoding="utf-8")

    source_cfg = SourceConfig(
        source_id="test_source",
        source_name="Test Source",
        platform="Test",
        kaggle_identifier=None,
        dataset_url=None,
        license=None,
        geographic_focus=None,
        snapshot_period=None,
        primary_key="item_id",
        paths={
            "extracted_dir": "data/source_csv/test_source/extracted",
            "bronze_dir": "data/bronze/test_source",
        },
        tables=[
            TableConfig(
                table_name="items",
                input_file="items.csv",
                output_subdir="items",
                format="csv",
                read_options={"header": "true"},
            )
        ],
    )

    ingestor = BronzeIngestor(spark_session, project_root=temp_project_dir)

    # Initial run
    res1 = ingestor.ingest_table(source_cfg, source_cfg.tables[0], overwrite=False)
    assert res1.ingestion_status == "SUCCESS"

    # Second run without overwrite -> must skip
    res2 = ingestor.ingest_table(source_cfg, source_cfg.tables[0], overwrite=False)
    assert res2.ingestion_status == "SKIPPED_ALREADY_EXISTS"
    assert res2.validation_status == "PASSED"

    # Third run with overwrite=True -> must re-ingest safely via staging
    res3 = ingestor.ingest_table(source_cfg, source_cfg.tables[0], overwrite=True)
    assert res3.ingestion_status == "SUCCESS"
    assert res3.validation_status == "PASSED"


def test_bronze_missing_file_failure(spark_session, temp_project_dir):
    """Test clear failure reporting when input file does not exist."""
    source_cfg = SourceConfig(
        source_id="missing_source",
        source_name="Missing Source",
        platform="Test",
        kaggle_identifier=None,
        dataset_url=None,
        license=None,
        geographic_focus=None,
        snapshot_period=None,
        primary_key=None,
        paths={
            "extracted_dir": "data/source_csv/missing/extracted",
            "bronze_dir": "data/bronze/missing",
        },
        tables=[
            TableConfig(
                table_name="nonexistent",
                input_file="does_not_exist.csv",
                output_subdir="nonexistent",
            )
        ],
    )

    ingestor = BronzeIngestor(spark_session, project_root=temp_project_dir)
    res = ingestor.ingest_table(source_cfg, source_cfg.tables[0])
    assert res.ingestion_status == "FAILED"
    assert "does not exist" in res.error_details
