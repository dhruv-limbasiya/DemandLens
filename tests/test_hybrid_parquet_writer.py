"""DemandLens - Synthetic Integration Tests for Bounded-Memory Hybrid Parquet Writer."""

import shutil
from datetime import datetime, timezone
from pathlib import Path
import pytest
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.demandlens.ingestion.bronze_ingestor import (
    BronzeIngestor,
    SourceConfig,
    TableConfig,
)
from src.demandlens.ingestion.dataset_inspector import configure_windows_environment


@pytest.fixture(scope="session")
def spark_session():
    """Create a lightweight PySpark session configured for D: drive execution."""
    configure_windows_environment()
    project_root = Path(__file__).resolve().parent.parent
    spark_tmp = (project_root / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-TestHybridWriter")
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
def temp_output_dir():
    """Create an isolated temporary output directory on D: drive."""
    out_dir = Path(__file__).resolve().parent.parent / "data" / "temp_validation" / "test_writer_workspace"
    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    yield out_dir
    shutil.rmtree(out_dir, ignore_errors=True)


def test_basic_parquet_write_and_readback(spark_session, temp_output_dir):
    """Test basic synthetic write and Spark readback preserving types and values."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("salary", DoubleType(), True),
    ])
    data = [
        (1, "Data Engineer", 85000.0),
        (2, "Software Architect", 120000.0),
        (3, "ML Researcher", 95000.0),
    ]
    df = spark_session.createDataFrame(data, schema=schema).coalesce(1)
    target_dir = temp_output_dir / "basic_output"

    written_count = BronzeIngestor.write_dataframe_to_parquet_pyarrow(
        df=df,
        output_dir=target_dir,
        batch_size=1000,
        compression="snappy",
    )
    assert written_count == 3

    # Verify Parquet files exist
    part_files = list(target_dir.glob("part-*.parquet"))
    assert len(part_files) == 1

    # Read back with Spark and validate
    read_df = spark_session.read.parquet(str(target_dir))
    assert read_df.count() == 3
    assert set(read_df.columns) == {"id", "title", "salary"}
    rows = {r["id"]: r for r in read_df.collect()}
    assert rows[1]["title"] == "Data Engineer"
    assert rows[2]["salary"] == 120000.0


def test_multi_batch_and_row_groups(spark_session, temp_output_dir):
    """Test bounded batch writing across multiple partitions generating row groups."""
    total_rows = 1200
    batch_size = 300
    target_dir = temp_output_dir / "batched_output"

    # Create 2 partitions with 1200 rows total (600 per partition)
    df = spark_session.range(0, total_rows, 1, numPartitions=2).toDF("row_id")

    written_count = BronzeIngestor.write_dataframe_to_parquet_pyarrow(
        df=df,
        output_dir=target_dir,
        batch_size=batch_size,
        compression="snappy",
    )
    assert written_count == total_rows

    part_files = sorted(target_dir.glob("part-*.parquet"))
    assert len(part_files) == 2

    # Verify each part file has multiple row groups (600 rows / 300 batch_size = 2 row groups)
    for pf in part_files:
        meta = pq.ParquetFile(str(pf)).metadata
        assert meta.num_row_groups == 2
        assert meta.num_rows == 600

    # Verify Spark read-back integrity
    read_df = spark_session.read.parquet(str(target_dir))
    assert read_df.count() == total_rows


def test_schema_and_null_preservation(spark_session, temp_output_dir):
    """Test preservation of diverse types and null values."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("created_at", TimestampType(), True),
    ])
    ts = datetime(2026, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    data = [
        (101, "Alpha", 4.5, ts),
        (102, None, None, None),  # All nulls except id
        (None, "Gamma", 3.8, None),  # Null id and timestamp
    ]
    df = spark_session.createDataFrame(data, schema=schema).coalesce(1)
    target_dir = temp_output_dir / "null_output"

    written = BronzeIngestor.write_dataframe_to_parquet_pyarrow(df, target_dir)
    assert written == 3

    read_df = spark_session.read.parquet(str(target_dir))
    assert read_df.count() == 3
    assert read_df.filter(read_df.name.isNull()).count() == 1
    assert read_df.filter(read_df.rating.isNull()).count() == 1
    assert read_df.filter(read_df.created_at.isNull()).count() == 2
    assert read_df.filter(read_df.id.isNull()).count() == 1


def test_empty_dataframe_behavior(spark_session, temp_output_dir):
    """Test safe handling of empty DataFrames (0 rows) producing valid Parquet schema."""
    schema = StructType([
        StructField("code", StringType(), False),
        StructField("value", IntegerType(), True),
    ])
    df = spark_session.createDataFrame([], schema=schema).coalesce(1)
    target_dir = temp_output_dir / "empty_output"

    written = BronzeIngestor.write_dataframe_to_parquet_pyarrow(df, target_dir)
    assert written == 0

    part_files = list(target_dir.glob("part-*.parquet"))
    assert len(part_files) == 1

    # Verify Spark reads back cleanly with 0 rows
    read_df = spark_session.read.parquet(str(target_dir))
    assert read_df.count() == 0
    assert read_df.columns == ["code", "value"]


def test_snappy_compression_enforced(spark_session, temp_output_dir):
    """Test that Snappy compression is actively applied to Parquet columns."""
    df = spark_session.range(0, 100, 1, 1).toDF("num")
    target_dir = temp_output_dir / "compressed_output"

    BronzeIngestor.write_dataframe_to_parquet_pyarrow(df, target_dir, compression="snappy")
    part_file = next(target_dir.glob("part-*.parquet"))

    meta = pq.ParquetFile(str(part_file)).metadata
    col_meta = meta.row_group(0).column(0)
    assert col_meta.compression.upper() == "SNAPPY"


def test_metadata_preservation(spark_session, temp_output_dir):
    """Test preservation of Bronze provenance metadata columns."""
    import pyspark.sql.functions as F

    raw_df = spark_session.createDataFrame([(1, "Dev"), (2, "QA")], ["id", "role"])
    bronze_df = (
        raw_df.withColumn("_source_id", F.lit("test_src"))
        .withColumn("_source_file", F.lit("test.csv"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_run_id", F.lit("run_12345"))
    )
    target_dir = temp_output_dir / "metadata_output"

    BronzeIngestor.write_dataframe_to_parquet_pyarrow(bronze_df, target_dir)
    read_df = spark_session.read.parquet(str(target_dir))

    for m in BronzeIngestor.METADATA_COLUMNS:
        assert m in read_df.columns

    rows = read_df.collect()
    assert all(r["_source_id"] == "test_src" for r in rows)
    assert all(r["_ingestion_run_id"] == "run_12345" for r in rows)
