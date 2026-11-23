"""DemandLens - Phase 4: Small Real-Data Bronze Validation Script.

Validates the hybrid PyArrow Parquet writer on a 5,000-row slice of the actual
LinkedIn 2024 job postings dataset in an isolated validation directory.
"""

import os
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(r"D:\DemandLens")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

import pyarrow.parquet as pq
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor

def main():
    print("=" * 70)
    print(" DemandLens — Phase 4: Small Real-Data Bronze Validation")
    print("=" * 70)

    # 1. Paths verification
    source_csv_path = PROJECT_ROOT / "data" / "source_csv" / "linkedin_2024" / "extracted" / "linkedin_job_postings.csv"
    target_output_dir = PROJECT_ROOT / "data" / "temp_validation" / "phase4_linkedin_postings"
    spark_tmp = PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp"

    print(f"[PATH] Source CSV        : {source_csv_path} (Exists: {source_csv_path.exists()})")
    print(f"[PATH] Target Output Dir : {target_output_dir}")
    print(f"[PATH] Spark Temp Dir    : {spark_tmp}")

    if not source_csv_path.exists():
        raise FileNotFoundError(f"Source file missing: {source_csv_path}")

    if target_output_dir.exists():
        raise FileExistsError(f"Target directory already exists: {target_output_dir}. Refusing to overwrite.")

    # 2. Spark Session Initialization
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Phase4-Validation")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
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

    t_start = time.time()

    try:
        # 3. Read Source CSV with exact configuration
        print("\n[STEP 1/5] Reading raw LinkedIn job postings CSV...")
        raw_df = spark.read.options(
            header="true",
            multiLine="true",
            escape='"',
        ).csv(str(source_csv_path))

        source_columns = list(raw_df.columns)
        print(f"  Source schema detected with {len(source_columns)} columns: {source_columns}")

        # 4. Select exactly 5,000 rows
        print("\n[STEP 2/5] Sampling exactly 5,000 records...")
        sample_size = 5000
        # Using limit(5000) and caching the sample so both write and validation compare exact same rows
        sample_df = raw_df.limit(sample_size).repartition(2).cache()
        actual_sample_count = sample_df.count()
        print(f"  Sampled rows: {actual_sample_count:,} across {sample_df.rdd.getNumPartitions()} partitions")
        assert actual_sample_count == sample_size, f"Expected {sample_size}, got {actual_sample_count}"

        # 5. Add Bronze provenance metadata
        print("\n[STEP 3/5] Applying standard Bronze metadata columns...")
        run_id = "phase4_validation_run_001"
        bronze_sample_df = (
            sample_df
            .withColumn("_source_id", F.lit("linkedin_2024"))
            .withColumn("_source_file", F.lit("linkedin_job_postings.csv"))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_ingestion_run_id", F.lit(run_id))
        )
        bronze_columns = list(bronze_sample_df.columns)
        print(f"  Bronze DataFrame schema has {len(bronze_columns)} columns (original: {len(source_columns)}, metadata: 4)")

        # 6. Write via Hybrid PyArrow Writer
        print("\n[STEP 4/5] Writing to isolated validation directory via PyArrow...")
        t_write_start = time.time()
        written_count = BronzeIngestor.write_dataframe_to_parquet_pyarrow(
            df=bronze_sample_df,
            output_dir=target_output_dir,
            batch_size=2000,
            compression="snappy",
        )
        write_duration = time.time() - t_write_start
        print(f"  Writing complete in {write_duration:.2f} seconds. Reported written rows: {written_count:,}")

        # 7. Comprehensive Validation of Output
        print("\n[STEP 5/5] Executing comprehensive output validation...")
        
        # Check files on disk
        part_files = sorted(target_output_dir.glob("part-*.parquet"))
        print(f"  Parquet files on disk: {len(part_files)}")
        for pf in part_files:
            sz_kb = pf.stat().st_size / 1024
            meta = pq.ParquetFile(str(pf)).metadata
            comp = meta.row_group(0).column(0).compression
            print(f"    {pf.name}: {sz_kb:.1f} KB | {meta.num_rows} rows | {meta.num_row_groups} row groups | Compression: {comp}")
            assert comp.upper() == "SNAPPY", f"Expected SNAPPY, got {comp}"

        total_bytes = sum(f.stat().st_size for f in part_files)
        print(f"  Total Parquet size: {total_bytes / 1024:.2f} KB ({total_bytes / (1024**2):.3f} MB)")

        # Read back with Spark
        read_back_df = spark.read.parquet(str(target_output_dir)).cache()
        read_back_count = read_back_df.count()
        print(f"  Spark read-back count: {read_back_count:,}")
        assert read_back_count == sample_size, f"Row count mismatch: expected {sample_size}, got {read_back_count}"

        # Schema & Columns Check
        assert list(read_back_df.columns) == bronze_columns, "Columns mismatch"
        for orig in source_columns:
            assert orig in read_back_df.columns, f"Original column missing: {orig}"
        for meta_col in BronzeIngestor.METADATA_COLUMNS:
            assert meta_col in read_back_df.columns, f"Metadata column missing: {meta_col}"

        # Metadata Values Verification
        meta_rows = read_back_df.select("_source_id", "_source_file", "_ingestion_run_id").distinct().collect()
        assert len(meta_rows) == 1, f"Expected 1 metadata permutation, got {len(meta_rows)}"
        assert meta_rows[0]["_source_id"] == "linkedin_2024"
        assert meta_rows[0]["_source_file"] == "linkedin_job_postings.csv"
        assert meta_rows[0]["_ingestion_run_id"] == run_id
        print("  [PASSED] Ingestion metadata values verified.")

        # Bounded Data Fidelity Check (Distributed subtract)
        print("  Comparing source vs output data values via distributed set difference...")
        src_clean = sample_df.select(*source_columns)
        out_clean = read_back_df.select(*source_columns)

        diff1 = src_clean.subtract(out_clean).count()
        diff2 = out_clean.subtract(src_clean).count()
        print(f"  Fidelity subtract diffs: src - out = {diff1}, out - src = {diff2}")
        assert diff1 == 0 and diff2 == 0, f"Data values difference detected: diff1={diff1}, diff2={diff2}"
        print("  [PASSED] 100% data fidelity confirmed. Zero row/value differences.")

        # Null Counts Comparison across all 14 columns
        print("  Verifying null counts consistency across all columns...")
        for col in source_columns:
            src_nulls = src_clean.filter(src_clean[col].isNull()).count()
            out_nulls = out_clean.filter(out_clean[col].isNull()).count()
            assert src_nulls == out_nulls, f"Null count mismatch on '{col}': source={src_nulls}, out={out_nulls}"
        print("  [PASSED] All null counts match source data exactly.")

        total_duration = time.time() - t_start
        print("\n" + "=" * 70)
        print(f" PHASE 4 VALIDATION: ALL CHECKS PASSED ({total_duration:.2f}s total)")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
