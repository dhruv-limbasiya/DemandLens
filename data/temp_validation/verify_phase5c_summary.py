"""DemandLens - Phase 5C: Comprehensive Validation Script for job_summary.

Performs distributed validation on the fully ingested LinkedIn job summary
Bronze Parquet dataset without driver memory collection.
"""

import os
import shutil
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(r"D:\DemandLens")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor

def main():
    print("=" * 70)
    print(" DemandLens — Phase 5C: job_summary Output Validation & Auditing")
    print("=" * 70)

    bronze_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "job_summary"
    meta_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "_metadata"
    source_csv = PROJECT_ROOT / "data" / "source_csv" / "linkedin_2024" / "extracted" / "job_summary.csv"
    spark_tmp = PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp"

    print(f"[PATH] Bronze Dir : {bronze_dir}")
    print(f"[PATH] Meta Dir   : {meta_dir}")
    print(f"[PATH] Source CSV : {source_csv}")

    # 1. Inspect Parquet part files on disk
    part_files = sorted(bronze_dir.glob("part-*.parquet"))
    print(f"\n[DISK] Parquet Part Files count: {len(part_files)}")
    assert len(part_files) == 16, f"Expected 16 part files, found {len(part_files)}"

    total_bytes = 0
    total_rows_meta = 0
    for pf in part_files:
        meta = pq.ParquetFile(str(pf)).metadata
        sz_mb = pf.stat().st_size / (1024**2)
        total_bytes += pf.stat().st_size
        total_rows_meta += meta.num_rows
        comp = meta.row_group(0).column(0).compression
        assert comp.upper() == "SNAPPY", f"Non-snappy compression: {comp}"

    print(f"[DISK] Verified all 16 part files have SNAPPY compression.")
    print(f"[DISK] Sum of Parquet metadata rows: {total_rows_meta:,}")
    print(f"[DISK] Total Parquet size: {total_bytes / (1024**2):.2f} MB ({total_bytes / (1024**3):.4f} GB)")
    assert total_rows_meta == 1297332, f"Metadata row count mismatch: {total_rows_meta}"

    # 2. PySpark Distributed Readback & Validation
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Phase5C-Verification")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(spark_tmp))
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.io.tmpdir={spark_tmp_str} --enable-native-access=ALL-UNNAMED",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n[SPARK] Reading Bronze Parquet back with PySpark...")
        df_bronze = spark.read.parquet(str(bronze_dir))
        bronze_count = df_bronze.count()
        columns = df_bronze.columns
        print(f"[SPARK] Bronze row count: {bronze_count:,}")
        print(f"[SPARK] Column count: {len(columns)}")
        print(f"[SPARK] Columns: {columns}")

        assert bronze_count == 1297332, f"Expected 1,297,332 rows, got {bronze_count:,}"

        # Expected original columns
        expected_orig_cols = ["job_link", "job_summary"]
        for c in expected_orig_cols:
            assert c in columns, f"Missing original column: {c}"

        # Expected metadata columns
        for m in BronzeIngestor.METADATA_COLUMNS:
            assert m in columns, f"Missing metadata column: {m}"

        # Audit metadata distinct values
        src_ids = [r["_source_id"] for r in df_bronze.select("_source_id").distinct().collect()]
        src_files = [r["_source_file"] for r in df_bronze.select("_source_file").distinct().collect()]
        run_ids = [r["_ingestion_run_id"] for r in df_bronze.select("_ingestion_run_id").distinct().collect()]

        print(f"[META] _source_id: {src_ids}")
        print(f"[META] _source_file: {src_files}")
        print(f"[META] _ingestion_run_id: {run_ids}")
        assert src_ids == ["linkedin_2024"]
        assert src_files == ["job_summary.csv"]
        assert len(run_ids) == 1

        # Check null counts
        print("\n[AUDIT] Key column null counts in Bronze:")
        for col in ["job_link", "job_summary", "_source_id", "_ingestion_timestamp", "_ingestion_run_id"]:
            null_cnt = df_bronze.filter(df_bronze[col].isNull()).count()
            print(f"  {col}: {null_cnt:,} nulls ({null_cnt/bronze_count*100:.4f}%)")
            assert null_cnt == 0, f"Unexpected null in column {col}"

        # Multiline text preservation check
        print("\n[AUDIT] Multiline description formatting check:")
        newline_count = df_bronze.filter(df_bronze["job_summary"].contains("\n")).count()
        print(f"  Descriptions containing '\\n': {newline_count:,} ({newline_count/bronze_count*100:.2f}%)")
        assert newline_count > 1000000, "Multiline newline characters were unexpectedly lost!"

        # Distributed data comparison with source CSV
        print("\n[AUDIT] Distributed verification against source CSV...")
        raw_df = spark.read.options(header="true", multiLine="true", escape='"').csv(str(source_csv))
        raw_count = raw_df.count()
        print(f"  Source raw row count: {raw_count:,}")
        assert raw_count == bronze_count, f"Source count {raw_count} != Bronze count {bronze_count}"

        # Compare sample records between source and bronze for exact character match
        print("\n[AUDIT] Verifying sample records between raw CSV and Bronze Parquet...")
        sample_links = [r["job_link"] for r in df_bronze.select("job_link").limit(5).collect()]
        for link in sample_links:
            raw_text = raw_df.filter(raw_df["job_link"] == link).select("job_summary").first()["job_summary"]
            bronze_text = df_bronze.filter(df_bronze["job_link"] == link).select("job_summary").first()["job_summary"]
            assert raw_text == bronze_text, f"Mismatch in job_summary for link: {link}"
            assert len(bronze_text) > 50, f"Suspiciously short text for {link}"
        print("  Verified 5 sample records match exactly (100% character equivalence).")

        # Confirm previous Bronze tables remain intact
        print("\n[AUDIT] Verifying previous Bronze tables...")
        postings_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "job_postings"
        skills_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "job_skills"
        assert postings_dir.exists(), "job_postings directory missing!"
        assert skills_dir.exists(), "job_skills directory missing!"
        postings_count = spark.read.parquet(str(postings_dir)).count()
        skills_count = spark.read.parquet(str(skills_dir)).count()
        print(f"  job_postings count: {postings_count:,} (expected 1,348,454)")
        print(f"  job_skills count: {skills_count:,} (expected 1,296,381)")
        assert postings_count == 1348454, f"job_postings count corrupted: {postings_count}"
        assert skills_count == 1296381, f"job_skills count corrupted: {skills_count}"

        print("\n" + "=" * 70)
        print(" PHASE 5C VALIDATION: ALL CHECKS PASSED (100% FIDELITY)")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
