"""DemandLens - Phase 5D: Comprehensive Validation Script for Indian Jobs Bronze Dataset.

Performs distributed validation on the fully ingested Indian Job Market Dataset
including full record comparison against the raw source CSV.
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
    print(" DemandLens — Phase 5D: Indian Jobs Bronze Validation & Full Record Audit")
    print("=" * 70)

    bronze_dir = PROJECT_ROOT / "data" / "bronze" / "indian_job_market_2025_2026" / "jobs"
    meta_dir = PROJECT_ROOT / "data" / "bronze" / "indian_job_market_2025_2026" / "_metadata"
    source_csv = PROJECT_ROOT / "data" / "source_csv" / "indian_job_market_2025_2026" / "extracted" / "indian-job-market-dataset-2025.csv"
    spark_tmp = PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp"

    print(f"[PATH] Bronze Dir : {bronze_dir}")
    print(f"[PATH] Meta Dir   : {meta_dir}")
    print(f"[PATH] Source CSV : {source_csv}")

    # 1. Inspect Parquet part files on disk
    part_files = sorted(bronze_dir.glob("part-*.parquet"))
    print(f"\n[DISK] Parquet Part Files count: {len(part_files)}")
    assert len(part_files) == 2, f"Expected 2 part files, found {len(part_files)}"

    total_bytes = 0
    total_rows_meta = 0
    for pf in part_files:
        meta = pq.ParquetFile(str(pf)).metadata
        sz_mb = pf.stat().st_size / (1024**2)
        total_bytes += pf.stat().st_size
        total_rows_meta += meta.num_rows
        comp = meta.row_group(0).column(0).compression
        print(f"  {pf.name}: {sz_mb:.2f} MB | {meta.num_rows:,} rows | Compression: {comp}")
        assert comp.upper() == "SNAPPY", f"Non-snappy compression: {comp}"

    print(f"[DISK] Sum of Parquet metadata rows: {total_rows_meta:,}")
    print(f"[DISK] Total Parquet size: {total_bytes / (1024**2):.2f} MB ({total_bytes:,} bytes)")
    assert total_rows_meta == 97929, f"Metadata row count mismatch: {total_rows_meta}"

    # 2. Check for leftover staging artifacts
    parent_dir = PROJECT_ROOT / "data" / "bronze" / "indian_job_market_2025_2026"
    staging_dirs = [p.name for p in parent_dir.iterdir() if p.name.startswith("_staging") or p.name.startswith("_backup")]
    print(f"[DISK] Leftover staging/backup directories: {staging_dirs}")
    assert len(staging_dirs) == 0, f"Found leftover staging directories: {staging_dirs}"

    # 3. PySpark Distributed Readback & Full Validation
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Phase5D-Verification")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
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

        assert bronze_count == 97929, f"Expected 97,929 rows, got {bronze_count:,}"

        # 17 Expected original columns
        expected_orig_cols = [
            'title', 'jobId', 'currency', 'jobUploaded', 'companyName',
            'tagsAndSkills', 'experience', 'salary', 'location', 'companyId',
            'ReviewsCount', 'AggregateRating', 'jobDescription', 'minimumSalary',
            'maximumSalary', 'minimumExperience', 'maximumExperience'
        ]
        for c in expected_orig_cols:
            assert c in columns, f"Missing original column: {c}"

        # 4 Expected metadata columns
        for m in BronzeIngestor.METADATA_COLUMNS:
            assert m in columns, f"Missing metadata column: {m}"

        # Audit metadata distinct values
        src_ids = [r["_source_id"] for r in df_bronze.select("_source_id").distinct().collect()]
        src_files = [r["_source_file"] for r in df_bronze.select("_source_file").distinct().collect()]
        run_ids = [r["_ingestion_run_id"] for r in df_bronze.select("_ingestion_run_id").distinct().collect()]

        print(f"\n[META] _source_id: {src_ids}")
        print(f"[META] _source_file: {src_files}")
        print(f"[META] _ingestion_run_id: {run_ids}")
        assert src_ids == ["indian_job_market_2025_2026"]
        assert src_files == ["indian-job-market-dataset-2025.csv"]
        assert len(run_ids) == 1

        # Check null counts across all columns in Bronze
        print("\n[AUDIT] Column null counts in Bronze:")
        bronze_nulls = {}
        for col in columns:
            cnt = df_bronze.filter(df_bronze[col].isNull()).count()
            pct = cnt / bronze_count * 100
            bronze_nulls[col] = cnt
            print(f"  {col:25s}: {cnt:7,d} nulls ({pct:6.2f}%)")

        for m in BronzeIngestor.METADATA_COLUMNS:
            assert bronze_nulls[m] == 0, f"Metadata column {m} has null values!"

        # Read raw CSV and compare row counts and null counts
        print("\n[AUDIT] Comparing against raw source CSV...")
        raw_df = spark.read.options(header="true", multiLine="true", escape='"').csv(str(source_csv))
        raw_count = raw_df.count()
        print(f"  Source raw row count: {raw_count:,}")
        assert raw_count == bronze_count, f"Source count {raw_count} != Bronze count {bronze_count}"

        # Compare null counts between source CSV and Bronze
        for col in expected_orig_cols:
            src_null = raw_df.filter(raw_df[col].isNull()).count()
            brz_null = bronze_nulls[col]
            assert src_null == brz_null, f"Null count mismatch on {col}: source={src_null}, bronze={brz_null}"
        print("  All 17 original column null counts match source CSV exactly!")

        # FULL RECORD EQUIVALENCE CHECK (100% record comparison)
        print("\n[AUDIT] FULL RECORD COMPARISON: Executing distributed set difference (exceptAll)...")
        t_diff0 = time.time()
        # Source minus Bronze (on the 17 original columns)
        diff_src_minus_brz = raw_df.select(*expected_orig_cols).exceptAll(df_bronze.select(*expected_orig_cols)).count()
        # Bronze minus Source
        diff_brz_minus_src = df_bronze.select(*expected_orig_cols).exceptAll(raw_df.select(*expected_orig_cols)).count()
        t_diff1 = time.time()
        print(f"  Source - Bronze differences: {diff_src_minus_brz} (computed in {t_diff1 - t_diff0:.2f} s)")
        print(f"  Bronze - Source differences: {diff_brz_minus_src}")
        assert diff_src_minus_brz == 0, f"Found {diff_src_minus_brz} records in source missing/altered in Bronze!"
        assert diff_brz_minus_src == 0, f"Found {diff_brz_minus_src} extra records in Bronze!"
        print("  FULL RECORD EQUIVALENCE CONFIRMED: 100% of all 97,929 rows are identical!")

        # Verify all 3 prior LinkedIn Bronze datasets remain completely intact
        print("\n[AUDIT] Verifying prior LinkedIn Bronze datasets remain intact...")
        linkedin_base = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024"
        postings_df = spark.read.parquet(str(linkedin_base / "job_postings"))
        skills_df = spark.read.parquet(str(linkedin_base / "job_skills"))
        summary_df = spark.read.parquet(str(linkedin_base / "job_summary"))

        postings_cnt = postings_df.count()
        skills_cnt = skills_df.count()
        summary_cnt = summary_df.count()

        print(f"  job_postings count: {postings_cnt:,} (expected: 1,348,454)")
        print(f"  job_skills count  : {skills_cnt:,} (expected: 1,296,381)")
        print(f"  job_summary count : {summary_cnt:,} (expected: 1,297,332)")

        assert postings_cnt == 1348454, f"job_postings count altered: {postings_cnt}"
        assert skills_cnt == 1296381, f"job_skills count altered: {skills_cnt}"
        assert summary_cnt == 1297332, f"job_summary count altered: {summary_cnt}"

        print("\n" + "=" * 70)
        print(" PHASE 5D VALIDATION: ALL CHECKS PASSED (100% FULL FIDELITY)")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
