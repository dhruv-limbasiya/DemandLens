"""DemandLens - Phase 5A: Comprehensive Output Validation Script.

Performs distributed validation on the fully ingested LinkedIn job postings
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
from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor

def main():
    print("=" * 70)
    print(" DemandLens — Phase 5A: Full Output Validation & Auditing")
    print("=" * 70)

    bronze_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "job_postings"
    meta_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "_metadata"
    source_csv = PROJECT_ROOT / "data" / "source_csv" / "linkedin_2024" / "extracted" / "linkedin_job_postings.csv"
    spark_tmp = PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp"

    print(f"[PATH] Bronze Dir : {bronze_dir}")
    print(f"[PATH] Meta Dir   : {meta_dir}")

    # 1. Inspect files on disk
    part_files = sorted(bronze_dir.glob("part-*.parquet"))
    print(f"\n[DISK] Parquet Part Files count: {len(part_files)}")
    total_bytes = 0
    for pf in part_files:
        sz_mb = pf.stat().st_size / (1024**2)
        total_bytes += pf.stat().st_size
        meta = pq.ParquetFile(str(pf)).metadata
        comp = meta.row_group(0).column(0).compression
        print(f"  {pf.name}: {sz_mb:.2f} MB | {meta.num_rows:,} rows | {meta.num_row_groups} row groups | Compression: {comp}")
        assert comp.upper() == "SNAPPY", f"Non-snappy compression: {comp}"

    print(f"[DISK] Total Parquet size: {total_bytes / (1024**2):.2f} MB ({total_bytes:,} bytes)")

    # 2. Inspect run metadata JSON
    meta_files = sorted(meta_dir.glob("job_postings_*.json"))
    print(f"\n[META] Ingestion Run Metadata JSON files: {len(meta_files)}")
    for mf in meta_files:
        print(f"  Found metadata record: {mf.name}")

    # 3. Disk Space on D:
    total_d, used_d, free_d = shutil.disk_usage("D:\\")
    print(f"[DISK] D: Drive Free Space: {free_d / (1024**3):.2f} GB (Used: {used_d / (1024**3):.2f} GB, Total: {total_d / (1024**3):.2f} GB)")

    # 4. PySpark Distributed Readback & Validation
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Phase5A-Verification")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(spark_tmp))
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.io.tmpdir={spark_tmp_str} --enable-native-access=ALL-UNNAMED",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n[SPARK] Reading Bronze Parquet back with PySpark...")
        df = spark.read.parquet(str(bronze_dir))
        bronze_count = df.count()
        columns = df.columns
        print(f"[SPARK] Bronze row count: {bronze_count:,}")
        print(f"[SPARK] Column count: {len(columns)}")
        print(f"[SPARK] Columns: {columns}")

        assert bronze_count == 1348454, f"Expected 1,348,454 rows, got {bronze_count:,}"

        # Expected original columns
        expected_orig_cols = [
            "job_link", "last_processed_time", "got_summary", "got_ner", "is_being_worked",
            "job_title", "company", "job_location", "first_seen", "search_city",
            "search_country", "search_position", "job_level", "job_type"
        ]
        for c in expected_orig_cols:
            assert c in columns, f"Missing original column: {c}"

        # Expected metadata columns
        for m in BronzeIngestor.METADATA_COLUMNS:
            assert m in columns, f"Missing metadata column: {m}"

        # Audit metadata distinct values (distributed count)
        src_ids = [r["_source_id"] for r in df.select("_source_id").distinct().collect()]
        src_files = [r["_source_file"] for r in df.select("_source_file").distinct().collect()]
        run_ids = [r["_ingestion_run_id"] for r in df.select("_ingestion_run_id").distinct().collect()]

        print(f"[META] _source_id: {src_ids}")
        print(f"[META] _source_file: {src_files}")
        print(f"[META] _ingestion_run_id: {run_ids}")
        assert src_ids == ["linkedin_2024"]
        assert src_files == ["linkedin_job_postings.csv"]
        assert len(run_ids) == 1

        # Check null count profile for key columns
        print("\n[AUDIT] Key column null counts in Bronze:")
        for col in ["job_link", "job_title", "company", "job_location", "_source_id", "_ingestion_timestamp"]:
            null_cnt = df.filter(df[col].isNull()).count()
            print(f"  {col}: {null_cnt:,} nulls ({null_cnt/bronze_count*100:.3f}%)")
            if col in ["job_link", "job_title", "_source_id", "_ingestion_timestamp"]:
                assert null_cnt == 0, f"Unexpected nulls in critical column {col}"

        print("\n" + "=" * 70)
        print(" PHASE 5A VALIDATION: ALL CHECKS PASSED (100% SUCCESS)")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
