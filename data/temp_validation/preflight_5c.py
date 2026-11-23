"""DemandLens - Phase 5C: Preflight Verification Script for job_summary.csv

Read-only inspection of job_summary.csv:
- Measures exact row count with PySpark
- Checks partition layout
- Measures null count profile
- Measures execution time
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

from pyspark.sql import SparkSession

def main():
    print("=" * 70)
    print(" DemandLens — Phase 5C Preflight Source Inspection")
    print("=" * 70)

    csv_path = PROJECT_ROOT / "data" / "source_csv" / "linkedin_2024" / "extracted" / "job_summary.csv"
    spark_tmp = PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    print(f"Source file: {csv_path}")
    print(f"File size: {csv_path.stat().st_size:,} bytes ({csv_path.stat().st_size / (1024**3):.4f} GB)")

    target_dir = PROJECT_ROOT / "data" / "bronze" / "linkedin_2024" / "job_summary"
    print(f"Target directory: {target_dir}")
    print(f"Target exists: {target_dir.exists()}")

    free_gb = shutil.disk_usage("D:\\").free / (1024**3)
    print(f"D: drive available free space: {free_gb:.2f} GB")

    spark = (
        SparkSession.builder.appName("DemandLens-Phase5C-Preflight")
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
        t0 = time.time()
        print("\nLoading CSV with read options: header=true, multiLine=true, escape=\"")
        df = spark.read.options(header="true", multiLine="true", escape='"').csv(str(csv_path))
        
        print(f"Columns: {df.columns}")
        print("Schema:")
        for col_name, col_type in df.dtypes:
            print(f"  - {col_name}: {col_type}")

        initial_partitions = df.rdd.getNumPartitions()
        print(f"Initial Spark partitions: {initial_partitions}")

        print("\nComputing row count...")
        total_rows = df.count()
        t1 = time.time()
        print(f"Total row count: {total_rows:,} (computed in {t1 - t0:.2f} s)")

        print("Auditing null counts...")
        null_link = df.filter(df["job_link"].isNull()).count()
        null_summary = df.filter(df["job_summary"].isNull()).count()
        print(f"Null job_link count: {null_link:,} ({null_link/total_rows*100:.4f}%)")
        print(f"Null job_summary count: {null_summary:,} ({null_summary/total_rows*100:.4f}%)")

        print("\nSummary statistics:")
        print(f"  Rows: {total_rows:,}")
        print(f"  Size per row (approx): {csv_path.stat().st_size / total_rows:.1f} bytes")

        print("\n" + "=" * 70)
        print(" PREFLIGHT INSPECTION COMPLETE")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
