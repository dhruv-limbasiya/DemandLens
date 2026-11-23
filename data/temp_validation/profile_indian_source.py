"""DemandLens - Phase 5D: Pre-Ingestion Source Profiling for Indian Job Market Dataset."""

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
    print(" DemandLens — Phase 5D: Indian Dataset Source Profiling")
    print("=" * 70)

    csv_path = PROJECT_ROOT / "data" / "source_csv" / "indian_job_market_2025_2026" / "extracted" / "indian-job-market-dataset-2025.csv"
    spark_tmp = PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    print(f"Source file: {csv_path}")
    print(f"File size: {csv_path.stat().st_size:,} bytes ({csv_path.stat().st_size / (1024**2):.2f} MB)")
    assert csv_path.exists(), "Source file missing!"

    free_gb = shutil.disk_usage("D:\\").free / (1024**3)
    print(f"D: drive available free space: {free_gb:.2f} GB")

    spark = (
        SparkSession.builder.appName("DemandLens-Phase5D-Profile")
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
        t0 = time.time()
        print("\nLoading CSV with read options: header=true, multiLine=true, escape=\"")
        df = spark.read.options(header="true", multiLine="true", escape='"').csv(str(csv_path))
        
        cols = df.columns
        print(f"Column count: {len(cols)}")
        print(f"Columns: {cols}")
        print("\nSchema:")
        for c, t in df.dtypes:
            print(f"  - {c}: {t}")

        initial_partitions = df.rdd.getNumPartitions()
        print(f"\nInitial Spark partitions: {initial_partitions}")

        print("Computing row count...")
        total_rows = df.count()
        t1 = time.time()
        print(f"Total row count: {total_rows:,} (computed in {t1 - t0:.2f} s)")

        print("\nNull count audit across all columns:")
        for col_name in cols:
            null_cnt = df.filter(df[col_name].isNull()).count()
            pct = (null_cnt / total_rows * 100) if total_rows > 0 else 0
            print(f"  {col_name:25s}: {null_cnt:7,d} nulls ({pct:6.2f}%)")

        print("\n" + "=" * 70)
        print(" PROFILING COMPLETED SUCCESSFULLY")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
