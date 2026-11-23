"""DemandLens - Run Indian Job Market Silver Transformation CLI.

Executes the Silver transformation pipeline for the Indian Job Market Dataset 2025-2026:
  - Input:  data/bronze/indian_job_market_2025_2026/jobs (97,929 rows)
  - Output: data/silver/indian_job_market_2025_2026/jobs (97,679 rows deduplicated)
  - Bridge: data/silver/indian_job_market_2025_2026/job_skills_bridge
"""

import argparse
import logging
import os
import shutil
import sys
import time
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
from src.demandlens.silver.indian_jobs_transformer import IndianSilverPipeline
from pyspark.sql import SparkSession

logger = logging.getLogger("demandlens.run_indian_silver")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def get_spark_session() -> SparkSession:
    """Initialize an optimized local SparkSession on D: drive."""
    configure_windows_environment()
    spark_tmp = (PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Indian-Silver-Pipeline")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "8")
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
    return spark


def main():
    parser = argparse.ArgumentParser(description="DemandLens Indian Silver Pipeline")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing Silver datasets")
    args = parser.parse_args()

    print("=" * 70)
    print(" DemandLens: Phase 6C Indian Job Market Silver Transformation")
    print(f" Overwrite: {args.overwrite}")
    print("=" * 70)

    spark = get_spark_session()
    try:
        pipeline = IndianSilverPipeline(spark, project_root=PROJECT_ROOT)
        result = pipeline.run(overwrite=args.overwrite)
        print("\n" + "=" * 70)
        print(" PHASE 6C INDIAN SILVER PIPELINE COMPLETE")
        print(f" Status: {result.get('status')}")
        print(f" Run ID: {result.get('silver_run_id')}")
        print(f" Bronze Rows In:       {result.get('bronze_row_count'):,}")
        print(f" Duplicates Removed:   {result.get('duplicates_removed'):,}")
        print(f" Silver Jobs Rows Out: {result.get('silver_jobs_row_count'):,}")
        print(f" Bridge Skills Rows:   {result.get('silver_bridge_row_count'):,}")
        print(f" Duration:             {result.get('duration_seconds'):.1f}s")
        print("=" * 70)
    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
