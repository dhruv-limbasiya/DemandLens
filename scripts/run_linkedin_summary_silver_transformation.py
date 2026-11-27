"""DemandLens - Run LinkedIn Job Summary Silver Transformation CLI.

Executes the Phase 6D.3 Silver transformation pipeline for LinkedIn job_summary,
producing the cleaned plain-text summary dimension in Parquet with Snappy compression.

Usage:
    python scripts/run_linkedin_summary_silver_transformation.py [--overwrite] [--partitions 16] [--batch-size 10000]
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure project root in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
from src.demandlens.silver.linkedin_summary_transformer import LinkedInSummarySilverPipeline

logger = logging.getLogger("demandlens.run_linkedin_summary_silver")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DemandLens Phase 6D.3 LinkedIn Summary Silver Pipeline CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Safely overwrite existing Silver summary output via staging promotion.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=16,
        help="Number of target Parquet partition files to write.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="PyArrow streaming batch size per partition worker.",
    )
    return parser


def get_spark_session() -> SparkSession:
    """Initialize an optimized local SparkSession with staging strictly on D: drive."""
    configure_windows_environment()
    spark_tmp = (PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    # Safe bounded concurrency (local[3]) to prevent Windows socket buffer overruns
    spark = (
        SparkSession.builder.appName("DemandLens-LinkedIn-JobSummary-Silver")
        .master("local[3]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.default.parallelism", "3")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(spark_tmp))
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.io.tmpdir={spark_tmp_str} -XX:MaxDirectMemorySize=4g -XX:+UseG1GC --enable-native-access=ALL-UNNAMED",
        )
        .config(
            "spark.executor.extraJavaOptions",
            f"-Djava.io.tmpdir={spark_tmp_str} -XX:MaxDirectMemorySize=4g -XX:+UseG1GC --enable-native-access=ALL-UNNAMED",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    parser = build_parser()
    args = parser.parse_args()

    print("=" * 70)
    print(" DemandLens: Phase 6D.3 LinkedIn Job Summary Silver Transformation")
    print(f" Overwrite: {args.overwrite} | Partitions: {args.partitions} | Batch Size: {args.batch_size}")
    print("=" * 70)

    spark = get_spark_session()
    try:
        pipeline = LinkedInSummarySilverPipeline(spark=spark, project_root=PROJECT_ROOT)
        result = pipeline.run(
            overwrite=args.overwrite,
            target_partitions=args.partitions,
            batch_size=args.batch_size,
        )

        print("\n" + "=" * 70)
        print(" PHASE 6D.3 LINKEDIN JOB SUMMARY SILVER PIPELINE COMPLETE")
        print(f" Status:                  {result.get('status')}")
        print(f" Run ID:                  {result.get('silver_run_id')}")
        print(f" Bronze Rows In:          {result.get('bronze_row_count'):,}")
        print(f" Silver Rows Out:         {result.get('silver_row_count'):,}")
        print(f" Partitions:              {result.get('partitions')}")
        print(f" Duration:                {result.get('duration_seconds')}s")
        print(f" Referential Integrity:   {result.get('referential_integrity')}")
        print("=" * 70)

    finally:
        print("\nStopping SparkSession...")
        spark.stop()
        print("SparkSession stopped.")


if __name__ == "__main__":
    main()
