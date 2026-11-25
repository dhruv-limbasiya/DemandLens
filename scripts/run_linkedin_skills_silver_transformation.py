"""DemandLens - Run LinkedIn Job Skills Silver Transformation CLI.

Executes the Phase 6D.2 Silver transformation pipeline for LinkedIn job_skills,
producing the normalized (job_link, skill) bridge table in Parquet with Snappy compression.

Usage:
    python scripts/run_linkedin_skills_silver_transformation.py --overwrite --partitions 8
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
from src.demandlens.silver.linkedin_skills_transformer import LinkedInSkillsSilverPipeline

logger = logging.getLogger("demandlens.run_linkedin_skills_silver")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DemandLens Phase 6D.2 LinkedIn Skills Silver Pipeline CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Safely overwrite existing Silver bridge output via staging promotion.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="Number of target Parquet partition files to write.",
    )
    return parser


def get_spark_session() -> SparkSession:
    """Initialize an optimized local SparkSession with staging on D:."""
    configure_windows_environment()
    spark_tmp = (PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-LinkedIn-Skills-Silver")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
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
    parser = build_parser()
    args = parser.parse_args()

    print("=" * 70)
    print(" DemandLens: Phase 6D.2 LinkedIn Job Skills Silver Transformation")
    print(f" Overwrite: {args.overwrite} | Partitions: {args.partitions}")
    print("=" * 70)

    spark = get_spark_session()
    try:
        pipeline = LinkedInSkillsSilverPipeline(spark=spark, project_root=PROJECT_ROOT)
        result = pipeline.run(overwrite=args.overwrite, target_partitions=args.partitions)

        print("\n" + "=" * 70)
        print(" PHASE 6D.2 LINKEDIN SKILLS SILVER PIPELINE COMPLETE")
        print(f" Status:                  {result.get('status')}")
        print(f" Run ID:                  {result.get('silver_run_id')}")
        print(f" Bronze Rows In:          {result.get('bronze_row_count'):,}")
        print(f" Silver Bridge Rows Out:  {result.get('silver_bridge_row_count'):,}")
        print(f" Partitions:              {result.get('partitions')}")
        print(f" Duration:                {result.get('duration_seconds')}s")
        print(f" Referential Integrity:   {result.get('referential_integrity')}")
        print("=" * 70)

    finally:
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
