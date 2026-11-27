"""DemandLens — Run Canonical Silver Integration Pipeline CLI.

Executes the Phase 6D.4 Canonical Silver integration pipeline, harmonizing:
  - LinkedIn 2024 (job_postings, job_skills_bridge, job_summary)
  - Indian Job Market 2025-2026 (jobs, job_skills_bridge)
into the unified canonical Silver layer:
  - data/silver/canonical/jobs
  - data/silver/canonical/job_skills_bridge
  - data/silver/canonical/job_text
  - data/silver/canonical/_metadata/

Usage:
    python scripts/run_canonical_silver_transformation.py [--overwrite] [--jobs-partitions 8] [--skills-partitions 16] [--text-partitions 22] [--batch-size 10000]
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
from src.demandlens.silver.canonical_transformer import CanonicalSilverPipeline

logger = logging.getLogger("demandlens.run_canonical_silver")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DemandLens Phase 6D.4 Canonical Silver Integration CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Safely overwrite existing canonical Silver tables via staging promotion.",
    )
    parser.add_argument(
        "--jobs-partitions",
        type=int,
        default=8,
        help="Number of target Parquet partition files for canonical_jobs.",
    )
    parser.add_argument(
        "--skills-partitions",
        type=int,
        default=16,
        help="Number of target Parquet partition files for canonical_job_skills_bridge.",
    )
    parser.add_argument(
        "--text-partitions",
        type=int,
        default=22,
        help="Number of target Parquet partition files for canonical_job_text.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="PyArrow streaming batch size per partition worker.",
    )
    return parser


def get_spark_session() -> SparkSession:
    """Initialize an optimized local SparkSession with staging strictly on D: drive."""
    configure_windows_environment()
    spark_tmp = (PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    spark_tmp_str = str(spark_tmp).replace("\\", "/")

    spark = (
        SparkSession.builder.appName("DemandLens-Canonical-Silver-Integration")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
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

    print("=" * 75)
    print(" DemandLens: Phase 6D.4 Canonical Silver Integration Pipeline")
    print(f" Overwrite: {args.overwrite}")
    print(f" Partitions -> Jobs: {args.jobs_partitions} | Skills: {args.skills_partitions} | Text: {args.text_partitions}")
    print(f" Batch Size: {args.batch_size}")
    print("=" * 75)

    spark = get_spark_session()
    try:
        pipeline = CanonicalSilverPipeline(spark=spark, project_root=PROJECT_ROOT)
        result = pipeline.run(
            overwrite=args.overwrite,
            jobs_partitions=args.jobs_partitions,
            skills_partitions=args.skills_partitions,
            text_partitions=args.text_partitions,
            batch_size=args.batch_size,
        )

        print("\n" + "=" * 75)
        print(" PHASE 6D.4 CANONICAL SILVER INTEGRATION COMPLETE")
        print(f" Status:                {result.get('status')}")
        print(f" Run ID:                {result.get('canonical_run_id')}")
        print(f" Duration:              {result.get('duration_seconds')}s")
        print(" Generated Tables:")
        for tbl, info in result.get("tables", {}).items():
            print(f"   - {tbl:20s}: {info['row_count']:,} rows, {info['partitions']} files, {info['bytes'] / (1024*1024):.2f} MB")
        print(" Source Counts:")
        for src, count in result.get("source_counts", {}).items():
            print(f"   - {src:20s}: {count:,}")
        print(" Referential Integrity:")
        ri = result.get("referential_integrity", {})
        print(f"   - Orphan-free:         {ri.get('orphan_free')}")
        print(f"   - Skills orphans:      {ri.get('skills_orphans')}")
        print(f"   - Text orphans:        {ri.get('text_orphans')}")
        print("=" * 75)

    finally:
        print("\nStopping SparkSession...")
        spark.stop()
        print("SparkSession stopped.")


if __name__ == "__main__":
    main()
