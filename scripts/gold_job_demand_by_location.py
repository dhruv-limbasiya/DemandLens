"""DemandLens — Phase 7C.2: Gold Job Demand by Location Aggregation.

Grain: (source_id, country_code, location_clean)
Source:
  - data/silver/canonical/jobs
Output:
  - data/gold/job_demand_by_location
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.demandlens.gold.gold_utils import (
    get_gold_spark_session,
    utc_now_iso,
    write_and_promote_gold,
)


def run_aggregation_location(
    spark: SparkSession, jobs_path: str, min_postings: int = 5
) -> DataFrame:
    """Aggregate canonical jobs by (source_id, country_code, location_clean)."""
    jobs_df = spark.read.parquet(jobs_path).select(
        "source_id", "country_code", "location_clean", "workplace_type"
    )

    country_totals = jobs_df.groupBy("source_id", "country_code").agg(
        F.count("*").alias("_country_total")
    )

    loc_clean = jobs_df.filter(
        F.col("location_clean").isNotNull() & (F.trim(F.col("location_clean")) != "")
    ).withColumn("location_clean", F.trim(F.col("location_clean")))

    agg_df = (
        loc_clean.groupBy("source_id", "country_code", "location_clean")
        .agg(
            F.count("*").alias("posting_count"),
            F.sum(F.when(F.col("workplace_type") == "REMOTE", 1).otherwise(0)).alias(
                "remote_posting_count"
            ),
            F.sum(F.when(F.col("workplace_type") == "HYBRID", 1).otherwise(0)).alias(
                "hybrid_posting_count"
            ),
            F.sum(F.when(F.col("workplace_type") == "ON_SITE", 1).otherwise(0)).alias(
                "onsite_posting_count"
            ),
        )
        .filter(F.col("posting_count") >= min_postings)
    )

    gold_ts = utc_now_iso()
    result_df = (
        agg_df.join(country_totals, on=["source_id", "country_code"], how="inner")
        .withColumn(
            "pct_of_country_postings",
            F.round((F.col("posting_count") / F.col("_country_total")) * 100.0, 4),
        )
        .withColumn(
            "remote_penetration_pct",
            F.round((F.col("remote_posting_count") / F.col("posting_count")) * 100.0, 4),
        )
        .withColumn(
            "hybrid_penetration_pct",
            F.round((F.col("hybrid_posting_count") / F.col("posting_count")) * 100.0, 4),
        )
        .drop("_country_total")
        .withColumn("_gold_timestamp", F.lit(gold_ts))
    )
    return result_df


def main() -> None:
    spark = get_gold_spark_session("GoldJobDemandByLocation", driver_memory="4g")
    jobs_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "jobs")
    result_df = run_aggregation_location(spark, jobs_path, min_postings=5).cache()
    final_dir, backup_dir = write_and_promote_gold(
        result_df, "job_demand_by_location", coalesce=2
    )
    print("Gold job_demand_by_location written to", final_dir)
    if backup_dir:
        print("Previous version backed up at", backup_dir)
    spark.stop()


if __name__ == "__main__":
    main()
