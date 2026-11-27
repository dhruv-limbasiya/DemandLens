"""DemandLens — Phase 7C.7: Gold India Salary Analytics Aggregation.

Grain: (standardized_role, seniority_tier, location_clean)
Source:
  - data/silver/canonical/jobs (strictly Indian annual INR disclosed salaries)
Output:
  - data/gold/salary_analytics_india
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

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
from src.demandlens.gold.role_standardizer import add_standardized_role_columns


def run_aggregation_salary_india(
    spark: SparkSession,
    jobs_path: str,
    min_sample_size: int = 3,
) -> DataFrame:
    """Compute Indian annual INR salary percentiles by (standardized_role, seniority_tier, location_clean)."""
    jobs_df = spark.read.parquet(jobs_path).select(
        "source_id",
        "job_title_clean",
        "seniority_tier",
        "location_clean",
        "experience_min_years",
        "experience_max_years",
        "salary_min",
        "salary_max",
        "salary_midpoint",
        "salary_currency",
        "salary_period",
        "salary_disclosed",
    )

    filtered = jobs_df.filter(
        (F.col("source_id") == "indian_job_market_2025_2026")
        & (F.col("salary_disclosed") == True)
        & F.col("salary_midpoint").isNotNull()
        & (F.col("salary_midpoint") > 0)
        & (F.col("salary_currency") == "INR")
        & (F.col("salary_period") == "ANNUAL")
        & F.col("location_clean").isNotNull()
        & (F.trim(F.col("location_clean")) != "")
    )

    with_role = add_standardized_role_columns(filtered, title_col="job_title_clean")

    gold_ts = utc_now_iso()
    agg_df = (
        with_role.groupBy("standardized_role", "seniority_tier", "location_clean")
        .agg(
            F.count("*").alias("sample_size"),
            F.round(F.avg("experience_min_years"), 2).alias("avg_experience_min_years"),
            F.round(F.avg("experience_max_years"), 2).alias("avg_experience_max_years"),
            F.round(F.percentile_approx("salary_min", 0.25), 2).alias("salary_min_p25_inr"),
            F.round(F.percentile_approx("salary_min", 0.50), 2).alias("salary_min_median_inr"),
            F.round(F.avg("salary_midpoint"), 2).alias("salary_mid_mean_inr"),
            F.round(F.percentile_approx("salary_midpoint", 0.50), 2).alias("salary_mid_median_inr"),
            F.round(F.percentile_approx("salary_max", 0.50), 2).alias("salary_max_median_inr"),
            F.round(F.percentile_approx("salary_max", 0.75), 2).alias("salary_max_p75_inr"),
        )
        .filter(F.col("sample_size") >= min_sample_size)
        .withColumn("salary_currency", F.lit("INR"))
        .withColumn("salary_period", F.lit("ANNUAL"))
        .withColumn("_gold_timestamp", F.lit(gold_ts))
    )
    return agg_df


def validate_salary_india(
    result_df: DataFrame, min_sample_size: int = 3
) -> List[str]:
    """Validate schema, grain uniqueness, currency/period invariants, and bounds."""
    errors: List[str] = []
    expected_cols = {
        "standardized_role",
        "seniority_tier",
        "location_clean",
        "sample_size",
        "avg_experience_min_years",
        "avg_experience_max_years",
        "salary_min_p25_inr",
        "salary_min_median_inr",
        "salary_mid_mean_inr",
        "salary_mid_median_inr",
        "salary_max_median_inr",
        "salary_max_p75_inr",
        "salary_currency",
        "salary_period",
        "_gold_timestamp",
    }
    missing = expected_cols - set(result_df.columns)
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
        return errors

    if result_df.count() == 0:
        errors.append("Output DataFrame is empty")
        return errors

    dupes = (
        result_df.groupBy("standardized_role", "seniority_tier", "location_clean")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dupes > 0:
        errors.append("Found duplicate (standardized_role, seniority_tier, location_clean) keys")

    bad_rows = result_df.filter(
        (F.col("sample_size") < min_sample_size)
        | (F.col("salary_mid_median_inr") <= 0)
        | (F.col("salary_currency") != "INR")
        | (F.col("salary_period") != "ANNUAL")
    ).limit(1).count()
    if bad_rows > 0:
        errors.append("Found invalid salary bounds or currency/period")

    return errors


def main() -> None:
    spark = get_gold_spark_session("GoldSalaryAnalyticsIndia", driver_memory="4g")
    jobs_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "jobs")

    result_df = run_aggregation_salary_india(spark, jobs_path, min_sample_size=3).cache()
    errors = validate_salary_india(result_df, min_sample_size=3)
    if errors:
        print("VALIDATION FAILED:", errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    final_dir, backup_dir = write_and_promote_gold(
        result_df, "salary_analytics_india", coalesce=1
    )
    print("Gold salary_analytics_india written to", final_dir)
    if backup_dir:
        print("Previous version backed up at", backup_dir)
    spark.stop()


if __name__ == "__main__":
    main()
