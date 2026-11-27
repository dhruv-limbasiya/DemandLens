"""DemandLens — Phase 7C.8: Gold Entry-Level Career Pathways Aggregation.

Grain: (source_id, country_code, standardized_role, skill)
Sources:
  - data/silver/canonical/jobs
  - data/silver/canonical/job_skills_bridge
Output:
  - data/gold/career_pathways_entry_level
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.demandlens.gold.gold_utils import (
    get_gold_spark_session,
    utc_now_iso,
    write_and_promote_gold,
)
from src.demandlens.gold.role_standardizer import add_standardized_role_columns


def run_aggregation_career_pathways(
    spark: SparkSession,
    jobs_path: str,
    bridge_path: str,
    min_entry_postings: int = 5,
) -> Tuple[DataFrame, DataFrame]:
    """Compute entry-level role & skill pathways by (source_id, country_code, standardized_role, skill).

    Returns (role_entry_totals_df, result_df).
    """
    jobs_df = spark.read.parquet(jobs_path).select(
        "canonical_job_id",
        "source_id",
        "country_code",
        "job_title_clean",
        "seniority_tier",
        "experience_min_years",
        "is_remote",
    )

    entry_jobs = jobs_df.filter(
        F.when(
            F.col("source_id") == "linkedin_2024",
            F.col("seniority_tier").isin("ENTRY", "ASSOCIATE"),
        )
        .when(
            F.col("source_id") == "indian_job_market_2025_2026",
            (F.col("seniority_tier") == "ENTRY")
            | (F.col("experience_min_years").isNotNull() & (F.col("experience_min_years") <= 2.0)),
        )
        .otherwise(
            F.col("seniority_tier").isin("ENTRY", "ASSOCIATE")
            | (F.col("experience_min_years").isNotNull() & (F.col("experience_min_years") <= 2.0))
        )
    )

    entry_with_role = add_standardized_role_columns(
        entry_jobs, title_col="job_title_clean"
    ).select("canonical_job_id", "source_id", "country_code", "standardized_role", "is_remote")

    role_entry_totals_df = entry_with_role.groupBy(
        "source_id", "country_code", "standardized_role"
    ).agg(F.count("*").alias("role_entry_total"))

    bridge_df = (
        spark.read.parquet(bridge_path)
        .select("canonical_job_id", "source_id", "skill")
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .withColumn("skill", F.trim(F.col("skill")))
    )

    joined_df = bridge_df.join(
        entry_with_role, on=["canonical_job_id", "source_id"], how="inner"
    ).dropDuplicates(
        ["source_id", "country_code", "standardized_role", "canonical_job_id", "skill"]
    )

    skill_stats = (
        joined_df.groupBy("source_id", "country_code", "standardized_role", "skill")
        .agg(
            F.count("*").alias("entry_posting_count"),
            F.sum(F.when(F.col("is_remote") == True, 1).otherwise(0)).alias("_remote_cnt"),
        )
        .filter(F.col("entry_posting_count") >= min_entry_postings)
    )

    enriched = (
        skill_stats.join(
            role_entry_totals_df,
            on=["source_id", "country_code", "standardized_role"],
            how="inner",
        )
        .withColumn(
            "skill_entry_prevalence_pct",
            F.round((F.col("entry_posting_count") / F.col("role_entry_total")) * 100.0, 4),
        )
        .withColumn(
            "remote_entry_pct",
            F.round((F.col("_remote_cnt") / F.col("entry_posting_count")) * 100.0, 4),
        )
        .drop("_remote_cnt")
    )

    rank_win = Window.partitionBy("source_id", "country_code", "standardized_role").orderBy(
        F.desc("entry_posting_count"), F.asc("skill")
    )
    gold_ts = utc_now_iso()
    result_df = enriched.withColumn(
        "entry_skill_rank", F.row_number().over(rank_win)
    ).withColumn("_gold_timestamp", F.lit(gold_ts))

    return role_entry_totals_df, result_df


def validate_career_pathways(
    result_df: DataFrame,
    role_entry_totals_df: DataFrame,
    min_entry_postings: int = 5,
) -> List[str]:
    """Validate schema, grain uniqueness, bounds, and entry role total reconciliation."""
    errors: List[str] = []
    expected_cols = {
        "source_id",
        "country_code",
        "standardized_role",
        "skill",
        "entry_posting_count",
        "role_entry_total",
        "skill_entry_prevalence_pct",
        "remote_entry_pct",
        "entry_skill_rank",
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
        result_df.groupBy("source_id", "country_code", "standardized_role", "skill")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dupes > 0:
        errors.append("Found duplicate (source_id, country_code, standardized_role, skill) keys")

    bad = result_df.filter(
        (F.col("entry_posting_count") < min_entry_postings)
        | (F.col("entry_posting_count") > F.col("role_entry_total"))
        | (F.col("skill_entry_prevalence_pct") <= 0.0)
        | (F.col("skill_entry_prevalence_pct") > 100.0)
        | (F.col("remote_entry_pct") < 0.0)
        | (F.col("remote_entry_pct") > 100.0)
    ).limit(1).count()
    if bad > 0:
        errors.append("Found out-of-bounds entry-level metrics")

    gold_totals = result_df.select(
        "source_id", "country_code", "standardized_role", "role_entry_total"
    ).distinct()
    src_totals = role_entry_totals_df.withColumnRenamed(
        "role_entry_total", "src_role_entry_total"
    )
    mismatch = (
        gold_totals.join(
            src_totals,
            on=["source_id", "country_code", "standardized_role"],
            how="left",
        )
        .filter(F.col("role_entry_total") != F.col("src_role_entry_total"))
        .limit(1)
        .count()
    )
    if mismatch > 0:
        errors.append("role_entry_total mismatch against entry-level canonical_jobs")

    return errors


def main() -> None:
    spark = get_gold_spark_session("GoldCareerPathwaysEntryLevel", driver_memory="4g")
    jobs_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "jobs")
    bridge_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "job_skills_bridge")

    role_entry_totals_df, result_df = run_aggregation_career_pathways(
        spark, jobs_path, bridge_path, min_entry_postings=5
    )
    result_df = result_df.cache()

    errors = validate_career_pathways(result_df, role_entry_totals_df, min_entry_postings=5)
    if errors:
        print("VALIDATION FAILED:", errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    final_dir, backup_dir = write_and_promote_gold(
        result_df, "career_pathways_entry_level", coalesce=4
    )
    print("Gold career_pathways_entry_level written to", final_dir)
    if backup_dir:
        print("Previous version backed up at", backup_dir)
    spark.stop()


if __name__ == "__main__":
    main()
