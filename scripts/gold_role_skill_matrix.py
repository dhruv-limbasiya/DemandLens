"""DemandLens — Phase 7C.5: Gold Role-Skill Matrix Aggregation.

Grain: (source_id, standardized_role, skill)
Sources:
  - data/silver/canonical/jobs
  - data/silver/canonical/job_skills_bridge
Output:
  - data/gold/role_skill_matrix
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


def run_aggregation_role_skill_matrix(
    spark: SparkSession,
    jobs_path: str,
    bridge_path: str,
    min_occurrences: int = 5,
) -> Tuple[DataFrame, DataFrame]:
    """Compute skill prevalence by (source_id, standardized_role, skill).

    Returns (role_totals_df, result_df).
    """
    jobs_df = spark.read.parquet(jobs_path).select(
        "canonical_job_id", "source_id", "job_title_clean"
    )
    jobs_with_role = add_standardized_role_columns(jobs_df, title_col="job_title_clean").select(
        "canonical_job_id", "source_id", "standardized_role"
    )

    role_totals_df = jobs_with_role.groupBy("source_id", "standardized_role").agg(
        F.count("*").alias("total_role_postings")
    )

    bridge_df = (
        spark.read.parquet(bridge_path)
        .select("canonical_job_id", "source_id", "skill")
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .withColumn("skill", F.trim(F.col("skill")))
    )

    joined_df = bridge_df.join(
        jobs_with_role, on=["canonical_job_id", "source_id"], how="inner"
    ).dropDuplicates(["source_id", "standardized_role", "canonical_job_id", "skill"])

    counts_df = (
        joined_df.groupBy("source_id", "standardized_role", "skill")
        .agg(F.count("*").alias("skill_occurrences"))
        .filter(F.col("skill_occurrences") >= min_occurrences)
    )

    enriched_df = counts_df.join(
        role_totals_df, on=["source_id", "standardized_role"], how="inner"
    ).withColumn(
        "requirement_frequency_pct",
        F.round((F.col("skill_occurrences") / F.col("total_role_postings")) * 100.0, 4),
    )

    rank_win = Window.partitionBy("source_id", "standardized_role").orderBy(
        F.desc("skill_occurrences"), F.asc("skill")
    )
    gold_ts = utc_now_iso()
    result_df = enriched_df.withColumn(
        "role_skill_rank", F.row_number().over(rank_win)
    ).withColumn("_gold_timestamp", F.lit(gold_ts))

    return role_totals_df, result_df


def validate_role_skill_matrix(
    result_df: DataFrame,
    role_totals_df: DataFrame,
    min_occurrences: int = 5,
) -> List[str]:
    """Validate schema, grain uniqueness, bounds, and role total reconciliation."""
    errors: List[str] = []
    expected_cols = {
        "source_id",
        "standardized_role",
        "skill",
        "skill_occurrences",
        "total_role_postings",
        "requirement_frequency_pct",
        "role_skill_rank",
        "_gold_timestamp",
    }
    missing = expected_cols - set(result_df.columns)
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
        return errors

    if result_df.count() == 0:
        errors.append("Output DataFrame is empty")
        return errors

    null_dims = result_df.filter(
        F.col("source_id").isNull()
        | F.col("standardized_role").isNull()
        | F.col("skill").isNull()
        | (F.trim(F.col("skill")) == "")
    ).limit(1).count()
    if null_dims > 0:
        errors.append("Found null or blank dimension keys")

    dupes = (
        result_df.groupBy("source_id", "standardized_role", "skill")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dupes > 0:
        errors.append("Found duplicate (source_id, standardized_role, skill) keys")

    invalid_metrics = result_df.filter(
        (F.col("skill_occurrences") < min_occurrences)
        | (F.col("skill_occurrences") > F.col("total_role_postings"))
        | (F.col("requirement_frequency_pct") <= 0.0)
        | (F.col("requirement_frequency_pct") > 100.0)
        | (F.col("role_skill_rank") < 1)
    ).limit(1).count()
    if invalid_metrics > 0:
        errors.append("Found out-of-bounds role-skill metrics")

    gold_totals = result_df.select(
        "source_id", "standardized_role", "total_role_postings"
    ).distinct()
    src_totals = role_totals_df.withColumnRenamed(
        "total_role_postings", "src_total_role_postings"
    )
    mismatch = (
        gold_totals.join(src_totals, on=["source_id", "standardized_role"], how="left")
        .filter(F.col("total_role_postings") != F.col("src_total_role_postings"))
        .limit(1)
        .count()
    )
    if mismatch > 0:
        errors.append("total_role_postings mismatch against canonical_jobs")

    return errors


def main() -> None:
    spark = get_gold_spark_session("GoldRoleSkillMatrix", driver_memory="4g")
    jobs_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "jobs")
    bridge_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "job_skills_bridge")

    role_totals_df, result_df = run_aggregation_role_skill_matrix(
        spark, jobs_path, bridge_path, min_occurrences=5
    )
    result_df = result_df.cache()

    errors = validate_role_skill_matrix(result_df, role_totals_df, min_occurrences=5)
    if errors:
        print("VALIDATION FAILED:", errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    final_dir, backup_dir = write_and_promote_gold(
        result_df, "role_skill_matrix", coalesce=4
    )
    print("Gold role_skill_matrix written to", final_dir)
    if backup_dir:
        print("Previous version backed up at", backup_dir)
    spark.stop()


if __name__ == "__main__":
    main()
