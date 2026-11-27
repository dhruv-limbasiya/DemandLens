"""DemandLens — Phase 7C.4: Gold Overall Skill Demand Aggregation.

Grain: (source_id, country_code, skill)
Sources:
  - data/silver/canonical/jobs
  - data/silver/canonical/job_skills_bridge
Output:
  - data/gold/skill_demand_overall
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


def run_aggregation_skill_demand(
    spark: SparkSession,
    jobs_path: str,
    bridge_path: str,
    min_posting_count: int = 10,
) -> Tuple[DataFrame, DataFrame]:
    """Compute overall skill demand and prevalence by (source_id, country_code, skill).

    Returns (market_totals_df, result_df).
    """
    jobs_df = spark.read.parquet(jobs_path).select(
        "canonical_job_id", "source_id", "country_code"
    )
    bridge_df = spark.read.parquet(bridge_path).select(
        "canonical_job_id", "source_id", "skill"
    )

    bridge_clean = bridge_df.filter(
        F.col("skill").isNotNull() & (F.trim(F.col("skill")) != "")
    ).withColumn("skill", F.trim(F.col("skill")))

    market_totals_df = jobs_df.groupBy("source_id", "country_code").agg(
        F.count("*").alias("total_market_postings")
    )

    joined_df = bridge_clean.join(
        jobs_df, on=["canonical_job_id", "source_id"], how="inner"
    ).dropDuplicates(["source_id", "country_code", "canonical_job_id", "skill"])

    skill_counts_df = (
        joined_df.groupBy("source_id", "country_code", "skill")
        .agg(F.count("*").alias("posting_count"))
        .filter(F.col("posting_count") >= min_posting_count)
    )

    enriched_df = skill_counts_df.join(
        market_totals_df, on=["source_id", "country_code"], how="inner"
    ).withColumn(
        "prevalence_pct",
        F.round((F.col("posting_count") / F.col("total_market_postings")) * 100.0, 4),
    )

    rank_win = Window.partitionBy("source_id", "country_code").orderBy(
        F.desc("posting_count"), F.asc("skill")
    )
    gold_ts = utc_now_iso()
    result_df = enriched_df.withColumn(
        "market_skill_rank", F.row_number().over(rank_win)
    ).withColumn("_gold_timestamp", F.lit(gold_ts))

    return market_totals_df, result_df


def validate_skill_demand(
    result_df: DataFrame,
    market_totals_df: DataFrame,
    min_posting_count: int = 10,
) -> List[str]:
    """Validate schema, grain uniqueness, bounds, and market total reconciliation."""
    errors: List[str] = []
    expected_cols = {
        "source_id",
        "country_code",
        "skill",
        "posting_count",
        "total_market_postings",
        "prevalence_pct",
        "market_skill_rank",
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
        | F.col("country_code").isNull()
        | F.col("skill").isNull()
        | (F.trim(F.col("skill")) == "")
    ).limit(1).count()
    if null_dims > 0:
        errors.append("Found null or blank dimension keys")

    dupes = (
        result_df.groupBy("source_id", "country_code", "skill")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dupes > 0:
        errors.append("Found duplicate (source_id, country_code, skill) keys")

    invalid_metrics = result_df.filter(
        (F.col("posting_count") < min_posting_count)
        | (F.col("posting_count") > F.col("total_market_postings"))
        | (F.col("prevalence_pct") <= 0.0)
        | (F.col("prevalence_pct") > 100.0)
        | (F.col("market_skill_rank") < 1)
    ).limit(1).count()
    if invalid_metrics > 0:
        errors.append("Found out-of-bounds skill metrics")

    # Reconcile total_market_postings against canonical_jobs market totals
    gold_totals = result_df.select(
        "source_id", "country_code", "total_market_postings"
    ).distinct()
    src_totals = market_totals_df.withColumnRenamed(
        "total_market_postings", "src_total_market_postings"
    )
    mismatch = (
        gold_totals.join(src_totals, on=["source_id", "country_code"], how="left")
        .filter(
            F.col("total_market_postings") != F.col("src_total_market_postings")
        )
        .limit(1)
        .count()
    )
    if mismatch > 0:
        errors.append("total_market_postings mismatch against canonical_jobs")

    return errors


def main() -> None:
    spark = get_gold_spark_session("GoldSkillDemandOverall", driver_memory="4g")
    jobs_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "jobs")
    bridge_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "job_skills_bridge")

    market_totals_df, result_df = run_aggregation_skill_demand(
        spark, jobs_path, bridge_path, min_posting_count=10
    )
    result_df = result_df.cache()

    errors = validate_skill_demand(result_df, market_totals_df, min_posting_count=10)
    if errors:
        print("VALIDATION FAILED:", errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    final_dir, backup_dir = write_and_promote_gold(
        result_df, "skill_demand_overall", coalesce=4
    )
    print("Gold skill_demand_overall written to", final_dir)
    if backup_dir:
        print("Previous version backed up at", backup_dir)
    spark.stop()


if __name__ == "__main__":
    main()
