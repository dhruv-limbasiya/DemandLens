"""DemandLens — Phase 7C.6: Gold Skill Co-occurrence Aggregation.

Grain: (source_id, skill_a, skill_b) where skill_a < skill_b
Source:
  - data/silver/canonical/job_skills_bridge
Output:
  - data/gold/skill_cooccurrence
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
from pyspark.sql.window import Window

from src.demandlens.gold.gold_utils import (
    get_gold_spark_session,
    utc_now_iso,
    write_and_promote_gold,
)


def run_aggregation_skill_cooccurrence(
    spark: SparkSession,
    bridge_path: str,
    top_n_skills: int = 300,
    min_cooccurrence: int = 15,
) -> DataFrame:
    """Compute skill co-occurrence pairs & Jaccard similarity among top_n_skills per source."""
    bridge_clean = (
        spark.read.parquet(bridge_path)
        .select("canonical_job_id", "source_id", "skill")
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .withColumn("skill", F.trim(F.col("skill")))
        .dropDuplicates(["source_id", "canonical_job_id", "skill"])
    )

    skill_totals = bridge_clean.groupBy("source_id", "skill").agg(
        F.count("*").alias("skill_total")
    )
    rank_win = Window.partitionBy("source_id").orderBy(
        F.desc("skill_total"), F.asc("skill")
    )
    top_skills = (
        skill_totals.withColumn("_rn", F.row_number().over(rank_win))
        .filter(F.col("_rn") <= top_n_skills)
        .drop("_rn")
    )

    # Keep only edges belonging to top_n_skills per source
    filtered_bridge = bridge_clean.join(
        top_skills.select("source_id", "skill"), on=["source_id", "skill"], how="inner"
    )

    left = filtered_bridge.select(
        F.col("source_id"),
        F.col("canonical_job_id"),
        F.col("skill").alias("skill_a"),
    )
    right = filtered_bridge.select(
        F.col("source_id").alias("r_source_id"),
        F.col("canonical_job_id").alias("r_job_id"),
        F.col("skill").alias("skill_b"),
    )

    pairs = (
        left.join(
            right,
            (left["source_id"] == right["r_source_id"])
            & (left["canonical_job_id"] == right["r_job_id"])
            & (left["skill_a"] < right["skill_b"]),
            how="inner",
        )
        .groupBy(left["source_id"].alias("source_id"), "skill_a", "skill_b")
        .agg(F.count("*").alias("cooccurrence_count"))
        .filter(F.col("cooccurrence_count") >= min_cooccurrence)
    )

    totals_a = top_skills.select(
        "source_id",
        F.col("skill").alias("skill_a"),
        F.col("skill_total").alias("skill_a_total"),
    )
    totals_b = top_skills.select(
        "source_id",
        F.col("skill").alias("skill_b"),
        F.col("skill_total").alias("skill_b_total"),
    )

    gold_ts = utc_now_iso()
    result_df = (
        pairs.join(totals_a, on=["source_id", "skill_a"], how="inner")
        .join(totals_b, on=["source_id", "skill_b"], how="inner")
        .withColumn(
            "jaccard_similarity",
            F.round(
                F.col("cooccurrence_count")
                / (
                    F.col("skill_a_total")
                    + F.col("skill_b_total")
                    - F.col("cooccurrence_count")
                ),
                6,
            ),
        )
        .withColumn("_gold_timestamp", F.lit(gold_ts))
    )
    return result_df


def validate_skill_cooccurrence(
    result_df: DataFrame, min_cooccurrence: int = 15
) -> List[str]:
    """Validate schema, ordering invariant (skill_a < skill_b), uniqueness, and Jaccard bounds."""
    errors: List[str] = []
    expected_cols = {
        "source_id",
        "skill_a",
        "skill_b",
        "cooccurrence_count",
        "skill_a_total",
        "skill_b_total",
        "jaccard_similarity",
        "_gold_timestamp",
    }
    missing = expected_cols - set(result_df.columns)
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
        return errors

    if result_df.count() == 0:
        errors.append("Output DataFrame is empty")
        return errors

    bad_order = result_df.filter(F.col("skill_a") >= F.col("skill_b")).limit(1).count()
    if bad_order > 0:
        errors.append("Found rows violating skill_a < skill_b")

    dupes = (
        result_df.groupBy("source_id", "skill_a", "skill_b")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dupes > 0:
        errors.append("Found duplicate (source_id, skill_a, skill_b) keys")

    bad_metrics = result_df.filter(
        (F.col("cooccurrence_count") < min_cooccurrence)
        | (F.col("jaccard_similarity") <= 0.0)
        | (F.col("jaccard_similarity") > 1.0)
    ).limit(1).count()
    if bad_metrics > 0:
        errors.append("Found out-of-bounds cooccurrence_count or jaccard_similarity")

    return errors


def main() -> None:
    spark = get_gold_spark_session("GoldSkillCooccurrence", driver_memory="4g")
    bridge_path = str(PROJECT_ROOT / "data" / "silver" / "canonical" / "job_skills_bridge")

    result_df = run_aggregation_skill_cooccurrence(
        spark, bridge_path, top_n_skills=300, min_cooccurrence=15
    ).cache()

    errors = validate_skill_cooccurrence(result_df, min_cooccurrence=15)
    if errors:
        print("VALIDATION FAILED:", errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    final_dir, backup_dir = write_and_promote_gold(
        result_df, "skill_cooccurrence", coalesce=4
    )
    print("Gold skill_cooccurrence written to", final_dir)
    if backup_dir:
        print("Previous version backed up at", backup_dir)
    spark.stop()


if __name__ == "__main__":
    main()
