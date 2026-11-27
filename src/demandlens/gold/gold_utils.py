"""Shared utilities for DemandLens Gold layer scripts (7C.4–7C.8)."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


def get_gold_spark_session(
    app_name: str,
    driver_memory: str = "4g",
    shuffle_partitions: int = 16,
) -> SparkSession:
    """Create a Windows-configured local SparkSession for Gold aggregations."""
    for var in ("PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "PYSPARK_GATEWAY_ADDRESS"):
        os.environ.pop(var, None)

    configure_windows_environment()

    spark_tmp = (PROJECT_ROOT / "data" / "temp_validation" / "spark_tmp").resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(spark_tmp))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def utc_now_iso() -> str:
    """Return current UTC timestamp formatted as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def promote_and_cleanup(
    staging_dir: str,
    final_dir: str,
    dataset_name: str = "gold_dataset",
) -> Optional[str]:
    """Atomically promote staging_dir to final_dir with backup & rollback protection."""
    backup_dir: Optional[str] = None
    try:
        if os.path.isdir(final_dir):
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            backup_dir = os.path.abspath(
                os.path.join(os.path.dirname(final_dir), f"_backup_{dataset_name}_{ts}")
            )
            os.rename(final_dir, backup_dir)
        os.rename(staging_dir, final_dir)
    except Exception as promo_err:
        rollback_err = None
        if backup_dir and os.path.isdir(backup_dir) and not os.path.isdir(final_dir):
            try:
                os.rename(backup_dir, final_dir)
            except Exception as rb_err:
                rollback_err = rb_err
        msg = f"PROMOTION FAILED: {promo_err}"
        if rollback_err:
            msg += f" | ROLLBACK FAILED: {rollback_err}"
        else:
            msg += " | ROLLBACK SUCCESSFUL"
        raise RuntimeError(msg) from promo_err
    return backup_dir


def write_and_promote_gold(
    result_df: DataFrame,
    dataset_name: str,
    partition_by: Optional[str] = None,
    coalesce: Optional[int] = None,
) -> tuple[str, Optional[str]]:
    """Write result_df to a staging directory and atomically promote to data/gold/<dataset_name>."""
    run_id = uuid.uuid4().hex[:8]
    gold_root = PROJECT_ROOT / "data" / "gold"
    staging_dir = str(gold_root / f"_staging_{dataset_name}_{run_id}")
    final_dir = str(gold_root / dataset_name)
    os.makedirs(staging_dir, exist_ok=True)

    df_to_write = result_df.coalesce(coalesce) if coalesce else result_df
    writer = df_to_write.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(staging_dir)

    backup_dir = promote_and_cleanup(staging_dir, final_dir, dataset_name=dataset_name)
    return final_dir, backup_dir
