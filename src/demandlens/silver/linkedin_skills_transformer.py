"""DemandLens - Silver Layer: LinkedIn Job Skills Bridge Transformer.

Transforms Bronze LinkedIn job_skills records into a standardized, normalized
Silver skill bridge table with strict (job_link, skill) uniqueness, technical
token preservation, referential integrity validation, and end-to-end provenance.

Phase: 6D.2
Source: data/bronze/linkedin_2024/job_skills
Target: data/silver/linkedin_2024/job_skills_bridge
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

# Ensure project root is available in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor
from src.demandlens.silver.skill_normalization import normalize_skills, normalize_skill_token

logger = logging.getLogger("demandlens.silver.linkedin_skills")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def transform_linkedin_skills(
    df_bronze: DataFrame,
    silver_run_id: str,
    silver_timestamp: Optional[str] = None,
) -> DataFrame:
    """Transform Bronze LinkedIn job_skills into normalized Silver bridge DataFrame.

    Rules applied:
    1. Filter out null/empty job_link or job_skills.
    2. Normalize skill tokens using `normalize_skills` (case folding to lower,
       whitespace collapsing, stripping outer noise while preserving technical
       symbols like 'c++', 'c#', '.net', 'node.js', 'pl/sql', 'sql', 'ci/cd').
    3. Explode normalized skills into one record per (job_link, skill).
    4. Enforce strict (job_link, skill) deduplication.
    5. Attach lineage metadata (_silver_run_id, _silver_timestamp, etc.).

    Args:
        df_bronze: Raw Bronze Spark DataFrame for job_skills.
        silver_run_id: Unique execution identifier for the Silver run.
        silver_timestamp: ISO 8601 UTC timestamp string for Silver processing.

    Returns:
        Spark DataFrame representing the normalized Silver skill bridge.
    """
    ts = silver_timestamp or datetime.now(timezone.utc).isoformat()

    # Define UDF for token normalization and intra-posting deduplication
    norm_udf = F.udf(lambda s: normalize_skills(s, delimiter=","), ArrayType(StringType()))

    # 1. Filter valid rows where job_skills is non-empty
    df_valid = df_bronze.filter(
        F.col("job_link").isNotNull()
        & (F.trim(F.col("job_link")) != "")
        & F.col("job_skills").isNotNull()
        & (F.trim(F.col("job_skills")) != "")
    )

    # 2. Extract and explode normalized skills
    df_bridge = (
        df_valid
        .withColumn("skills_array", norm_udf(F.col("job_skills")))
        .select(
            F.col("job_link"),
            F.explode(F.col("skills_array")).alias("skill"),
            F.col("_source_id"),
            F.col("_source_file"),
            F.col("_ingestion_timestamp"),
            F.col("_ingestion_run_id"),
        )
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .withColumn("_silver_run_id", F.lit(silver_run_id))
        .withColumn("_silver_timestamp", F.to_timestamp(F.lit(ts)))
        .select(
            "job_link",
            "skill",
            "_source_id",
            "_source_file",
            "_ingestion_timestamp",
            "_ingestion_run_id",
            "_silver_run_id",
            "_silver_timestamp",
        )
    )

    return df_bridge


class LinkedInSkillsSilverPipeline:
    """Manages staging, validation, promotion, and metadata for LinkedIn Skills Silver."""

    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or PROJECT_ROOT
        self.bronze_skills_path = self.project_root / "data" / "bronze" / "linkedin_2024" / "job_skills"
        self.silver_postings_path = self.project_root / "data" / "silver" / "linkedin_2024" / "job_postings"
        self.silver_base = self.project_root / "data" / "silver" / "linkedin_2024"
        self.silver_bridge_path = self.silver_base / "job_skills_bridge"
        self.meta_dir = self.silver_base / "_metadata"

    def validate_referential_integrity(
        self,
        df_bridge: DataFrame,
    ) -> Dict[str, Any]:
        """Validate foreign-key relationship between skills bridge and job postings.

        Returns:
            Dictionary with integrity metrics (matched jobs, orphans, etc.).
        """
        logger.info("Validating referential integrity against Silver job_postings...")
        if not self.silver_postings_path.exists():
            logger.warning(f"Silver job_postings not found at {self.silver_postings_path}. Skipping integrity join.")
            return {"status": "SKIPPED", "reason": "silver_postings_not_found"}

        df_postings = self.spark.read.parquet(str(self.silver_postings_path)).select("job_link")
        total_postings = df_postings.count()

        df_bridge_jobs = df_bridge.select("job_link").distinct()
        total_bridge_jobs = df_bridge_jobs.count()

        # Inner join to find matched jobs
        matched_jobs = df_bridge_jobs.join(df_postings, on="job_link", how="inner").count()

        # Left anti join to find orphans (skills referencing non-existent postings)
        orphan_jobs = df_bridge_jobs.join(df_postings, on="job_link", how="left_anti").count()

        # Postings without skills
        postings_without_skills = total_postings - matched_jobs

        metrics = {
            "total_postings": total_postings,
            "bridge_distinct_jobs": total_bridge_jobs,
            "matched_jobs": matched_jobs,
            "orphan_jobs": orphan_jobs,
            "postings_without_skills": postings_without_skills,
            "orphan_free": orphan_jobs == 0,
        }
        logger.info(f"Referential integrity results: {metrics}")
        return metrics

    def validate_staging(self, staging_path: Path, expected_min_rows: int = 1_000_000) -> Dict[str, Any]:
        """Perform comprehensive data quality and sanity checks on staging Parquet."""
        logger.info(f"Validating staging dataset: {staging_path}")
        part_files = list(staging_path.glob("*.parquet"))
        if not part_files:
            raise FileNotFoundError(f"No Parquet files found in staging directory: {staging_path}")

        total_bytes = sum(f.stat().st_size for f in part_files)
        total_rows = 0
        for f in part_files:
            meta = pq.ParquetFile(str(f)).metadata
            total_rows += meta.num_rows

        if total_rows < expected_min_rows:
            raise ValueError(
                f"Staging validation failed: Row count {total_rows:,} is less than minimum expected {expected_min_rows:,}."
            )

        logger.info(
            f"Staging validation PASSED: {total_rows:,} rows across {len(part_files)} files ({total_bytes / (1024**2):.2f} MB)."
        )
        return {
            "total_rows": total_rows,
            "total_files": len(part_files),
            "total_bytes": total_bytes,
        }

    def run(
        self,
        overwrite: bool = False,
        target_partitions: int = 8,
    ) -> Dict[str, Any]:
        """Execute end-to-end LinkedIn Skills Silver Pipeline with staging and promotion."""
        t0 = time.time()
        run_id = f"silver_skills_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"

        logger.info(f"Starting LinkedIn Job Skills Silver Pipeline [Run: {run_id}]")
        logger.info(f"  Bronze Source: {self.bronze_skills_path}")
        logger.info(f"  Silver Bridge: {self.silver_bridge_path}")

        if self.silver_bridge_path.exists() and not overwrite:
            raise FileExistsError(
                f"Silver skills bridge destination already exists at {self.silver_bridge_path}. Specify overwrite=True."
            )

        # 1. Read Bronze data
        logger.info("Reading Bronze Parquet job_skills data...")
        df_bronze = self.spark.read.parquet(str(self.bronze_skills_path))
        bronze_count = df_bronze.count()
        logger.info(f"Loaded {bronze_count:,} Bronze job_skills records.")

        # 2. Transform into Silver bridge
        logger.info("Applying Silver skill normalization and exploding bridge records...")
        df_silver_bridge = transform_linkedin_skills(df_bronze, silver_run_id=run_id)

        # Repartition to target partitions for bounded file sizes
        df_silver_bridge = df_silver_bridge.repartition(target_partitions)

        # 3. Staging path strictly on D:
        staging_dir = self.silver_base / f"_staging_job_skills_bridge_{uuid4().hex[:8]}"

        try:
            logger.info(f"Writing Parquet to staging path via PyArrow: {staging_dir}")
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_silver_bridge,
                output_dir=staging_dir,
                batch_size=20000,
                compression="snappy",
            )

            # 4. Validate staging
            staging_metrics = self.validate_staging(staging_dir)
            silver_count = staging_metrics["total_rows"]

            # 5. Read back staging to validate referential integrity
            df_staging_readback = self.spark.read.parquet(str(staging_dir))
            integrity_metrics = self.validate_referential_integrity(df_staging_readback)

            if not integrity_metrics.get("orphan_free", True):
                raise ValueError(f"Orphan records found in staging skills bridge: {integrity_metrics['orphan_jobs']}")

            # 6. Atomic promotion
            if self.silver_bridge_path.exists():
                logger.info(f"Removing existing Silver output at {self.silver_bridge_path} (overwrite=True)...")
                shutil.rmtree(self.silver_bridge_path)

            logger.info(f"Promoting staging dataset to final destination: {self.silver_bridge_path}")
            shutil.move(str(staging_dir), str(self.silver_bridge_path))

            duration = round(time.time() - t0, 2)
            logger.info(f"Silver promotion SUCCESS in {duration}s.")

            # 7. Write Manifest
            self.meta_dir.mkdir(parents=True, exist_ok=True)
            manifest = {
                "silver_run_id": run_id,
                "source_id": "linkedin_2024",
                "table_name": "job_skills_bridge",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": duration,
                "bronze_source_path": str(self.bronze_skills_path),
                "bronze_row_count": bronze_count,
                "silver_bridge_row_count": silver_count,
                "silver_output_path": str(self.silver_bridge_path),
                "partitions": target_partitions,
                "compression": "snappy",
                "referential_integrity": integrity_metrics,
                "status": "SUCCESS",
            }
            manifest_file = self.meta_dir / f"silver_job_skills_bridge_{run_id}.json"
            with open(manifest_file, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            logger.info(f"Manifest written to: {manifest_file}")

            return manifest

        except Exception as e:
            logger.error(f"Silver transformation FAILED: {e}", exc_info=True)
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            raise
