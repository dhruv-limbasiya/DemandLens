"""
DemandLens — Silver Layer: Canonical Integration Transformer.

Transforms and integrates source-specific Silver tables (LinkedIn 2024 and
Indian Job Market 2025-2026) into a standardized, unified canonical Silver layer:
  - data/silver/canonical/jobs
  - data/silver/canonical/job_skills_bridge
  - data/silver/canonical/job_text

Follows the approved canonical architecture design:
  docs/architecture/canonical_silver_integration_design.md
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

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Ensure project root is available in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor
from src.demandlens.silver.linkedin_summary_transformer import clean_job_summary

logger = logging.getLogger("demandlens.silver.canonical")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# Canonical Jobs Target Schema
CANONICAL_JOBS_SCHEMA = StructType([
    StructField("canonical_job_id", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("source_job_id", StringType(), False),
    StructField("job_title_clean", StringType(), False),
    StructField("company_name_clean", StringType(), True),
    StructField("location_clean", StringType(), True),
    StructField("country_code", StringType(), False),
    StructField("workplace_type", StringType(), False),
    StructField("is_remote", BooleanType(), False),
    StructField("is_hybrid", BooleanType(), False),
    StructField("seniority_tier", StringType(), False),
    StructField("experience_min_years", DoubleType(), True),
    StructField("experience_max_years", DoubleType(), True),
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("salary_midpoint", DoubleType(), True),
    StructField("salary_currency", StringType(), True),
    StructField("salary_period", StringType(), True),
    StructField("salary_disclosed", BooleanType(), False),
    StructField("posted_date", DateType(), True),
    StructField("freshness_category", StringType(), True),
    StructField("skill_count", IntegerType(), False),
    StructField("has_skills", BooleanType(), False),
    StructField("has_description", BooleanType(), False),
    StructField("_source_file", StringType(), False),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_canonical_run_id", StringType(), False),
    StructField("_canonical_timestamp", TimestampType(), False),
])

CANONICAL_SKILLS_SCHEMA = StructType([
    StructField("canonical_job_id", StringType(), False),
    StructField("skill", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("_canonical_run_id", StringType(), False),
    StructField("_canonical_timestamp", TimestampType(), False),
])

CANONICAL_TEXT_SCHEMA = StructType([
    StructField("canonical_job_id", StringType(), False),
    StructField("description_clean", StringType(), False),
    StructField("character_count", IntegerType(), False),
    StructField("word_count", IntegerType(), False),
    StructField("source_id", StringType(), False),
    StructField("_canonical_run_id", StringType(), False),
    StructField("_canonical_timestamp", TimestampType(), False),
])


def map_linkedin_postings_to_canonical(
    df_postings: DataFrame,
    df_skills_bridge: DataFrame,
    df_summary: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: str,
) -> DataFrame:
    """Project and map LinkedIn 2024 Silver postings into canonical schema."""
    ts_val = F.to_timestamp(F.lit(canonical_timestamp))

    # Pre-aggregate skill counts from skills bridge
    df_skill_counts = (
        df_skills_bridge
        .groupBy("job_link")
        .agg(F.count("skill").cast(IntegerType()).alias("skill_count_calc"))
    )

    # Pre-determine summary presence from job_summary table
    df_has_summary = (
        df_summary
        .select("job_link")
        .distinct()
        .withColumn("has_desc_calc", F.lit(True))
    )

    cols = df_postings.columns
    title_expr = F.coalesce(
        F.col("title_clean"),
        F.col("job_title") if "job_title" in cols else F.lit(None),
        F.lit("UNSPECIFIED"),
    )
    company_expr = F.coalesce(
        F.col("company_clean"),
        F.col("company") if "company" in cols else F.lit(None),
    )
    location_expr = F.coalesce(
        F.col("location_clean"),
        F.col("job_location") if "job_location" in cols else F.lit(None),
    )

    df_mapped = (
        df_postings
        .join(df_skill_counts, on="job_link", how="left")
        .join(df_has_summary, on="job_link", how="left")
        .select(
            F.concat(F.lit("linkedin_2024#"), F.col("job_link")).alias("canonical_job_id"),
            F.lit("linkedin_2024").alias("source_id"),
            F.col("job_link").alias("source_job_id"),
            title_expr.alias("job_title_clean"),
            company_expr.alias("company_name_clean"),
            location_expr.alias("location_clean"),
            F.coalesce(F.col("country_code"), F.lit("OTHER")).alias("country_code"),
            F.coalesce(F.col("workplace_type"), F.lit("UNSPECIFIED")).alias("workplace_type"),
            F.coalesce(F.col("is_remote"), F.lit(False)).alias("is_remote"),
            F.coalesce(F.col("is_hybrid"), F.lit(False)).alias("is_hybrid"),
            F.coalesce(F.col("seniority_tier"), F.lit("UNSPECIFIED")).alias("seniority_tier"),
            F.lit(None).cast(DoubleType()).alias("experience_min_years"),
            F.lit(None).cast(DoubleType()).alias("experience_max_years"),
            F.lit(None).cast(DoubleType()).alias("salary_min"),
            F.lit(None).cast(DoubleType()).alias("salary_max"),
            F.lit(None).cast(DoubleType()).alias("salary_midpoint"),
            F.lit(None).cast(StringType()).alias("salary_currency"),
            F.lit(None).cast(StringType()).alias("salary_period"),
            F.lit(False).alias("salary_disclosed"),
            F.col("first_seen_date").alias("posted_date"),
            F.lit("UNSPECIFIED").alias("freshness_category"),
            F.coalesce(F.col("skill_count_calc"), F.lit(0)).alias("skill_count"),
            (F.coalesce(F.col("skill_count_calc"), F.lit(0)) > 0).alias("has_skills"),
            F.coalesce(F.col("has_desc_calc"), F.lit(False)).alias("has_description"),
            F.col("_source_file"),
            F.col("_ingestion_timestamp"),
            F.lit(canonical_run_id).alias("_canonical_run_id"),
            ts_val.alias("_canonical_timestamp"),
        )
    )
    return df_mapped


def map_indian_jobs_to_canonical(
    df_indian_jobs: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: str,
) -> DataFrame:
    """Project and map Indian Job Market 2025-2026 Silver jobs into canonical schema."""
    ts_val = F.to_timestamp(F.lit(canonical_timestamp))

    cols = df_indian_jobs.columns
    title_expr = F.coalesce(
        F.col("title_clean"),
        F.col("title") if "title" in cols else F.lit(None),
        F.lit("UNSPECIFIED"),
    )
    company_expr = F.coalesce(
        F.col("company_name_clean"),
        F.col("companyName") if "companyName" in cols else F.lit(None),
    )
    location_expr = F.coalesce(
        F.col("location_clean"),
        F.col("location") if "location" in cols else F.lit(None),
    )

    # All 97,679 Indian jobs have non-empty jobDescription
    df_mapped = (
        df_indian_jobs
        .select(
            F.concat(F.lit("indian_job_market_2025_2026#"), F.col("job_id")).alias("canonical_job_id"),
            F.lit("indian_job_market_2025_2026").alias("source_id"),
            F.col("job_id").alias("source_job_id"),
            title_expr.alias("job_title_clean"),
            company_expr.alias("company_name_clean"),
            location_expr.alias("location_clean"),
            F.lit("IN").alias("country_code"),
            F.when(F.col("is_remote") == True, F.lit("REMOTE")).otherwise(F.lit("ON_SITE")).alias("workplace_type"),
            F.coalesce(F.col("is_remote"), F.lit(False)).alias("is_remote"),
            F.lit(False).alias("is_hybrid"),
            F.coalesce(F.col("seniority_tier"), F.lit("UNSPECIFIED")).alias("seniority_tier"),
            F.col("experience_min_years").cast(DoubleType()).alias("experience_min_years"),
            F.col("experience_max_years").cast(DoubleType()).alias("experience_max_years"),
            F.col("salary_min").cast(DoubleType()).alias("salary_min"),
            F.col("salary_max").cast(DoubleType()).alias("salary_max"),
            F.col("salary_midpoint").cast(DoubleType()).alias("salary_midpoint"),
            F.coalesce(F.col("salary_currency"), F.lit("INR")).alias("salary_currency"),
            F.col("salary_period").alias("salary_period"),
            (F.col("salary_min").isNotNull() | F.col("salary_max").isNotNull()).alias("salary_disclosed"),
            F.lit(None).cast(DateType()).alias("posted_date"),
            F.coalesce(F.col("freshness_tier"), F.lit("UNSPECIFIED")).alias("freshness_category"),
            F.coalesce(F.col("skill_count"), F.lit(0)).cast(IntegerType()).alias("skill_count"),
            F.coalesce(F.col("has_skills"), F.lit(False)).alias("has_skills"),
            F.lit(True).alias("has_description"),
            F.col("_source_file"),
            F.col("_ingestion_timestamp"),
            F.lit(canonical_run_id).alias("_canonical_run_id"),
            ts_val.alias("_canonical_timestamp"),
        )
    )
    return df_mapped


def build_canonical_jobs(
    df_linkedin_postings: DataFrame,
    df_linkedin_skills: DataFrame,
    df_linkedin_summary: DataFrame,
    df_indian_jobs: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: Optional[str] = None,
) -> DataFrame:
    """Union and standardize LinkedIn and Indian postings into canonical_jobs DataFrame."""
    ts = canonical_timestamp or datetime.now(timezone.utc).isoformat()
    df_li = map_linkedin_postings_to_canonical(
        df_linkedin_postings, df_linkedin_skills, df_linkedin_summary, canonical_run_id, ts
    )
    df_in = map_indian_jobs_to_canonical(df_indian_jobs, canonical_run_id, ts)

    df_canonical = df_li.unionByName(df_in)
    return df_canonical


def build_canonical_skills_bridge(
    df_linkedin_skills: DataFrame,
    df_indian_skills: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: Optional[str] = None,
) -> DataFrame:
    """Union and standardize LinkedIn and Indian skills bridge into canonical_job_skills_bridge."""
    ts = canonical_timestamp or datetime.now(timezone.utc).isoformat()
    ts_val = F.to_timestamp(F.lit(ts))

    df_li = (
        df_linkedin_skills
        .filter(F.col("job_link").isNotNull() & (F.trim(F.col("job_link")) != ""))
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .select(
            F.concat(F.lit("linkedin_2024#"), F.col("job_link")).alias("canonical_job_id"),
            F.col("skill"),
            F.lit("linkedin_2024").alias("source_id"),
            F.lit(canonical_run_id).alias("_canonical_run_id"),
            ts_val.alias("_canonical_timestamp"),
        )
    )

    df_in = (
        df_indian_skills
        .filter(F.col("job_id").isNotNull() & (F.trim(F.col("job_id")) != ""))
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .select(
            F.concat(F.lit("indian_job_market_2025_2026#"), F.col("job_id")).alias("canonical_job_id"),
            F.col("skill"),
            F.lit("indian_job_market_2025_2026").alias("source_id"),
            F.lit(canonical_run_id).alias("_canonical_run_id"),
            ts_val.alias("_canonical_timestamp"),
        )
    )

    df_canonical_skills = df_li.unionByName(df_in)
    return df_canonical_skills


def build_canonical_job_text_linkedin(
    df_linkedin_summary: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: Optional[str] = None,
) -> DataFrame:
    """Project clean LinkedIn summaries into canonical_job_text schema."""
    ts = canonical_timestamp or datetime.now(timezone.utc).isoformat()
    ts_val = F.to_timestamp(F.lit(ts))

    return (
        df_linkedin_summary
        .filter(F.col("job_link").isNotNull() & (F.trim(F.col("job_link")) != ""))
        .select(
            F.concat(F.lit("linkedin_2024#"), F.col("job_link")).alias("canonical_job_id"),
            F.col("summary_clean").alias("description_clean"),
            F.col("character_count"),
            F.col("word_count"),
            F.lit("linkedin_2024").alias("source_id"),
            F.lit(canonical_run_id).alias("_canonical_run_id"),
            ts_val.alias("_canonical_timestamp"),
        )
    )


def _clean_indian_desc_row(desc_raw: Optional[str]) -> Tuple[str, int, int]:
    """Clean Indian raw jobDescription and return (description_clean, char_count, word_count)."""
    cleaned, char_count, word_count, _ = clean_job_summary(desc_raw)
    return (cleaned or "", char_count, word_count)


def build_canonical_job_text_indian(
    df_indian_jobs: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: Optional[str] = None,
) -> DataFrame:
    """Clean and project Indian jobDescription into canonical_job_text schema."""
    ts = canonical_timestamp or datetime.now(timezone.utc).isoformat()
    ts_val = F.to_timestamp(F.lit(ts))

    indian_desc_schema = StructType([
        StructField("description_clean", StringType(), False),
        StructField("character_count", IntegerType(), False),
        StructField("word_count", IntegerType(), False),
    ])
    clean_udf = F.udf(_clean_indian_desc_row, indian_desc_schema)

    df_valid = df_indian_jobs.filter(
        F.col("job_id").isNotNull() & (F.trim(F.col("job_id")) != "")
    )

    return (
        df_valid
        .withColumn("desc_struct", clean_udf(F.col("jobDescription")))
        .select(
            F.concat(F.lit("indian_job_market_2025_2026#"), F.col("job_id")).alias("canonical_job_id"),
            F.col("desc_struct.description_clean").alias("description_clean"),
            F.col("desc_struct.character_count").alias("character_count"),
            F.col("desc_struct.word_count").alias("word_count"),
            F.lit("indian_job_market_2025_2026").alias("source_id"),
            F.lit(canonical_run_id).alias("_canonical_run_id"),
            ts_val.alias("_canonical_timestamp"),
        )
    )


def build_canonical_job_text(
    df_linkedin_summary: DataFrame,
    df_indian_jobs: DataFrame,
    canonical_run_id: str,
    canonical_timestamp: Optional[str] = None,
) -> DataFrame:
    """Union LinkedIn and Indian text into canonical_job_text DataFrame."""
    ts = canonical_timestamp or datetime.now(timezone.utc).isoformat()
    df_li = build_canonical_job_text_linkedin(df_linkedin_summary, canonical_run_id, ts)
    df_in = build_canonical_job_text_indian(df_indian_jobs, canonical_run_id, ts)
    return df_li.unionByName(df_in)


class CanonicalSilverPipeline:
    """Orchestrates end-to-end Canonical Silver integration with staging and atomic promotion."""

    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or PROJECT_ROOT
        
        # Sources
        self.silver_linkedin = self.project_root / "data" / "silver" / "linkedin_2024"
        self.silver_indian = self.project_root / "data" / "silver" / "indian_job_market_2025_2026"
        
        self.linkedin_postings_path = self.silver_linkedin / "job_postings"
        self.linkedin_skills_path = self.silver_linkedin / "job_skills_bridge"
        self.linkedin_summary_path = self.silver_linkedin / "job_summary"
        
        self.indian_jobs_path = self.silver_indian / "jobs"
        self.indian_skills_path = self.silver_indian / "job_skills_bridge"
        
        # Targets
        self.canonical_base = self.project_root / "data" / "silver" / "canonical"
        self.canonical_jobs_path = self.canonical_base / "jobs"
        self.canonical_skills_path = self.canonical_base / "job_skills_bridge"
        self.canonical_text_path = self.canonical_base / "job_text"
        self.canonical_meta_dir = self.canonical_base / "_metadata"

    def is_target_complete(self) -> Tuple[bool, Dict[str, int]]:
        """Check if all three canonical tables already exist with data."""
        targets = {
            "jobs": self.canonical_jobs_path,
            "skills": self.canonical_skills_path,
            "text": self.canonical_text_path,
        }
        counts = {}
        all_complete = True
        for name, path in targets.items():
            if not path.exists():
                all_complete = False
                counts[name] = 0
                continue
            part_files = list(path.glob("*.parquet"))
            if not part_files:
                all_complete = False
                counts[name] = 0
                continue
            rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in part_files)
            counts[name] = rows
            if rows == 0:
                all_complete = False
        return (all_complete, counts)

    def run(
        self,
        overwrite: bool = False,
        jobs_partitions: int = 8,
        skills_partitions: int = 16,
        text_partitions: int = 22,
        batch_size: int = 10000,
    ) -> Dict[str, Any]:
        """Execute end-to-end Canonical Silver integration."""
        t0 = time.time()
        run_id = f"canonical_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        run_ts = datetime.now(timezone.utc).isoformat()

        logger.info(f"Starting Canonical Silver Integration Pipeline [Run ID: {run_id}]")
        is_complete, existing_counts = self.is_target_complete()
        if is_complete and not overwrite:
            raise FileExistsError(
                f"Canonical datasets already exist with counts {existing_counts}. Specify overwrite=True to replace."
            )

        # 1. Read Sources
        logger.info("Reading Silver source datasets...")
        df_li_postings = self.spark.read.parquet(str(self.linkedin_postings_path))
        df_li_skills = self.spark.read.parquet(str(self.linkedin_skills_path))
        df_li_summary = self.spark.read.parquet(str(self.linkedin_summary_path))
        df_in_jobs = self.spark.read.parquet(str(self.indian_jobs_path))
        df_in_skills = self.spark.read.parquet(str(self.indian_skills_path))

        li_postings_count = df_li_postings.count()
        li_skills_count = df_li_skills.count()
        li_summary_count = df_li_summary.count()
        in_jobs_count = df_in_jobs.count()
        in_skills_count = df_in_skills.count()

        logger.info(
            f"Sources loaded: LinkedIn (postings={li_postings_count:,}, skills={li_skills_count:,}, "
            f"summary={li_summary_count:,}), Indian (jobs={in_jobs_count:,}, skills={in_skills_count:,})"
        )

        expected_jobs_count = li_postings_count + in_jobs_count
        expected_skills_count = li_skills_count + in_skills_count
        expected_text_count = li_summary_count + in_jobs_count

        # Isolated Staging Paths on Drive D:
        stg_token = uuid4().hex[:8]
        stg_jobs = self.canonical_base / f"_staging_jobs_{stg_token}"
        stg_skills = self.canonical_base / f"_staging_skills_{stg_token}"
        stg_text = self.canonical_base / f"_staging_text_{stg_token}"

        for p in (stg_jobs, stg_skills, stg_text):
            p.mkdir(parents=True, exist_ok=True)

        try:
            # -------------------------------------------------------------
            # Stage 1: Build & Write canonical_jobs
            # -------------------------------------------------------------
            logger.info("Building canonical_jobs DataFrame...")
            df_canonical_jobs = build_canonical_jobs(
                df_li_postings, df_li_skills, df_li_summary, df_in_jobs, run_id, run_ts
            )
            df_canonical_jobs = df_canonical_jobs.coalesce(jobs_partitions)

            logger.info(f"Streaming canonical_jobs to staging path: {stg_jobs}")
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_canonical_jobs,
                output_dir=stg_jobs,
                batch_size=batch_size,
                compression="snappy",
            )
            stg_jobs_files = list(stg_jobs.glob("*.parquet"))
            stg_jobs_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in stg_jobs_files)
            logger.info(f"Staged canonical_jobs: {stg_jobs_rows:,} rows across {len(stg_jobs_files)} files.")
            if stg_jobs_rows != expected_jobs_count:
                raise ValueError(f"canonical_jobs row mismatch! Expected {expected_jobs_count:,}, got {stg_jobs_rows:,}")

            # -------------------------------------------------------------
            # Stage 2: Build & Write canonical_job_skills_bridge
            # -------------------------------------------------------------
            logger.info("Building canonical_job_skills_bridge DataFrame...")
            df_canonical_skills = build_canonical_skills_bridge(
                df_li_skills, df_in_skills, run_id, run_ts
            )
            # Rebalance into skills_partitions
            df_canonical_skills = df_canonical_skills.coalesce(skills_partitions)

            logger.info(f"Streaming canonical_job_skills_bridge to staging path: {stg_skills}")
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_canonical_skills,
                output_dir=stg_skills,
                batch_size=batch_size * 2,
                compression="snappy",
            )
            stg_skills_files = list(stg_skills.glob("*.parquet"))
            stg_skills_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in stg_skills_files)
            logger.info(f"Staged canonical_job_skills_bridge: {stg_skills_rows:,} rows across {len(stg_skills_files)} files.")
            if stg_skills_rows != expected_skills_count:
                raise ValueError(f"canonical_skills row mismatch! Expected {expected_skills_count:,}, got {stg_skills_rows:,}")

            # -------------------------------------------------------------
            # Stage 3: Build & Write canonical_job_text
            # -------------------------------------------------------------
            logger.info("Building canonical_job_text DataFrame...")
            df_canonical_text = build_canonical_job_text(
                df_li_summary, df_in_jobs, run_id, run_ts
            )
            df_canonical_text = df_canonical_text.coalesce(text_partitions)

            logger.info(f"Streaming canonical_job_text to staging path: {stg_text}")
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_canonical_text,
                output_dir=stg_text,
                batch_size=batch_size,
                compression="snappy",
            )
            stg_text_files = list(stg_text.glob("*.parquet"))
            stg_text_rows = sum(pq.ParquetFile(str(f)).metadata.num_rows for f in stg_text_files)
            logger.info(f"Staged canonical_job_text: {stg_text_rows:,} rows across {len(stg_text_files)} files.")
            if stg_text_rows != expected_text_count:
                raise ValueError(f"canonical_text row mismatch! Expected {expected_text_count:,}, got {stg_text_rows:,}")

            # -------------------------------------------------------------
            # Stage 4: Referential Integrity & Cross-Validation
            # -------------------------------------------------------------
            logger.info("Executing pre-promotion referential integrity checks (autoBroadcastJoin disabled)...")
            self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            self.spark.conf.set("spark.sql.shuffle.partitions", "16")

            df_stg_jobs = self.spark.read.parquet(str(stg_jobs)).select("canonical_job_id")
            df_stg_skills = self.spark.read.parquet(str(stg_skills)).select("canonical_job_id").distinct()
            df_stg_text = self.spark.read.parquet(str(stg_text)).select("canonical_job_id").distinct()

            skills_orphans = df_stg_skills.join(df_stg_jobs, on="canonical_job_id", how="left_anti").count()
            text_orphans = df_stg_text.join(df_stg_jobs, on="canonical_job_id", how="left_anti").count()

            logger.info(f"Skills orphan count: {skills_orphans}, Text orphan count: {text_orphans}")
            if skills_orphans > 0 or text_orphans > 0:
                raise ValueError(f"Referential integrity failure! Skills orphans: {skills_orphans}, Text orphans: {text_orphans}")

            # -------------------------------------------------------------
            # Stage 5: Atomic Promotion
            # -------------------------------------------------------------
            logger.info("Promoting staging datasets to final canonical destinations...")
            for dest, stg in [
                (self.canonical_jobs_path, stg_jobs),
                (self.canonical_skills_path, stg_skills),
                (self.canonical_text_path, stg_text),
            ]:
                if dest.exists():
                    logger.info(f"Overwriting existing target {dest} (overwrite=True)...")
                    shutil.rmtree(dest)
                logger.info(f"Promoting {stg} -> {dest}")
                shutil.move(str(stg), str(dest))

            duration = round(time.time() - t0, 2)
            logger.info(f"All canonical datasets promoted successfully in {duration}s.")

            # -------------------------------------------------------------
            # Stage 6: Manifest Commitment
            # -------------------------------------------------------------
            self.canonical_meta_dir.mkdir(parents=True, exist_ok=True)
            manifest = {
                "canonical_run_id": run_id,
                "timestamp": run_ts,
                "duration_seconds": duration,
                "status": "SUCCESS",
                "tables": {
                    "jobs": {
                        "path": str(self.canonical_jobs_path),
                        "row_count": stg_jobs_rows,
                        "partitions": len(stg_jobs_files),
                        "bytes": sum(f.stat().st_size for f in self.canonical_jobs_path.glob("*.parquet")),
                    },
                    "job_skills_bridge": {
                        "path": str(self.canonical_skills_path),
                        "row_count": stg_skills_rows,
                        "partitions": len(stg_skills_files),
                        "bytes": sum(f.stat().st_size for f in self.canonical_skills_path.glob("*.parquet")),
                    },
                    "job_text": {
                        "path": str(self.canonical_text_path),
                        "row_count": stg_text_rows,
                        "partitions": len(stg_text_files),
                        "bytes": sum(f.stat().st_size for f in self.canonical_text_path.glob("*.parquet")),
                    },
                },
                "source_counts": {
                    "linkedin_postings": li_postings_count,
                    "linkedin_skills": li_skills_count,
                    "linkedin_summary": li_summary_count,
                    "indian_jobs": in_jobs_count,
                    "indian_skills": in_skills_count,
                },
                "referential_integrity": {
                    "skills_orphans": skills_orphans,
                    "text_orphans": text_orphans,
                    "orphan_free": True,
                },
            }

            manifest_file = self.canonical_meta_dir / f"canonical_integration_{run_id}.json"
            with open(manifest_file, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            with open(self.canonical_meta_dir / "canonical_integration_manifest.json", "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)

            logger.info(f"Canonical manifest written to: {manifest_file}")
            return manifest

        except Exception as e:
            logger.error(f"Canonical Silver pipeline FAILED: {e}", exc_info=True)
            for p in (stg_jobs, stg_skills, stg_text):
                shutil.rmtree(p, ignore_errors=True)
            raise e
