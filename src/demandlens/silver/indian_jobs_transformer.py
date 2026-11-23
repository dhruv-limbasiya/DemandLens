"""DemandLens - Indian Job Market Silver Transformation Pipeline.

Transforms raw Bronze Parquet data for the Indian Job Market Dataset 2025-2026
into curated, standardized, validated Silver datasets:
  1. Silver Jobs Table: data/silver/indian_job_market_2025_2026/jobs
  2. Silver Job-Skills Bridge Table: data/silver/indian_job_market_2025_2026/job_skills_bridge

Pillars:
  - Strict deduplication on primary key `jobId`.
  - Reusable Phase 6B cleaning utilities for text, skills, location, experience, salary, and freshness.
  - Preservation of original fields and raw data for provenance.
  - Safe staging, pre-promotion validation, and atomic promotion.
"""

from __future__ import annotations

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
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Ensure project root is available
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor
from src.demandlens.silver.experience_parser import (
    classify_experience_tier,
    parse_experience_range,
    validate_experience_bounds,
)
from src.demandlens.silver.freshness_classifier import classify_freshness
from src.demandlens.silver.location_cleaning import (
    clean_location_string,
    detect_remote,
)
from src.demandlens.silver.salary_parser import (
    detect_salary_periodicity,
    parse_numeric_salary,
    validate_salary_range,
)
from src.demandlens.silver.skill_normalization import normalize_skills
from src.demandlens.silver.text_cleaning import clean_text

logger = logging.getLogger("demandlens.silver.indian_jobs")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ==============================================================================
# Helper Transformation Functions (Python Worker Level)
# ==============================================================================

def _py_clean_text(val: Optional[str]) -> Optional[str]:
    return clean_text(val)


def _py_clean_location(val: Optional[str]) -> Optional[str]:
    return clean_location_string(val)


def _py_detect_remote(val: Optional[str]) -> Optional[bool]:
    return detect_remote(val)


def _py_parse_salary(
    raw_salary: Optional[str],
    raw_min: Optional[str],
    raw_max: Optional[str],
    raw_currency: Optional[str],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str], Optional[str], bool]:
    """Parse and validate salary attributes into structured tuple."""
    min_sal = parse_numeric_salary(raw_min)
    max_sal = parse_numeric_salary(raw_max)

    v_min, v_max, is_valid = validate_salary_range(min_sal, max_sal)
    if not is_valid:
        v_min, v_max = None, None

    midpoint = None
    if v_min is not None and v_max is not None:
        midpoint = round((v_min + v_max) / 2.0, 2)
    elif v_min is not None:
        midpoint = v_min
    elif v_max is not None:
        midpoint = v_max

    is_disclosed = midpoint is not None
    periodicity = detect_salary_periodicity(raw_salary) if raw_salary else None

    currency = None
    if is_disclosed:
        if raw_currency and raw_currency.strip():
            currency = raw_currency.strip().upper()
        else:
            currency = "INR"

    return (v_min, v_max, midpoint, currency, periodicity, is_valid)


def _py_parse_experience(
    raw_exp: Optional[str],
    raw_min_exp: Optional[str],
    raw_max_exp: Optional[str],
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Parse and validate experience into numeric bounds and seniority tier."""
    min_e = None
    max_e = None

    # First attempt numeric bounds from source numeric columns
    try:
        if raw_min_exp is not None and str(raw_min_exp).strip():
            min_e = float(raw_min_exp)
    except (ValueError, TypeError):
        pass

    try:
        if raw_max_exp is not None and str(raw_max_exp).strip():
            max_e = float(raw_max_exp)
    except (ValueError, TypeError):
        pass

    # Fall back to parsing raw experience text if numeric bounds are missing
    if min_e is None and max_e is None and raw_exp:
        p_min, p_max = parse_experience_range(raw_exp)
        min_e, max_e = p_min, p_max

    # Validate bounds
    v_min, v_max, is_valid = validate_experience_bounds(min_e, max_e)
    if not is_valid:
        v_min, v_max = None, None

    tier = classify_experience_tier(min_exp=v_min, max_exp=v_max, raw_level=raw_exp)
    return (v_min, v_max, tier)


def _py_normalize_skills(val: Optional[str]) -> List[str]:
    return normalize_skills(val, delimiter=",")


def _py_classify_freshness(val: Optional[str]) -> str:
    return classify_freshness(val)


# ==============================================================================
# Spark UDF Definitions
# ==============================================================================

_clean_text_udf = F.udf(_py_clean_text, StringType())
_clean_location_udf = F.udf(_py_clean_location, StringType())
_detect_remote_udf = F.udf(_py_detect_remote, BooleanType())

_salary_schema = StructType([
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("salary_midpoint", DoubleType(), True),
    StructField("salary_currency", StringType(), True),
    StructField("salary_period", StringType(), True),
    StructField("salary_is_valid", BooleanType(), False),
])
_parse_salary_udf = F.udf(_py_parse_salary, _salary_schema)

_exp_schema = StructType([
    StructField("experience_min_years", DoubleType(), True),
    StructField("experience_max_years", DoubleType(), True),
    StructField("seniority_tier", StringType(), True),
])
_parse_experience_udf = F.udf(_py_parse_experience, _exp_schema)

_normalize_skills_udf = F.udf(_py_normalize_skills, ArrayType(StringType()))
_classify_freshness_udf = F.udf(_py_classify_freshness, StringType())


# ==============================================================================
# Core Transformation Functions
# ==============================================================================

def deduplicate_indian_jobs(df: DataFrame) -> Tuple[DataFrame, int]:
    """Deterministically deduplicate Indian job postings on primary key `jobId`.

    Selection rule:
      1. Rank by completeness: rows with fewer null fields are preferred.
      2. If tied, pick the row with the latest `_ingestion_timestamp`.
      3. If still tied, pick deterministic order.

    Returns:
        (deduplicated_df, duplicates_removed_count)
    """
    initial_count = df.count()

    # Calculate non-null score across all columns to pick the richest row
    null_expr = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in df.columns)
    df_scored = df.withColumn("_completeness_score", null_expr)

    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("jobId").orderBy(
        F.desc("_completeness_score"),
        F.desc("_ingestion_timestamp")
    )

    df_ranked = df_scored.withColumn("_row_num", F.row_number().over(window_spec))
    df_dedup = df_ranked.filter(F.col("_row_num") == 1).drop("_completeness_score", "_row_num")

    final_count = df_dedup.count()
    dupes_removed = initial_count - final_count

    logger.info(f"Deduplication complete: {initial_count:,} -> {final_count:,} ({dupes_removed:,} duplicates removed)")
    return df_dedup, dupes_removed


def transform_indian_jobs(
    df_bronze: DataFrame,
    silver_run_id: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Execute complete Silver transformation for the Indian Job Market Dataset.

    Args:
        df_bronze: Raw Bronze DataFrame with 21 columns.
        silver_run_id: Unique execution identifier.

    Returns:
        (df_silver_jobs, df_silver_bridge)
    """
    run_id = silver_run_id or f"silver_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    silver_timestamp = datetime.now(timezone.utc)

    # 1. Deduplicate by primary key jobId
    df_dedup, _ = deduplicate_indian_jobs(df_bronze)

    # 2. Text Cleaning on core attributes
    df_cleaned = (
        df_dedup
        .withColumn("title_clean", _clean_text_udf(F.col("title")))
        .withColumn("company_name_clean", _clean_text_udf(F.col("companyName")))
        .withColumn("location_clean", _clean_location_udf(F.col("location")))
        .withColumn("is_remote", _detect_remote_udf(F.col("location")))
        .withColumn("country", F.lit("India"))
    )

    # 3. Parse Salary
    df_sal = df_cleaned.withColumn(
        "_sal_struct",
        _parse_salary_udf(
            F.col("salary"),
            F.col("minimumSalary"),
            F.col("maximumSalary"),
            F.col("currency"),
        )
    ).select(
        "*",
        F.col("_sal_struct.salary_min").alias("salary_min"),
        F.col("_sal_struct.salary_max").alias("salary_max"),
        F.col("_sal_struct.salary_midpoint").alias("salary_midpoint"),
        F.col("_sal_struct.salary_currency").alias("salary_currency"),
        F.col("_sal_struct.salary_period").alias("salary_period"),
        F.col("_sal_struct.salary_is_valid").alias("salary_is_valid"),
    ).drop("_sal_struct")

    # 4. Parse Experience
    df_exp = df_sal.withColumn(
        "_exp_struct",
        _parse_experience_udf(
            F.col("experience"),
            F.col("minimumExperience"),
            F.col("maximumExperience"),
        )
    ).select(
        "*",
        F.col("_exp_struct.experience_min_years").alias("experience_min_years"),
        F.col("_exp_struct.experience_max_years").alias("experience_max_years"),
        F.col("_exp_struct.seniority_tier").alias("seniority_tier"),
    ).drop("_exp_struct")

    # 5. Skills Normalization (Array in Jobs table)
    df_skills = (
        df_exp
        .withColumn("skills_normalized", _normalize_skills_udf(F.col("tagsAndSkills")))
        .withColumn("skill_count", F.size(F.col("skills_normalized")))
        .withColumn("has_skills", F.when(F.col("skill_count") > 0, True).otherwise(False))
    )

    # 6. Freshness Classification
    df_fresh = df_skills.withColumn(
        "freshness_tier",
        _classify_freshness_udf(F.col("jobUploaded"))
    )

    # 7. Add Silver Metadata & Select Standardized Column Layout
    df_silver_jobs = (
        df_fresh
        .withColumn("job_id", F.col("jobId"))  # Standardized key
        .withColumn("_silver_run_id", F.lit(run_id))
        .withColumn("_silver_timestamp", F.lit(silver_timestamp))
        .select(
            # Standardized primary attributes
            "job_id",
            "title_clean",
            "company_name_clean",
            "location_clean",
            "country",
            "is_remote",
            "salary_min",
            "salary_max",
            "salary_midpoint",
            "salary_currency",
            "salary_period",
            "salary_is_valid",
            "experience_min_years",
            "experience_max_years",
            "seniority_tier",
            "skills_normalized",
            "skill_count",
            "has_skills",
            "freshness_tier",
            # Original attributes preserved for auditability
            "title",
            "jobId",
            "currency",
            "jobUploaded",
            "companyName",
            "tagsAndSkills",
            "experience",
            "salary",
            "location",
            "companyId",
            "ReviewsCount",
            "AggregateRating",
            "jobDescription",
            "minimumSalary",
            "maximumSalary",
            "minimumExperience",
            "maximumExperience",
            # Bronze & Silver Provenance
            "_source_id",
            "_source_file",
            "_ingestion_timestamp",
            "_ingestion_run_id",
            "_silver_run_id",
            "_silver_timestamp",
        )
    )

    # 8. Create Dedicated Exploded Job-Skills Bridge Table
    df_silver_bridge = (
        df_silver_jobs
        .filter(F.col("has_skills") == True)
        .select(
            F.col("job_id"),
            F.explode(F.col("skills_normalized")).alias("skill"),
            F.col("_source_id"),
            F.col("_silver_run_id"),
            F.col("_silver_timestamp"),
        )
        .filter(F.col("skill").isNotNull() & (F.trim(F.col("skill")) != ""))
        .distinct()  # Ensure strict (job_id, skill) uniqueness
    )

    return df_silver_jobs, df_silver_bridge


class IndianSilverPipeline:
    """Manages the execution, staging, validation, and promotion of Indian Silver data."""

    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or PROJECT_ROOT
        self.bronze_path = self.project_root / "data" / "bronze" / "indian_job_market_2025_2026" / "jobs"
        self.silver_base = self.project_root / "data" / "silver" / "indian_job_market_2025_2026"
        self.silver_jobs_path = self.silver_base / "jobs"
        self.silver_bridge_path = self.silver_base / "job_skills_bridge"
        self.meta_dir = self.silver_base / "_metadata"

    def run(self, overwrite: bool = False) -> Dict[str, Any]:
        """Execute the end-to-end Silver pipeline with staging and validation."""
        t0 = time.time()
        run_id = f"silver_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"

        logger.info(f"Starting Indian Job Market Silver Pipeline [Run: {run_id}]")
        logger.info(f"  Bronze Source: {self.bronze_path}")
        logger.info(f"  Silver Jobs:   {self.silver_jobs_path}")
        logger.info(f"  Silver Bridge: {self.silver_bridge_path}")

        # Check existing output
        if self.silver_jobs_path.exists() and not overwrite:
            raise FileExistsError(
                f"Silver jobs output already exists at {self.silver_jobs_path}. Specify overwrite=True to replace."
            )

        # 1. Read Bronze data
        logger.info("Reading Bronze Parquet data...")
        df_bronze = self.spark.read.parquet(str(self.bronze_path))
        bronze_count = df_bronze.count()
        logger.info(f"Loaded {bronze_count:,} Bronze records.")

        # 2. Transform to Silver
        logger.info("Applying Silver cleaning, deduplication, and schema standardization...")
        df_silver_jobs, df_silver_bridge = transform_indian_jobs(df_bronze, silver_run_id=run_id)

        # Re-partition cleanly (2 partitions for jobs, 2 partitions for bridge)
        df_silver_jobs = df_silver_jobs.coalesce(2)
        df_silver_bridge = df_silver_bridge.coalesce(2)

        # 3. Staging directories on D:
        staging_jobs = self.silver_base / f"_staging_jobs_{uuid4().hex[:8]}"
        staging_bridge = self.silver_base / f"_staging_bridge_{uuid4().hex[:8]}"

        try:
            logger.info(f"Writing Silver jobs to staging: {staging_jobs}")
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_silver_jobs,
                output_dir=staging_jobs,
                batch_size=10000,
                compression="snappy",
            )

            logger.info(f"Writing Silver skills bridge to staging: {staging_bridge}")
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_silver_bridge,
                output_dir=staging_bridge,
                batch_size=10000,
                compression="snappy",
            )

            # 4. Validate staging
            logger.info("Validating staging datasets before promotion...")
            stg_jobs_df = self.spark.read.parquet(str(staging_jobs))
            stg_bridge_df = self.spark.read.parquet(str(staging_bridge))

            jobs_count = stg_jobs_df.count()
            bridge_count = stg_bridge_df.count()
            dupe_job_ids = jobs_count - stg_jobs_df.select("job_id").distinct().count()

            logger.info(f"Staging validated: jobs_count={jobs_count:,}, bridge_count={bridge_count:,}, dupe_job_ids={dupe_job_ids}")

            if dupe_job_ids != 0:
                raise ValueError(f"Staging jobs contains {dupe_job_ids} duplicate job_ids!")
            if jobs_count != 97679:
                raise ValueError(f"Expected 97,679 deduplicated jobs, got {jobs_count:,}!")

            # 5. Atomic Promotion
            logger.info("Promoting staging to final destinations...")
            self.silver_base.mkdir(parents=True, exist_ok=True)

            if self.silver_jobs_path.exists():
                shutil.rmtree(self.silver_jobs_path, ignore_errors=True)
            staging_jobs.rename(self.silver_jobs_path)

            if self.silver_bridge_path.exists():
                shutil.rmtree(self.silver_bridge_path, ignore_errors=True)
            staging_bridge.rename(self.silver_bridge_path)

            duration = round(time.time() - t0, 2)
            logger.info(f"Silver promotion SUCCESS in {duration:.1f}s.")

            # 6. Save Manifest
            manifest = {
                "silver_run_id": run_id,
                "source_id": "indian_job_market_2025_2026",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": duration,
                "bronze_source_path": str(self.bronze_path),
                "bronze_row_count": bronze_count,
                "silver_jobs_row_count": jobs_count,
                "duplicates_removed": bronze_count - jobs_count,
                "silver_bridge_row_count": bridge_count,
                "silver_jobs_path": str(self.silver_jobs_path),
                "silver_bridge_path": str(self.silver_bridge_path),
                "status": "SUCCESS",
            }
            self.meta_dir.mkdir(parents=True, exist_ok=True)
            manifest_file = self.meta_dir / f"silver_{run_id}.json"
            with open(manifest_file, "w", encoding="utf-8") as mf:
                import json
                json.dump(manifest, mf, indent=2)

            return manifest

        finally:
            # Cleanup any remaining staging folders
            for p in [staging_jobs, staging_bridge]:
                if p.exists():
                    shutil.rmtree(p, ignore_errors=True)
