"""DemandLens - LinkedIn Job Postings Silver Transformation Pipeline.

Transforms raw Bronze Parquet data for LinkedIn 2024 `job_postings`
into curated, standardized, validated Silver dataset:
  Destination: data/silver/linkedin_2024/job_postings

Core Transformation Invariants:
  1. Record count fidelity: Preserves all 1,348,454 records without deduplication.
  2. Conservative cleaning: Cleans job_title, company, and location while preserving genuine NULLs.
  3. Strict workplace classification:
       - Remote -> REMOTE (is_remote=True, is_hybrid=False)
       - Hybrid -> HYBRID (is_remote=False, is_hybrid=True)
       - Onsite -> ON_SITE (is_remote=False, is_hybrid=False)
       - Unknown -> UNSPECIFIED (is_remote=False, is_hybrid=False)
       Ensures hybrid roles are never classified as fully remote.
  4. Country code standardization: Maps confirmed search_country values to ISO alpha-2 codes.
  5. Date parsing: Parses first_seen to DateType and last_processed_time to TimestampType.
  6. Seniority harmonization: Maps confirmed job_level values into standardized tiers.
  7. No artificial compensation fields: Omit synthetic salary columns.
  8. Full provenance & non-destructive layout: Preserves all 14 original columns and 4 Bronze metadata columns.
  9. Safe staging, validation, and atomic promotion via hybrid PyArrow Parquet writer.
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
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

# Ensure project root is available
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor
from src.demandlens.silver.location_cleaning import clean_location_string
from src.demandlens.silver.text_cleaning import clean_text

logger = logging.getLogger("demandlens.silver.linkedin_postings")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ==============================================================================
# Helper Transformation Functions (Python Level - for standalone unit testing)
# ==============================================================================

COUNTRY_ISO_MAP = {
    "united states": "US",
    "united kingdom": "GB",
    "canada": "CA",
    "australia": "AU",
}

SENIORITY_MAP = {
    "mid senior": "MID_SENIOR",
    "associate": "ASSOCIATE",
    "entry level": "ENTRY",
    "internship": "INTERNSHIP",
    "director": "DIRECTOR",
    "executive": "EXECUTIVE",
}


def _py_clean_title(val: Optional[str]) -> Optional[str]:
    """Clean job title conservatively, preserving genuine NULLs."""
    return clean_text(val)


def _py_clean_company(val: Optional[str]) -> Optional[str]:
    """Clean company name conservatively, preserving genuine NULLs."""
    return clean_text(val)


def _py_clean_location(val: Optional[str]) -> Optional[str]:
    """Clean location string conservatively, preserving genuine NULLs."""
    return clean_location_string(val)


def _py_classify_workplace(job_type: Optional[str]) -> Tuple[str, bool, bool]:
    """Classify workplace arrangement strictly from source `job_type`.

    Rules:
      - Remote -> ('REMOTE', True, False)
      - Hybrid -> ('HYBRID', False, True)
      - Onsite -> ('ON_SITE', False, False)
      - Unknown / other -> ('UNSPECIFIED', False, False)

    Guarantees hybrid is never marked as fully remote (is_remote is False for Hybrid).
    """
    if job_type is None or not str(job_type).strip():
        return ("UNSPECIFIED", False, False)

    clean_jt = str(job_type).strip().lower()
    if clean_jt == "remote":
        return ("REMOTE", True, False)
    elif clean_jt == "hybrid":
        return ("HYBRID", False, True)
    elif clean_jt in ("onsite", "on-site", "on site"):
        return ("ON_SITE", False, False)
    else:
        return ("UNSPECIFIED", False, False)


def _py_map_country_code(country: Optional[str]) -> Optional[str]:
    """Map validated country name to ISO alpha-2 code."""
    if country is None or not str(country).strip():
        return None
    c_lower = str(country).strip().lower()
    return COUNTRY_ISO_MAP.get(c_lower, "OTHER")


def _py_harmonize_seniority(level: Optional[str]) -> str:
    """Harmonize categorical job level to standardized seniority tier."""
    if level is None or not str(level).strip():
        return "UNSPECIFIED"
    lvl_lower = str(level).strip().lower()
    return SENIORITY_MAP.get(lvl_lower, "UNSPECIFIED")


# ==============================================================================
# Core Transformation Logic (Native Spark SQL Expressions for High Performance)
# ==============================================================================

def transform_linkedin_postings(
    df_bronze: DataFrame,
    silver_run_id: Optional[str] = None,
) -> DataFrame:
    """Transform Bronze LinkedIn job postings to Silver schema.

    Uses native Spark SQL Catalyst expressions to maximize memory efficiency
    and avoid Python worker socket serialization overhead across 1.35M rows.

    Preserves 100% of rows (zero deduplication).
    Adds standardized fields while preserving all original columns and provenance.

    Args:
        df_bronze: Raw Bronze DataFrame (18 columns: 14 original + 4 metadata).
        silver_run_id: Unique Silver execution identifier.

    Returns:
        Curated Silver DataFrame.
    """
    run_id = silver_run_id or f"silver_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    silver_timestamp = datetime.now(timezone.utc)

    # 1. Conservative text and location cleaning (preserving genuine NULLs)
    # Replaces repeated spaces with single space and trims edges
    title_clean_col = F.when(
        F.col("job_title").isNotNull(),
        F.trim(F.regexp_replace(F.col("job_title"), r"\s+", " "))
    ).otherwise(None)

    company_clean_col = F.when(
        F.col("company").isNotNull(),
        F.trim(F.regexp_replace(F.col("company"), r"\s+", " "))
    ).otherwise(None)

    location_clean_col = F.when(
        F.col("job_location").isNotNull(),
        F.regexp_replace(
            F.trim(F.regexp_replace(F.col("job_location"), r"\s+", " ")),
            r"^[ \t\r\n,;-]+|[ \t\r\n,;-]+$",
            ""
        )
    ).otherwise(None)

    # 2. Strict Workplace Classification using native expressions
    clean_jt = F.lower(F.trim(F.col("job_type")))
    workplace_type_col = (
        F.when(clean_jt == "remote", "REMOTE")
        .when(clean_jt == "hybrid", "HYBRID")
        .when(clean_jt.isin("onsite", "on-site", "on site"), "ON_SITE")
        .otherwise("UNSPECIFIED")
    )

    is_remote_col = F.when(workplace_type_col == "REMOTE", True).otherwise(False)
    is_hybrid_col = F.when(workplace_type_col == "HYBRID", True).otherwise(False)

    # 3. Country code mapping & Seniority harmonization
    clean_country = F.lower(F.trim(F.col("search_country")))
    country_code_col = (
        F.when(clean_country == "united states", "US")
        .when(clean_country == "united kingdom", "GB")
        .when(clean_country == "canada", "CA")
        .when(clean_country == "australia", "AU")
        .when(F.col("search_country").isNotNull() & (F.trim(F.col("search_country")) != ""), "OTHER")
        .otherwise(None)
    )

    clean_level = F.lower(F.trim(F.col("job_level")))
    seniority_tier_col = (
        F.when(clean_level == "mid senior", "MID_SENIOR")
        .when(clean_level == "associate", "ASSOCIATE")
        .when(clean_level == "entry level", "ENTRY")
        .when(clean_level == "internship", "INTERNSHIP")
        .when(clean_level == "director", "DIRECTOR")
        .when(clean_level == "executive", "EXECUTIVE")
        .otherwise("UNSPECIFIED")
    )

    # 4. Conservative date and timestamp parsing
    first_seen_date_col = F.to_date(F.col("first_seen"), "yyyy-MM-dd")
    last_processed_timestamp_col = F.to_timestamp(F.col("last_processed_time"))

    # Apply all expressions simultaneously in a single Catalyst projection
    df_silver = (
        df_bronze
        .withColumn("title_clean", title_clean_col)
        .withColumn("company_clean", company_clean_col)
        .withColumn("location_clean", location_clean_col)
        .withColumn("workplace_type", workplace_type_col)
        .withColumn("is_remote", is_remote_col)
        .withColumn("is_hybrid", is_hybrid_col)
        .withColumn("country_code", country_code_col)
        .withColumn("seniority_tier", seniority_tier_col)
        .withColumn("first_seen_date", first_seen_date_col)
        .withColumn("last_processed_timestamp", last_processed_timestamp_col)
        .withColumn("_silver_run_id", F.lit(run_id))
        .withColumn("_silver_timestamp", F.lit(silver_timestamp))
        .select(
            # Standardized primary attributes
            "job_link",
            "title_clean",
            "company_clean",
            "location_clean",
            "country_code",
            "workplace_type",
            "is_remote",
            "is_hybrid",
            "seniority_tier",
            "first_seen_date",
            "last_processed_timestamp",
            # Original source attributes preserved for auditability
            "job_title",
            "company",
            "job_location",
            "first_seen",
            "last_processed_time",
            "search_city",
            "search_country",
            "search_position",
            "job_level",
            "job_type",
            "got_summary",
            "got_ner",
            "is_being_worked",
            # Bronze Provenance
            "_source_id",
            "_source_file",
            "_ingestion_timestamp",
            "_ingestion_run_id",
            # Silver Provenance
            "_silver_run_id",
            "_silver_timestamp",
        )
    )

    return df_silver


class LinkedInPostingsSilverPipeline:
    """Manages execution, staging, validation, and promotion for LinkedIn Job Postings Silver."""

    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or PROJECT_ROOT
        self.bronze_path = self.project_root / "data" / "bronze" / "linkedin_2024" / "job_postings"
        self.silver_base = self.project_root / "data" / "silver" / "linkedin_2024"
        self.silver_output_path = self.silver_base / "job_postings"
        self.meta_dir = self.silver_base / "_metadata"

    def run(self, overwrite: bool = False, target_partitions: int = 4) -> Dict[str, Any]:
        """Execute end-to-end Silver pipeline with staging, validation, and manifest generation."""
        t0 = time.time()
        run_id = f"silver_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"

        logger.info(f"Starting LinkedIn Job Postings Silver Pipeline [Run: {run_id}]")
        logger.info(f"  Bronze Source: {self.bronze_path}")
        logger.info(f"  Silver Output: {self.silver_output_path}")

        if not self.bronze_path.exists():
            raise FileNotFoundError(f"Bronze input directory missing: {self.bronze_path}")

        if self.silver_output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Silver output already exists at {self.silver_output_path}. Specify overwrite=True to replace."
            )

        # 1. Read Bronze data
        logger.info("Reading Bronze Parquet data...")
        df_bronze = self.spark.read.parquet(str(self.bronze_path))
        bronze_count = df_bronze.count()
        logger.info(f"Loaded {bronze_count:,} Bronze LinkedIn posting records.")

        if bronze_count != 1348454:
            raise ValueError(f"Expected 1,348,454 Bronze records, found {bronze_count:,}")

        # 2. Transform using pure Catalyst expressions
        logger.info("Applying Silver cleaning, workplace classification, and standardization...")
        df_silver = transform_linkedin_postings(df_bronze, silver_run_id=run_id)

        # Coalesce into optimal target partitions (4 partitions ~30-40 MB each)
        if target_partitions and target_partitions > 0:
            df_silver = df_silver.coalesce(target_partitions)

        # 3. Write to temporary staging directory on D:
        staging_dir = self.silver_base / f"_staging_job_postings_{uuid4().hex[:8]}"
        logger.info(f"Writing Parquet to staging path via PyArrow: {staging_dir}")
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)

        try:
            BronzeIngestor.write_dataframe_to_parquet_pyarrow(
                df=df_silver,
                output_dir=staging_dir,
                batch_size=10000,
                compression="snappy",
            )

            # 4. Validate staging output before promotion
            logger.info("Validating staging dataset before promotion...")
            df_stg = self.spark.read.parquet(str(staging_dir))
            stg_count = df_stg.count()
            logger.info(f"Staging row count: {stg_count:,} (Expected: {bronze_count:,})")

            if stg_count != bronze_count:
                raise ValueError(
                    f"Staging row count mismatch! Expected {bronze_count:,}, got {stg_count:,}"
                )

            # Check primary key uniqueness
            distinct_pks = df_stg.select("job_link").distinct().count()
            if distinct_pks != stg_count:
                raise ValueError(
                    f"Duplicate job_link detected in staging! Distinct: {distinct_pks:,}, Total: {stg_count:,}"
                )

            # Check null primary keys
            null_pks = df_stg.filter(F.col("job_link").isNull() | (F.trim(F.col("job_link")) == "")).count()
            if null_pks != 0:
                raise ValueError(f"Null or empty job_link found in staging: {null_pks}")

            # Validate workplace counts
            wp_counts = {r["workplace_type"]: r["count"] for r in df_stg.groupBy("workplace_type").count().collect()}
            logger.info(f"Workplace type counts: {wp_counts}")
            if wp_counts.get("ON_SITE") != 1337633:
                raise ValueError(f"Unexpected ON_SITE count: {wp_counts.get('ON_SITE')}")
            if wp_counts.get("HYBRID") != 6562:
                raise ValueError(f"Unexpected HYBRID count: {wp_counts.get('HYBRID')}")
            if wp_counts.get("REMOTE") != 4259:
                raise ValueError(f"Unexpected REMOTE count: {wp_counts.get('REMOTE')}")

            # Verify hybrid is never classified as remote
            hybrid_as_remote = df_stg.filter((F.col("is_hybrid") == True) & (F.col("is_remote") == True)).count()
            if hybrid_as_remote != 0:
                raise ValueError(f"Violated invariant: {hybrid_as_remote} hybrid records classified as remote!")

            # 5. Atomic Promotion
            logger.info(f"Promoting staging dataset to final destination: {self.silver_output_path}")
            self.silver_base.mkdir(parents=True, exist_ok=True)

            if self.silver_output_path.exists():
                backup_dir = self.silver_base / f"_backup_job_postings_{uuid4().hex[:6]}"
                self.silver_output_path.rename(backup_dir)
                staging_dir.rename(self.silver_output_path)
                shutil.rmtree(backup_dir, ignore_errors=True)
            else:
                staging_dir.rename(self.silver_output_path)

            duration = round(time.time() - t0, 2)
            logger.info(f"Silver promotion SUCCESS in {duration:.1f}s.")

            # 6. Generate Provenance Manifest
            manifest = {
                "silver_run_id": run_id,
                "source_id": "linkedin_2024",
                "table_name": "job_postings",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": duration,
                "bronze_source_path": str(self.bronze_path),
                "bronze_row_count": bronze_count,
                "silver_row_count": stg_count,
                "duplicates_removed": 0,
                "silver_output_path": str(self.silver_output_path),
                "partitions": target_partitions,
                "compression": "snappy",
                "workplace_counts": wp_counts,
                "status": "SUCCESS",
            }

            self.meta_dir.mkdir(parents=True, exist_ok=True)
            manifest_file = self.meta_dir / f"silver_job_postings_{run_id}.json"
            with open(manifest_file, "w", encoding="utf-8") as mf:
                json.dump(manifest, mf, indent=2)

            return manifest

        finally:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
