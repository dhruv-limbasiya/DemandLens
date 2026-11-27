"""DemandLens - Silver Layer: LinkedIn Job Summary Transformer.

Transforms Bronze LinkedIn job_summary records into a standardized, cleaned
Silver text dimension with HTML stripping, entity unescaping, paragraph
preservation, word and character counts, provenance tracking, and atomic promotion.

Phase: 6D.3
Source: data/bronze/linkedin_2024/job_summary
Target: data/silver/linkedin_2024/job_summary
"""

from __future__ import annotations

import json
import logging
import os
import re
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
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.demandlens.ingestion.bronze_ingestor import BronzeIngestor
from src.demandlens.silver.text_cleaning import strip_html_artifacts, normalize_multiline_text

logger = logging.getLogger("demandlens.silver.linkedin_summary")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def clean_job_summary(summary_raw: Optional[str]) -> Tuple[Optional[str], int, int, bool]:
    """Clean and normalize raw job summary text and compute derived lexical metrics.

    Rules applied:
    1. Returns (None, 0, 0, False) for None, empty, or whitespace-only inputs.
    2. Translates structural HTML tags (<br>, <p>, <li>, <div>, <tr>, <h1-6>) into line breaks.
    3. Strips remaining inline HTML markup.
    4. Decodes HTML entities (&amp;, &quot;, &#39;, &lt;, &gt;, &nbsp;, etc.) after tag stripping
       so that escaped brackets like &lt;exp&gt; are not accidentally deleted as tags.
    5. Normalizes line endings (CRLF/CR -> LF), collapses inline multiple spaces, removes
       excess space before punctuation, and collapses 3+ consecutive newlines into paragraph boundaries.
    6. Preserves technical tokens (C++, C#, .NET, Node.js, SQL), punctuation, and financial figures.
    7. Computes deterministic character_count, word_count, and has_summary.

    Args:
        summary_raw: Raw job summary string from Bronze.

    Returns:
        Tuple of (summary_clean, character_count, word_count, has_summary).
    """
    if summary_raw is None or not summary_raw.strip():
        return (None, 0, 0, False)

    import html

    # 1. Structural tags to newlines
    converted = re.sub(r"(?i)<br\s*/?>", "\n", summary_raw)
    converted = re.sub(r"(?i)</?(p|li|div|tr|h[1-6]|ul|ol|table|thead|tbody)\b[^>]*>", "\n", converted)

    # 2. Inline tags to space
    stripped = re.sub(r"<[^>]+>", " ", converted)

    # 3. Decode HTML entities and replace non-breaking space
    decoded = html.unescape(stripped)
    if "&amp;" in decoded:
        decoded = html.unescape(decoded)
    decoded = decoded.replace("\xa0", " ")
    # Handle any decoded structural tags from encoded entities (e.g. &lt;br/&gt;)
    decoded = re.sub(r"(?i)</?(?:br|p|li|div|tr|h[1-6]|ul|ol|table)\b[^>]*>", "\n", decoded)

    # 4. Multiline normalization
    decoded = decoded.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in decoded.split("\n"):
        cl = re.sub(r"[ \t]+", " ", line).strip()
        cl = re.sub(r"\s+([,!?;:])", r"\1", cl)
        cl = re.sub(r"\s+\.(?=\s|$)", ".", cl)
        lines.append(cl)

    cleaned = "\n".join(lines)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()

    if not cleaned:
        return (None, 0, 0, False)

    char_count = len(cleaned)
    # Deterministic word count via whitespace splitting
    words = cleaned.split()
    word_count = len(words)
    has_summary = bool(char_count > 0)

    return (cleaned, char_count, word_count, has_summary)


# Schema for the UDF output struct
SUMMARY_STRUCT_SCHEMA = StructType([
    StructField("summary_clean", StringType(), True),
    StructField("character_count", IntegerType(), False),
    StructField("word_count", IntegerType(), False),
    StructField("has_summary", BooleanType(), False),
])


def transform_linkedin_job_summary(
    df_bronze: DataFrame,
    silver_run_id: str,
    silver_timestamp: Optional[str] = None,
) -> DataFrame:
    """Transform Bronze LinkedIn job_summary into standardized Silver DataFrame.

    Schema:
    - job_link: string (Non-null Primary Key)
    - summary_clean: string (Cleaned text, paragraphs preserved)
    - character_count: integer
    - word_count: integer
    - has_summary: boolean
    - _source_id: string ('linkedin_2024')
    - _source_file: string ('job_summary.csv')
    - _ingestion_timestamp: timestamp
    - _ingestion_run_id: string
    - _silver_run_id: string
    - _silver_timestamp: timestamp

    Args:
        df_bronze: Bronze Spark DataFrame.
        silver_run_id: Execution run ID.
        silver_timestamp: Optional ISO 8601 UTC timestamp.

    Returns:
        Transformed Silver DataFrame.
    """
    ts = silver_timestamp or datetime.now(timezone.utc).isoformat()

    clean_udf = F.udf(clean_job_summary, SUMMARY_STRUCT_SCHEMA)

    # 1. Filter out null/empty job_link (primary key invariant)
    df_valid = df_bronze.filter(
        F.col("job_link").isNotNull() & (F.trim(F.col("job_link")) != "")
    )

    # 2. Apply cleaning UDF and extract struct fields
    df_transformed = (
        df_valid
        .withColumn("cleaned_struct", clean_udf(F.col("job_summary")))
        .withColumn("_silver_run_id", F.lit(silver_run_id))
        .withColumn("_silver_timestamp", F.to_timestamp(F.lit(ts)))
        .select(
            F.col("job_link"),
            F.col("cleaned_struct.summary_clean").alias("summary_clean"),
            F.col("cleaned_struct.character_count").alias("character_count"),
            F.col("cleaned_struct.word_count").alias("word_count"),
            F.col("cleaned_struct.has_summary").alias("has_summary"),
            F.col("_source_id"),
            F.col("_source_file"),
            F.col("_ingestion_timestamp"),
            F.col("_ingestion_run_id"),
            F.col("_silver_run_id"),
            F.col("_silver_timestamp"),
        )
    )

    return df_transformed


def _write_silver_summary_partition_worker(
    part_idx: int,
    iterator,
    output_dir_str: str,
    silver_run_id: str,
    silver_timestamp_str: str,
    batch_size: int = 10000,
    compression: str = "snappy",
) -> List[int]:
    """Partition worker executing streaming text cleansing and PyArrow Parquet emission."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pathlib import Path
    from datetime import datetime, timezone

    out_dir = Path(output_dir_str)
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"part-{part_idx:05d}.parquet"

    silver_schema = pa.schema([
        ("job_link", pa.string()),
        ("summary_clean", pa.string()),
        ("character_count", pa.int32()),
        ("word_count", pa.int32()),
        ("has_summary", pa.bool_()),
        ("_source_id", pa.string()),
        ("_source_file", pa.string()),
        ("_ingestion_timestamp", pa.timestamp("us", tz="UTC")),
        ("_ingestion_run_id", pa.string()),
        ("_silver_run_id", pa.string()),
        ("_silver_timestamp", pa.timestamp("us", tz="UTC")),
    ])

    writer = None
    row_count = 0
    batch_records = []

    ts_dt = datetime.fromisoformat(silver_timestamp_str)

    try:
        for row in iterator:
            link = row.job_link
            if link is None or not str(link).strip():
                continue

            raw_summary = row.job_summary
            cleaned_text, c_count, w_count, has_sum = clean_job_summary(raw_summary)

            batch_records.append({
                "job_link": link,
                "summary_clean": cleaned_text,
                "character_count": c_count,
                "word_count": w_count,
                "has_summary": has_sum,
                "_source_id": row._source_id or "linkedin_2024",
                "_source_file": row._source_file or "job_summary.csv",
                "_ingestion_timestamp": row._ingestion_timestamp,
                "_ingestion_run_id": row._ingestion_run_id,
                "_silver_run_id": silver_run_id,
                "_silver_timestamp": ts_dt,
            })

            if len(batch_records) >= batch_size:
                table = pa.Table.from_pylist(batch_records, schema=silver_schema)
                if writer is None:
                    writer = pq.ParquetWriter(str(file_path), silver_schema, compression=compression)
                writer.write_table(table)
                row_count += len(batch_records)
                batch_records.clear()

        if batch_records:
            table = pa.Table.from_pylist(batch_records, schema=silver_schema)
            if writer is None:
                writer = pq.ParquetWriter(str(file_path), silver_schema, compression=compression)
            writer.write_table(table)
            row_count += len(batch_records)
            batch_records.clear()

        if writer is None:
            writer = pq.ParquetWriter(str(file_path), silver_schema, compression=compression)

    finally:
        if writer is not None:
            writer.close()

    return [row_count]


class LinkedInSummarySilverPipeline:
    """Manages staging, validation, promotion, and metadata for LinkedIn Job Summary Silver."""

    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or PROJECT_ROOT
        self.bronze_summary_path = self.project_root / "data" / "bronze" / "linkedin_2024" / "job_summary"
        self.silver_base = self.project_root / "data" / "silver" / "linkedin_2024"
        self.silver_summary_path = self.silver_base / "job_summary"
        self.silver_postings_path = self.silver_base / "job_postings"
        self.bronze_postings_path = self.project_root / "data" / "bronze" / "linkedin_2024" / "job_postings"
        self.meta_dir = self.silver_base / "_metadata"

    def is_target_complete(self) -> Tuple[bool, int]:
        """Check if target destination already contains a valid completed dataset."""
        if not self.silver_summary_path.exists():
            return (False, 0)
        part_files = list(self.silver_summary_path.glob("*.parquet"))
        if not part_files:
            return (False, 0)
        total_rows = 0
        for f in part_files:
            try:
                total_rows += pq.ParquetFile(str(f)).metadata.num_rows
            except Exception:
                return (False, 0)
        return (total_rows > 0, total_rows)

    def validate_referential_integrity(self, df_summary: DataFrame) -> Dict[str, Any]:
        """Validate foreign-key relationship between job_summary and job_postings."""
        logger.info("Validating referential integrity...")
        postings_path = None
        if self.silver_postings_path.exists() and list(self.silver_postings_path.glob("*.parquet")):
            postings_path = self.silver_postings_path
            postings_tier = "Silver"
        elif self.bronze_postings_path.exists() and list(self.bronze_postings_path.glob("*.parquet")):
            postings_path = self.bronze_postings_path
            postings_tier = "Bronze"

        if not postings_path:
            logger.warning("No job_postings dataset found for referential integrity check. Skipping.")
            return {"status": "SKIPPED", "reason": "postings_not_found"}

        logger.info(f"Checking foreign-key integrity against {postings_tier} job_postings at {postings_path}")
        df_postings = self.spark.read.parquet(str(postings_path)).select("job_link").distinct()
        total_postings = df_postings.count()

        df_summary_keys = df_summary.select("job_link").distinct()
        total_summary_keys = df_summary_keys.count()

        matched = df_summary_keys.join(df_postings, on="job_link", how="inner").count()
        orphans = df_summary_keys.join(df_postings, on="job_link", how="left_anti").count()

        metrics = {
            "postings_tier_evaluated": postings_tier,
            "total_postings": total_postings,
            "total_summary_keys": total_summary_keys,
            "matched_keys": matched,
            "orphan_keys": orphans,
            "orphan_free": orphans == 0,
        }
        logger.info(f"Referential integrity result: {metrics}")
        return metrics

    def validate_staging(self, staging_path: Path, expected_rows: int = 1_297_332) -> Dict[str, Any]:
        """Validate staging Parquet files for row count, non-empty files, and basic sanity."""
        logger.info(f"Validating staging dataset: {staging_path}")
        part_files = sorted(list(staging_path.glob("*.parquet")))
        if not part_files:
            raise FileNotFoundError(f"No Parquet files found in staging path: {staging_path}")

        total_bytes = 0
        total_rows = 0
        codecs = set()
        for f in part_files:
            meta = pq.ParquetFile(str(f)).metadata
            total_bytes += f.stat().st_size
            total_rows += meta.num_rows
            codecs.add(meta.row_group(0).column(0).compression)

        if total_rows != expected_rows:
            raise ValueError(
                f"Staging validation FAILED: expected exactly {expected_rows:,} rows, but got {total_rows:,} rows."
            )

        logger.info(
            f"Staging validation PASSED: {total_rows:,} rows across {len(part_files)} files "
            f"({total_bytes / (1024**2):.2f} MB, Codecs: {codecs})."
        )
        return {
            "total_rows": total_rows,
            "total_files": len(part_files),
            "total_bytes": total_bytes,
            "codecs": list(codecs),
        }

    def run(
        self,
        overwrite: bool = False,
        target_partitions: int = 16,
        batch_size: int = 10000,
    ) -> Dict[str, Any]:
        """Execute the end-to-end LinkedIn Job Summary Silver Pipeline."""
        t0 = time.time()
        run_id = f"silver_summary_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"

        logger.info(f"Starting LinkedIn Job Summary Silver Pipeline [Run ID: {run_id}]")
        logger.info(f"  Bronze Source: {self.bronze_summary_path}")
        logger.info(f"  Silver Target: {self.silver_summary_path}")

        # Check existing target
        target_complete, existing_rows = self.is_target_complete()
        if target_complete and not overwrite:
            msg = (
                f"Target destination already contains a valid completed Silver dataset with {existing_rows:,} rows at "
                f"{self.silver_summary_path}. Run with --overwrite to replace it explicitly."
            )
            logger.error(msg)
            raise FileExistsError(msg)

        # 1. Read Bronze data
        logger.info("Reading Bronze job_summary dataset...")
        df_bronze = self.spark.read.parquet(str(self.bronze_summary_path))
        bronze_count = df_bronze.count()
        logger.info(f"Loaded {bronze_count:,} Bronze job_summary records.")

        # 2. Stream to staging path strictly on D: drive
        staging_dir = self.silver_base / f"_staging_job_summary_{uuid4().hex[:8]}"
        staging_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat()
        out_dir_str = str(staging_dir.resolve())

        try:
            logger.info(f"Streaming transformed Parquet to staging path via PyArrow: {staging_dir}")
            
            def _worker_closure(part_idx, it):
                return _write_silver_summary_partition_worker(
                    part_idx=part_idx,
                    iterator=it,
                    output_dir_str=out_dir_str,
                    silver_run_id=run_id,
                    silver_timestamp_str=ts,
                    batch_size=batch_size,
                    compression="snappy",
                )

            counts = df_bronze.rdd.mapPartitionsWithIndex(_worker_closure).collect()
            logger.info(f"Wrote {sum(counts):,} rows across {len(counts)} partitions to staging.")

            # 5. Validate staging
            staging_metrics = self.validate_staging(staging_dir, expected_rows=bronze_count)
            silver_count = staging_metrics["total_rows"]

            # 6. Referential integrity check
            df_staging_readback = self.spark.read.parquet(str(staging_dir))
            integrity_metrics = self.validate_referential_integrity(df_staging_readback)

            if not integrity_metrics.get("orphan_free", True):
                raise ValueError(f"Orphan summary keys found in staging: {integrity_metrics['orphan_keys']}")

            # 7. Atomic promotion
            if self.silver_summary_path.exists():
                logger.info(f"Overwriting existing target {self.silver_summary_path} (overwrite=True)...")
                shutil.rmtree(self.silver_summary_path)

            logger.info(f"Promoting staging dataset to final destination: {self.silver_summary_path}")
            shutil.move(str(staging_dir), str(self.silver_summary_path))

            duration = round(time.time() - t0, 2)
            logger.info(f"Silver promotion SUCCESS in {duration}s.")

            # 8. Commit Manifest
            self.meta_dir.mkdir(parents=True, exist_ok=True)
            manifest = {
                "silver_run_id": run_id,
                "source_id": "linkedin_2024",
                "table_name": "job_summary",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": duration,
                "bronze_source_path": str(self.bronze_summary_path),
                "bronze_row_count": bronze_count,
                "silver_row_count": silver_count,
                "silver_output_path": str(self.silver_summary_path),
                "partitions": len(counts),
                "compression": "snappy",
                "referential_integrity": integrity_metrics,
                "status": "SUCCESS",
            }
            manifest_file = self.meta_dir / f"silver_job_summary_{run_id}.json"
            with open(manifest_file, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            with open(self.meta_dir / "job_summary_manifest.json", "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
            logger.info(f"Manifest written to: {manifest_file}")

            return manifest

        except Exception as e:
            logger.error(f"Silver transformation FAILED: {e}", exc_info=True)
            if staging_dir.exists():
                logger.info(f"Rolling back: cleaning up failed staging directory {staging_dir}")
                shutil.rmtree(staging_dir, ignore_errors=True)
            raise
