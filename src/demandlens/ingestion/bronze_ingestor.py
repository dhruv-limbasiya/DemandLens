"""DemandLens - Reusable Scalable Bronze Ingestion Engine.

Provides source-driven, memory-safe ingestion of raw datasets into the Medallion
Bronze layer as raw Parquet files. Preserves original schemas, source columns,
and row counts while adding standard ingestion metadata and verifying integrity.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

# Ensure project root is available
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment

logger = logging.getLogger("demandlens.ingestion.bronze")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


@dataclass
class TableConfig:
    """Configuration for an individual source table/dataset."""
    table_name: str
    input_file: str
    output_subdir: str
    format: str = "csv"
    read_options: Dict[str, str] = field(default_factory=lambda: {"header": "true", "multiLine": "true", "escape": '"'})
    target_partitions: int = 4
    description: str = ""


@dataclass
class SourceConfig:
    """Declarative configuration for a source data provider."""
    source_id: str
    source_name: str
    platform: str
    kaggle_identifier: Optional[str]
    dataset_url: Optional[str]
    license: Optional[str]
    geographic_focus: Optional[str]
    snapshot_period: Optional[str]
    primary_key: Optional[str]
    paths: Dict[str, str]
    tables: List[TableConfig]
    capabilities: Dict[str, bool] = field(default_factory=dict)
    raw_config_path: Optional[str] = None

    @classmethod
    def from_json(cls, config_path: Path) -> SourceConfig:
        """Load and parse a SourceConfig from a JSON file."""
        if not config_path.is_file():
            raise FileNotFoundError(f"Source configuration file not found: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        raw_tables = data.get("tables", [])
        parsed_tables = []
        for t in raw_tables:
            parsed_tables.append(
                TableConfig(
                    table_name=t["table_name"],
                    input_file=t["input_file"],
                    output_subdir=t.get("output_subdir", t["table_name"]),
                    format=t.get("format", "csv"),
                    read_options=t.get("read_options", {"header": "true", "multiLine": "true", "escape": '"'}),
                    target_partitions=t.get("target_partitions", 4),
                    description=t.get("description", ""),
                )
            )

        return cls(
            source_id=data["source_id"],
            source_name=data.get("source_name", data["source_id"]),
            platform=data.get("platform", "Unknown"),
            kaggle_identifier=data.get("kaggle_identifier"),
            dataset_url=data.get("dataset_url"),
            license=data.get("license"),
            geographic_focus=data.get("geographic_focus"),
            snapshot_period=data.get("snapshot_period"),
            primary_key=data.get("primary_key"),
            paths=data.get("paths", {}),
            tables=parsed_tables,
            capabilities=data.get("capabilities", {}),
            raw_config_path=str(config_path),
        )


@dataclass
class ValidationResult:
    """Result of validating a Bronze Parquet dataset."""
    table_name: str
    output_path: str
    is_valid: bool
    exists: bool = False
    is_readable: bool = False
    row_count: int = 0
    column_count: int = 0
    columns: List[str] = field(default_factory=list)
    original_columns_preserved: bool = False
    metadata_columns_present: bool = False
    source_row_count_match: bool = False
    error_messages: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class IngestionResult:
    """Provenance and execution outcome for a Bronze table ingestion run."""
    ingestion_run_id: str
    source_id: str
    table_name: str
    source_file_path: str
    source_file_size_bytes: int
    output_path: str
    output_format: str
    ingestion_timestamp: str
    source_row_count: int = 0
    output_row_count: int = 0
    source_column_names: List[str] = field(default_factory=list)
    output_column_names: List[str] = field(default_factory=list)
    ingestion_status: str = "PENDING"  # SUCCESS, SKIPPED_ALREADY_EXISTS, FAILED
    validation_status: str = "PENDING"  # PASSED, FAILED, SKIPPED
    processing_duration_seconds: float = 0.0
    error_details: Optional[str] = None
    validation_result: Optional[ValidationResult] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if self.validation_result:
            d["validation_result"] = self.validation_result.to_dict()
        return d


class BronzeIngestor:
    """Reusable, idempotent engine to ingest raw files into Bronze Parquet format."""

    METADATA_COLUMNS = [
        "_source_id",
        "_source_file",
        "_ingestion_timestamp",
        "_ingestion_run_id",
    ]

    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or PROJECT_ROOT

    def get_input_file_path(self, source_config: SourceConfig, table_config: TableConfig) -> Path:
        """Resolve full absolute path to the table's raw input file."""
        extracted_rel = source_config.paths.get("extracted_dir", "")
        if not extracted_rel:
            extracted_rel = f"data/source_csv/{source_config.source_id}/extracted"
        return (self.project_root / extracted_rel / table_config.input_file).resolve()

    def get_output_dir_path(self, source_config: SourceConfig, table_config: TableConfig) -> Path:
        """Resolve full absolute path to the target Bronze Parquet directory."""
        bronze_rel = source_config.paths.get("bronze_dir", "")
        if not bronze_rel:
            bronze_rel = f"data/bronze/{source_config.source_id}"
        return (self.project_root / bronze_rel / table_config.output_subdir).resolve()

    def get_metadata_dir_path(self, source_config: SourceConfig) -> Path:
        """Resolve directory where Bronze ingestion run metadata is recorded."""
        bronze_rel = source_config.paths.get("bronze_dir", f"data/bronze/{source_config.source_id}")
        meta_dir = (self.project_root / bronze_rel / "_metadata").resolve()
        meta_dir.mkdir(parents=True, exist_ok=True)
        return meta_dir

    def is_already_ingested(self, output_path: Path) -> bool:
        """Check if output directory exists and contains valid Parquet files."""
        if not output_path.exists() or not output_path.is_dir():
            return False
        parquet_files = [f for f in output_path.glob("*.parquet") if f.is_file() and f.stat().st_size > 0]
        return len(parquet_files) > 0

    def validate_table(
        self,
        source_config: SourceConfig,
        table_config: TableConfig,
        expected_source_row_count: Optional[int] = None,
        expected_source_columns: Optional[List[str]] = None,
    ) -> ValidationResult:
        """Validate that Bronze Parquet output exists, is readable, and preserves integrity."""
        output_path = self.get_output_dir_path(source_config, table_config)
        res = ValidationResult(
            table_name=table_config.table_name,
            output_path=str(output_path),
            is_valid=False,
            exists=output_path.exists(),
        )

        if not res.exists:
            res.error_messages.append(f"Output directory does not exist: {output_path}")
            return res

        try:
            df = self.spark.read.parquet(str(output_path))
            res.is_readable = True
            res.row_count = df.count()
            res.columns = df.columns
            res.column_count = len(df.columns)

            if res.row_count == 0:
                res.error_messages.append(f"Parquet dataset is empty (0 rows): {output_path}")

            # Check metadata columns
            missing_meta = [m for m in self.METADATA_COLUMNS if m not in res.columns]
            if missing_meta:
                res.error_messages.append(f"Missing required ingestion metadata columns: {missing_meta}")
            else:
                res.metadata_columns_present = True

            # Check original columns preserved
            if expected_source_columns:
                missing_orig = [c for c in expected_source_columns if c not in res.columns]
                if missing_orig:
                    res.error_messages.append(f"Original source columns missing from Bronze: {missing_orig}")
                else:
                    res.original_columns_preserved = True
            else:
                res.original_columns_preserved = True

            # Check row count match
            if expected_source_row_count is not None:
                if res.row_count == expected_source_row_count:
                    res.source_row_count_match = True
                else:
                    res.error_messages.append(
                        f"Row count mismatch: Source had {expected_source_row_count:,}, "
                        f"Bronze has {res.row_count:,}"
                    )
            else:
                res.source_row_count_match = True

            res.is_valid = len(res.error_messages) == 0

        except Exception as exc:
            res.is_readable = False
            res.error_messages.append(f"Error reading Parquet dataset: {exc}")
            res.is_valid = False

        return res

    def ingest_table(
        self,
        source_config: SourceConfig,
        table_config: TableConfig,
        overwrite: bool = False,
        run_id: Optional[str] = None,
    ) -> IngestionResult:
        """Ingest a single table into Bronze with safe staging and full validation."""
        run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        input_path = self.get_input_file_path(source_config, table_config)
        output_path = self.get_output_dir_path(source_config, table_config)
        timestamp_str = datetime.now(timezone.utc).isoformat()

        logger.info(f"Starting Bronze ingestion for [{source_config.source_id}] table: {table_config.table_name}")
        logger.info(f"  Input:  {input_path}")
        logger.info(f"  Output: {output_path}")

        t0 = time.time()
        result = IngestionResult(
            ingestion_run_id=run_id,
            source_id=source_config.source_id,
            table_name=table_config.table_name,
            source_file_path=str(input_path),
            source_file_size_bytes=input_path.stat().st_size if input_path.exists() else 0,
            output_path=str(output_path),
            output_format="parquet",
            ingestion_timestamp=timestamp_str,
        )

        if not input_path.is_file():
            err = f"Source input file does not exist: {input_path}"
            logger.error(err)
            result.ingestion_status = "FAILED"
            result.error_details = err
            result.processing_duration_seconds = round(time.time() - t0, 2)
            self._save_run_metadata(source_config, result)
            return result

        # Check existing destination
        if self.is_already_ingested(output_path) and not overwrite:
            logger.info(f"  Table output already exists at {output_path} and overwrite=False. Validating existing.")
            validation = self.validate_table(source_config, table_config)
            result.ingestion_status = "SKIPPED_ALREADY_EXISTS"
            result.validation_status = "PASSED" if validation.is_valid else "FAILED"
            result.validation_result = validation
            result.output_row_count = validation.row_count
            result.output_column_names = validation.columns
            result.processing_duration_seconds = round(time.time() - t0, 2)
            self._save_run_metadata(source_config, result)
            return result

        # Read source file
        try:
            logger.info("  Reading raw source data with PySpark...")
            reader = self.spark.read.options(**table_config.read_options)
            if table_config.format.lower() == "csv":
                df = reader.csv(str(input_path))
            else:
                df = reader.format(table_config.format).load(str(input_path))

            source_cols = list(df.columns)
            result.source_column_names = source_cols
            source_count = df.count()
            result.source_row_count = source_count
            logger.info(f"  Source loaded: {source_count:,} rows, {len(source_cols)} columns")

            # Add standard ingestion metadata columns without touching source columns
            df_bronze = (
                df.withColumn("_source_id", F.lit(source_config.source_id))
                .withColumn("_source_file", F.lit(table_config.input_file))
                .withColumn("_ingestion_timestamp", F.current_timestamp())
                .withColumn("_ingestion_run_id", F.lit(run_id))
            )

            # Partition management
            if table_config.target_partitions and table_config.target_partitions > 0:
                current_parts = df_bronze.rdd.getNumPartitions()
                if current_parts > table_config.target_partitions:
                    df_bronze = df_bronze.coalesce(table_config.target_partitions)
                elif current_parts < table_config.target_partitions:
                    df_bronze = df_bronze.repartition(table_config.target_partitions)

            # Safe Staging Directory Write Strategy
            staging_path = output_path.parent / f"_staging_{table_config.output_subdir}_{uuid4().hex[:8]}"
            logger.info(f"  Writing Parquet to staging path: {staging_path}")
            if staging_path.exists():
                shutil.rmtree(staging_path, ignore_errors=True)

            (
                df_bronze.write.mode("overwrite")
                .format("parquet")
                .option("compression", "snappy")
                .save(str(staging_path))
            )

            # Validate staging output before swapping
            logger.info("  Validating written Parquet in staging...")
            staging_df = self.spark.read.parquet(str(staging_path))
            staging_count = staging_df.count()
            if staging_count != source_count:
                raise ValueError(
                    f"Staging validation row count mismatch: Expected {source_count:,}, got {staging_count:,}"
                )

            # Atomic swap to target directory
            logger.info(f"  Staging valid ({staging_count:,} rows). Promoting to final destination: {output_path}")
            if output_path.exists():
                backup_path = output_path.parent / f"_backup_{table_config.output_subdir}_{uuid4().hex[:6]}"
                output_path.rename(backup_path)
                staging_path.rename(output_path)
                shutil.rmtree(backup_path, ignore_errors=True)
            else:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                staging_path.rename(output_path)

            # Full post-promotion validation
            val_result = self.validate_table(
                source_config,
                table_config,
                expected_source_row_count=source_count,
                expected_source_columns=source_cols,
            )

            result.validation_result = val_result
            result.validation_status = "PASSED" if val_result.is_valid else "FAILED"
            result.output_row_count = val_result.row_count
            result.output_column_names = val_result.columns
            result.ingestion_status = "SUCCESS" if val_result.is_valid else "FAILED"

            if not val_result.is_valid:
                result.error_details = "; ".join(val_result.error_messages)

            logger.info(f"  Ingestion finished with status: {result.ingestion_status} (Validation: {result.validation_status})")

        except Exception as exc:
            logger.error(f"  Ingestion FAILED for {table_config.table_name}: {exc}", exc_info=True)
            result.ingestion_status = "FAILED"
            result.validation_status = "FAILED"
            result.error_details = str(exc)

        result.processing_duration_seconds = round(time.time() - t0, 2)
        self._save_run_metadata(source_config, result)
        return result

    def ingest_source(
        self,
        source_config: SourceConfig,
        overwrite: bool = False,
        run_id: Optional[str] = None,
    ) -> List[IngestionResult]:
        """Ingest all configured tables for a given source dataset."""
        logger.info(f"============================================================")
        logger.info(f" Ingesting Source Dataset: {source_config.source_name} ({source_config.source_id})")
        logger.info(f" Tables to ingest: {len(source_config.tables)}")
        logger.info(f"============================================================")

        run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        results = []
        for tbl in source_config.tables:
            res = self.ingest_table(source_config, tbl, overwrite=overwrite, run_id=run_id)
            results.append(res)
        return results

    def _save_run_metadata(self, source_config: SourceConfig, result: IngestionResult) -> None:
        """Persist structured run metadata JSON to the source's Bronze _metadata directory."""
        meta_dir = self.get_metadata_dir_path(source_config)
        meta_file = meta_dir / f"{result.table_name}_{result.ingestion_run_id}.json"
        try:
            with open(meta_file, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, indent=2, default=str)
        except Exception as exc:
            logger.warning(f"Could not write run metadata to {meta_file}: {exc}")


def load_all_source_configs(config_dir: Optional[Path] = None) -> List[SourceConfig]:
    """Discover and load all source configuration JSON files from config/sources/."""
    cfg_dir = config_dir or (PROJECT_ROOT / "config" / "sources")
    if not cfg_dir.exists():
        return []
    configs = []
    for f in sorted(cfg_dir.glob("*.json")):
        try:
            configs.append(SourceConfig.from_json(f))
        except Exception as exc:
            logger.error(f"Error loading source config {f.name}: {exc}")
    return configs
