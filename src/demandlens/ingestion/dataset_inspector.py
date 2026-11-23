"""DemandLens Dataset Inspector Module.

Provides memory-safe inspection of raw job-posting datasets using either
PySpark (for large, distributed CSV scans) or Pandas (for lightweight sample
previews). Never modifies source files or loads large datasets into memory.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


def configure_windows_environment() -> None:
    """Auto-detect and configure JAVA_HOME and HADOOP_HOME for Windows execution."""
    # Detect Project Root
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    local_hadoop = project_root / "hadoop"
    if "HADOOP_HOME" not in os.environ and local_hadoop.exists():
        os.environ["HADOOP_HOME"] = str(local_hadoop)
        hadoop_bin = str(local_hadoop / "bin")
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    if "JAVA_HOME" not in os.environ:
        candidate_javas = [
            Path(r"C:\Program Files\Java\jdk-24"),
            Path(r"C:\Program Files\Java\jdk-21"),
            Path(r"C:\Program Files\Java\jdk-17"),
            Path(r"C:\Program Files\Java\jdk-11"),
        ]
        for candidate in candidate_javas:
            if candidate.exists() and (candidate / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                java_bin = str(candidate / "bin")
                os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
                break

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Configure Spark temporary directory on D: drive
    spark_tmp = project_root / "data" / "temp_validation" / "spark_tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)
    if "SPARK_LOCAL_DIRS" not in os.environ:
        os.environ["SPARK_LOCAL_DIRS"] = str(spark_tmp)



@dataclass
class ColumnProfile:
    """Profile of an individual dataset column."""
    name: str
    data_type: str
    null_count: Optional[int] = None
    null_percentage: Optional[float] = None
    distinct_sample_count: Optional[int] = None


@dataclass
class InspectionReport:
    """Structured report produced by inspecting a dataset or CSV file."""
    file_path: str
    file_name: str
    file_size_bytes: int
    file_size_formatted: str
    file_format: str
    engine_used: str
    total_records: Optional[int] = None
    columns_count: int = 0
    columns: List[str] = field(default_factory=list)
    column_profiles: List[ColumnProfile] = field(default_factory=list)
    duplicate_records_estimate: Optional[int] = None
    sample_records: List[Dict[str, Any]] = field(default_factory=list)
    date_coverage: Dict[str, Any] = field(default_factory=dict)
    detected_salary_fields: List[str] = field(default_factory=list)
    detected_skills_fields: List[str] = field(default_factory=list)
    detected_description_fields: List[str] = field(default_factory=list)
    detected_potential_keys: List[str] = field(default_factory=list)
    detected_comparison_dimensions: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def summary_markdown(self) -> str:
        """Render a readable Markdown summary of the inspection report."""
        lines = [
            f"### Dataset Inspection: `{self.file_name}`",
            f"- **Path:** `{self.file_path}`",
            f"- **Size:** {self.file_size_formatted} ({self.file_size_bytes:,} bytes)",
            f"- **Engine:** {self.engine_used}",
            f"- **Record Count:** {f'{self.total_records:,}' if self.total_records is not None else 'N/A'}",
            f"- **Columns ({self.columns_count}):** `{', '.join(self.columns)}`",
            "",
            "#### Detected Semantic Capabilities",
            f"- **Potential Join Keys:** {', '.join(self.detected_potential_keys) if self.detected_potential_keys else 'None detected'}",
            f"- **Skills Fields:** {', '.join(self.detected_skills_fields) if self.detected_skills_fields else 'None detected'}",
            f"- **Salary Fields:** {', '.join(self.detected_salary_fields) if self.detected_salary_fields else 'None detected'}",
            f"- **Description Fields:** {', '.join(self.detected_description_fields) if self.detected_description_fields else 'None detected'}",
            f"- **Comparison Dimensions:** {', '.join(self.detected_comparison_dimensions) if self.detected_comparison_dimensions else 'None detected'}",
            "",
            "#### Column Summary & Null Rates",
            "| Column Name | Data Type | Null Count | Null % |",
            "|---|---|---|---|",
        ]
        for col in self.column_profiles:
            null_cnt = f"{col.null_count:,}" if col.null_count is not None else "N/A"
            null_pct = f"{col.null_percentage:.2f}%" if col.null_percentage is not None else "N/A"
            lines.append(f"| `{col.name}` | `{col.data_type}` | {null_cnt} | {null_pct} |")

        if self.sample_records:
            lines.append("\n#### Sample Record (First Record)")
            first_rec = self.sample_records[0]
            for k, v in first_rec.items():
                str_val = str(v)
                if len(str_val) > 120:
                    str_val = str_val[:117] + "..."
                lines.append(f"- **`{k}`:** `{str_val}`")

        if self.notes:
            lines.append("\n#### Inspection Notes")
            for note in self.notes:
                lines.append(f"- {note}")

        return "\n".join(lines)


class DatasetInspector:
    """Inspector providing memory-safe Pandas sampling and distributed PySpark analysis."""

    def __init__(self, spark_session: Any = None) -> None:
        self._spark = spark_session

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    @staticmethod
    def _detect_semantics(columns: List[str]) -> Dict[str, List[str]]:
        """Identify candidate columns for skills, salary, descriptions, keys, and dimensions."""
        lowered = {col: col.lower().replace("_", "").replace("-", "") for col in columns}

        keys = []
        skills = []
        salaries = []
        descriptions = []
        dimensions = []

        for original, norm in lowered.items():
            if any(k in norm for k in ["joblink", "jobid", "id", "url", "link"]):
                keys.append(original)
            if any(k in norm for k in ["skill", "tag", "stack", "technology"]):
                skills.append(original)
            if any(k in norm for k in ["salary", "compensation", "ctc", "pay", "wage", "currency"]):
                salaries.append(original)
            if any(k in norm for k in ["summary", "description", "desc", "details", "jobsummary"]):
                descriptions.append(original)
            if any(k in norm for k in ["title", "role", "location", "city", "country", "state", "experience", "level", "company"]):
                dimensions.append(original)

        return {
            "keys": keys,
            "skills": skills,
            "salaries": salaries,
            "descriptions": descriptions,
            "dimensions": dimensions,
        }

    def inspect_with_pandas(self, file_path: Path | str, sample_rows: int = 5) -> InspectionReport:
        """Inspect a CSV file using Pandas for low-memory header and sample extraction.

        Reads ONLY the first few rows (nrows=sample_rows) to avoid memory overhead.
        """
        import pandas as pd

        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_size = path.stat().st_size
        file_format = path.suffix.lower()

        # Read only sample_rows to preserve memory
        df_sample = pd.read_csv(path, nrows=sample_rows)
        columns = list(df_sample.columns)
        semantics = self._detect_semantics(columns)

        col_profiles: List[ColumnProfile] = []
        for col in columns:
            col_profiles.append(
                ColumnProfile(
                    name=col,
                    data_type=str(df_sample[col].dtype),
                    null_count=int(df_sample[col].isna().sum()),
                    null_percentage=round((df_sample[col].isna().sum() / len(df_sample)) * 100.0, 2)
                    if len(df_sample) > 0 else 0.0,
                )
            )

        # Approximate line count without reading whole file into memory
        total_records_est = None
        try:
            with open(path, "rb") as f:
                # Count newline characters for record count estimate
                line_count = sum(1 for _ in f) - 1  # subtract header
                total_records_est = max(0, line_count)
        except Exception:
            pass

        sample_records = df_sample.head(sample_rows).to_dict(orient="records")

        # Detect date coverage from sample if date columns are present
        date_info: Dict[str, Any] = {}
        for col in columns:
            if any(d in col.lower() for d in ["date", "seen", "posted", "time", "uploaded"]):
                sample_vals = df_sample[col].dropna().astype(str).tolist()
                date_info[col] = {
                    "sample_values": sample_vals[:3],
                    "apparent_precision": "RELATIVE_STRING" if any("ago" in v.lower() for v in sample_vals) else "TIMESTAMP_OR_DATE",
                }

        notes = [
            f"Inspected first {len(df_sample)} rows using Pandas preview.",
            f"Approximate disk file size: {self._format_size(file_size)}.",
        ]
        if total_records_est is not None:
            notes.append(f"Fast line-count estimate: ~{total_records_est:,} records.")

        return InspectionReport(
            file_path=str(path),
            file_name=path.name,
            file_size_bytes=file_size,
            file_size_formatted=self._format_size(file_size),
            file_format=file_format,
            engine_used="pandas_sample_preview",
            total_records=total_records_est,
            columns_count=len(columns),
            columns=columns,
            column_profiles=col_profiles,
            sample_records=sample_records,
            date_coverage=date_info,
            detected_salary_fields=semantics["salaries"],
            detected_skills_fields=semantics["skills"],
            detected_description_fields=semantics["descriptions"],
            detected_potential_keys=semantics["keys"],
            detected_comparison_dimensions=semantics["dimensions"],
            notes=notes,
        )

    def inspect_with_pyspark(
        self, file_path: Path | str, sample_rows: int = 5, compute_full_nulls: bool = True
    ) -> InspectionReport:
        """Inspect a large CSV file using PySpark distributed compute.

        Never uses .collect() on the entire dataset. Uses .limit() for preview
        and Spark aggregations for metrics.
        """
        configure_windows_environment()
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F

        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        file_size = path.stat().st_size
        file_format = path.suffix.lower()

        spark = self._spark
        created_local_session = False
        if spark is None:
            spark = (
                SparkSession.builder.appName("DemandLens-Inspector")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
                .getOrCreate()
            )
            created_local_session = True

        try:
            # Read CSV with schema inference and multiLine support
            df = spark.read.option("header", "true").option("multiLine", "true").csv(str(path))
            columns = df.columns
            semantics = self._detect_semantics(columns)

            total_records = df.count()

            col_profiles: List[ColumnProfile] = []
            if compute_full_nulls and total_records > 0:
                # Compute null counts in a single aggregated Spark pass
                null_exprs = [
                    F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
                    for c in columns
                ]
                null_row = df.select(null_exprs).first()
                for c, data_type in df.dtypes:
                    null_cnt = null_row[c] if null_row else 0
                    null_pct = round((null_cnt / total_records) * 100.0, 2)
                    col_profiles.append(
                        ColumnProfile(
                            name=c,
                            data_type=data_type,
                            null_count=null_cnt,
                            null_percentage=null_pct,
                        )
                    )
            else:
                for c, data_type in df.dtypes:
                    col_profiles.append(ColumnProfile(name=c, data_type=data_type))

            # Sample records using .limit() - memory safe
            sample_df = df.limit(sample_rows).toPandas()
            sample_records = sample_df.to_dict(orient="records")

            # Duplicate check on potential keys or first column if key detected
            duplicate_estimate = None
            if semantics["keys"] and total_records > 0:
                key_col = semantics["keys"][0]
                distinct_keys = df.select(key_col).distinct().count()
                duplicate_estimate = max(0, total_records - distinct_keys)

            date_info: Dict[str, Any] = {}
            for col in columns:
                if any(d in col.lower() for d in ["date", "seen", "posted", "time", "uploaded"]):
                    sample_vals = [
                        str(row[col]) for row in sample_records if row.get(col) is not None
                    ]
                    date_info[col] = {
                        "sample_values": sample_vals[:3],
                        "apparent_precision": "RELATIVE_STRING" if any("ago" in v.lower() for v in sample_vals) else "EXACT_OR_TIMESTAMP",
                    }

            notes = [
                f"Full distributed inspection executed using PySpark {spark.version}.",
                f"Total row count verified: {total_records:,}.",
                f"No full collect() executed; memory protected.",
            ]
            if duplicate_estimate is not None:
                notes.append(f"Estimated non-unique records on key `{semantics['keys'][0]}`: {duplicate_estimate:,}.")

            return InspectionReport(
                file_path=str(path),
                file_name=path.name,
                file_size_bytes=file_size,
                file_size_formatted=self._format_size(file_size),
                file_format=file_format,
                engine_used="pyspark_distributed",
                total_records=total_records,
                columns_count=len(columns),
                columns=columns,
                column_profiles=col_profiles,
                duplicate_records_estimate=duplicate_estimate,
                sample_records=sample_records,
                date_coverage=date_info,
                detected_salary_fields=semantics["salaries"],
                detected_skills_fields=semantics["skills"],
                detected_description_fields=semantics["descriptions"],
                detected_potential_keys=semantics["keys"],
                detected_comparison_dimensions=semantics["dimensions"],
                notes=notes,
            )

        finally:
            if created_local_session and spark is not None:
                spark.stop()


def inspect_dataset_directory(
    directory_path: Path | str,
    engine: str = "pandas",
    sample_rows: int = 5,
) -> List[InspectionReport]:
    """Inspect all CSV files within a given directory."""
    path = Path(directory_path).resolve()
    if not path.is_dir():
        raise NotADirectoryError(f"Directory not found: {path}")

    csv_files = sorted(path.glob("*.csv"))
    if not csv_files:
        return []

    inspector = DatasetInspector()
    reports = []
    for csv_file in csv_files:
        if engine.lower() == "pyspark":
            report = inspector.inspect_with_pyspark(csv_file, sample_rows=sample_rows)
        else:
            report = inspector.inspect_with_pandas(csv_file, sample_rows=sample_rows)
        reports.append(report)

    return reports

