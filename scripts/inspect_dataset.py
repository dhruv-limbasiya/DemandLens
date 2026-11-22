"""DemandLens CLI Tool - Dataset Inspector.

Usage:
    python scripts/inspect_dataset.py --dataset a
    python scripts/inspect_dataset.py --dataset b
    python scripts/inspect_dataset.py --dataset both
    python scripts/inspect_dataset.py --path path/to/file.csv --engine pyspark
"""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path so demandlens package is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.ingestion.dataset_inspector import (
    DatasetInspector,
    configure_windows_environment,
    inspect_dataset_directory,
)

DATA_SOURCE_PATHS = {
    "a": PROJECT_ROOT / "data" / "source_csv" / "linkedin_2024",
    "b": PROJECT_ROOT / "data" / "source_csv" / "indian_job_market_2025_2026",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DemandLens Memory-Safe Dataset Inspector",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dataset",
        choices=["a", "b", "both", "linkedin", "india", "all"],
        default="both",
        help="Predefined dataset cohort to inspect from data/source_csv/",
    )
    parser.add_argument(
        "--path",
        type=str,
        default=None,
        help="Optional custom CSV file or directory path to inspect directly.",
    )
    parser.add_argument(
        "--engine",
        choices=["pandas", "pyspark"],
        default="pandas",
        help="Engine to use: 'pandas' (fast sample preview) or 'pyspark' (distributed full-scan).",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of sample records to preview.",
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output format for the generated inspection report.",
    )
    parser.add_argument(
        "--save-report",
        type=str,
        default=None,
        help="Optional file path to write the inspection report (e.g., docs/inspection_report.md).",
    )
    return parser


def check_dataset_availability(dataset_key: str, directory: Path) -> list[Path]:
    """Check for CSV files in the designated source directory."""
    if not directory.exists():
        print(f"[WARNING] Directory does not exist: {directory}")
        return []

    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        print(f"\n[INFO] No CSV files found in `{directory.relative_to(PROJECT_ROOT)}`.")
        print("  To inspect this dataset, download the dataset from Kaggle and extract CSV files into:")
        print(f"  --> {directory}")
        if dataset_key in ["a", "linkedin"]:
            print("  Expected files: linkedin_job_postings.csv, job_skills.csv, job_summary.csv")
            print("  Kaggle: https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024")
        elif dataset_key in ["b", "india"]:
            print("  Expected file: indian_job_market_dataset_2025_2026.csv (or variant)")
            print("  Kaggle: https://www.kaggle.com/datasets/shivamshrivastava21/indian-job-market-dataset-2025-2026")
    return csv_files


def main():
    parser = build_parser()
    args = parser.parse_args()

    print("=" * 70)
    print(" DemandLens: Job Market Dataset Inspector")
    print(f" Engine: {args.engine.upper()} | Sample Rows: {args.sample_rows}")
    print("=" * 70)

    # Configure Windows environment variables for PySpark support
    configure_windows_environment()

    files_to_inspect: list[Path] = []

    if args.path:
        custom_path = Path(args.path).resolve()
        if custom_path.is_file():
            files_to_inspect.append(custom_path)
        elif custom_path.is_dir():
            files_to_inspect.extend(sorted(custom_path.glob("*.csv")))
            if not files_to_inspect:
                print(f"[ERROR] No CSV files found in directory: {custom_path}")
                sys.exit(1)
        else:
            print(f"[ERROR] Specified path does not exist: {custom_path}")
            sys.exit(1)
    else:
        # Determine cohorts to inspect
        target_keys = []
        if args.dataset in ["a", "linkedin"]:
            target_keys = ["a"]
        elif args.dataset in ["b", "india"]:
            target_keys = ["b"]
        elif args.dataset in ["both", "all"]:
            target_keys = ["a", "b"]

        for key in target_keys:
            dir_path = DATA_SOURCE_PATHS[key]
            found = check_dataset_availability(key, dir_path)
            files_to_inspect.extend(found)

    if not files_to_inspect:
        print("\n[SUMMARY] Inspection completed: 0 files currently available for inspection.")
        print("Note: In Phase 1, full datasets are not yet downloaded per project guidelines.")
        print("You can test this tool by placing any sample CSV or using --path <path_to_csv>.")
        sys.exit(0)

    print(f"\n[INFO] Found {len(files_to_inspect)} file(s) to inspect.\n")

    inspector = DatasetInspector()
    all_reports = []

    for f in files_to_inspect:
        print(f"[PROGRESS] Inspecting `{f.name}` ({f.stat().st_size:,} bytes)...")
        try:
            if args.engine == "pyspark":
                report = inspector.inspect_with_pyspark(f, sample_rows=args.sample_rows)
            else:
                report = inspector.inspect_with_pandas(f, sample_rows=args.sample_rows)
            all_reports.append(report)
        except Exception as exc:
            print(f"[ERROR] Failed to inspect `{f.name}`: {exc}")
            import traceback
            traceback.print_exc()

    # Output reports
    output_text = ""
    if args.format == "json":
        import json
        output_text = json.dumps([r.to_dict() for r in all_reports], indent=2, default=str)
    else:
        output_text = "\n\n---\n\n".join(r.summary_markdown() for r in all_reports)

    print("\n" + "=" * 70)
    print(" INSPECTION RESULTS")
    print("=" * 70 + "\n")
    print(output_text)

    if args.save_report:
        save_path = Path(args.save_report).resolve()
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_text(output_text, encoding="utf-8")
        print(f"\n[SUCCESS] Report successfully saved to: {save_path}")


if __name__ == "__main__":
    main()

