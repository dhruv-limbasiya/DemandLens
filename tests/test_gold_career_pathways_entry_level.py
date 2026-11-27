"""Focused tests for Phase 7C.8 Entry-Level Career Pathways."""

from __future__ import annotations

import os
import shutil
import sys
import unittest
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.gold.gold_utils import get_gold_spark_session
from scripts.gold_career_pathways_entry_level import (
    run_aggregation_career_pathways,
    validate_career_pathways,
)


class GoldCareerPathwaysTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_gold_spark_session("TestGoldCareerPathways", driver_memory="2g", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _write_parquet(self, path: str, rows: list, cols: list) -> None:
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
        pdf = pd.DataFrame(rows, columns=cols)
        pq.write_table(pa.Table.from_pandas(pdf), os.path.join(path, "part-0.parquet"))

    def test_entry_level_filtering_and_metrics(self) -> None:
        base = os.path.join("tmp", self._testMethodName)
        jobs_path = os.path.join(base, "jobs")
        bridge_path = os.path.join(base, "bridge")

        jobs_cols = [
            "canonical_job_id",
            "source_id",
            "country_code",
            "job_title_clean",
            "seniority_tier",
            "experience_min_years",
            "is_remote",
        ]
        jobs_rows = [
            ("j1", "linkedin_2024", "US", "Junior Data Analyst", "ENTRY", None, True),
            ("j2", "linkedin_2024", "US", "Associate Data Analyst", "ASSOCIATE", None, False),
            ("j3", "linkedin_2024", "US", "Senior Data Analyst", "SENIOR", None, False),  # excluded
            ("j4", "indian_job_market_2025_2026", "IN", "Software Engineer", "MID", 1.5, True),  # included (exp <= 2)
            ("j5", "indian_job_market_2025_2026", "IN", "Software Engineer", "SENIOR", 6.0, False),  # excluded
        ]
        bridge_rows = [
            ("j1", "linkedin_2024", "SQL"),
            ("j2", "linkedin_2024", "SQL"),
            ("j3", "linkedin_2024", "SQL"),
            ("j4", "indian_job_market_2025_2026", "Python"),
            ("j5", "indian_job_market_2025_2026", "Python"),
        ]
        self._write_parquet(jobs_path, jobs_rows, jobs_cols)
        self._write_parquet(bridge_path, bridge_rows, ["canonical_job_id", "source_id", "skill"])

        role_totals_df, result_df = run_aggregation_career_pathways(
            self.spark, jobs_path, bridge_path, min_entry_postings=1
        )
        self.assertEqual(validate_career_pathways(result_df, role_totals_df, min_entry_postings=1), [])

        rows = {(r["source_id"], r["country_code"], r["standardized_role"], r["skill"]): r for r in result_df.collect()}
        us_row = rows[("linkedin_2024", "US", "DATA_ANALYST", "SQL")]
        self.assertEqual(us_row["entry_posting_count"], 2)
        self.assertEqual(us_row["role_entry_total"], 2)
        self.assertAlmostEqual(us_row["skill_entry_prevalence_pct"], 100.0)
        self.assertAlmostEqual(us_row["remote_entry_pct"], 50.0)

        in_row = rows[("indian_job_market_2025_2026", "IN", "SOFTWARE_ENGINEER", "Python")]
        self.assertEqual(in_row["entry_posting_count"], 1)
        self.assertAlmostEqual(in_row["remote_entry_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()
