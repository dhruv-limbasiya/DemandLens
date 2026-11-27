"""Focused tests for Phase 7C.7 India Salary Analytics."""

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
from scripts.gold_salary_analytics_india import (
    run_aggregation_salary_india,
    validate_salary_india,
)


class GoldSalaryAnalyticsIndiaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_gold_spark_session("TestGoldSalaryIndia", driver_memory="2g", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_salary_filtering_and_percentiles(self) -> None:
        jobs_path = os.path.join("tmp", self._testMethodName, "jobs")
        shutil.rmtree(jobs_path, ignore_errors=True)
        os.makedirs(jobs_path, exist_ok=True)
        cols = [
            "source_id",
            "job_title_clean",
            "seniority_tier",
            "location_clean",
            "experience_min_years",
            "experience_max_years",
            "salary_min",
            "salary_max",
            "salary_midpoint",
            "salary_currency",
            "salary_period",
            "salary_disclosed",
        ]
        rows = [
            ("indian_job_market_2025_2026", "Data Engineer", "MID", "Bengaluru", 2.0, 5.0, 1000000.0, 1400000.0, 1200000.0, "INR", "ANNUAL", True),
            ("indian_job_market_2025_2026", "Senior Data Engineer", "MID", "Bengaluru", 3.0, 6.0, 1200000.0, 1800000.0, 1500000.0, "INR", "ANNUAL", True),
            ("indian_job_market_2025_2026", "Big Data Engineer", "MID", "Bengaluru", 4.0, 7.0, 1600000.0, 2200000.0, 1900000.0, "INR", "ANNUAL", True),
            # Excluded: monthly
            ("indian_job_market_2025_2026", "Data Engineer", "MID", "Bengaluru", 1.0, 2.0, 50000.0, 60000.0, 55000.0, "INR", "MONTHLY", True),
            # Excluded: undisclosed
            ("indian_job_market_2025_2026", "Data Engineer", "MID", "Bengaluru", 1.0, 2.0, None, None, None, "INR", "ANNUAL", False),
            # Excluded: non-Indian source
            ("linkedin_2024", "Data Engineer", "MID", "Bengaluru", 2.0, 4.0, 100000.0, 150000.0, 125000.0, "USD", "ANNUAL", True),
        ]
        pdf = pd.DataFrame(rows, columns=cols)
        pq.write_table(pa.Table.from_pandas(pdf), os.path.join(jobs_path, "part-0.parquet"))

        result_df = run_aggregation_salary_india(self.spark, jobs_path, min_sample_size=3)
        self.assertEqual(validate_salary_india(result_df, min_sample_size=3), [])

        out = result_df.collect()
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertEqual(row["standardized_role"], "DATA_ENGINEER")
        self.assertEqual(row["sample_size"], 3)
        self.assertAlmostEqual(row["salary_mid_median_inr"], 1500000.0)
        self.assertEqual(row["salary_currency"], "INR")
        self.assertEqual(row["salary_period"], "ANNUAL")


if __name__ == "__main__":
    unittest.main()
