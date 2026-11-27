"""Focused tests for Phase 7C.5 Role-Skill Matrix."""

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
from scripts.gold_role_skill_matrix import (
    run_aggregation_role_skill_matrix,
    validate_role_skill_matrix,
)


class GoldRoleSkillMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_gold_spark_session("TestGoldRoleSkillMatrix", driver_memory="2g", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _write_parquet(self, path: str, rows: list, cols: list) -> None:
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
        pdf = pd.DataFrame(rows, columns=cols)
        pq.write_table(pa.Table.from_pandas(pdf), os.path.join(path, "part-0.parquet"))

    def test_role_skill_matrix_aggregation(self) -> None:
        base = os.path.join("tmp", self._testMethodName)
        jobs_path = os.path.join(base, "jobs")
        bridge_path = os.path.join(base, "bridge")

        jobs_rows = [
            ("j1", "src1", "Senior Data Scientist"),
            ("j2", "src1", "Data Scientist"),
            ("j3", "src1", "Software Engineer"),
        ]
        bridge_rows = [
            ("j1", "src1", "Python"),
            ("j2", "src1", "Python"),
            ("j1", "src1", "SQL"),
            ("j3", "src1", "Java"),
            ("j3", "src1", "Java"),  # dup edge
        ]
        self._write_parquet(jobs_path, jobs_rows, ["canonical_job_id", "source_id", "job_title_clean"])
        self._write_parquet(bridge_path, bridge_rows, ["canonical_job_id", "source_id", "skill"])

        role_totals_df, result_df = run_aggregation_role_skill_matrix(
            self.spark, jobs_path, bridge_path, min_occurrences=1
        )
        self.assertEqual(validate_role_skill_matrix(result_df, role_totals_df, min_occurrences=1), [])

        rows = {(r["standardized_role"], r["skill"]): r for r in result_df.collect()}
        self.assertEqual(rows[("DATA_SCIENTIST", "Python")]["skill_occurrences"], 2)
        self.assertEqual(rows[("DATA_SCIENTIST", "Python")]["total_role_postings"], 2)
        self.assertAlmostEqual(rows[("DATA_SCIENTIST", "Python")]["requirement_frequency_pct"], 100.0)
        self.assertEqual(rows[("DATA_SCIENTIST", "Python")]["role_skill_rank"], 1)
        self.assertEqual(rows[("DATA_SCIENTIST", "SQL")]["skill_occurrences"], 1)
        self.assertAlmostEqual(rows[("DATA_SCIENTIST", "SQL")]["requirement_frequency_pct"], 50.0)
        self.assertEqual(rows[("SOFTWARE_ENGINEER", "Java")]["skill_occurrences"], 1)


if __name__ == "__main__":
    unittest.main()
