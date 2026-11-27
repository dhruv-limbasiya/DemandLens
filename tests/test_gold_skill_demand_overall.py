"""Focused tests for Phase 7C.4 Overall Skill Demand."""

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
from scripts.gold_skill_demand_overall import (
    run_aggregation_skill_demand,
    validate_skill_demand,
)


class GoldSkillDemandOverallTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_gold_spark_session("TestGoldSkillDemandOverall", driver_memory="2g", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _write_parquet(self, path: str, rows: list, cols: list) -> None:
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path, exist_ok=True)
        pdf = pd.DataFrame(rows, columns=cols)
        pq.write_table(pa.Table.from_pandas(pdf), os.path.join(path, "part-0.parquet"))

    def test_skill_demand_aggregation_and_threshold(self) -> None:
        base = os.path.join("tmp", self._testMethodName)
        jobs_path = os.path.join(base, "jobs")
        bridge_path = os.path.join(base, "bridge")

        jobs_rows = [
            ("j1", "src1", "US"),
            ("j2", "src1", "US"),
            ("j3", "src1", "US"),
            ("j4", "src2", "IN"),
            ("j5", "src2", "IN"),
        ]
        bridge_rows = [
            ("j1", "src1", "Python"),
            ("j1", "src1", "Python"),  # duplicate edge should be deduped
            ("j2", "src1", "Python"),
            ("j3", "src1", "Python"),
            ("j1", "src1", "SQL"),
            ("j2", "src1", "SQL"),
            ("j3", "src1", "RareSkill"),  # below threshold=2
            ("j3", "src1", None),  # null skill excluded
            ("j3", "src1", "   "),  # blank skill excluded
            ("j4", "src2", "Python"),
            ("j5", "src2", "Python"),
        ]
        self._write_parquet(jobs_path, jobs_rows, ["canonical_job_id", "source_id", "country_code"])
        self._write_parquet(bridge_path, bridge_rows, ["canonical_job_id", "source_id", "skill"])

        market_totals_df, result_df = run_aggregation_skill_demand(
            self.spark, jobs_path, bridge_path, min_posting_count=2
        )
        self.assertEqual(validate_skill_demand(result_df, market_totals_df, min_posting_count=2), [])

        rows = {(r["source_id"], r["country_code"], r["skill"]): r for r in result_df.collect()}
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[("src1", "US", "Python")]["posting_count"], 3)
        self.assertEqual(rows[("src1", "US", "Python")]["market_skill_rank"], 1)
        self.assertAlmostEqual(rows[("src1", "US", "Python")]["prevalence_pct"], 100.0)
        self.assertEqual(rows[("src1", "US", "SQL")]["posting_count"], 2)
        self.assertEqual(rows[("src1", "US", "SQL")]["market_skill_rank"], 2)
        self.assertAlmostEqual(rows[("src1", "US", "SQL")]["prevalence_pct"], 66.6667, places=3)
        self.assertEqual(rows[("src2", "IN", "Python")]["posting_count"], 2)
        self.assertNotIn(("src1", "US", "RareSkill"), rows)


if __name__ == "__main__":
    unittest.main()
