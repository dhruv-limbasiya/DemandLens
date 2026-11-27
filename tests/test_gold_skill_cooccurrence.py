"""Focused tests for Phase 7C.6 Skill Co-occurrence."""

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
from scripts.gold_skill_cooccurrence import (
    run_aggregation_skill_cooccurrence,
    validate_skill_cooccurrence,
)


class GoldSkillCooccurrenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = get_gold_spark_session("TestGoldSkillCooccurrence", driver_memory="2g", shuffle_partitions=2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_cooccurrence_and_jaccard(self) -> None:
        bridge_path = os.path.join("tmp", self._testMethodName, "bridge")
        shutil.rmtree(bridge_path, ignore_errors=True)
        os.makedirs(bridge_path, exist_ok=True)
        rows = [
            ("j1", "src1", "Python"),
            ("j1", "src1", "SQL"),
            ("j2", "src1", "Python"),
            ("j2", "src1", "SQL"),
            ("j3", "src1", "Python"),
            ("j3", "src1", "Spark"),
        ]
        pdf = pd.DataFrame(rows, columns=["canonical_job_id", "source_id", "skill"])
        pq.write_table(pa.Table.from_pandas(pdf), os.path.join(bridge_path, "part-0.parquet"))

        result_df = run_aggregation_skill_cooccurrence(
            self.spark, bridge_path, top_n_skills=10, min_cooccurrence=2
        )
        self.assertEqual(validate_skill_cooccurrence(result_df, min_cooccurrence=2), [])

        rows_out = {(r["source_id"], r["skill_a"], r["skill_b"]): r for r in result_df.collect()}
        self.assertEqual(len(rows_out), 1)
        pair = rows_out[("src1", "Python", "SQL")]
        self.assertEqual(pair["cooccurrence_count"], 2)
        self.assertEqual(pair["skill_a_total"], 3)
        self.assertEqual(pair["skill_b_total"], 2)
        # Jaccard = 2 / (3 + 2 - 2) = 2/3
        self.assertAlmostEqual(pair["jaccard_similarity"], 0.666667, places=5)


if __name__ == "__main__":
    unittest.main()
