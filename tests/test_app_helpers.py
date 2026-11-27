"""Unit tests for Phase 1 presentation helpers (labels, findings, build date, kitchen sink)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

from streamlit.testing.v1 import AppTest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.app import components, labels


class AppHelperTests(unittest.TestCase):
    def test_role_display_formatting(self) -> None:
        self.assertEqual(labels.format_role_name("SOFTWARE_ENGINEER"), "Software engineer")
        self.assertEqual(labels.format_role_name("MACHINE_LEARNING_ENGINEER"), "ML engineer")
        self.assertEqual(labels.format_role_name("QA_ENGINEER"), "QA engineer")
        self.assertEqual(labels.format_role_name("DEVOPS_ENGINEER"), "DevOps engineer")
        self.assertEqual(labels.format_role_name("UNCLASSIFIED"), "Unclassified")
        self.assertEqual(labels.format_role_name("OTHER_SPECIALIZED"), "Other specialized")

    def test_skill_display_casing(self) -> None:
        self.assertEqual(labels.format_skill_name("sql"), "SQL")
        self.assertEqual(labels.format_skill_name("aws"), "AWS")
        self.assertEqual(labels.format_skill_name("pyspark"), "PySpark")
        self.assertEqual(labels.format_skill_name("c#"), "C#")
        self.assertEqual(labels.format_skill_name("power bi"), "Power BI")
        self.assertEqual(labels.format_skill_name("python"), "Python")

    def test_number_and_currency_formatting(self) -> None:
        self.assertEqual(labels.format_count(16783), "16,783")
        self.assertEqual(labels.format_pct(12.345), "12.3%")
        self.assertEqual(labels.format_inr_lpa(412000.0), "₹4.1 LPA")
        self.assertEqual(labels.format_jaccard(0.666667), "0.667")

    def test_finding_sentence_builder(self) -> None:
        s1 = labels.build_leader_finding("Bengaluru", 16783, "Hyderabad", 7657, unit="postings")
        self.assertEqual(s1, "Bengaluru has 16,783 postings, 2.2x Hyderabad (7,657).")
        s2 = labels.build_leader_finding("Python", 12.3, "SQL", 10.1, is_pct=True)
        self.assertEqual(s2, "Python leads at 12.3%, 2.2 percentage points ahead of SQL (10.1%).")

    def test_gold_build_date_reads_from_parquet(self) -> None:
        date_str = components.get_gold_build_date()
        self.assertIsNotNone(date_str)
        self.assertRegex(date_str, r"^\d{4}-\d{2}-\d{2}$")

    def test_kitchen_sink_renders_without_exceptions(self) -> None:
        ks_path = str(PROJECT_ROOT / "src" / "demandlens" / "app" / "kitchen_sink.py")
        at = AppTest.from_file(ks_path, default_timeout=30)
        at.run()
        self.assertFalse(at.exception, f"Kitchen sink raised exception: {at.exception}")


if __name__ == "__main__":
    unittest.main()
