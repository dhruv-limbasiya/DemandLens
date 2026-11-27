"""Regression tests for Salary Analytics filter combinations (real Gold data + synthetic threshold cases)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.app.data_loader import filter_salary_cohorts, load_gold_table
from src.demandlens.app.pages import salary


class SalaryAnalyticsFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sal_df = load_gold_table("salary_analytics_india")

    def test_all_filters_selected(self) -> None:
        res = filter_salary_cohorts(self.sal_df, role="All", tier="All", location="All")
        self.assertEqual(len(res), len(self.sal_df))
        self.assertTrue((res["sample_size"] >= 3).all())

    def test_single_filters_independently(self) -> None:
        by_role = filter_salary_cohorts(self.sal_df, role="DATA_ENGINEER")
        self.assertGreater(len(by_role), 0)
        self.assertTrue((by_role["standardized_role"] == "DATA_ENGINEER").all())

        by_tier = filter_salary_cohorts(self.sal_df, tier="SENIOR")
        self.assertGreater(len(by_tier), 0)
        self.assertTrue((by_tier["seniority_tier"] == "SENIOR").all())

        by_loc = filter_salary_cohorts(self.sal_df, location="Bengaluru")
        self.assertGreater(len(by_loc), 0)
        self.assertTrue((by_loc["location_clean"] == "Bengaluru").all())

    def test_two_way_filter_combinations(self) -> None:
        role_tier = filter_salary_cohorts(self.sal_df, role="DATA_ENGINEER", tier="SENIOR")
        self.assertGreater(len(role_tier), 0)
        self.assertTrue((role_tier["standardized_role"] == "DATA_ENGINEER").all())
        self.assertTrue((role_tier["seniority_tier"] == "SENIOR").all())

        role_loc = filter_salary_cohorts(self.sal_df, role="DATA_ENGINEER", location="Bengaluru")
        self.assertGreater(len(role_loc), 0)
        self.assertTrue((role_loc["standardized_role"] == "DATA_ENGINEER").all())
        self.assertTrue((role_loc["location_clean"] == "Bengaluru").all())

        tier_loc = filter_salary_cohorts(self.sal_df, tier="SENIOR", location="Bengaluru")
        self.assertGreater(len(tier_loc), 0)
        self.assertTrue((tier_loc["seniority_tier"] == "SENIOR").all())
        self.assertTrue((tier_loc["location_clean"] == "Bengaluru").all())

    def test_three_way_filter_combination_valid_and_empty(self) -> None:
        valid = filter_salary_cohorts(
            self.sal_df, role="DATA_ENGINEER", tier="SENIOR", location="Bengaluru"
        )
        self.assertEqual(len(valid), 1)
        self.assertGreaterEqual(int(valid["sample_size"].iloc[0]), 3)

        empty = filter_salary_cohorts(
            self.sal_df, role="AI_ENGINEER", tier="ENTRY", location="Bengaluru"
        )
        self.assertTrue(empty.empty)

    def test_min_sample_size_threshold_enforced(self) -> None:
        synthetic = pd.DataFrame(
            [
                {"standardized_role": "DATA_ENGINEER", "seniority_tier": "MID", "location_clean": "Pune", "sample_size": 2},
                {"standardized_role": "DATA_ENGINEER", "seniority_tier": "MID", "location_clean": "Bengaluru", "sample_size": 3},
                {"standardized_role": "DATA_ENGINEER", "seniority_tier": "MID", "location_clean": "Chennai", "sample_size": 5},
            ]
        )
        below = filter_salary_cohorts(synthetic, role="DATA_ENGINEER", tier="MID", location="Pune")
        self.assertTrue(below.empty)

        at_threshold = filter_salary_cohorts(synthetic, role="DATA_ENGINEER", tier="MID", location="Bengaluru")
        self.assertEqual(len(at_threshold), 1)
        self.assertEqual(int(at_threshold["sample_size"].iloc[0]), 3)

    def test_ui_cascading_filters_on_salary_page(self) -> None:
        script = (
            "import sys\n"
            f"sys.path.insert(0, {str(PROJECT_ROOT)!r})\n"
            "from src.demandlens.app.pages.salary import render\n"
            "render()\n"
        )
        at = AppTest.from_string(script, default_timeout=30)
        at.run()

        # Select DATA_ENGINEER -> SENIOR -> Bengaluru
        at.selectbox[0].set_value("DATA_ENGINEER").run()
        self.assertIn("Senior", at.selectbox[1].options)
        at.selectbox[1].set_value("SENIOR").run()
        self.assertIn("Bengaluru", at.selectbox[2].options)
        at.selectbox[2].set_value("Bengaluru").run()

        self.assertFalse(at.exception)
        self.assertEqual(len(at.warning), 0)
        self.assertTrue(len(at.html) > 0)

        # Switching to AI_ENGINEER should cascade Seniority Tier back to 'All' (since SENIOR has no AI_ENGINEER cohorts),
        # while preserving Location='Bengaluru' (which exists for AI_ENGINEER).
        at.selectbox[0].set_value("AI_ENGINEER").run()
        self.assertEqual(at.selectbox[1].value, "All")
        self.assertEqual(set(at.selectbox[1].options), {"All tiers", "Mid level"})
        self.assertEqual(at.selectbox[2].value, "Bengaluru")

        # Setting Location back to All shows all 3 AI_ENGINEER cohorts without error
        at.selectbox[2].set_value("All").run()
        self.assertFalse(at.exception)


if __name__ == "__main__":
    unittest.main()
