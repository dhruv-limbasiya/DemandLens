"""Smoke test verifying st.navigation shell and all 8 DemandLens pages render without errors."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

from streamlit.testing.v1 import AppTest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class StreamlitDashboardTests(unittest.TestCase):
    def test_navigation_shell_renders_without_exceptions(self) -> None:
        app_path = str(PROJECT_ROOT / "src" / "demandlens" / "app" / "streamlit_app.py")
        at = AppTest.from_file(app_path, default_timeout=30)
        at.run()
        self.assertFalse(at.exception, f"Shell raised exception: {at.exception}")

    def test_all_eight_pages_render_without_exceptions(self) -> None:
        page_names = [
            "overview",
            "locations",
            "companies",
            "skill_demand",
            "role_skill_matrix",
            "skill_cooccurrence",
            "salary",
            "career_pathways",
        ]
        for name in page_names:
            script = (
                "import sys\n"
                f"sys.path.insert(0, {str(PROJECT_ROOT)!r})\n"
                f"from src.demandlens.app.pages.{name} import render\n"
                "render()\n"
            )
            at = AppTest.from_string(script, default_timeout=30)
            at.run()
            self.assertFalse(
                at.exception, f"Page '{name}' raised exception: {at.exception}"
            )


if __name__ == "__main__":
    unittest.main()
