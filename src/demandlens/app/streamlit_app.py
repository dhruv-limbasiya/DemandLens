"""DemandLens — Interactive Labor & Skill Intelligence Dashboard."""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from src.demandlens.app import components
from src.demandlens.app.pages import (
    career_pathways,
    companies,
    locations,
    overview,
    role_skill_matrix,
    salary,
    skill_cooccurrence,
    skill_demand,
)

st.set_page_config(
    page_title="DemandLens — Labor & skill intelligence",
    page_icon="DL",
    layout="wide",
    initial_sidebar_state="expanded",
)

components.load_stylesheet()

st.sidebar.html(
    '<div class="dl-wordmark">DemandLens</div>'
    '<div class="dl-wordmark-sub">Labor &amp; skill intelligence</div>'
)

nav_groups = {
    "Market": [
        st.Page(overview.render, title="Overview", url_path="overview", default=True),
        st.Page(locations.render, title="Locations", url_path="locations"),
        st.Page(companies.render, title="Companies", url_path="companies"),
    ],
    "Skills": [
        st.Page(skill_demand.render, title="Skill demand", url_path="skill-demand"),
        st.Page(role_skill_matrix.render, title="Role-skill matrix", url_path="role-skill-matrix"),
        st.Page(skill_cooccurrence.render, title="Skill co-occurrence", url_path="skill-cooccurrence"),
    ],
    "Outcomes": [
        st.Page(salary.render, title="Salary", url_path="salary"),
        st.Page(career_pathways.render, title="Career pathways", url_path="career-pathways"),
    ],
}

# Hidden dev-only Kitchen Sink (never shown in normal navigation)
if os.environ.get("DEMANDLENS_DEV_UI") == "1":
    nav_groups["Dev"] = [
        st.Page(str(Path(__file__).parent / "kitchen_sink.py"), title="Kitchen sink", url_path="kitchen-sink"),
    ]

pg = st.navigation(nav_groups)
pg.run()

components.render_footer()
