"""DemandLens — Phase 1 Internal Kitchen Sink (component & chart verification)."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st

from src.demandlens.app import charts, components, labels, theme

st.set_page_config(
    page_title="DemandLens — Design System Kitchen Sink",
    page_icon="DL",
    layout="wide",
    initial_sidebar_state="expanded",
)

components.load_stylesheet()

st.sidebar.html(
    '<div class="dl-wordmark">DemandLens</div>'
    '<div class="dl-wordmark-sub">Labor &amp; skill intelligence</div>'
)
st.sidebar.caption("Internal design system reference")

components.render_page_header(
    title="Design system kitchen sink",
    question="Fixture verification of every DemandLens typography, stat, component, and chart primitive.",
    extra_scope="Fixture data only — used to verify visual tokens and Streamlit 1.67.0 APIs.",
)

# 1. Hairline Stat Strip
components.render_stat_strip(
    [
        {"label": "Total canonical postings", "value": labels.format_count(1446133), "note": "Two static snapshots"},
        {"label": "LinkedIn 2024", "value": labels.format_count(1348454)},
        {"label": "India 2025–26", "value": labels.format_count(97679)},
        {"label": "Remote share", "value": labels.format_pct(0.83)},
    ]
)

# 2. Horizontal Ranking Bar + Finding Line
components.render_section_title("Top hiring locations")
loc_fixture = pd.DataFrame(
    [
        {"location_clean": "Bengaluru", "posting_count": 16783},
        {"location_clean": "Hyderabad", "posting_count": 7657},
        {"location_clean": "Pune", "posting_count": 6420},
        {"location_clean": "Gurugram", "posting_count": 5110},
        {"location_clean": "Chennai", "posting_count": 4890},
    ]
)
components.render_finding(
    labels.build_leader_finding("Bengaluru", 16783, "Hyderabad", 7657, unit="postings")
)
col_main, col_side = st.columns([62, 38])
with col_main:
    fig_rank = charts.horizontal_ranking_bar(
        loc_fixture,
        label_col="location_clean",
        value_col="posting_count",
        emphasis_color=theme.SOURCE_COLORS["indian_job_market_2025_2026"],
        x_title="Postings",
        height=260,
    )
    charts.render_chart(fig_rank)
with col_side:
    components.render_empty_state(
        "No locations meet the 5-posting minimum for this selection.",
        "Choose All countries or lower the minimum threshold.",
    )

# 3. 100% Stacked Workplace Bar & Grouped Source Comparison
col_a, col_b = st.columns(2)
with col_a:
    components.render_section_title("Workplace mode share")
    wp_fixture = pd.DataFrame(
        [
            {"Source": "LinkedIn 2024", "Workplace": "Remote", "Share": 0.3},
            {"Source": "LinkedIn 2024", "Workplace": "Hybrid", "Share": 0.5},
            {"Source": "LinkedIn 2024", "Workplace": "Onsite", "Share": 99.2},
            {"Source": "Indian Job Market 2025–26", "Workplace": "Remote", "Share": 7.9},
            {"Source": "Indian Job Market 2025–26", "Workplace": "Hybrid", "Share": 0.0},
            {"Source": "Indian Job Market 2025–26", "Workplace": "Onsite", "Share": 92.1},
        ]
    )
    charts.render_chart(charts.workplace_stacked_bar(wp_fixture, height=200))

with col_b:
    components.render_section_title("Source-aware skill prevalence")
    src_fixture = pd.DataFrame(
        [
            {"skill": "Python", "source_id": "linkedin_2024", "Source": "LinkedIn 2024", "prevalence_pct": 10.8},
            {"skill": "Python", "source_id": "indian_job_market_2025_2026", "Source": "Indian Job Market 2025–26", "prevalence_pct": 12.3},
            {"skill": "SQL", "source_id": "linkedin_2024", "Source": "LinkedIn 2024", "prevalence_pct": 9.4},
            {"skill": "SQL", "source_id": "indian_job_market_2025_2026", "Source": "Indian Job Market 2025–26", "prevalence_pct": 14.1},
        ]
    )
    charts.render_chart(
        charts.grouped_source_bar(
            src_fixture,
            category_col="skill",
            value_col="prevalence_pct",
            is_pct=True,
            x_title="Prevalence (%)",
            height=200,
        )
    )

# 4. Heatmap & Salary Range Bar
col_hm, col_sal = st.columns(2)
with col_hm:
    components.render_section_title("Role-skill requirement frequency")
    hm_fixture = pd.DataFrame(
        {
            "Data engineer": [78.4, 72.1, 41.2],
            "Data scientist": [82.0, 54.3, 18.5],
            "Software engineer": [36.2, 29.8, 22.0],
        },
        index=["Python", "SQL", "AWS"],
    )
    charts.render_chart(charts.role_skill_heatmap(hm_fixture, height=260))

with col_sal:
    components.render_section_title("Sample-weighted cohort P25–P75 range (INR LPA)")
    sal_fixture = pd.DataFrame(
        [
            {"role_label": "Data architect", "p25_lpa": 18.0, "median_lpa": 24.5, "p75_lpa": 32.0, "sample_size": 14},
            {"role_label": "Data engineer", "p25_lpa": 11.0, "median_lpa": 16.5, "p75_lpa": 22.0, "sample_size": 138},
            {"role_label": "Data analyst", "p25_lpa": 5.5, "median_lpa": 8.2, "p75_lpa": 12.0, "sample_size": 96},
        ]
    )
    charts.render_chart(charts.salary_range_bar(sal_fixture, height=260))
    components.render_caption("Disclosed on ~34% of India postings; LinkedIn 2024 excluded (0% disclosed).")

# 5. Collapsed Underlying Data Expander
components.render_underlying_data(
    loc_fixture.assign(remote_penetration_pct=[12.4, 8.1, 6.5, 4.2, 5.0]),
    filename="kitchen_sink_locations.csv",
    int_cols=["posting_count"],
    pct_cols=["remote_penetration_pct"],
    key="ks_download",
)

components.render_footer()
