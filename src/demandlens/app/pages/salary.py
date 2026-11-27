"""Salary analytics page — India annual INR compensation benchmarks with cascading filters."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.demandlens.app import charts, components, labels
from src.demandlens.app.data_loader import filter_salary_cohorts, load_gold_table


def render() -> None:
    components.render_page_header(
        title="Salary analytics",
        question="How does disclosed annual INR compensation vary across roles, seniority tiers, and Indian cities?",
        extra_scope="LinkedIn 2024 has 0% disclosed salary data and is excluded. Indian Job Market 2025–26 discloses annual INR compensation on ~34% of postings; cohorts require sample size >= 3.",
    )

    try:
        sal = load_gold_table("salary_analytics_india")
    except Exception as exc:
        components.render_error_state(f"Unable to load India salary Gold data: {exc}")
        return

    valid_sal = sal[sal["sample_size"] >= 3].copy()

    f1, f2, f3, f4, _ = st.columns([2, 1.5, 2, 2, 1.5])
    with f4:
        st.write("")
        include_unclassified = st.checkbox(
            "Include unclassified and other roles",
            value=False,
            key="sal_include_unclassified",
        )

    role_pool = (
        valid_sal
        if include_unclassified
        else valid_sal[~valid_sal["standardized_role"].isin(["UNCLASSIFIED", "OTHER_SPECIALIZED"])]
    )
    roles = ["All"] + sorted(role_pool["standardized_role"].dropna().unique())
    if st.session_state.get("sal_role") not in roles:
        st.session_state["sal_role"] = "All"
    with f1:
        role = st.selectbox(
            "Standardized role",
            options=roles,
            format_func=lambda r: "All roles" if r == "All" else labels.format_role_name(r),
            key="sal_role",
        )

    role_scoped = role_pool if role == "All" else role_pool[role_pool["standardized_role"] == role]
    tiers = ["All"] + sorted(role_scoped["seniority_tier"].dropna().unique())
    if st.session_state.get("sal_tier") not in tiers:
        st.session_state["sal_tier"] = "All"
    with f2:
        tier = st.selectbox(
            "Seniority tier",
            options=tiers,
            format_func=lambda t: "All tiers" if t == "All" else labels.format_seniority_name(t),
            key="sal_tier",
        )

    tier_scoped = role_scoped if tier == "All" else role_scoped[role_scoped["seniority_tier"] == tier]
    locs = ["All"] + sorted(tier_scoped["location_clean"].dropna().unique())
    if st.session_state.get("sal_loc") not in locs:
        st.session_state["sal_loc"] = "All"
    with f3:
        loc = st.selectbox(
            "Location",
            options=locs,
            format_func=lambda x: "All locations" if x == "All" else x,
            key="sal_loc",
        )

    base_df = role_pool
    sub = filter_salary_cohorts(base_df, role=role, tier=tier, location=loc, min_sample_size=3)

    if sub.empty:
        components.render_empty_state(
            "No salary cohorts meet the minimum sample size of 3 for this filter combination.",
            "Reset Location or Seniority tier to All, or include unclassified roles.",
        )
        return

    total_samples = int(sub["sample_size"].sum())
    weighted_mid_inr = float((sub["salary_mid_median_inr"] * sub["sample_size"]).sum() / total_samples)
    components.render_stat_strip(
        [
            {"label": "Sample-weighted median midpoint", "value": labels.format_inr_lpa(weighted_mid_inr), "note": "India annual INR"},
            {"label": "Disclosed cohorts (n >= 3)", "value": labels.format_count(len(sub))},
            {"label": "Disclosed postings in selection", "value": labels.format_count(total_samples)},
        ]
    )

    # Sample-weighted role summary
    role_summary = (
        sub.groupby("standardized_role", as_index=False)
        .apply(
            lambda g: pd.Series(
                {
                    "p25_lpa": float((g["salary_min_p25_inr"] * g["sample_size"]).sum() / g["sample_size"].sum() / 100_000.0),
                    "median_lpa": float((g["salary_mid_median_inr"] * g["sample_size"]).sum() / g["sample_size"].sum() / 100_000.0),
                    "p75_lpa": float((g["salary_max_p75_inr"] * g["sample_size"]).sum() / g["sample_size"].sum() / 100_000.0),
                    "sample_size": int(g["sample_size"].sum()),
                }
            ),
            include_groups=False,
        )
        .sort_values("median_lpa", ascending=False)
        .head(15)
    )
    role_summary["role_label"] = role_summary["standardized_role"].map(labels.format_role_name)

    top_r = role_summary.iloc[0]
    bot_r = role_summary.iloc[-1]
    components.render_section_title("Sample-weighted cohort P25–P75 range (INR LPA)")
    if len(role_summary) > 1:
        components.render_finding(
            f"{top_r['role_label']} has the highest weighted median at ₹{top_r['median_lpa']:.1f} LPA (n = {int(top_r['sample_size']):,}), compared with {bot_r['role_label']} at ₹{bot_r['median_lpa']:.1f} LPA."
        )
    else:
        components.render_finding(
            f"{top_r['role_label']} has a weighted median of ₹{top_r['median_lpa']:.1f} LPA across {int(top_r['sample_size']):,} disclosed postings."
        )

    charts.render_chart(charts.salary_range_bar(role_summary, height=max(260, len(role_summary) * 32 + 60)))
    components.render_caption(
        "Disclosed on ~34% of Indian Job Market 2025–26 postings; LinkedIn 2024 is excluded (0% disclosed). Minimum cohort sample size n = 3."
    )

    export_df = sub.sort_values("sample_size", ascending=False).head(200).copy()
    export_df["standardized_role"] = export_df["standardized_role"].map(labels.format_role_name)
    export_df["seniority_tier"] = export_df["seniority_tier"].map(labels.format_seniority_name)
    export_df["salary_mid_median_inr"] = export_df["salary_mid_median_inr"] / 100_000.0
    export_df["salary_min_p25_inr"] = export_df["salary_min_p25_inr"] / 100_000.0
    export_df["salary_max_p75_inr"] = export_df["salary_max_p75_inr"] / 100_000.0
    export_df = export_df[
        [
            "standardized_role",
            "seniority_tier",
            "location_clean",
            "sample_size",
            "salary_min_p25_inr",
            "salary_mid_median_inr",
            "salary_max_p75_inr",
        ]
    ]
    components.render_underlying_data(
        export_df,
        filename="india_salary_cohorts.csv",
        int_cols=["sample_size"],
        inr_lpa_cols=["salary_min_p25_inr", "salary_mid_median_inr", "salary_max_p75_inr"],
        key="sal_csv",
    )
