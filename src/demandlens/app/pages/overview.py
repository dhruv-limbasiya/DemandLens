"""Overview page — macro labor demand, workplace mix, and standardized role distribution."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import get_role_totals, load_gold_table


def render() -> None:
    components.render_page_header(
        title="Market overview",
        question="How do job volume, workplace mode, and role mix compare across the two snapshots?",
    )

    try:
        overview = load_gold_table("job_market_overview")
        role_totals = get_role_totals()
    except Exception as exc:
        components.render_error_state(f"Unable to load overview Gold data: {exc}")
        return

    total_jobs = int(overview["total_postings"].sum())
    li_jobs = int(overview.loc[overview["source_id"] == "linkedin_2024", "total_postings"].sum())
    in_jobs = int(overview.loc[overview["source_id"] == "indian_job_market_2025_2026", "total_postings"].sum())
    remote_total = int(overview["remote_postings"].sum())
    remote_pct = (remote_total / total_jobs * 100.0) if total_jobs else 0.0
    countries_count = int(overview["country_code"].nunique())

    # Three computed finding sentences at the top of Overview
    by_country = (
        overview.groupby(["source_id", "country_code"], as_index=False)["total_postings"]
        .sum()
        .sort_values("total_postings", ascending=False)
    )
    by_country["country_label"] = by_country["country_code"].map(labels.format_country_name)
    by_country["Source"] = by_country["source_id"].map(labels.format_source_name)

    wp = overview.groupby("source_id", as_index=False)[
        ["remote_postings", "hybrid_postings", "onsite_postings", "total_postings"]
    ].sum()
    wp["remote_pct"] = (wp["remote_postings"] / wp["total_postings"]) * 100.0
    li_rem = float(wp.loc[wp["source_id"] == "linkedin_2024", "remote_pct"].iloc[0]) if len(wp) else 0.0
    in_rem = float(wp.loc[wp["source_id"] == "indian_job_market_2025_2026", "remote_pct"].iloc[0]) if len(wp) else 0.0

    in_roles_all = role_totals[role_totals["source_id"] == "indian_job_market_2025_2026"]
    in_unclass_pct = (
        float(
            in_roles_all[in_roles_all["standardized_role"].isin(["UNCLASSIFIED", "OTHER_SPECIALIZED"])][
                "total_role_postings"
            ].sum()
        )
        / float(in_roles_all["total_role_postings"].sum())
        * 100.0
        if len(in_roles_all)
        else 0.0
    )

    top_c1 = by_country.iloc[0]
    top_c2 = by_country.iloc[1]
    components.render_finding(
        labels.build_leader_finding(
            top_c1["country_label"], int(top_c1["total_postings"]),
            top_c2["country_label"], int(top_c2["total_postings"]),
            unit="postings",
        )
    )
    components.render_finding(
        f"Remote work accounts for {labels.format_pct(in_rem)} of Indian postings compared with {labels.format_pct(li_rem)} in LinkedIn 2024."
    )
    components.render_finding(
        f"Taxonomy classification covers {labels.format_pct(100.0 - in_unclass_pct)} of Indian postings; {labels.format_pct(in_unclass_pct)} fall into Unclassified or Other specialized."
    )

    components.render_stat_strip(
        [
            {"label": "Total canonical postings", "value": labels.format_count(total_jobs)},
            {"label": "LinkedIn 2024", "value": labels.format_count(li_jobs)},
            {"label": "Indian Job Market 2025–26", "value": labels.format_count(in_jobs)},
            {"label": "Countries covered", "value": labels.format_count(countries_count)},
            {"label": "Combined remote share", "value": labels.format_pct(remote_pct)},
        ]
    )

    # Primary chart (~62%) + Workplace stacked bar (~38%)
    col_main, col_side = st.columns([62, 38], gap="large")

    with col_main:
        components.render_section_title("Postings by country")
        components.render_finding(
            f"{top_c1['country_label']} represents {labels.format_pct(top_c1['total_postings'] / total_jobs * 100.0)} of combined snapshot volume."
        )
        fig_country = charts.grouped_source_bar(
            by_country.sort_values("total_postings", ascending=True),
            category_col="country_label",
            value_col="total_postings",
            is_pct=False,
            x_title="Job postings",
            height=300,
        )
        charts.render_chart(fig_country)

    with col_side:
        components.render_section_title("Workplace mode share")
        components.render_finding(
            "Onsite postings dominate both snapshots; hybrid appears only in LinkedIn 2024."
        )
        wp_melted = wp.melt(
            id_vars=["source_id", "total_postings"],
            value_vars=["remote_postings", "hybrid_postings", "onsite_postings"],
            var_name="Workplace",
            value_name="Postings",
        )
        wp_melted["Share"] = (wp_melted["Postings"] / wp_melted["total_postings"]) * 100.0
        wp_melted["Source"] = wp_melted["source_id"].map(labels.format_source_name)
        wp_melted["Workplace"] = wp_melted["Workplace"].str.replace("_postings", "").str.title()
        charts.render_chart(charts.workplace_stacked_bar(wp_melted, height=230))
        components.render_caption("Hybrid share is 0.0% in the Indian 2025–26 snapshot.")

    # Role rankings below the fold
    components.render_section_title("Top standardized roles")
    f1, f2, _ = st.columns([2, 2, 4])
    with f1:
        sel_src = components.source_selector(
            sorted(role_totals["source_id"].unique()),
            key_prefix="overview_role",
        )
    with f2:
        st.write("")
        include_unclassified = st.checkbox(
            "Include unclassified and other roles",
            value=False,
            key="overview_include_unclassified",
        )

    src_roles = role_totals[role_totals["source_id"] == sel_src].copy()
    total_src_posts = int(src_roles["total_role_postings"].sum())
    classified_posts = int(
        src_roles[~src_roles["standardized_role"].isin(["UNCLASSIFIED", "OTHER_SPECIALIZED"])][
            "total_role_postings"
        ].sum()
    )
    coverage_pct = (classified_posts / total_src_posts * 100.0) if total_src_posts else 0.0

    filtered_roles = (
        src_roles
        if include_unclassified
        else src_roles[~src_roles["standardized_role"].isin(["UNCLASSIFIED", "OTHER_SPECIALIZED"])]
    )
    top_roles = filtered_roles.sort_values("total_role_postings", ascending=False).head(15).copy()
    top_roles["role_label"] = top_roles["standardized_role"].map(labels.format_role_name)

    if top_roles.empty:
        components.render_empty_state(
            "No standardized roles match the current filter.",
            "Enable 'Include unclassified and other roles' to view fallback categories.",
        )
    else:
        r1 = top_roles.iloc[0]
        r2 = top_roles.iloc[1] if len(top_roles) > 1 else None
        components.render_finding(
            labels.build_leader_finding(
                r1["role_label"], int(r1["total_role_postings"]),
                r2["role_label"] if r2 is not None else None,
                int(r2["total_role_postings"]) if r2 is not None else None,
                unit="postings",
            )
            + f" Classified taxonomy coverage: {labels.format_pct(coverage_pct)}."
        )
        fig_roles = charts.horizontal_ranking_bar(
            top_roles,
            label_col="role_label",
            value_col="total_role_postings",
            emphasis_color=theme.SOURCE_COLORS.get(sel_src, theme.INK),
            x_title="Postings",
            height=420,
        )
        charts.render_chart(fig_roles)

        export_df = top_roles[["role_label", "total_role_postings"]].rename(
            columns={"role_label": "standardized_role"}
        )
        components.render_underlying_data(
            export_df,
            filename=f"overview_top_roles_{sel_src}.csv",
            int_cols=["total_role_postings"],
            key="overview_roles_csv",
        )
