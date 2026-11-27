"""Location analytics page — geographic posting distribution and remote penetration."""

from __future__ import annotations

import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import load_gold_table


def render() -> None:
    components.render_page_header(
        title="Location analytics",
        question="Which cities and regions concentrate job postings, and how does remote share vary across them?",
        extra_scope="Includes locations with at least 5 job postings in the Gold location table.",
    )

    try:
        loc_df = load_gold_table("job_demand_by_location")
    except Exception as exc:
        components.render_error_state(f"Unable to load location Gold data: {exc}")
        return

    f1, f2, f3, _ = st.columns([2, 1.5, 1.5, 3])
    with f1:
        src = components.source_selector(sorted(loc_df["source_id"].unique()), key_prefix="loc")
    sub = loc_df[loc_df["source_id"] == src]
    countries = ["All"] + sorted(sub["country_code"].unique())
    with f2:
        ctry = st.selectbox(
            "Country",
            options=countries,
            format_func=lambda c: "All countries" if c == "All" else labels.format_country_name(c),
            key="loc_ctry",
        )
    with f3:
        top_n = st.slider("Locations shown", min_value=10, max_value=25, value=15, key="loc_topn")

    if ctry != "All":
        sub = sub[sub["country_code"] == ctry]

    if sub.empty:
        components.render_empty_state(
            "No locations meet the 5-posting minimum for this selection.",
            "Choose All countries or switch snapshot source.",
        )
        return

    total_posts = int(sub["posting_count"].sum())
    remote_posts = int(sub["remote_posting_count"].sum())
    remote_pct = (remote_posts / total_posts * 100.0) if total_posts else 0.0
    top_locs = sub.sort_values("posting_count", ascending=False).head(top_n).copy()

    l1 = top_locs.iloc[0]
    l2 = top_locs.iloc[1] if len(top_locs) > 1 else None

    components.render_stat_strip(
        [
            {"label": "Top location", "value": str(l1["location_clean"]), "note": f"{labels.format_count(int(l1['posting_count']))} postings"},
            {"label": "Locations (>= 5 postings)", "value": labels.format_count(len(sub))},
            {"label": "Postings in selection", "value": labels.format_count(total_posts)},
            {"label": "Remote share", "value": labels.format_pct(remote_pct)},
        ]
    )

    col_main, col_side = st.columns([62, 38], gap="large")
    with col_main:
        components.render_section_title(f"Top {len(top_locs)} hiring locations")
        components.render_finding(
            labels.build_leader_finding(
                str(l1["location_clean"]), int(l1["posting_count"]),
                str(l2["location_clean"]) if l2 is not None else None,
                int(l2["posting_count"]) if l2 is not None else None,
                unit="postings",
            )
        )
        fig = charts.horizontal_ranking_bar(
            top_locs,
            label_col="location_clean",
            value_col="posting_count",
            emphasis_color=theme.SOURCE_COLORS.get(src, theme.INK),
            x_title="Postings",
            height=460,
        )
        charts.render_chart(fig)

    with col_side:
        components.render_section_title("Remote share by top location")
        components.render_finding(
            f"Highest remote share among top {len(top_locs)} locations: {top_locs.sort_values('remote_penetration_pct', ascending=False).iloc[0]['location_clean']} ({labels.format_pct(float(top_locs['remote_penetration_pct'].max()))})."
        )
        summary_tbl = top_locs[["location_clean", "posting_count", "remote_penetration_pct"]].copy()
        st.dataframe(
            summary_tbl,
            column_config={
                "location_clean": st.column_config.TextColumn("Location"),
                "posting_count": st.column_config.NumberColumn("Postings", format="%d"),
                "remote_penetration_pct": st.column_config.NumberColumn("Remote share (%)", format="%.1f%%"),
            },
            hide_index=True,
            row_height=40,
            width="stretch",
        )
        components.render_caption("Locations with fewer than 5 postings are excluded.")

    export_df = top_locs[
        [
            "country_code",
            "location_clean",
            "posting_count",
            "pct_of_country_postings",
            "remote_posting_count",
            "remote_penetration_pct",
            "hybrid_penetration_pct",
        ]
    ].copy()
    export_df["country_code"] = export_df["country_code"].map(labels.format_country_name)
    components.render_underlying_data(
        export_df,
        filename=f"locations_{src}_{ctry}.csv",
        int_cols=["posting_count", "remote_posting_count"],
        pct_cols=["pct_of_country_postings", "remote_penetration_pct", "hybrid_penetration_pct"],
        key="loc_csv",
    )
