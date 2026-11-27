"""Company analytics page — employer hiring volume, workplace mix, and role diversity."""

from __future__ import annotations

import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import load_gold_table


def render() -> None:
    components.render_page_header(
        title="Company analytics",
        question="Which employers account for the largest posting volumes, and how broad is their role mix?",
        extra_scope="Excludes postings with missing or blank company names. Company names reflect source-cleaned strings.",
    )

    try:
        comp_df = load_gold_table("job_demand_by_company")
    except Exception as exc:
        components.render_error_state(f"Unable to load company Gold data: {exc}")
        return

    f1, f2, f3, _ = st.columns([2, 1.5, 2, 2.5])
    with f1:
        src = components.source_selector(sorted(comp_df["source_id"].unique()), key_prefix="comp")
    sub = comp_df[comp_df["source_id"] == src]
    countries = ["All"] + sorted(sub["country_code"].dropna().unique())
    with f2:
        ctry = st.selectbox(
            "Country",
            options=countries,
            format_func=lambda c: "All countries" if c == "All" else labels.format_country_name(c),
            key="comp_ctry",
        )
    with f3:
        search = st.text_input("Filter company name", value="", key="comp_search").strip()

    if ctry != "All":
        sub = sub[sub["country_code"] == ctry]
    if search:
        sub = sub[sub["company_name_clean"].str.contains(search, case=False, na=False)]

    if sub.empty:
        components.render_empty_state(
            "No companies match the current filter.",
            "Clear the company name filter or choose All countries.",
        )
        return

    total_posts = int(sub["total_postings"].sum())
    top_comp = sub.sort_values("total_postings", ascending=False).head(15).copy()
    c1 = top_comp.iloc[0]
    c2 = top_comp.iloc[1] if len(top_comp) > 1 else None

    components.render_stat_strip(
        [
            {"label": "Top employer", "value": str(c1["company_name_clean"]), "note": f"{labels.format_count(int(c1['total_postings']))} postings"},
            {"label": "Distinct companies", "value": labels.format_count(len(sub))},
            {"label": "Total postings", "value": labels.format_count(total_posts)},
            {"label": "Median roles / company", "value": f"{sub['distinct_roles'].median():.0f}"},
        ]
    )

    col_main, col_side = st.columns([62, 38], gap="large")
    with col_main:
        components.render_section_title(f"Top {len(top_comp)} hiring companies")
        components.render_finding(
            labels.build_leader_finding(
                str(c1["company_name_clean"]), int(c1["total_postings"]),
                str(c2["company_name_clean"]) if c2 is not None else None,
                int(c2["total_postings"]) if c2 is not None else None,
                unit="postings",
            )
        )
        fig = charts.horizontal_ranking_bar(
            top_comp,
            label_col="company_name_clean",
            value_col="total_postings",
            emphasis_color=theme.SOURCE_COLORS.get(src, theme.INK),
            x_title="Postings",
            height=460,
        )
        charts.render_chart(fig)

    with col_side:
        components.render_section_title("Role & location breadth")
        components.render_finding(
            f"Top {len(top_comp)} employers span up to {int(top_comp['distinct_roles'].max())} distinct roles and {int(top_comp['distinct_locations'].max())} locations."
        )
        breadth_tbl = top_comp[["company_name_clean", "distinct_roles", "distinct_locations", "remote_postings"]].copy()
        st.dataframe(
            breadth_tbl,
            column_config={
                "company_name_clean": st.column_config.TextColumn("Company", width="medium"),
                "distinct_roles": st.column_config.NumberColumn("Roles", format="%d"),
                "distinct_locations": st.column_config.NumberColumn("Locations", format="%d"),
                "remote_postings": st.column_config.NumberColumn("Remote", format="%d"),
            },
            hide_index=True,
            row_height=40,
            width="stretch",
        )
        components.render_caption("Staffing agencies and direct employers are both present in source data.")

    export_df = top_comp[
        [
            "country_code",
            "company_name_clean",
            "total_postings",
            "remote_postings",
            "hybrid_postings",
            "onsite_postings",
            "distinct_roles",
            "distinct_locations",
        ]
    ].copy()
    export_df["country_code"] = export_df["country_code"].map(labels.format_country_name)
    components.render_underlying_data(
        export_df,
        filename=f"companies_{src}_{ctry}.csv",
        int_cols=["total_postings", "remote_postings", "hybrid_postings", "onsite_postings", "distinct_roles", "distinct_locations"],
        key="comp_csv",
    )
