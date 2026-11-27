"""Skill demand page — market skill rankings by relative prevalence (%) and cross-source comparison."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import load_gold_table


def render() -> None:
    components.render_page_header(
        title="Skill demand",
        question="Which skills appear most frequently within each market, and how does relative prevalence compare across US and India?",
        extra_scope="Includes skills appearing in at least 10 postings per market. Cross-source comparison uses relative prevalence (%) to avoid 13.8:1 sample-size distortion.",
    )

    try:
        skills_df = load_gold_table("skill_demand_overall")
    except Exception as exc:
        components.render_error_state(f"Unable to load skill demand Gold data: {exc}")
        return

    f1, f2, f3, _ = st.columns([2, 1.5, 1.5, 3])
    with f1:
        src = components.source_selector(
            sorted(skills_df["source_id"].unique()),
            key_prefix="sk",
        )
    sub_src = skills_df[skills_df["source_id"] == src]
    countries = sorted(sub_src["country_code"].unique())
    with f2:
        ctry = st.selectbox(
            "Country",
            options=countries,
            format_func=labels.format_country_name,
            key="sk_ctry",
        )
    with f3:
        top_n = st.slider("Skills shown", min_value=10, max_value=30, value=15, key="sk_topn")

    sub = sub_src[sub_src["country_code"] == ctry].copy()
    if sub.empty:
        components.render_empty_state(
            "No skills meet the 10-posting minimum for this selection.",
            "Select another country or snapshot source.",
        )
        return

    sub["skill_label"] = sub["skill"].map(labels.format_skill_name)
    # Deduplicate after display-casing normalization by keeping highest posting count
    sub = (
        sub.sort_values("posting_count", ascending=False)
        .drop_duplicates(subset=["skill_label"])
        .reset_index(drop=True)
    )
    top_skills = sub.head(top_n)
    market_total = int(top_skills["total_market_postings"].iloc[0])

    s1 = top_skills.iloc[0]
    s2 = top_skills.iloc[1] if len(top_skills) > 1 else None

    components.render_stat_strip(
        [
            {"label": f"Top skill ({labels.format_country_name(ctry)})", "value": s1["skill_label"], "note": labels.format_pct(float(s1["prevalence_pct"]))},
            {"label": "Market postings", "value": labels.format_count(market_total)},
            {"label": "Skills >= 10 postings", "value": labels.format_count(len(sub))},
        ]
    )

    col_main, col_comp = st.columns([58, 42], gap="large")

    with col_main:
        components.render_section_title(f"Top {top_n} skills in {labels.format_country_name(ctry)}")
        components.render_finding(
            labels.build_leader_finding(
                s1["skill_label"], float(s1["prevalence_pct"]),
                s2["skill_label"] if s2 is not None else None,
                float(s2["prevalence_pct"]) if s2 is not None else None,
                is_pct=True,
            )
        )
        fig_sk = charts.horizontal_ranking_bar(
            top_skills,
            label_col="skill_label",
            value_col="prevalence_pct",
            emphasis_color=theme.SOURCE_COLORS.get(src, theme.INK),
            is_pct=True,
            x_title="Prevalence (%)",
            height=460,
        )
        charts.render_chart(fig_sk)

    with col_comp:
        components.render_section_title("US vs India overlapping skills")
        us_df = (
            skills_df[(skills_df["source_id"] == "linkedin_2024") & (skills_df["country_code"] == "US")]
            .assign(skill_label=lambda d: d["skill"].map(labels.format_skill_name))
            .sort_values("posting_count", ascending=False)
            .drop_duplicates(subset=["skill_label"])[["skill_label", "prevalence_pct"]]
            .assign(source_id="linkedin_2024", Source=labels.format_source_name("linkedin_2024"))
        )
        in_df = (
            skills_df[(skills_df["source_id"] == "indian_job_market_2025_2026") & (skills_df["country_code"] == "IN")]
            .assign(skill_label=lambda d: d["skill"].map(labels.format_skill_name))
            .sort_values("posting_count", ascending=False)
            .drop_duplicates(subset=["skill_label"])[["skill_label", "prevalence_pct"]]
            .assign(source_id="indian_job_market_2025_2026", Source=labels.format_source_name("indian_job_market_2025_2026"))
        )
        overlap = set(us_df["skill_label"]).intersection(set(in_df["skill_label"]))
        if overlap:
            merged = us_df[us_df["skill_label"].isin(overlap)].merge(
                in_df[in_df["skill_label"].isin(overlap)],
                on="skill_label",
                suffixes=("_us", "_in"),
            )
            merged["combined"] = merged["prevalence_pct_us"] + merged["prevalence_pct_in"]
            top_overlap_names = merged.sort_values("combined", ascending=False).head(10)["skill_label"].tolist()
            comp_long = pd.concat(
                [
                    us_df[us_df["skill_label"].isin(top_overlap_names)],
                    in_df[in_df["skill_label"].isin(top_overlap_names)],
                ],
                ignore_index=True,
            )
            order_map = {name: i for i, name in enumerate(reversed(top_overlap_names))}
            comp_long["_ord"] = comp_long["skill_label"].map(order_map)
            comp_long = comp_long.sort_values("_ord")

            top_gap = merged.assign(gap=lambda d: (d["prevalence_pct_in"] - d["prevalence_pct_us"]).abs()).sort_values("gap", ascending=False).iloc[0]
            components.render_finding(
                f"{top_gap['skill_label']} shows the widest US–India prevalence gap ({labels.format_pct(float(top_gap['prevalence_pct_in']))} India vs {labels.format_pct(float(top_gap['prevalence_pct_us']))} US)."
            )
            fig_comp = charts.grouped_source_bar(
                comp_long,
                category_col="skill_label",
                value_col="prevalence_pct",
                is_pct=True,
                x_title="Prevalence (%)",
                height=460,
            )
            charts.render_chart(fig_comp)
            components.render_caption("Compares US (LinkedIn 2024) against India (2025–26) using relative prevalence.")

    export_df = top_skills[["market_skill_rank", "skill_label", "posting_count", "prevalence_pct"]].rename(
        columns={"skill_label": "skill"}
    )
    components.render_underlying_data(
        export_df,
        filename=f"skill_demand_{src}_{ctry}.csv",
        int_cols=["market_skill_rank", "posting_count"],
        pct_cols=["prevalence_pct"],
        key="sk_csv",
    )
