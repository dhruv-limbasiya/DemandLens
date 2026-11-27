"""Career pathways page — entry-level role volumes and starter skill requirements."""

from __future__ import annotations

import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import load_gold_table


def render() -> None:
    components.render_page_header(
        title="Career pathways",
        question="Which roles offer the most entry-level openings, and which starter skills do they require?",
        extra_scope="Entry-level is defined as ENTRY/ASSOCIATE in LinkedIn 2024, and ENTRY or <= 2 years experience in Indian Job Market 2025–26. Minimum 5 entry postings per role-skill pair.",
    )

    try:
        cp = load_gold_table("career_pathways_entry_level")
    except Exception as exc:
        components.render_error_state(f"Unable to load career pathways Gold data: {exc}")
        return

    f1, f2, f3, f4, _ = st.columns([2, 1.5, 2, 2, 1])
    with f1:
        src = components.source_selector(sorted(cp["source_id"].unique()), key_prefix="cp")
    sub_src = cp[cp["source_id"] == src]
    countries = sorted(sub_src["country_code"].unique())
    with f2:
        ctry = st.selectbox(
            "Country",
            options=countries,
            format_func=labels.format_country_name,
            key="cp_ctry",
        )
    with f4:
        st.write("")
        include_unclassified = st.checkbox(
            "Include unclassified and other roles",
            value=False,
            key="cp_include_unclassified",
        )

    sub = sub_src[sub_src["country_code"] == ctry]
    if not include_unclassified:
        sub = sub[~sub["standardized_role"].isin(["UNCLASSIFIED", "OTHER_SPECIALIZED"])]

    roles = sorted(sub["standardized_role"].unique())
    if not roles:
        components.render_empty_state(
            "No entry-level roles meet the 5-posting threshold for this selection.",
            "Enable 'Include unclassified and other roles' or switch country/source.",
        )
        return

    default_idx = roles.index("SOFTWARE_ENGINEER") if "SOFTWARE_ENGINEER" in roles else 0
    with f3:
        role = st.selectbox(
            "Target entry-level role",
            options=roles,
            index=default_idx,
            format_func=labels.format_role_name,
            key="cp_role",
        )

    role_totals = (
        sub.drop_duplicates(subset=["standardized_role"])
        .sort_values("role_entry_total", ascending=False)
        .head(12)
        .copy()
    )
    role_totals["role_label"] = role_totals["standardized_role"].map(labels.format_role_name)

    role_skills = (
        sub[sub["standardized_role"] == role]
        .assign(skill_label=lambda d: d["skill"].map(labels.format_skill_name))
        .sort_values("entry_posting_count", ascending=False)
        .drop_duplicates(subset=["skill_label"])
        .head(15)
    )

    if role_skills.empty:
        components.render_empty_state(
            f"No starter skills meet the 5-posting threshold for entry-level {labels.format_role_name(role)}.",
            "Select another target role.",
        )
        return

    top_role = role_totals.iloc[0]
    top_sk1 = role_skills.iloc[0]
    top_sk2 = role_skills.iloc[1] if len(role_skills) > 1 else None
    target_entry_total = int(role_skills["role_entry_total"].iloc[0])

    components.render_stat_strip(
        [
            {"label": f"Entry {labels.format_role_name(role)} postings", "value": labels.format_count(target_entry_total)},
            {"label": "Top starter skill", "value": str(top_sk1["skill_label"]), "note": labels.format_pct(float(top_sk1["skill_entry_prevalence_pct"]))},
            {"label": "Leading entry role", "value": str(top_role["role_label"]), "note": f"{labels.format_count(int(top_role['role_entry_total']))} postings"},
        ]
    )

    col_left, col_right = st.columns([44, 56], gap="large")
    with col_left:
        components.render_section_title(f"Top entry-level roles in {labels.format_country_name(ctry)}")
        r2 = role_totals.iloc[1] if len(role_totals) > 1 else None
        components.render_finding(
            labels.build_leader_finding(
                str(top_role["role_label"]), int(top_role["role_entry_total"]),
                str(r2["role_label"]) if r2 is not None else None,
                int(r2["role_entry_total"]) if r2 is not None else None,
                unit="postings",
            )
        )
        fig_roles = charts.horizontal_ranking_bar(
            role_totals,
            label_col="role_label",
            value_col="role_entry_total",
            highlight_label=labels.format_role_name(role),
            emphasis_color=theme.SOURCE_COLORS.get(src, theme.INK),
            x_title="Entry-level postings",
            height=440,
        )
        charts.render_chart(fig_roles)

    with col_right:
        components.render_section_title(f"Starter skills for {labels.format_role_name(role)}")
        components.render_finding(
            labels.build_leader_finding(
                str(top_sk1["skill_label"]), float(top_sk1["skill_entry_prevalence_pct"]),
                str(top_sk2["skill_label"]) if top_sk2 is not None else None,
                float(top_sk2["skill_entry_prevalence_pct"]) if top_sk2 is not None else None,
                is_pct=True,
            )
        )
        fig_sk = charts.horizontal_ranking_bar(
            role_skills,
            label_col="skill_label",
            value_col="skill_entry_prevalence_pct",
            emphasis_color=theme.SOURCE_COLORS.get(src, theme.INK),
            is_pct=True,
            x_title="Entry-level prevalence (%)",
            height=440,
        )
        charts.render_chart(fig_sk)
        components.render_caption("Minimum 5 entry-level postings per role-skill pair.")

    export_df = role_skills[
        ["entry_skill_rank", "skill_label", "entry_posting_count", "role_entry_total", "skill_entry_prevalence_pct", "remote_entry_pct"]
    ].rename(columns={"skill_label": "skill"})
    components.render_underlying_data(
        export_df,
        filename=f"career_pathways_{src}_{ctry}_{role}.csv",
        int_cols=["entry_skill_rank", "entry_posting_count", "role_entry_total"],
        pct_cols=["skill_entry_prevalence_pct", "remote_entry_pct"],
        key="cp_csv",
    )
