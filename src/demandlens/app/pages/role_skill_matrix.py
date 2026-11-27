"""Role-skill matrix page — required competencies by standardized role and cross-role heatmap."""

from __future__ import annotations

import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import load_gold_table


def render() -> None:
    components.render_page_header(
        title="Role-skill matrix",
        question="Which skills are most frequently required for each standardized role?",
        extra_scope="Includes skills occurring in at least 5 postings per role within each snapshot.",
    )

    try:
        rsm = load_gold_table("role_skill_matrix")
    except Exception as exc:
        components.render_error_state(f"Unable to load role-skill matrix Gold data: {exc}")
        return

    f1, f2, f3, f4, _ = st.columns([2, 2, 1.5, 2, 1.5])
    with f1:
        src = components.source_selector(
            sorted(rsm["source_id"].unique()),
            key_prefix="rsm",
        )
    sub = rsm[rsm["source_id"] == src]
    with f4:
        st.write("")
        include_unclassified = st.checkbox(
            "Include unclassified and other roles",
            value=False,
            key="rsm_include_unclassified",
        )

    avail_roles = sorted(sub["standardized_role"].unique())
    if not include_unclassified:
        avail_roles = [r for r in avail_roles if r not in ("UNCLASSIFIED", "OTHER_SPECIALIZED")]

    if not avail_roles:
        components.render_empty_state(
            "No standardized roles available for this filter.",
            "Enable 'Include unclassified and other roles' or switch snapshot source.",
        )
        return

    default_idx = avail_roles.index("DATA_ENGINEER") if "DATA_ENGINEER" in avail_roles else 0
    with f2:
        role = st.selectbox(
            "Standardized role",
            options=avail_roles,
            index=default_idx,
            format_func=labels.format_role_name,
            key="rsm_role",
        )
    with f3:
        top_n = st.slider("Skills shown", min_value=10, max_value=25, value=15, key="rsm_topn")

    role_df = (
        sub[sub["standardized_role"] == role]
        .assign(skill_label=lambda d: d["skill"].map(labels.format_skill_name))
        .sort_values("skill_occurrences", ascending=False)
        .drop_duplicates(subset=["skill_label"])
        .head(top_n)
    )

    if role_df.empty:
        components.render_empty_state(
            f"No skills meet the 5-posting threshold for {labels.format_role_name(role)}.",
            "Select another standardized role.",
        )
        return

    role_total = int(role_df["total_role_postings"].iloc[0])
    top1 = role_df.iloc[0]
    top2 = role_df.iloc[1] if len(role_df) > 1 else None

    components.render_stat_strip(
        [
            {"label": f"{labels.format_role_name(role)} postings", "value": labels.format_count(role_total)},
            {"label": "Leading skill", "value": top1["skill_label"], "note": labels.format_pct(float(top1["requirement_frequency_pct"]))},
            {"label": "Skills >= 5 postings", "value": labels.format_count(len(sub[sub["standardized_role"] == role]))},
        ]
    )

    col_bar, col_hm = st.columns([55, 45], gap="large")

    with col_bar:
        components.render_section_title(f"Required skills for {labels.format_role_name(role)}")
        components.render_finding(
            labels.build_leader_finding(
                top1["skill_label"], float(top1["requirement_frequency_pct"]),
                top2["skill_label"] if top2 is not None else None,
                float(top2["requirement_frequency_pct"]) if top2 is not None else None,
                is_pct=True,
            )
        )
        fig_bar = charts.horizontal_ranking_bar(
            role_df,
            label_col="skill_label",
            value_col="requirement_frequency_pct",
            emphasis_color=theme.SOURCE_COLORS.get(src, theme.INK),
            is_pct=True,
            x_title="Requirement frequency (%)",
            height=440,
        )
        charts.render_chart(fig_bar)

    with col_hm:
        components.render_section_title("Cross-role skill frequency (%)")
        tech_roles = [
            r
            for r in [
                "SOFTWARE_ENGINEER",
                "DATA_ENGINEER",
                "DATA_SCIENTIST",
                "DATA_ANALYST",
                "MACHINE_LEARNING_ENGINEER",
                "DEVOPS_ENGINEER",
                "FULL_STACK_DEVELOPER",
                "BACKEND_DEVELOPER",
            ]
            if r in sub["standardized_role"].unique()
        ]
        if tech_roles:
            hm_sub = sub[sub["standardized_role"].isin(tech_roles)].copy()
            hm_sub["skill_label"] = hm_sub["skill"].map(labels.format_skill_name)
            hm_sub["role_label"] = hm_sub["standardized_role"].map(labels.format_role_name)
            hm_sub = (
                hm_sub.sort_values("skill_occurrences", ascending=False)
                .drop_duplicates(subset=["role_label", "skill_label"])
            )
            top_hm_skills = (
                hm_sub[hm_sub["role_skill_rank"] <= 6]["skill_label"]
                .value_counts()
                .head(12)
                .index.tolist()
            )
            hm_pivot = hm_sub[hm_sub["skill_label"].isin(top_hm_skills)].pivot_table(
                index="skill_label",
                columns="role_label",
                values="requirement_frequency_pct",
                fill_value=0.0,
            )
            # Sort skills by mean frequency descending
            hm_pivot = hm_pivot.loc[hm_pivot.mean(axis=1).sort_values(ascending=True).index]
            components.render_finding(
                f"Compares {len(top_hm_skills)} core skills across {len(tech_roles)} technical roles with normalized casing."
            )
            charts.render_chart(charts.role_skill_heatmap(hm_pivot, height=440))

    export_df = role_df[
        ["role_skill_rank", "skill_label", "skill_occurrences", "total_role_postings", "requirement_frequency_pct"]
    ].rename(columns={"skill_label": "skill"})
    components.render_underlying_data(
        export_df,
        filename=f"role_skill_matrix_{src}_{role}.csv",
        int_cols=["role_skill_rank", "skill_occurrences", "total_role_postings"],
        pct_cols=["requirement_frequency_pct"],
        key="rsm_csv",
    )
