"""Skill co-occurrence page — companion skills ranked by Jaccard similarity."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.demandlens.app import charts, components, labels, theme
from src.demandlens.app.data_loader import load_gold_table


def render() -> None:
    components.render_page_header(
        title="Skill co-occurrence",
        question="Which companion skills appear most frequently alongside a selected anchor skill?",
        extra_scope="Computed across the top 300 skills per snapshot with a minimum of 15 co-occurrences.",
    )

    try:
        cooc = load_gold_table("skill_cooccurrence")
    except Exception as exc:
        components.render_error_state(f"Unable to load skill co-occurrence Gold data: {exc}")
        return

    f1, f2, f3, _ = st.columns([2, 2, 1.5, 2.5])
    with f1:
        src = components.source_selector(sorted(cooc["source_id"].unique()), key_prefix="cooc")

    sub = cooc[cooc["source_id"] == src].copy()
    sub["skill_a_label"] = sub["skill_a"].map(labels.format_skill_name)
    sub["skill_b_label"] = sub["skill_b"].map(labels.format_skill_name)

    all_labels = sorted(set(sub["skill_a_label"]).union(set(sub["skill_b_label"])))
    if not all_labels:
        components.render_empty_state(
            "No skill co-occurrence pairs available for this snapshot.",
            "Select another snapshot source.",
        )
        return

    default_idx = all_labels.index("Python") if "Python" in all_labels else 0
    with f2:
        anchor_label = st.selectbox("Anchor skill", options=all_labels, index=default_idx, key="cooc_skill")
    with f3:
        top_n = st.slider("Companions shown", min_value=10, max_value=25, value=15, key="cooc_topn")

    pa = sub[sub["skill_a_label"] == anchor_label].assign(partner_skill=lambda d: d["skill_b_label"])
    pb = sub[sub["skill_b_label"] == anchor_label].assign(partner_skill=lambda d: d["skill_a_label"])
    partners = (
        pd.concat([pa, pb], ignore_index=True)
        .query("partner_skill != @anchor_label")
        .sort_values("jaccard_similarity", ascending=False)
        .drop_duplicates(subset=["partner_skill"])
        .head(top_n)
    )

    if partners.empty:
        components.render_empty_state(
            f"No companion skills meet the 15-co-occurrence minimum for {anchor_label}.",
            "Choose another anchor skill.",
        )
        return

    p1 = partners.iloc[0]
    p2 = partners.iloc[1] if len(partners) > 1 else None

    components.render_stat_strip(
        [
            {"label": f"Closest companion to {anchor_label}", "value": str(p1["partner_skill"]), "note": f"Jaccard {labels.format_jaccard(float(p1['jaccard_similarity']))}"},
            {"label": "Top co-occurrence count", "value": labels.format_count(int(p1["cooccurrence_count"]))},
            {"label": "Qualifying companions", "value": labels.format_count(len(pd.concat([pa, pb])["partner_skill"].unique()))},
        ]
    )

    col_main, col_side = st.columns([65, 35], gap="large")
    with col_main:
        components.render_section_title(f"Top {len(partners)} companion skills by Jaccard similarity")
        second_txt = (
            f", followed by {p2['partner_skill']} ({labels.format_jaccard(float(p2['jaccard_similarity']))})"
            if p2 is not None
            else ""
        )
        components.render_finding(
            f"{p1['partner_skill']} pairs most strongly with {anchor_label} at Jaccard {labels.format_jaccard(float(p1['jaccard_similarity']))} ({labels.format_count(int(p1['cooccurrence_count']))} co-occurrences){second_txt}."
        )

        ordered = partners.sort_values("jaccard_similarity", ascending=True).reset_index(drop=True)
        colors = [
            theme.SOURCE_COLORS.get(src, theme.INK) if i == len(ordered) - 1 else theme.DATA_NEUTRAL
            for i in range(len(ordered))
        ]
        import plotly.graph_objects as go
        fig = go.Figure(
            go.Bar(
                x=ordered["jaccard_similarity"],
                y=ordered["partner_skill"],
                orientation="h",
                marker_color=colors,
                text=[labels.format_jaccard(float(v)) for v in ordered["jaccard_similarity"]],
                textposition="outside",
                cliponaxis=False,
                customdata=ordered["cooccurrence_count"],
                hovertemplate="%{y}<br>Jaccard: %{x:.3f}<br>Co-occurrences: %{customdata:,}<extra></extra>",
            )
        )
        max_j = float(ordered["jaccard_similarity"].max()) if len(ordered) else 1.0
        charts.apply_chart_theme(fig, height=460, x_grid=True, x_title="Jaccard similarity", margin_right=64)
        fig.update_xaxes(range=[0, max_j * 1.18 if max_j > 0 else 1])
        charts.render_chart(fig)
        components.render_caption("Top 300 skills per snapshot; minimum 15 co-occurrences. Hover for exact co-occurrence counts.")

    with col_side:
        components.render_section_title("Pairing counts")
        tbl = partners[["partner_skill", "jaccard_similarity", "cooccurrence_count"]].copy()
        st.dataframe(
            tbl,
            column_config={
                "partner_skill": st.column_config.TextColumn("Companion skill"),
                "jaccard_similarity": st.column_config.NumberColumn("Jaccard", format="%.3f"),
                "cooccurrence_count": st.column_config.NumberColumn("Co-occurrences", format="%d"),
            },
            hide_index=True,
            row_height=40,
            width="stretch",
        )

    components.render_underlying_data(
        partners[["partner_skill", "jaccard_similarity", "cooccurrence_count"]],
        filename=f"skill_cooccurrence_{src}_{anchor_label}.csv",
        jaccard_cols=["jaccard_similarity"],
        int_cols=["cooccurrence_count"],
        key="cooc_csv",
    )
