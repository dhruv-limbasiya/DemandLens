"""Plotly chart template and reusable statistical chart builders for DemandLens."""

from __future__ import annotations

from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.demandlens.app import theme

PLOTLY_CONFIG = {
    "displayModeBar": False,
    "scrollZoom": False,
    "doubleClick": False,
}


def apply_chart_theme(
    fig: go.Figure,
    height: int = 380,
    x_grid: bool = True,
    y_grid: bool = False,
    x_title: str = "",
    y_title: str = "",
    show_legend: bool = False,
    margin_left: int = 16,
    margin_right: int = 48,
) -> go.Figure:
    """Apply the DemandLens research-publication Plotly template."""
    fig.update_layout(
        template="none",
        paper_bgcolor=theme.PAGE_BG,
        plot_bgcolor=theme.PAGE_BG,
        font=dict(
            family=theme.FONT_FAMILY,
            size=12,
            color=theme.INK,
        ),
        height=height,
        margin=dict(t=28 if show_legend else 12, r=margin_right, b=36 if x_title else 24, l=margin_left),
        showlegend=show_legend,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            title_text="",
            font=dict(size=12, color=theme.TEXT_SECONDARY),
        ),
        hoverlabel=dict(
            bgcolor=theme.PAGE_BG,
            bordercolor=theme.BORDER_HAIRLINE,
            font=dict(family=theme.FONT_FAMILY, size=12, color=theme.INK),
        ),
    )
    fig.update_xaxes(
        showline=False,
        zeroline=False,
        showgrid=x_grid,
        gridcolor=theme.GRIDLINE,
        gridwidth=1,
        fixedrange=True,
        title_text=x_title,
        title_font=dict(size=12, color=theme.TEXT_SECONDARY),
        tickfont=dict(size=12, color=theme.TEXT_MUTED),
    )
    fig.update_yaxes(
        showline=False,
        zeroline=False,
        showgrid=y_grid,
        gridcolor=theme.GRIDLINE,
        gridwidth=1,
        fixedrange=True,
        automargin=True,
        title_text=y_title,
        title_font=dict(size=12, color=theme.TEXT_SECONDARY),
        tickfont=dict(size=12, color=theme.INK),
    )
    return fig


def render_chart(fig: go.Figure) -> None:
    """Render a Plotly figure with theme=None and modebar hidden."""
    st.plotly_chart(fig, width="stretch", theme=None, config=PLOTLY_CONFIG)


def horizontal_ranking_bar(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    emphasis_color: str = theme.INK,
    highlight_label: Optional[str] = None,
    is_pct: bool = False,
    x_title: str = "",
    height: int = 380,
) -> go.Figure:
    """Horizontal descending ranking bar with outside value labels and single emphasis bar."""
    ordered = df.sort_values(value_col, ascending=True).reset_index(drop=True)
    target = highlight_label if highlight_label else (ordered[label_col].iloc[-1] if len(ordered) else None)
    colors = [
        emphasis_color if lbl == target else theme.DATA_NEUTRAL
        for lbl in ordered[label_col]
    ]
    text_vals = [
        f"{v:.1f}%" if is_pct else f"{int(round(v)):,}"
        for v in ordered[value_col]
    ]
    max_val = float(ordered[value_col].max()) if len(ordered) else 1.0

    fig = go.Figure(
        go.Bar(
            x=ordered[value_col],
            y=ordered[label_col],
            orientation="h",
            marker_color=colors,
            text=text_vals,
            textposition="outside",
            cliponaxis=False,
            textfont=dict(size=12, color=theme.INK),
            hovertemplate="%{y}: %{text}<extra></extra>",
        )
    )
    apply_chart_theme(
        fig,
        height=height,
        x_grid=True,
        y_grid=False,
        x_title=x_title,
        show_legend=False,
        margin_right=64,
    )
    fig.update_xaxes(range=[0, max_val * 1.18 if max_val > 0 else 1])
    return fig


def workplace_stacked_bar(df: pd.DataFrame, height: int = 190) -> go.Figure:
    """100% stacked horizontal bar per source using dedicated workplace palette."""
    fig = go.Figure()
    for mode in ["Remote", "Hybrid", "Onsite"]:
        sub = df[df["Workplace"] == mode]
        fig.add_trace(
            go.Bar(
                name=mode,
                x=sub["Share"],
                y=sub["Source"],
                orientation="h",
                marker_color=theme.WORKPLACE_COLORS[mode],
                text=[f"{v:.1f}%" if v >= 6.0 else "" for v in sub["Share"]],
                textposition="inside",
                insidetextanchor="middle",
                textfont=dict(size=12, color="#FFFFFF" if mode != "Hybrid" else theme.INK),
                hovertemplate="%{y} — " + mode + ": %{x:.1f}%<extra></extra>",
            )
        )
    apply_chart_theme(
        fig,
        height=height,
        x_grid=True,
        y_grid=False,
        x_title="Share of postings (%)",
        show_legend=True,
        margin_right=24,
    )
    fig.update_layout(barmode="stack")
    fig.update_xaxes(range=[0, 100])
    return fig


def grouped_source_bar(
    df: pd.DataFrame,
    category_col: str,
    value_col: str,
    source_col: str = "source_id",
    source_label_col: str = "Source",
    is_pct: bool = True,
    x_title: str = "",
    height: int = 420,
) -> go.Figure:
    """Grouped horizontal comparison bar colored strictly by source_id."""
    fig = go.Figure()
    for src_id, grp in df.groupby(source_col, sort=False):
        label = grp[source_label_col].iloc[0] if source_label_col in grp.columns else src_id
        color = theme.SOURCE_COLORS.get(src_id, theme.DATA_NEUTRAL)
        text_vals = [
            f"{v:.1f}%" if is_pct else f"{int(round(v)):,}"
            for v in grp[value_col]
        ]
        fig.add_trace(
            go.Bar(
                name=label,
                x=grp[value_col],
                y=grp[category_col],
                orientation="h",
                marker_color=color,
                text=text_vals,
                textposition="outside",
                cliponaxis=False,
                textfont=dict(size=11, color=theme.INK),
                hovertemplate="%{y} (" + label + "): %{text}<extra></extra>",
            )
        )
    max_val = float(df[value_col].max()) if len(df) else 1.0
    apply_chart_theme(
        fig,
        height=height,
        x_grid=True,
        y_grid=False,
        x_title=x_title,
        show_legend=True,
        margin_right=64,
    )
    fig.update_layout(barmode="group")
    fig.update_xaxes(range=[0, max_val * 1.18 if max_val > 0 else 1])
    return fig


def role_skill_heatmap(
    matrix_df: pd.DataFrame,
    height: int = 440,
) -> go.Figure:
    """Single-hue heatmap with skills on y-axis, human role names on x-axis, and auto-contrast 1-decimal text."""
    z = matrix_df.values
    z_max = float(z.max()) if z.size else 100.0
    threshold = z_max * 0.52

    colorscale = [
        [0.0, theme.SEQUENTIAL_RAMP[0]],
        [0.25, theme.SEQUENTIAL_RAMP[1]],
        [0.5, theme.SEQUENTIAL_RAMP[2]],
        [0.75, theme.SEQUENTIAL_RAMP[3]],
        [1.0, theme.SEQUENTIAL_RAMP[4]],
    ]

    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=list(matrix_df.columns),
            y=list(matrix_df.index),
            colorscale=colorscale,
            zmin=0,
            zmax=z_max if z_max > 0 else 100,
            showscale=True,
            colorbar=dict(
                title=dict(text="Freq (%)", font=dict(size=12, color=theme.TEXT_SECONDARY)),
                tickfont=dict(size=11, color=theme.TEXT_MUTED),
                thickness=12,
                outlinewidth=0,
            ),
            hovertemplate="Skill: %{y}<br>Role: %{x}<br>Frequency: %{z:.1f}%<extra></extra>",
        )
    )
    annotations = []
    for r_idx, skill in enumerate(matrix_df.index):
        for c_idx, role in enumerate(matrix_df.columns):
            val = float(z[r_idx][c_idx])
            txt_color = "#FFFFFF" if val >= threshold else theme.INK
            annotations.append(
                dict(
                    x=role,
                    y=skill,
                    text=f"{val:.1f}",
                    showarrow=False,
                    font=dict(size=11, color=txt_color, family=theme.FONT_FAMILY),
                )
            )
    apply_chart_theme(
        fig,
        height=height,
        x_grid=False,
        y_grid=False,
        show_legend=False,
        margin_right=24,
    )
    fig.update_layout(annotations=annotations)
    fig.update_xaxes(side="top", tickangle=0)
    return fig


def salary_range_bar(
    df: pd.DataFrame,
    role_col: str = "role_label",
    p25_col: str = "p25_lpa",
    median_col: str = "median_lpa",
    p75_col: str = "p75_lpa",
    sample_col: str = "sample_size",
    height: int = 420,
) -> go.Figure:
    """Horizontal P25–P75 range bar with a median tick, sorted by median, n in hover."""
    ordered = df.sort_values(median_col, ascending=True).reset_index(drop=True)
    spans = ordered[p75_col] - ordered[p25_col]
    custom = list(zip(ordered[p25_col], ordered[median_col], ordered[p75_col], ordered[sample_col]))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=ordered[role_col],
            x=spans,
            base=ordered[p25_col],
            orientation="h",
            marker_color=theme.SEQUENTIAL_RAMP[2],
            name="P25–P75 range",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>"
                "Lower P25: ₹%{customdata[0]:.1f} LPA<br>"
                "Median: ₹%{customdata[1]:.1f} LPA<br>"
                "Upper P75: ₹%{customdata[2]:.1f} LPA<br>"
                "Sample size n = %{customdata[3]:,}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            y=ordered[role_col],
            x=ordered[median_col],
            mode="markers+text",
            marker=dict(symbol="line-ns", size=14, line=dict(width=3, color=theme.INK)),
            text=[f"₹{v:.1f}" for v in ordered[median_col]],
            textposition="top center",
            textfont=dict(size=11, color=theme.INK),
            name="Median",
            hoverinfo="skip",
        )
    )
    max_x = float(ordered[p75_col].max()) if len(ordered) else 10.0
    apply_chart_theme(
        fig,
        height=height,
        x_grid=True,
        y_grid=False,
        x_title="Annual compensation (₹ LPA)",
        show_legend=True,
        margin_right=36,
    )
    fig.update_layout(barmode="overlay")
    fig.update_xaxes(range=[0, max_x * 1.12 if max_x > 0 else 10])
    return fig
