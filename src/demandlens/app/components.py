"""Reusable UI components for the DemandLens dashboard."""

from __future__ import annotations

import glob
import html
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from src.demandlens.app.labels import format_column_name, format_source_name

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
STYLESHEET_PATH = PROJECT_ROOT / "static" / "styles.css"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"

SNAPSHOT_SCOPE_TEXT = (
    "DemandLens analyzes two orthogonal static job-posting snapshots: "
    "**LinkedIn 2024** (1,348,454 Western postings across US, GB, CA, AU) and "
    "**Indian Job Market 2025–26** (97,679 Indian tech-skewed postings). "
    "These datasets are not live feeds and cover different geographies, industries, and periods. "
    "Cross-source comparisons use relative prevalence (%) rather than raw posting counts."
)


def load_stylesheet() -> None:
    """Inject the single DemandLens CSS stylesheet once per page render."""
    if STYLESHEET_PATH.exists():
        css = STYLESHEET_PATH.read_text(encoding="utf-8")
        st.html(f"<style>{css}</style>")


def render_page_header(title: str, question: str, extra_scope: str = "") -> None:
    """Render page title, plain one-sentence analytical question, and right-aligned Data scope popover."""
    col_title, col_scope = st.columns([5, 1], vertical_alignment="center")
    with col_title:
        st.html(
            f'<h1 class="dl-page-title">{html.escape(title)}</h1>'
            f'<p class="dl-question">{html.escape(question)}</p>'
        )
    with col_scope:
        with st.popover("Data scope", width="stretch"):
            st.markdown(SNAPSHOT_SCOPE_TEXT)
            if extra_scope:
                st.markdown(extra_scope)


def render_stat_strip(stats: Sequence[Dict[str, Any]]) -> None:
    """Render one hairline-bordered stat strip. First item is the 42px anchor figure (2x width)."""
    cells: List[str] = []
    for idx, item in enumerate(stats):
        cls = "dl-stat-cell dl-stat-cell--anchor" if idx == 0 else "dl-stat-cell"
        label = html.escape(str(item["label"]))
        value = html.escape(str(item["value"]))
        note = item.get("note")
        note_html = f'<div class="dl-stat-note">{html.escape(str(note))}</div>' if note else ""
        cells.append(
            f'<div class="{cls}">'
            f'<div class="dl-stat-label">{label}</div>'
            f'<div class="dl-stat-value">{value}</div>'
            f"{note_html}"
            f"</div>"
        )
    st.html(f'<div class="dl-stat-strip">{"".join(cells)}</div>')


def render_section_title(text: str) -> None:
    st.html(f'<h2 class="dl-section-title">{html.escape(text)}</h2>')


def render_finding(sentence: str) -> None:
    st.html(f'<p class="dl-finding">{html.escape(sentence)}</p>')


def render_caption(text: str) -> None:
    st.html(f'<p class="dl-caption">{html.escape(text)}</p>')


def render_empty_state(title: str, fix_hint: str) -> None:
    st.html(
        f'<div class="dl-empty-state">'
        f'<div class="dl-empty-title">{html.escape(title)}</div>'
        f'<div class="dl-empty-body">{html.escape(fix_hint)}</div>'
        f"</div>"
    )


def render_error_state(message: str) -> None:
    st.error(message)


def render_underlying_data(
    df: pd.DataFrame,
    filename: str,
    pct_cols: Optional[Sequence[str]] = None,
    int_cols: Optional[Sequence[str]] = None,
    jaccard_cols: Optional[Sequence[str]] = None,
    inr_lpa_cols: Optional[Sequence[str]] = None,
    key: str = "dl_export",
) -> None:
    """Render a collapsed 'Underlying data' expander with formatted column_config and CSV download."""
    pct_set = set(pct_cols or [])
    int_set = set(int_cols or [])
    jac_set = set(jaccard_cols or [])
    lpa_set = set(inr_lpa_cols or [])

    col_config: Dict[str, Any] = {}
    for col in df.columns:
        label = format_column_name(col)
        if col in pct_set:
            col_config[col] = st.column_config.NumberColumn(label, format="%.1f%%")
        elif col in int_set:
            col_config[col] = st.column_config.NumberColumn(label, format="%d")
        elif col in jac_set:
            col_config[col] = st.column_config.NumberColumn(label, format="%.3f")
        elif col in lpa_set:
            col_config[col] = st.column_config.NumberColumn(label, format="₹%.1f LPA")
        else:
            col_config[col] = st.column_config.TextColumn(label)

    with st.expander("Underlying data", expanded=False):
        st.dataframe(
            df,
            column_config=col_config,
            hide_index=True,
            row_height=40,
            width="stretch",
        )
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
            key=key,
        )


@st.cache_data(ttl=3600, show_spinner=False)
def get_gold_build_date() -> Optional[str]:
    """Read the actual _gold_timestamp build date from Gold Parquet files. Returns None if unavailable."""
    if not GOLD_ROOT.exists():
        return None
    dates: List[str] = []
    for entry in sorted(os.listdir(GOLD_ROOT)):
        sub = GOLD_ROOT / entry
        if not sub.is_dir():
            continue
        files = sorted(glob.glob(os.path.join(str(sub), "*.parquet")))
        if not files:
            continue
        try:
            tbl = pq.read_table(files[:1], columns=["_gold_timestamp"])
            vals = tbl.to_pydict().get("_gold_timestamp", [])
            if vals and isinstance(vals[0], str) and len(vals[0]) >= 10:
                dates.append(vals[0][:10])
        except Exception:
            continue
    return max(dates) if dates else None


def source_selector(
    options: Sequence[str],
    label: str = "Snapshot source",
    key_prefix: str = "src",
) -> str:
    """Render a source selectbox that persists selection across all pages."""
    opts = list(options)
    current = st.session_state.get("selected_source")
    if current not in opts and opts:
        current = opts[0]
        st.session_state["selected_source"] = current
    idx = opts.index(current) if current in opts else 0
    chosen = st.selectbox(
        label,
        options=opts,
        index=idx,
        format_func=format_source_name,
        key=f"{key_prefix}_source_widget",
    )
    st.session_state["selected_source"] = chosen
    return chosen


def render_footer() -> None:
    """Render a quiet footer with snapshot names and Gold build date read from Gold data."""
    build_date = get_gold_build_date()
    if not build_date:
        return
    li = format_source_name("linkedin_2024")
    india = format_source_name("indian_job_market_2025_2026")
    st.html(
        f'<div class="dl-footer">'
        f"Snapshots: {html.escape(li)} and {html.escape(india)} · Gold build {html.escape(build_date)}"
        f"</div>"
    )
