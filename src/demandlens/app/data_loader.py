"""Cached Gold-layer Parquet data loaders for the DemandLens Streamlit dashboard."""

from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"

SOURCE_LABELS = {
    "linkedin_2024": "LinkedIn 2024 (US / GB / CA / AU)",
    "indian_job_market_2025_2026": "Indian Job Market 2025–2026 (IN)",
}


def format_source_label(source_id: str) -> str:
    return SOURCE_LABELS.get(source_id, source_id)


@st.cache_data(ttl=3600, show_spinner=False)
def load_gold_table(
    dataset_name: str, columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Load a Gold Parquet dataset directly via PyArrow (no Spark, no raw CSVs)."""
    dataset_dir = GOLD_ROOT / dataset_name
    files = sorted(glob.glob(os.path.join(str(dataset_dir), "*.parquet")))
    if not files:
        raise FileNotFoundError(
            f"Gold dataset '{dataset_name}' not found at {dataset_dir}"
        )
    table = pq.read_table(files, columns=columns)
    return table.to_pandas()


@st.cache_data(ttl=3600, show_spinner=False)
def get_role_totals() -> pd.DataFrame:
    """Extract unique (source_id, standardized_role, total_role_postings) from role_skill_matrix."""
    df = load_gold_table(
        "role_skill_matrix",
        columns=["source_id", "standardized_role", "total_role_postings"],
    )
    return (
        df.drop_duplicates(subset=["source_id", "standardized_role"])
        .sort_values(["source_id", "total_role_postings"], ascending=[True, False])
        .reset_index(drop=True)
    )


def filter_salary_cohorts(
    df: pd.DataFrame,
    role: str = "All",
    tier: str = "All",
    location: str = "All",
    min_sample_size: int = 3,
) -> pd.DataFrame:
    """Filter India salary cohorts consistently by role, seniority tier, location, and min sample size."""
    if df.empty:
        return df.copy()

    mask = df["sample_size"] >= min_sample_size
    if role and role != "All":
        mask = mask & (df["standardized_role"].astype(str).str.strip() == role.strip())
    if tier and tier != "All":
        mask = mask & (df["seniority_tier"].astype(str).str.strip() == tier.strip())
    if location and location != "All":
        mask = mask & (df["location_clean"].astype(str).str.strip() == location.strip())
    return df.loc[mask].copy()
