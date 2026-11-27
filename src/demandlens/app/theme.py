"""Design tokens for the DemandLens presentation layer."""

from __future__ import annotations

from typing import Dict, List

# Surface & ink colors
PAGE_BG = "#FFFFFF"
SIDEBAR_BG = "#F5F6F7"
BORDER_HAIRLINE = "#DDE1E6"
GRIDLINE = "#E9ECEF"
INK = "#12161C"
TEXT_SECONDARY = "#4B5563"
TEXT_MUTED = "#6B7280"

# Source colors (strictly reserved for source-encoded data)
SOURCE_COLORS: Dict[str, str] = {
    "linkedin_2024": "#2B5C8A",
    "indian_job_market_2025_2026": "#D9822B",
}

# Neutral & emphasis ink
DATA_NEUTRAL = "#AEB8C3"

# Workplace mode colors (never reused for source)
WORKPLACE_COLORS: Dict[str, str] = {
    "Remote": "#1F8A7D",
    "Hybrid": "#9FB0C0",
    "Onsite": "#3F4A55",
}

# Sequential single-hue ramp (light to dark)
SEQUENTIAL_RAMP: List[str] = [
    "#F3F5F8",
    "#C9D4E0",
    "#8FA6BF",
    "#4F7096",
    "#1D3B5A",
]

# Status colors (solid, strictly for real status)
STATUS_SUCCESS = "#2E7D4F"
STATUS_WARNING = "#B7791F"
STATUS_DANGER = "#B3382C"

# Typography
FONT_FAMILY = '"IBM Plex Sans", sans-serif'
TYPE_SCALE: Dict[str, int] = {
    "xs": 12,
    "body": 14,
    "section": 17,
    "h3": 20,
    "secondary_stat": 24,
    "page_title": 29,
    "h1": 35,
    "anchor_stat": 42,
}

# Radii & spacing (4px base)
RADIUS_CONTROL = "3px"
RADIUS_PANEL = "6px"
SPACE = (4, 8, 12, 16, 24, 32, 48)
