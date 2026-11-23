"""DemandLens - Silver Layer Posting Freshness Classification.

Provides deterministic classification of relative posting age strings
(such as '1 Day Ago', '30+ Days Ago') without fabricating synthetic timestamps.
"""

from __future__ import annotations

import re
from typing import Optional


# Deterministic freshness category constants
FRESH_24H = "FRESH_24H"
RECENT_7D = "RECENT_7D"
MODERATE_30D = "MODERATE_30D"
STALE_30D_PLUS = "STALE_30D_PLUS"
UNKNOWN_FRESHNESS = "UNKNOWN"

# Regular expressions for confirmed relative time patterns
_FEW_HOURS_PATTERN = re.compile(
    r"(?i)\b(just now|few hours ago|an hour ago|today|hours? ago)\b"
)
_DAYS_AGO_PATTERN = re.compile(
    r"(?i)\b(\d+)\s*(?:days?|day)\s+ago\b"
)
_YESTERDAY_PATTERN = re.compile(
    r"(?i)\b(yesterday|1 day ago)\b"
)
_WEEKS_AGO_PATTERN = re.compile(
    r"(?i)\b(\d+)\s*(?:weeks?|week)\s+ago\b"
)
_MONTHS_AGO_PATTERN = re.compile(
    r"(?i)\b(\d+)\s*(?:months?|month)\s+ago\b"
)
_STALE_EXPLICIT_PATTERN = re.compile(
    r"(?i)\b(30\+\s*days?|30\+\s*days?\s*ago|[2-9]\s*months?\s*ago)\b"
)


def parse_relative_days(uploaded_text: Optional[str]) -> Optional[int]:
    """Extract approximate minimum relative age in days from a relative text string.

    Does NOT fabricate a calendar date. Extracts a relative duration metric.

    Examples:
      - "Few Hours Ago", "Just Now", "Today" -> 0
      - "1 Day Ago", "Yesterday" -> 1
      - "5 Days Ago" -> 5
      - "1 Week Ago" -> 7
      - "2 Weeks Ago" -> 14
      - "30+ Days Ago" -> 30
      - None or unparseable -> None

    Args:
        uploaded_text: Relative date string (e.g. from 'jobUploaded').

    Returns:
        Integer day estimate or None.
    """
    if uploaded_text is None:
        return None

    text = uploaded_text.strip()
    if not text:
        return None

    if _FEW_HOURS_PATTERN.search(text):
        return 0

    if _YESTERDAY_PATTERN.search(text):
        return 1

    # Check "X Days Ago"
    m_days = _DAYS_AGO_PATTERN.search(text)
    if m_days:
        try:
            return int(m_days.group(1))
        except (ValueError, TypeError):
            pass

    # Check "X Weeks Ago"
    m_weeks = _WEEKS_AGO_PATTERN.search(text)
    if m_weeks:
        try:
            return int(m_weeks.group(1)) * 7
        except (ValueError, TypeError):
            pass

    # Check "X Months Ago"
    m_months = _MONTHS_AGO_PATTERN.search(text)
    if m_months:
        try:
            return int(m_months.group(1)) * 30
        except (ValueError, TypeError):
            pass

    if _STALE_EXPLICIT_PATTERN.search(text):
        return 30

    return None


def classify_freshness(uploaded_text: Optional[str]) -> str:
    """Classify relative upload text into a deterministic freshness tier.

    Tiers:
      - 'FRESH_24H': <= 1 day old (Today, Just Now, 1 Day Ago).
      - 'RECENT_7D': 2 to 7 days old (2-6 Days Ago, 1 Week Ago).
      - 'MODERATE_30D': 8 to 30 days old (2-4 Weeks Ago, 10-29 Days Ago).
      - 'STALE_30D_PLUS': > 30 days old (30+ Days Ago, 1+ Months Ago).
      - 'UNKNOWN': Null, empty, or unparseable text.

    Args:
        uploaded_text: Relative date string (e.g. from 'jobUploaded').

    Returns:
        Standard freshness tier string.
    """
    if uploaded_text is None or not uploaded_text.strip():
        return UNKNOWN_FRESHNESS

    text = uploaded_text.strip()

    # Direct keyword matches
    if _FEW_HOURS_PATTERN.search(text) or _YESTERDAY_PATTERN.search(text):
        return FRESH_24H

    if _STALE_EXPLICIT_PATTERN.search(text):
        return STALE_30D_PLUS

    # Estimate days
    days = parse_relative_days(text)
    if days is not None:
        if days <= 1:
            return FRESH_24H
        elif days <= 7:
            return RECENT_7D
        elif days <= 30:
            return MODERATE_30D
        else:
            return STALE_30D_PLUS

    return UNKNOWN_FRESHNESS
