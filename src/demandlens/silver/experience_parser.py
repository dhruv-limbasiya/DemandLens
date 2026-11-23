"""DemandLens - Silver Layer Experience Parsing Utilities.

Parses experience ranges, validates numeric boundaries, and harmonizes
categorical seniority levels without fabricating unsupported numbers.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple


# Regex patterns for confirmed experience formats
_RANGE_PATTERN = re.compile(
    r"(?i)\b(\d+(?:\.\d+)?)\s*(?:-|to|–|—)\s*(\d+(?:\.\d+)?)\s*(?:yrs|years|yr|year)?\b"
)
_PLUS_PATTERN = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*(?:\+|\s+plus|\s+and\s+above|\s+or\s+more)(?:\s*(?:yrs|years|yr|year))?"
)
_SINGLE_PATTERN = re.compile(
    r"(?i)\b(\d+(?:\.\d+)?)\s*(?:yrs|years|yr|year)\b"
)
_FRESHER_PATTERN = re.compile(
    r"(?i)\b(fresher|freshers|intern|internship|trainee|0\s*-\s*0)\b"
)


def parse_experience_range(
    text: Optional[str],
) -> Tuple[Optional[float], Optional[float]]:
    """Extract minimum and maximum years of experience from raw text.

    Supported patterns:
      - "2-5 Yrs", "2 to 5 years", "0-1 Yrs" -> (2.0, 5.0)
      - "5+ Yrs", "5 + years", "5 or more" -> (5.0, None)
      - "Fresher", "Internship" -> (0.0, 0.0)
      - "3 Years", "3 Yrs" -> (3.0, 3.0)
      - Unparseable, null, or empty string -> (None, None)

    Args:
        text: Raw experience string (e.g. from 'experience' column).

    Returns:
        Tuple of (min_years, max_years) as floats or None.
    """
    if text is None:
        return (None, None)

    cleaned = text.strip()
    if not cleaned:
        return (None, None)

    # 1. Check for fresher / entry indicators
    if _FRESHER_PATTERN.search(cleaned):
        # If range like "0 - 1 Yrs", range pattern below will capture it
        if not re.search(r"\d+\s*[-to]\s*\d+", cleaned):
            return (0.0, 0.0)

    # 2. Check for range: "X - Y Yrs"
    match_range = _RANGE_PATTERN.search(cleaned)
    if match_range:
        try:
            v_min = float(match_range.group(1))
            v_max = float(match_range.group(2))
            if v_min > v_max:
                v_min, v_max = v_max, v_min
            return (v_min, v_max)
        except (ValueError, TypeError):
            pass

    # 3. Check for plus notation: "X+ Yrs"
    match_plus = _PLUS_PATTERN.search(cleaned)
    if match_plus:
        try:
            v_min = float(match_plus.group(1))
            return (v_min, None)
        except (ValueError, TypeError):
            pass

    # 4. Check for single duration: "X Yrs"
    match_single = _SINGLE_PATTERN.search(cleaned)
    if match_single:
        try:
            val = float(match_single.group(1))
            return (val, val)
        except (ValueError, TypeError):
            pass

    return (None, None)


def validate_experience_bounds(
    min_exp: Optional[float],
    max_exp: Optional[float],
    max_reasonable_years: float = 50.0,
) -> Tuple[Optional[float], Optional[float], bool]:
    """Validate numeric experience boundaries.

    Guards against:
      - Negative experience values.
      - min_exp > max_exp.
      - Impossibly high career durations (> 50 years).

    Args:
        min_exp: Minimum experience in years.
        max_exp: Maximum experience in years.
        max_reasonable_years: Upper sanity cap (default 50.0).

    Returns:
        Tuple of (validated_min, validated_max, is_valid).
    """
    if min_exp is None and max_exp is None:
        return (None, None, True)

    # Check non-negative
    if (min_exp is not None and min_exp < 0) or (max_exp is not None and max_exp < 0):
        return (None, None, False)

    # Check upper cap
    if (min_exp is not None and min_exp > max_reasonable_years) or (
        max_exp is not None and max_exp > max_reasonable_years
    ):
        return (None, None, False)

    # Check ordering
    if min_exp is not None and max_exp is not None and min_exp > max_exp:
        return (None, None, False)

    return (min_exp, max_exp, True)


def classify_experience_tier(
    min_exp: Optional[float] = None,
    max_exp: Optional[float] = None,
    raw_level: Optional[str] = None,
) -> Optional[str]:
    """Classify career level into standardized enterprise seniority tiers.

    Standard tiers:
      - 'ENTRY': 0 to 2 years, or raw level 'entry level', 'internship', 'fresher'
      - 'MID': 3 to 5 years, or raw level 'associate', 'mid-senior level' (lower)
      - 'SENIOR': 6 to 9 years, or raw level 'senior'
      - 'LEAD_EXECUTIVE': 10+ years, or raw level 'director', 'executive', 'lead'
      - None: when missing or unclassifiable.

    Args:
        min_exp: Numeric minimum experience years.
        max_exp: Numeric maximum experience years.
        raw_level: Categorical seniority string (e.g. LinkedIn 'job_level').

    Returns:
        Standard tier string or None.
    """
    # 1. Prefer numeric minimum experience if available
    if min_exp is not None and min_exp >= 0:
        if min_exp <= 2.0:
            return "ENTRY"
        elif min_exp <= 5.0:
            return "MID"
        elif min_exp <= 9.0:
            return "SENIOR"
        else:
            return "LEAD_EXECUTIVE"

    # 2. Fall back to categorical raw_level string
    if raw_level:
        lvl = raw_level.strip().lower()
        if any(k in lvl for k in ["intern", "fresher", "entry", "graduate", "trainee"]):
            return "ENTRY"
        if any(k in lvl for k in ["associate", "junior", "mid-senior", "mid level", "mid"]):
            return "MID"
        if any(k in lvl for k in ["senior", "specialist", "principal"]):
            return "SENIOR"
        if any(k in lvl for k in ["director", "executive", "vp", "vice president", "head", "chief", "lead"]):
            return "LEAD_EXECUTIVE"

    return None
