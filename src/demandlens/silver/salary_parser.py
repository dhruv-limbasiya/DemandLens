"""DemandLens - Silver Layer Salary Parsing Utilities.

Parses, validates, and structures compensation attributes for job postings.

Guiding constraints:
  - Builds parsers only for formats confirmed in the source datasets.
  - Never treats missing or undisclosed salary as zero ($0).
  - Keeps currency explicit and never converts USD to INR without explicit parameters.
  - Preserves original raw salary text for auditability.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple


# Periodicity detection patterns
_ANNUAL_PATTERN = re.compile(
    r"(?i)\b(pa|p\.a\.|per annum|annual|annually|lpa|lacs pa|lac pa|lakhs pa|lakh pa|year|yearly)\b"
)
_MONTHLY_PATTERN = re.compile(
    r"(?i)\b(pm|p\.m\.|per month|monthly|month)\b"
)
_HOURLY_PATTERN = re.compile(
    r"(?i)\b(ph|p\.h\.|per hour|hourly|hr|hour)\b"
)
_UNDISCLOSED_PATTERN = re.compile(
    r"(?i)\b(not disclosed|undisclosed|unspecified|confidential|best in industry|competitive|negotiable|na|n/a)\b"
)


def parse_numeric_salary(value: Any) -> Optional[float]:
    """Safely cast a raw salary value to a non-zero float.

    - Returns None if input is None, empty, 'Not disclosed', or <= 0.
    - Strips currency symbols (₹, $, £, €) and thousand-separators (,).
    - Prevents treating missing or undisclosed compensation as $0 or ₹0.

    Args:
        value: Numeric float/int or string value.

    Returns:
        Positive float value or None.
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        v = float(value)
        return v if v > 0 else None

    # Handle string value
    s = str(value).strip()
    if not s or _UNDISCLOSED_PATTERN.search(s):
        return None

    # Strip currency signs, commas, and whitespace
    clean_s = re.sub(r"[₹$,£€\s]", "", s)
    try:
        val = float(clean_s)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def detect_salary_periodicity(salary_text: Optional[str]) -> Optional[str]:
    """Detect periodicity from raw salary text.

    Args:
        salary_text: Raw salary string (e.g. '3-6 Lacs PA', '25,000 PM').

    Returns:
        'ANNUAL', 'MONTHLY', 'HOURLY', or None if undisclosed or unparseable.
    """
    if not salary_text:
        return None

    text = salary_text.strip()
    if _UNDISCLOSED_PATTERN.search(text):
        return None

    if _ANNUAL_PATTERN.search(text):
        return "ANNUAL"
    if _MONTHLY_PATTERN.search(text):
        return "MONTHLY"
    if _HOURLY_PATTERN.search(text):
        return "HOURLY"

    return None


def validate_salary_range(
    min_salary: Optional[float],
    max_salary: Optional[float],
    max_cap: float = 500_000_000.0,
) -> Tuple[Optional[float], Optional[float], bool]:
    """Validate numeric salary boundaries and sanity rules.

    Guards against:
      - Negative salaries.
      - min_salary > max_salary.
      - Outlier amounts exceeding sanity cap (default 50 Crore INR = 500M).

    Args:
        min_salary: Lower bound salary.
        max_salary: Upper bound salary.
        max_cap: Maximum realistic threshold.

    Returns:
        Tuple of (validated_min, validated_max, is_valid).
    """
    if min_salary is None and max_salary is None:
        return (None, None, True)

    if (min_salary is not None and min_salary <= 0) or (
        max_salary is not None and max_salary <= 0
    ):
        return (None, None, False)

    if (min_salary is not None and min_salary > max_cap) or (
        max_salary is not None and max_salary > max_cap
    ):
        return (None, None, False)

    if min_salary is not None and max_salary is not None and min_salary > max_salary:
        return (None, None, False)

    return (min_salary, max_salary, True)


def parse_salary_record(
    raw_salary: Optional[str] = None,
    raw_min_salary: Any = None,
    raw_max_salary: Any = None,
    raw_currency: Optional[str] = None,
) -> Dict[str, Any]:
    """Produce a standardized salary record from raw source attributes.

    Args:
        raw_salary: Raw salary descriptive string.
        raw_min_salary: Raw minimum salary attribute.
        raw_max_salary: Raw maximum salary attribute.
        raw_currency: Raw currency string (e.g. 'INR').

    Returns:
        Dictionary with clean, typed, auditable salary attributes.
    """
    min_sal = parse_numeric_salary(raw_min_salary)
    max_sal = parse_numeric_salary(raw_max_salary)

    # Validate range
    v_min, v_max, is_valid = validate_salary_range(min_sal, max_sal)
    if not is_valid:
        v_min, v_max = None, None

    # Calculate midpoint if disclosed
    midpoint = None
    if v_min is not None and v_max is not None:
        midpoint = round((v_min + v_max) / 2.0, 2)
    elif v_min is not None:
        midpoint = v_min
    elif v_max is not None:
        midpoint = v_max

    is_disclosed = midpoint is not None
    periodicity = detect_salary_periodicity(raw_salary) if raw_salary else None

    # Determine currency
    currency = None
    if is_disclosed:
        if raw_currency and raw_currency.strip():
            currency = raw_currency.strip().upper()
        else:
            currency = "INR"  # Default for confirmed Indian listings

    return {
        "raw_salary": raw_salary.strip() if raw_salary else None,
        "salary_currency": currency,
        "salary_min": v_min,
        "salary_max": v_max,
        "salary_midpoint": midpoint,
        "salary_periodicity": periodicity,
        "is_salary_disclosed": is_disclosed,
        "is_valid": is_valid,
    }
