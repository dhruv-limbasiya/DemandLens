"""DemandLens - Silver Layer Location Cleaning Utilities.

Cleans location attributes, detects remote work arrangements, and formats
locations without fabricating unsupported geographic entities.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# Verified remote / hybrid keywords
_REMOTE_PATTERN = re.compile(
    r"(?i)\b(remote|hybrid|work from home|wfh|telecommute|anywhere|virtual)\b"
)
_ON_SITE_PATTERN = re.compile(r"(?i)\b(on-site|onsite|in-office|office)\b")


def clean_location_string(
    location: Optional[str], default: Optional[str] = None
) -> Optional[str]:
    """Clean and normalize a raw location string.

    - Trims whitespace and collapses repeated spaces.
    - Strips outer punctuation noise.
    - Preserves compound locations (e.g. "Bengaluru, Mumbai, Pune").
    - Does NOT invent or guess missing cities/countries.

    Args:
        location: Raw location string.
        default: Fallback if empty or None.

    Returns:
        Cleaned location string or default.
    """
    if location is None:
        return default

    cleaned = re.sub(r"\s+", " ", location).strip(" \t\r\n,;-")
    return cleaned if cleaned else default


def detect_remote(location: Optional[str]) -> Optional[bool]:
    """Detect whether a job posting indicates a remote or hybrid arrangement.

    Args:
        location: Raw or cleaned location string.

    Returns:
        True if remote/hybrid indicators are found.
        False if explicitly on-site or a regular location string without remote markers.
        None if location is null or empty.
    """
    if location is None or not location.strip():
        return None

    text = location.strip()
    if _REMOTE_PATTERN.search(text):
        return True

    return False


def extract_location_parts(location: Optional[str]) -> Dict[str, Any]:
    """Safely split location string into distinct location tokens.

    Splits compound location strings (e.g. "Bengaluru, Karnataka, India" or
    "Noida, Greater Noida") into individual trimmed parts without guessing
    which token is city vs state vs country unless explicitly supported.

    Args:
        location: Raw location string.

    Returns:
        Dictionary with:
          - 'raw_location': original string
          - 'cleaned_location': normalized string
          - 'location_tokens': list of non-empty sub-components
          - 'is_remote': boolean remote indicator
    """
    if location is None:
        return {
            "raw_location": None,
            "cleaned_location": None,
            "location_tokens": [],
            "is_remote": None,
        }

    cleaned = clean_location_string(location)
    is_rem = detect_remote(cleaned)

    if not cleaned:
        return {
            "raw_location": location,
            "cleaned_location": None,
            "location_tokens": [],
            "is_remote": is_rem,
        }

    # Split on commas or slashes for compound locations
    tokens = [
        re.sub(r"\s+", " ", part).strip(" \t\r\n,;-")
        for part in re.split(r"[,/|]", cleaned)
    ]
    tokens = [t for t in tokens if t]

    return {
        "raw_location": location,
        "cleaned_location": cleaned,
        "location_tokens": tokens,
        "is_remote": is_rem,
    }
