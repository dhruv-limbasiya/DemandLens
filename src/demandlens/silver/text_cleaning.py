"""DemandLens - Silver Layer Text Cleaning Utilities.

Reusable string cleansing functions for tabular fields and long-form descriptions.
Preserves original values where required for auditability and lineage.
"""

from __future__ import annotations

import html
import re
from typing import Optional


def clean_text(text: Optional[str], default: Optional[str] = None) -> Optional[str]:
    """Clean and normalize a single-line text attribute.

    - Trims leading and trailing whitespace.
    - Collapses consecutive whitespace (spaces, tabs, newlines) to a single space.
    - Converts empty strings to `default`.
    - Preserves case and characters.

    Args:
        text: Raw input string.
        default: Return value if text is null or empty (defaults to None).

    Returns:
        Cleaned string or default value.
    """
    if text is None:
        return default

    # Strip and collapse repeated whitespace
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned if cleaned else default


def normalize_multiline_text(
    text: Optional[str], default: Optional[str] = None
) -> Optional[str]:
    """Normalize multi-line text (e.g. job descriptions) while preserving paragraphs.

    - Normalizes CRLF and CR to standard LF (\n).
    - Trims trailing whitespace from each individual line.
    - Collapses 3 or more consecutive blank lines into a clean double newline (\n\n).
    - Trims overall leading and trailing whitespace.

    Args:
        text: Raw multi-line string.
        default: Fallback if text is empty or None.

    Returns:
        Normalized multi-line string or default.
    """
    if text is None:
        return default

    # Standardize line endings to \n
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Strip trailing whitespace per line
    lines = [line.rstrip() for line in text.split("\n")]
    text = "\n".join(lines)

    # Collapse 3+ newlines to double newline (paragraph boundary)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text if text else default


def strip_html_artifacts(text: Optional[str]) -> Optional[str]:
    """Unescape HTML entities and strip markup tags from job descriptions.

    - Decodes entities (e.g., &amp; -> &, &quot; -> ", &#39; -> ').
    - Replaces common block-level closing tags (<p>, <br>, <li>, </div>) with spaces/newlines.
    - Strips remaining HTML tags.
    - Normalizes whitespace.

    Args:
        text: Raw string possibly containing HTML markup.

    Returns:
        Plain-text string or None if empty.
    """
    if text is None:
        return None

    # Decode HTML entities
    decoded = html.unescape(text)

    # Convert line-break tags to spaces or newlines
    converted = re.sub(r"(?i)<br\s*/?>", "\n", decoded)
    converted = re.sub(r"(?i)</?(p|li|div|tr|h[1-6])\b[^>]*>", "\n", converted)

    # Strip remaining HTML tags
    stripped = re.sub(r"<[^>]+>", " ", converted)

    # Normalize multiline spacing
    return normalize_multiline_text(stripped)
