"""DemandLens - Silver Layer Skill Normalization Utilities.

Normalizes skill tokens, handles delimiters, eliminates duplicate skills within
individual postings, and preserves meaningful technology names.

Design constraint: Does NOT build an aggressive synthetic taxonomy or merge
similar skills without empirical justification.
"""

from __future__ import annotations

import re
from typing import List, Optional, Set, Union


# Punctuation to safely strip from the outer edges of a skill token.
# Retains internal punctuation such as in 'c++', 'c#', '.net', 'node.js', 'pl/sql'.
_OUTER_PUNCTUATION = " \t\r\n'\"`([{<>]})*•-—–,;:"


def normalize_skill_token(token: Optional[str]) -> Optional[str]:
    """Normalize an individual skill token.

    - Trims whitespace and outer punctuation noise.
    - Collapses internal whitespace.
    - Normalizes case to lowercase for consistent grouping and deduplication.
    - Preserves meaningful technical symbols ('c++', 'c#', '.net', 'node.js', 'pl/sql').
    - Returns None if the token is empty, purely numeric, or purely punctuation.

    Args:
        token: Raw skill string (e.g., "  Python 3.x  ", " C++ ").

    Returns:
        Cleaned lowercase skill token, or None if invalid.
    """
    if token is None:
        return None

    # Strip outer noise
    cleaned = token.strip(_OUTER_PUNCTUATION)

    # Collapse internal whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned:
        return None

    # If token consists entirely of punctuation or symbols (e.g. '...', '---', '***')
    if all(ch in ".-_/,\\|;: \t*•~`'\"" for ch in cleaned):
        return None

    if cleaned.isdigit() and len(cleaned) < 4:
        # Standalone small numbers like "1", "2" are noisy artifacts
        return None

    # Case normalization to lowercase
    return cleaned.lower()


def normalize_skills(
    skills_input: Union[str, List[str], None],
    delimiter: str = ",",
) -> List[str]:
    """Parse, clean, and deduplicate skills for a single job posting.

    - Splits string input by delimiter if a string is provided.
    - Normalizes each token via `normalize_skill_token`.
    - Eliminates duplicate skills within the posting while preserving original order.
    - Discards null and empty tokens.

    Args:
        skills_input: Comma-separated string or list of raw skill tokens.
        delimiter: Regex or literal string delimiter (default: comma ',').

    Returns:
        List of distinct normalized skill strings. Returns empty list [] if none found.
    """
    if skills_input is None:
        return []

    raw_tokens: List[str] = []
    if isinstance(skills_input, str):
        # Split on delimiter
        raw_tokens = re.split(re.escape(delimiter), skills_input)
    elif isinstance(skills_input, (list, tuple, set)):
        raw_tokens = [str(item) for item in skills_input if item is not None]
    else:
        return []

    normalized_list: List[str] = []
    seen: Set[str] = set()

    for token in raw_tokens:
        clean_tok = normalize_skill_token(token)
        if clean_tok and clean_tok not in seen:
            seen.add(clean_tok)
            normalized_list.append(clean_tok)

    return normalized_list


def skills_to_string(skills: Optional[List[str]], delimiter: str = ", ") -> Optional[str]:
    """Convert a list of normalized skills into a canonical delimited string.

    Args:
        skills: List of normalized skill strings.
        delimiter: Joining delimiter.

    Returns:
        Joined string or None if list is empty.
    """
    if not skills:
        return None
    valid = [s for s in skills if s]
    return delimiter.join(valid) if valid else None
