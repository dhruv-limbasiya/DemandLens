"""DemandLens - Silver Layer Foundation & Cleaning Rules Package.

Provides modular, reusable, deterministic data cleaning and normalization
utilities for the Silver Medallion pipeline.
"""

from src.demandlens.silver.text_cleaning import (
    clean_text,
    normalize_multiline_text,
    strip_html_artifacts,
)
from src.demandlens.silver.skill_normalization import (
    normalize_skill_token,
    normalize_skills,
    skills_to_string,
)
from src.demandlens.silver.location_cleaning import (
    clean_location_string,
    detect_remote,
    extract_location_parts,
)
from src.demandlens.silver.experience_parser import (
    parse_experience_range,
    validate_experience_bounds,
    classify_experience_tier,
)
from src.demandlens.silver.salary_parser import (
    parse_numeric_salary,
    validate_salary_range,
    detect_salary_periodicity,
    parse_salary_record,
)
from src.demandlens.silver.freshness_classifier import (
    classify_freshness,
    parse_relative_days,
)

__all__ = [
    "clean_text",
    "normalize_multiline_text",
    "strip_html_artifacts",
    "normalize_skill_token",
    "normalize_skills",
    "skills_to_string",
    "clean_location_string",
    "detect_remote",
    "extract_location_parts",
    "parse_experience_range",
    "validate_experience_bounds",
    "classify_experience_tier",
    "parse_numeric_salary",
    "validate_salary_range",
    "detect_salary_periodicity",
    "parse_salary_record",
    "classify_freshness",
    "parse_relative_days",
]
