"""DemandLens - Unit Tests for Silver Foundation & Cleaning Rules.

Tests cover:
  - Text cleaning (normal, null, empty, multiline, HTML tags/entities)
  - Skill normalization (token cleanup, lowercasing, duplicates, punctuation preservation)
  - Location cleaning (whitespace, missing, remote detection, compound locations)
  - Experience parsing (ranges, plus notation, freshers, validation, tiers)
  - Salary parsing (numeric conversion, missing/undisclosed, ranges, periodicity, currency)
  - Freshness classification (relative date strings, day extraction, unknown formats)
  - Robustness against unexpected/malformed inputs
"""

from __future__ import annotations

import pytest

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
    FRESH_24H,
    RECENT_7D,
    MODERATE_30D,
    STALE_30D_PLUS,
    UNKNOWN_FRESHNESS,
)


# ==============================================================================
# 1. Text Cleaning Tests
# ==============================================================================

class TestTextCleaning:
    def test_clean_text_normal(self):
        assert clean_text("Senior Data Engineer") == "Senior Data Engineer"

    def test_clean_text_whitespace(self):
        assert clean_text("   Senior    Data   Engineer\t\n  ") == "Senior Data Engineer"

    def test_clean_text_null_and_empty(self):
        assert clean_text(None) is None
        assert clean_text("") is None
        assert clean_text("   ") is None
        assert clean_text(None, default="Unknown") == "Unknown"
        assert clean_text("   ", default="Unknown") == "Unknown"

    def test_normalize_multiline_text(self):
        raw = "Line 1\r\nLine 2   \r\n\r\n\r\n\r\nLine 3\n"
        expected = "Line 1\nLine 2\n\nLine 3"
        assert normalize_multiline_text(raw) == expected

    def test_normalize_multiline_null_empty(self):
        assert normalize_multiline_text(None) is None
        assert normalize_multiline_text("   \n\n  ") is None
        assert normalize_multiline_text("", default="None") == "None"

    def test_strip_html_artifacts(self):
        html_input = "<p>Join our <strong>innovative</strong> team &amp; grow!</p><br/><ul><li>Skill 1</li><li>Skill 2</li></ul>"
        cleaned = strip_html_artifacts(html_input)
        assert "&amp;" not in cleaned
        assert "&" in cleaned
        assert "<p>" not in cleaned
        assert "<strong>" not in cleaned
        assert "innovative" in cleaned
        assert "Skill 1" in cleaned

    def test_strip_html_null_empty(self):
        assert strip_html_artifacts(None) is None
        assert strip_html_artifacts("   ") is None


# ==============================================================================
# 2. Skill Normalization Tests
# ==============================================================================

class TestSkillNormalization:
    def test_normalize_skill_token_basic(self):
        assert normalize_skill_token("Python") == "python"
        assert normalize_skill_token("   Apache Spark   ") == "apache spark"

    def test_normalize_skill_token_preserves_special_symbols(self):
        # Technical symbols must be preserved
        assert normalize_skill_token("C++") == "c++"
        assert normalize_skill_token("C#") == "c#"
        assert normalize_skill_token(".NET") == ".net"
        assert normalize_skill_token("Node.js") == "node.js"
        assert normalize_skill_token("PL/SQL") == "pl/sql"

    def test_normalize_skill_token_strips_punctuation_noise(self):
        assert normalize_skill_token("'Python'") == "python"
        assert normalize_skill_token('"Docker"') == "docker"
        assert normalize_skill_token("• Kubernetes") == "kubernetes"
        assert normalize_skill_token("- AWS -") == "aws"

    def test_normalize_skill_token_invalid(self):
        assert normalize_skill_token(None) is None
        assert normalize_skill_token("") is None
        assert normalize_skill_token("   ") is None
        assert normalize_skill_token("...") is None
        assert normalize_skill_token("1") is None

    def test_normalize_skills_string(self):
        raw = "Python, SQL, python, , Apache Spark, C++, Python, Docker"
        expected = ["python", "sql", "apache spark", "c++", "docker"]
        result = normalize_skills(raw)
        assert result == expected

    def test_normalize_skills_list_input(self):
        raw_list = ["Python", "SQL", "python", "AWS", None, ""]
        expected = ["python", "sql", "aws"]
        assert normalize_skills(raw_list) == expected

    def test_normalize_skills_null_and_empty(self):
        assert normalize_skills(None) == []
        assert normalize_skills("") == []
        assert normalize_skills("   , ,   ") == []

    def test_skills_to_string(self):
        assert skills_to_string(["python", "sql", "aws"]) == "python, sql, aws"
        assert skills_to_string([]) is None
        assert skills_to_string(None) is None


# ==============================================================================
# 3. Location Cleaning Tests
# ==============================================================================

class TestLocationCleaning:
    def test_clean_location_string(self):
        assert clean_location_string("  Bengaluru, Karnataka  ") == "Bengaluru, Karnataka"
        assert clean_location_string("Austin, TX, ") == "Austin, TX"
        assert clean_location_string(None) is None
        assert clean_location_string("   ") is None
        assert clean_location_string(None, default="Unknown") == "Unknown"

    def test_detect_remote(self):
        assert detect_remote("Remote - US") is True
        assert detect_remote("Bengaluru (Hybrid)") is True
        assert detect_remote("Work from Home, India") is True
        assert detect_remote("New York, NY") is False
        assert detect_remote("Mumbai, Maharashtra") is False
        assert detect_remote(None) is None
        assert detect_remote("   ") is None

    def test_extract_location_parts(self):
        parts = extract_location_parts("Bengaluru, Karnataka, India")
        assert parts["raw_location"] == "Bengaluru, Karnataka, India"
        assert parts["cleaned_location"] == "Bengaluru, Karnataka, India"
        assert parts["location_tokens"] == ["Bengaluru", "Karnataka", "India"]
        assert parts["is_remote"] is False

    def test_extract_location_parts_compound_remote(self):
        parts = extract_location_parts("Noida / Greater Noida (Remote)")
        assert parts["is_remote"] is True
        assert "Noida" in parts["location_tokens"]

    def test_extract_location_parts_null(self):
        parts = extract_location_parts(None)
        assert parts["raw_location"] is None
        assert parts["location_tokens"] == []
        assert parts["is_remote"] is None


# ==============================================================================
# 4. Experience Parsing Tests
# ==============================================================================

class TestExperienceParser:
    def test_parse_experience_range_standard(self):
        assert parse_experience_range("2-5 Yrs") == (2.0, 5.0)
        assert parse_experience_range("0-1 Yrs") == (0.0, 1.0)
        assert parse_experience_range("3 to 6 years") == (3.0, 6.0)
        assert parse_experience_range("5 - 8 Years") == (5.0, 8.0)

    def test_parse_experience_range_plus_and_single(self):
        assert parse_experience_range("5+ Yrs") == (5.0, None)
        assert parse_experience_range("10+ years") == (10.0, None)
        assert parse_experience_range("3 Yrs") == (3.0, 3.0)

    def test_parse_experience_range_fresher(self):
        assert parse_experience_range("Fresher") == (0.0, 0.0)
        assert parse_experience_range("Internship") == (0.0, 0.0)

    def test_parse_experience_range_null_and_malformed(self):
        assert parse_experience_range(None) == (None, None)
        assert parse_experience_range("") == (None, None)
        assert parse_experience_range("Not Specified") == (None, None)
        assert parse_experience_range("Competitive") == (None, None)

    def test_validate_experience_bounds(self):
        assert validate_experience_bounds(2.0, 5.0) == (2.0, 5.0, True)
        assert validate_experience_bounds(0.0, 0.0) == (0.0, 0.0, True)
        assert validate_experience_bounds(None, None) == (None, None, True)
        assert validate_experience_bounds(5.0, None) == (5.0, None, True)

        # Invalid bounds
        assert validate_experience_bounds(-1.0, 5.0)[2] is False
        assert validate_experience_bounds(6.0, 3.0)[2] is False
        assert validate_experience_bounds(60.0, 70.0)[2] is False

    def test_classify_experience_tier_numeric(self):
        assert classify_experience_tier(min_exp=0.0) == "ENTRY"
        assert classify_experience_tier(min_exp=1.0) == "ENTRY"
        assert classify_experience_tier(min_exp=3.0) == "MID"
        assert classify_experience_tier(min_exp=5.0) == "MID"
        assert classify_experience_tier(min_exp=7.0) == "SENIOR"
        assert classify_experience_tier(min_exp=10.0) == "LEAD_EXECUTIVE"
        assert classify_experience_tier(min_exp=15.0) == "LEAD_EXECUTIVE"

    def test_classify_experience_tier_categorical(self):
        assert classify_experience_tier(raw_level="Entry level") == "ENTRY"
        assert classify_experience_tier(raw_level="Internship") == "ENTRY"
        assert classify_experience_tier(raw_level="Associate") == "MID"
        assert classify_experience_tier(raw_level="Mid-Senior level") == "MID"
        assert classify_experience_tier(raw_level="Director") == "LEAD_EXECUTIVE"
        assert classify_experience_tier(raw_level="Executive") == "LEAD_EXECUTIVE"
        assert classify_experience_tier(raw_level=None) is None


# ==============================================================================
# 5. Salary Parsing Tests
# ==============================================================================

class TestSalaryParser:
    def test_parse_numeric_salary_valid(self):
        assert parse_numeric_salary(300000) == 300000.0
        assert parse_numeric_salary("500000") == 500000.0
        assert parse_numeric_salary("₹ 6,00,000") == 600000.0
        assert parse_numeric_salary("$ 120,000") == 120000.0

    def test_parse_numeric_salary_missing_or_zero(self):
        # Crucial requirement: Missing and zero salaries must NOT be treated as valid 0
        assert parse_numeric_salary(0) is None
        assert parse_numeric_salary("0") is None
        assert parse_numeric_salary(None) is None
        assert parse_numeric_salary("") is None
        assert parse_numeric_salary("Not disclosed") is None
        assert parse_numeric_salary("Undisclosed") is None
        assert parse_numeric_salary(-5000) is None

    def test_detect_salary_periodicity(self):
        assert detect_salary_periodicity("3-6 Lacs PA") == "ANNUAL"
        assert detect_salary_periodicity("12 - 24 LPA") == "ANNUAL"
        assert detect_salary_periodicity("25,000 PM") == "MONTHLY"
        assert detect_salary_periodicity("$50 / Hour") == "HOURLY"
        assert detect_salary_periodicity("Not disclosed") is None
        assert detect_salary_periodicity(None) is None

    def test_validate_salary_range(self):
        assert validate_salary_range(300000.0, 600000.0) == (300000.0, 600000.0, True)
        assert validate_salary_range(None, None) == (None, None, True)
        assert validate_salary_range(500000.0, None) == (500000.0, None, True)

        # Invalid: min > max
        assert validate_salary_range(800000.0, 400000.0)[2] is False
        # Invalid: negative
        assert validate_salary_range(-1000.0, 5000.0)[2] is False
        # Invalid: exceeding cap
        assert validate_salary_range(1_000_000_000.0, None)[2] is False

    def test_parse_salary_record_disclosed(self):
        rec = parse_salary_record(
            raw_salary="3-6 Lacs PA",
            raw_min_salary="300000",
            raw_max_salary="600000",
            raw_currency="INR",
        )
        assert rec["is_salary_disclosed"] is True
        assert rec["salary_currency"] == "INR"
        assert rec["salary_min"] == 300000.0
        assert rec["salary_max"] == 600000.0
        assert rec["salary_midpoint"] == 450000.0
        assert rec["salary_periodicity"] == "ANNUAL"
        assert rec["is_valid"] is True

    def test_parse_salary_record_undisclosed(self):
        rec = parse_salary_record(
            raw_salary="Not disclosed",
            raw_min_salary=None,
            raw_max_salary=None,
            raw_currency=None,
        )
        assert rec["is_salary_disclosed"] is False
        assert rec["salary_min"] is None
        assert rec["salary_max"] is None
        assert rec["salary_midpoint"] is None
        assert rec["salary_currency"] is None
        assert rec["is_valid"] is True


# ==============================================================================
# 6. Freshness Classification Tests
# ==============================================================================

class TestFreshnessClassification:
    def test_classify_freshness_fresh_24h(self):
        assert classify_freshness("Just Now") == FRESH_24H
        assert classify_freshness("Few Hours Ago") == FRESH_24H
        assert classify_freshness("Today") == FRESH_24H
        assert classify_freshness("1 Day Ago") == FRESH_24H
        assert classify_freshness("Yesterday") == FRESH_24H

    def test_classify_freshness_recent_7d(self):
        assert classify_freshness("2 Days Ago") == RECENT_7D
        assert classify_freshness("4 Days Ago") == RECENT_7D
        assert classify_freshness("6 Days Ago") == RECENT_7D
        assert classify_freshness("1 Week Ago") == RECENT_7D

    def test_classify_freshness_moderate_30d(self):
        assert classify_freshness("2 Weeks Ago") == MODERATE_30D
        assert classify_freshness("3 Weeks Ago") == MODERATE_30D
        assert classify_freshness("15 Days Ago") == MODERATE_30D
        assert classify_freshness("1 Month Ago") == MODERATE_30D

    def test_classify_freshness_stale_30d_plus(self):
        assert classify_freshness("30+ Days Ago") == STALE_30D_PLUS
        assert classify_freshness("30+ Days") == STALE_30D_PLUS
        assert classify_freshness("2 Months Ago") == STALE_30D_PLUS

    def test_classify_freshness_unknown_and_null(self):
        assert classify_freshness(None) == UNKNOWN_FRESHNESS
        assert classify_freshness("") == UNKNOWN_FRESHNESS
        assert classify_freshness("Unknown Date Format") == UNKNOWN_FRESHNESS

    def test_parse_relative_days(self):
        assert parse_relative_days("Few Hours Ago") == 0
        assert parse_relative_days("1 Day Ago") == 1
        assert parse_relative_days("5 Days Ago") == 5
        assert parse_relative_days("2 Weeks Ago") == 14
        assert parse_relative_days("30+ Days Ago") == 30
        assert parse_relative_days(None) is None
        assert parse_relative_days("Invalid") is None
