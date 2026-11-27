"""Single source of truth for human-readable labels, casing, formatting, and finding sentences."""

from __future__ import annotations

import re
from typing import Dict

SOURCE_DISPLAY_NAMES: Dict[str, str] = {
    "linkedin_2024": "LinkedIn 2024",
    "indian_job_market_2025_2026": "Indian Job Market 2025–26",
}

COUNTRY_DISPLAY_NAMES: Dict[str, str] = {
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "AU": "Australia",
    "IN": "India",
}

SENIORITY_DISPLAY_NAMES: Dict[str, str] = {
    "ENTRY": "Entry level",
    "ASSOCIATE": "Associate",
    "MID": "Mid level",
    "SENIOR": "Senior",
    "LEAD_EXECUTIVE": "Lead / Executive",
    "UNSPECIFIED": "Unspecified",
}

ROLE_OVERRIDES: Dict[str, str] = {
    "SOFTWARE_ENGINEER": "Software engineer",
    "ENGINEERING_MANAGER": "Engineering manager",
    "PRODUCT_MANAGER": "Product manager",
    "PROJECT_MANAGER": "Project manager",
    "DATA_ARCHITECT": "Data architect",
    "CLOUD_ARCHITECT": "Cloud architect",
    "MACHINE_LEARNING_ENGINEER": "ML engineer",
    "AI_ENGINEER": "AI engineer",
    "DATA_ENGINEER": "Data engineer",
    "DATA_SCIENTIST": "Data scientist",
    "DATABASE_ADMINISTRATOR": "Database administrator",
    "CYBERSECURITY_ANALYST": "Cybersecurity analyst",
    "BUSINESS_ANALYST": "Business analyst",
    "DATA_ANALYST": "Data analyst",
    "SITE_RELIABILITY_ENGINEER": "Site reliability engineer",
    "CLOUD_DEVOPS_ENGINEER": "Cloud DevOps engineer",
    "DEVOPS_ENGINEER": "DevOps engineer",
    "CLOUD_ENGINEER": "Cloud engineer",
    "SECURITY_ENGINEER": "Security engineer",
    "QA_ENGINEER": "QA engineer",
    "FULL_STACK_DEVELOPER": "Full-stack developer",
    "FRONTEND_DEVELOPER": "Frontend developer",
    "BACKEND_DEVELOPER": "Backend developer",
    "OTHER_SPECIALIZED": "Other specialized",
    "UNCLASSIFIED": "Unclassified",
}

SKILL_OVERRIDES: Dict[str, str] = {
    "sql": "SQL",
    "aws": "AWS",
    "sap": "SAP",
    "css": "CSS",
    "html": "HTML",
    "gcp": "GCP",
    "etl": "ETL",
    "c#": "C#",
    "c++": "C++",
    "pyspark": "PySpark",
    "api": "API",
    "rest": "REST",
    "rest api": "REST API",
    "ci/cd": "CI/CD",
    "nlp": "NLP",
    "llm": "LLM",
    "bi": "BI",
    "power bi": "Power BI",
    "ui/ux": "UI/UX",
    "ml": "ML",
    "ai": "AI",
    "nosql": "NoSQL",
    "postgresql": "PostgreSQL",
    "mysql": "MySQL",
    "javascript": "JavaScript",
    "typescript": "TypeScript",
    "node.js": "Node.js",
    "nodejs": "Node.js",
    "devops": "DevOps",
    "mlops": "MLOps",
    "git": "Git",
    "ios": "iOS",
    ".net": ".NET",
    "asp.net": "ASP.NET",
    "excel": "Excel",
    "ms excel": "MS Excel",
    "microsoft excel": "Microsoft Excel",
    "microsoft office": "Microsoft Office",
}

COLUMN_DISPLAY_NAMES: Dict[str, str] = {
    "source_id": "Snapshot source",
    "country_code": "Country",
    "location_clean": "Location",
    "company_name_clean": "Company",
    "standardized_role": "Standardized role",
    "seniority_tier": "Seniority tier",
    "skill": "Skill",
    "partner_skill": "Companion skill",
    "posting_count": "Postings",
    "total_postings": "Total postings",
    "total_market_postings": "Market postings",
    "total_role_postings": "Role postings",
    "remote_postings": "Remote postings",
    "hybrid_postings": "Hybrid postings",
    "onsite_postings": "Onsite postings",
    "remote_posting_count": "Remote postings",
    "hybrid_posting_count": "Hybrid postings",
    "onsite_posting_count": "Onsite postings",
    "pct_of_country_postings": "Country share (%)",
    "remote_penetration_pct": "Remote share (%)",
    "hybrid_penetration_pct": "Hybrid share (%)",
    "prevalence_pct": "Prevalence (%)",
    "requirement_frequency_pct": "Requirement freq. (%)",
    "skill_entry_prevalence_pct": "Entry prevalence (%)",
    "remote_entry_pct": "Remote entry (%)",
    "market_skill_rank": "Rank",
    "role_skill_rank": "Rank",
    "entry_skill_rank": "Rank",
    "skill_occurrences": "Postings with skill",
    "entry_posting_count": "Entry postings",
    "role_entry_total": "Role entry postings",
    "distinct_roles": "Distinct roles",
    "distinct_locations": "Distinct locations",
    "cooccurrence_count": "Co-occurrences",
    "jaccard_similarity": "Jaccard similarity",
    "sample_size": "Sample size (n)",
    "avg_experience_min_years": "Min exp. (yrs)",
    "avg_experience_max_years": "Max exp. (yrs)",
    "salary_mid_median_inr": "Median midpoint (INR)",
    "salary_min_p25_inr": "Lower quartile P25 (INR)",
    "salary_max_p75_inr": "Upper quartile P75 (INR)",
}


def format_source_name(source_id: str) -> str:
    return SOURCE_DISPLAY_NAMES.get(source_id, source_id.replace("_", " ").title())


def format_country_name(country_code: str) -> str:
    return COUNTRY_DISPLAY_NAMES.get(country_code, country_code)


def format_seniority_name(tier: str) -> str:
    return SENIORITY_DISPLAY_NAMES.get(tier, tier.replace("_", " ").capitalize())


def format_role_name(role: str) -> str:
    if not role:
        return ""
    if role in ROLE_OVERRIDES:
        return ROLE_OVERRIDES[role]
    words = role.replace("_", " ").strip().lower()
    return words.capitalize()


def format_skill_name(skill: str) -> str:
    if not skill:
        return ""
    cleaned = re.sub(r"\s+", " ", skill.strip())
    lower = cleaned.lower()
    if lower in SKILL_OVERRIDES:
        return SKILL_OVERRIDES[lower]
    parts = cleaned.split(" ")
    formatted = []
    for part in parts:
        pl = part.lower()
        if pl in SKILL_OVERRIDES:
            formatted.append(SKILL_OVERRIDES[pl])
        elif len(part) <= 2 and part.isalpha():
            formatted.append(part.upper())
        else:
            formatted.append(part.capitalize())
    return " ".join(formatted)


def format_column_name(col: str) -> str:
    return COLUMN_DISPLAY_NAMES.get(col, col.replace("_", " ").capitalize())


def format_count(value: float | int) -> str:
    return f"{int(round(value)):,}"


def format_pct(value: float, decimals: int = 1) -> str:
    return f"{value:.{decimals}f}%"


def format_inr_lpa(inr_value: float, decimals: int = 1) -> str:
    lpa = inr_value / 100_000.0
    return f"₹{lpa:.{decimals}f} LPA"


def format_jaccard(value: float) -> str:
    return f"{value:.3f}"


def build_leader_finding(
    top_label: str,
    top_val: float | int,
    second_label: str | None = None,
    second_val: float | int | None = None,
    unit: str = "postings",
    is_pct: bool = False,
) -> str:
    """Build a plain, evidence-first finding sentence comparing #1 to #2."""
    top_fmt = format_pct(float(top_val)) if is_pct else f"{format_count(top_val)} {unit}"
    if second_label is None or second_val is None or second_val == 0:
        return f"{top_label} leads at {top_fmt}."
    sec_fmt = format_pct(float(second_val)) if is_pct else format_count(second_val)
    if is_pct:
        diff = float(top_val) - float(second_val)
        return f"{top_label} leads at {top_fmt}, {diff:.1f} percentage points ahead of {second_label} ({sec_fmt})."
    ratio = float(top_val) / float(second_val)
    if ratio >= 1.2:
        return f"{top_label} has {top_fmt}, {ratio:.1f}x {second_label} ({sec_fmt})."
    return f"{top_label} leads with {top_fmt}, followed by {second_label} ({sec_fmt})."
