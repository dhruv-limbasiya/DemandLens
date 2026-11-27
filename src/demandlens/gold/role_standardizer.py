"""DemandLens — Gold Layer: Job Role Standardization Engine (Phase 7B).

Provides deterministic, specificity-first classification of cleaned job titles
into standardized role families for the DemandLens Gold analytics layer.

Implements:
1. Pure Python function `standardize_job_title(title)` for unit testing and ad-hoc single-string parsing.
2. Spark-native Catalyst transformation `add_standardized_role_columns(df, title_col)`
   evaluating regular expressions directly inside the JVM without Python UDF serialization overhead.

Follows the approved Phase 7A Gold Architecture:
  docs/architecture/gold_layer_design.md
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Standard Taxonomy Definitions
# ---------------------------------------------------------------------------

TAXONOMY_ROLES: List[str] = [
    # 1. Management & Leadership (Highest Precedence)
    "ENGINEERING_MANAGER",
    "PRODUCT_MANAGER",
    "PROJECT_MANAGER",
    # 2. Specialized Architects
    "DATA_ARCHITECT",
    "CLOUD_ARCHITECT",
    # 3. Specialized Data & AI
    "MACHINE_LEARNING_ENGINEER",
    "AI_ENGINEER",
    "DATA_ENGINEER",
    "DATA_SCIENTIST",
    "DATABASE_ADMINISTRATOR",
    "CYBERSECURITY_ANALYST",
    "BUSINESS_ANALYST",
    "DATA_ANALYST",
    # 4. Specialized Infrastructure & Cloud
    "SITE_RELIABILITY_ENGINEER",
    "CLOUD_DEVOPS_ENGINEER",
    "DEVOPS_ENGINEER",
    "CLOUD_ENGINEER",
    "SECURITY_ENGINEER",
    "QA_ENGINEER",
    # 5. Software Engineering Tiers
    "FULL_STACK_DEVELOPER",
    "FRONTEND_DEVELOPER",
    "BACKEND_DEVELOPER",
    "SOFTWARE_ENGINEER",
    # 6. Safe Fallbacks
    "OTHER_SPECIALIZED",
    "UNCLASSIFIED",
]


# ---------------------------------------------------------------------------
# Declarative Rules in Specificity-First Order
# Each rule is a tuple: (Role, Rule_ID, Regex Pattern)
# ---------------------------------------------------------------------------

ROLE_RULES: List[Tuple[str, str, str]] = [
    # --- Priority 1: Engineering & Product Management ---
    (
        "ENGINEERING_MANAGER",
        "RULE_ENG_MGR",
        r"\b(engineering manager|software engineering manager|technical manager|manager of engineering|director of engineering|vp of engineering|head of engineering|data engineering manager|(?:manager|senior manager|director|head|vp)\s*,?\s+(?:of\s+)?data\s+engineering)\b",
    ),
    (
        "PRODUCT_MANAGER",
        "RULE_PROD_MGR",
        r"\b(product manager|group product manager|technical product manager|principal product manager|director of product|vp of product|head of product|associate product manager)\b",
    ),
    (
        "PROJECT_MANAGER",
        "RULE_PROJ_MGR",
        r"\b(project manager|technical project manager|scrum master|agile coach|program manager|technical program manager|pmo manager|delivery manager)\b",
    ),

    # --- Priority 2: System Architects ---
    (
        "DATA_ARCHITECT",
        "RULE_DATA_ARCH",
        r"\b(data architect|big data architect|enterprise data architect|bi architect|data solutions architect|data engineering architect|architect\s*[-–—]\s*data\s+engineering)\b",
    ),
    (
        "CLOUD_ARCHITECT",
        "RULE_CLOUD_ARCH",
        r"\b(cloud architect|aws architect|azure architect|gcp architect|cloud solutions architect|cloud infrastructure architect|(?:aws|azure|gcp|cloud)\s+solution[s]?\s+architect)\b",
    ),

    # --- Priority 3: Data & Artificial Intelligence ---
    (
        "MACHINE_LEARNING_ENGINEER",
        "RULE_ML_ENG",
        r"\b(machine learning engineer|ml engineer|mlops engineer|machine learning operations|deep learning engineer|machine learning scientist|nlp engineer|computer vision engineer)\b",
    ),
    (
        "AI_ENGINEER",
        "RULE_AI_ENG",
        r"\b(ai engineer|artificial intelligence engineer|genai engineer|generative ai engineer|ai prompt engineer|ai developer)\b",
    ),
    (
        "DATA_ENGINEER",
        "RULE_DATA_ENG",
        r"\b(data engineer|big data engineer|data platform eng(?:ineer(?:ing)?)?|etl developer|data warehouse engineer|analytics engineer|data pipeline engineer|etl engineer|data integration engineer|hadoop dev(?:eloper)?|(?:spark|pyspark)\s+(?:dev(?:eloper)?|eng(?:ineer)?)|data\s+engineering\s+(?:consultant|specialist|professional|lead|advisor)|engineer(?:\s+[i|v|x]+)?\s*[-–—]\s*data\s+engineering)\b",
    ),
    (
        "DATA_SCIENTIST",
        "RULE_DATA_SCI",
        r"\b(data scientist|decision scientist|applied scientist|quantitative analyst|quant analyst|data science specialist)\b",
    ),
    (
        "DATABASE_ADMINISTRATOR",
        "RULE_DBA",
        r"\b(database administrator|dba|database engineer|database specialist|database dev(?:eloper)?|sql dba|oracle dba|postgres dba|mysql dba|(?:oracle\s+)?pl/?sql\s+dev(?:eloper)?|t-?sql\s+dev(?:eloper)?)\b",
    ),
    (
        "CYBERSECURITY_ANALYST",
        "RULE_CYBER_ANALYST",
        r"\b(cybersecurity analyst|cyber security analyst|information security analyst|infosec analyst|soc analyst|security operations analyst|threat intelligence analyst|vulnerability analyst)\b",
    ),
    (
        "BUSINESS_ANALYST",
        "RULE_BIZ_ANALYST",
        r"\b(business analyst|business systems analyst|requirements analyst|business operations analyst|functional analyst)\b",
    ),
    (
        "DATA_ANALYST",
        "RULE_DATA_ANALYST",
        r"\b(data analyst|bi analyst|business intelligence analyst|bi developer|power bi developer|tableau developer|reporting analyst|data reporting specialist|insights analyst|(?:business\s+intelligence|bi|power\s*bi)\s+eng(?:ineer)?)\b",
    ),

    # --- Priority 4: Cloud, DevOps & Reliability ---
    (
        "SITE_RELIABILITY_ENGINEER",
        "RULE_SRE",
        r"\b(site reliability engineer|sre|site reliability|reliability engineer)\b",
    ),
    (
        "CLOUD_DEVOPS_ENGINEER",
        "RULE_CLOUD_DEVOPS",
        r"\b(cloud devops|devops cloud|aws devops|azure devops)\b",
    ),
    (
        "DEVOPS_ENGINEER",
        "RULE_DEVOPS",
        r"\b(devops engineer|devops specialist|devsecops|devops lead|devops consultant|devops)\b",
    ),
    (
        "CLOUD_ENGINEER",
        "RULE_CLOUD_ENG",
        r"\b(cloud engineer|cloud infrastructure engineer|aws engineer|azure engineer|gcp engineer|cloud specialist|cloud operations engineer|cloud administrator)\b",
    ),
    (
        "SECURITY_ENGINEER",
        "RULE_SEC_ENG",
        r"\b(security engineer|information security engineer|cyber security engineer|cybersecurity engineer|application security engineer|appsec engineer|cloud security engineer|network security engineer|(?:cyber\s*|information\s+|network\s+|cloud\s+|enterprise\s+)?security\s+architect)\b",
    ),
    (
        "QA_ENGINEER",
        "RULE_QA_ENG",
        r"\b(qa engineer|quality assurance engineer|software test engineer|automation test engineer|sdet|test automation engineer|qa automation|software development engineer in test|qa analyst|quality analyst|test lead)\b",
    ),

    # --- Priority 5: Specialized Software Development Tiers ---
    (
        "FULL_STACK_DEVELOPER",
        "RULE_FULL_STACK",
        r"\b(full stack|fullstack|full-stack)\b",
    ),
    (
        "FRONTEND_DEVELOPER",
        "RULE_FRONTEND",
        r"\b(frontend|front end|front-end|ui developer|web developer|react developer|angular developer|vue developer|ui/ux developer|ui engineer)\b",
    ),
    (
        "BACKEND_DEVELOPER",
        "RULE_BACKEND",
        r"\b(backend|back end|back-end|node developer|django developer|spring boot developer|java developer|python developer|golang developer|go developer|ruby on rails developer|\.net developer|dot net developer|c# developer|c\+\+ developer|api developer)\b",
    ),
    (
        "SOFTWARE_ENGINEER",
        "RULE_SWE",
        r"\b(software engineer|software developer|software development engineer|software development lead|sde|application developer|application lead|systems software engineer|embedded software engineer|programmer|software specialist|technical lead|tech lead|developer\s*[-–—]\s*l[1-5])\b",
    ),

    # --- Priority 6: Recognized Non-Tech / Industry Specialized Roles ---
    (
        "OTHER_SPECIALIZED",
        "RULE_OTHER_SPECIALIZED",
        r"\b(nurse|nursing|therapist|physician|dental|medical|clinical|healthcare|pharmacy|pharmacist|caregiver|sales|sales associate|account executive|business development|telecaller|cashier|retail|store manager|shift supervisor|shift manager|assistant manager|clerk|teller|driver|delivery|warehouse|cook|chef|server|bartender|barista|customer service|customer support|call center|receptionist|administrative assistant|office assistant|accountant|accounting|tax professional|auditor|financial analyst|recruiter|human resources|hr generalist|teacher|instructor|tutor|legal|attorney|paralegal|graphic designer|designer|copywriter)\b",
    ),
]


# ---------------------------------------------------------------------------
# Precompiled Regexes for Fast Pure-Python Execution
# ---------------------------------------------------------------------------

_COMPILED_RULES = [(role, rule_id, re.compile(pat, re.IGNORECASE)) for role, rule_id, pat in ROLE_RULES]


def standardize_job_title(title: Optional[str]) -> Tuple[str, str]:
    """Standardize a single cleaned job title into a canonical role and rule identifier.

    Args:
        title: Cleaned job title string or None.

    Returns:
        Tuple of (standardized_role, classification_method).
    """
    if not title or not isinstance(title, str):
        return ("UNCLASSIFIED", "FALLBACK_NULL_OR_EMPTY")

    cleaned = title.strip()
    if not cleaned:
        return ("UNCLASSIFIED", "FALLBACK_NULL_OR_EMPTY")

    for role, rule_id, regex in _COMPILED_RULES:
        if regex.search(cleaned):
            return (role, rule_id)

    return ("UNCLASSIFIED", "FALLBACK_UNCLASSIFIED")


def add_standardized_role_columns(
    df: DataFrame,
    title_col: str = "job_title_clean",
) -> DataFrame:
    """Spark-native transformation adding standardized role columns without UDF serialization.

    Builds Catalyst expressions evaluated in optimized native bytecode.
    Adds:
      - `standardized_role` (StringType)
      - `role_classification_method` (StringType)

    Preserves 100% of input rows and existing columns.

    Args:
        df: Input Spark DataFrame.
        title_col: Name of the cleaned title column to evaluate.

    Returns:
        DataFrame with standardized role columns added.
    """
    # Start building Catalyst chained when/otherwise expression
    norm_title = F.lower(F.trim(F.coalesce(F.col(title_col), F.lit(""))))

    role_expr = None
    method_expr = None

    for role, rule_id, pattern in ROLE_RULES:
        condition = norm_title.rlike(pattern)
        if role_expr is None:
            role_expr = F.when(condition, F.lit(role))
            method_expr = F.when(condition, F.lit(rule_id))
        else:
            role_expr = role_expr.when(condition, F.lit(role))
            method_expr = method_expr.when(condition, F.lit(rule_id))

    # Fallbacks for empty/null and unmatched
    empty_cond = norm_title == ""
    final_role_expr = (
        F.when(empty_cond, F.lit("UNCLASSIFIED"))
        .otherwise(role_expr.otherwise(F.lit("UNCLASSIFIED")))
    )
    final_method_expr = (
        F.when(empty_cond, F.lit("FALLBACK_NULL_OR_EMPTY"))
        .otherwise(method_expr.otherwise(F.lit("FALLBACK_UNCLASSIFIED")))
    )

    return df.withColumn("standardized_role", final_role_expr).withColumn(
        "role_classification_method", final_method_expr
    )
