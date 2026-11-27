"""Tests for DemandLens Gold Layer Role Standardization Engine (Phase 7B).

Verifies:
- Exact title matches across all 23 core taxonomy roles
- Case-insensitivity (lowercase, uppercase, mixed)
- Outer & internal whitespace normalization
- Punctuation handling (slashes, hyphens, periods)
- Specificity-first precedence:
    - Data Engineer vs Data Analyst
    - Machine Learning Engineer vs Data Scientist vs Software Engineer
    - Engineering Manager vs Software Engineer
    - Product Manager vs Project Manager
    - Cloud Architect vs Cloud Engineer
    - Site Reliability Engineer vs DevOps vs Cloud DevOps
    - Full Stack vs Frontend vs Backend Developer
- Prefixes/Suffixes: Intern, Junior, Senior, Staff, Principal, Lead
- Fallbacks: OTHER_SPECIALIZED and UNCLASSIFIED
- Empty, None, and whitespace-only handling
- Equivalence between pure Python standardize_job_title and Spark-native add_standardized_role_columns
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
from src.demandlens.gold.role_standardizer import (
    TAXONOMY_ROLES,
    add_standardized_role_columns,
    standardize_job_title,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped lightweight local SparkSession for testing Spark-native transformations."""
    configure_windows_environment()
    session = (
        SparkSession.builder.appName("TestGoldRoleStandardization")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class TestExactAndPrecedenceMatching:
    """Verifies exact matches and critical specificity-first precedence rules."""

    def test_management_precedence_over_engineering(self):
        """Verify Engineering Manager is NOT classified as Software Engineer."""
        role, method = standardize_job_title("Engineering Manager")
        assert role == "ENGINEERING_MANAGER"
        assert method == "RULE_ENG_MGR"

        role, _ = standardize_job_title("Software Engineering Manager - Core Platform")
        assert role == "ENGINEERING_MANAGER"

        role, _ = standardize_job_title("Director of Engineering")
        assert role == "ENGINEERING_MANAGER"

    def test_product_vs_project_manager(self):
        """Verify Product Manager vs Project Manager distinction."""
        role, method = standardize_job_title("Senior Product Manager")
        assert role == "PRODUCT_MANAGER"
        assert method == "RULE_PROD_MGR"

        role, method = standardize_job_title("Technical Project Manager")
        assert role == "PROJECT_MANAGER"
        assert method == "RULE_PROJ_MGR"

        role, _ = standardize_job_title("Scrum Master / Agile Coach")
        assert role == "PROJECT_MANAGER"

    def test_architect_precedence_over_engineer(self):
        """Verify Architect roles take precedence over Engineer roles."""
        role, method = standardize_job_title("Enterprise Data Architect")
        assert role == "DATA_ARCHITECT"
        assert method == "RULE_DATA_ARCH"

        role, method = standardize_job_title("Principal Cloud Solutions Architect")
        assert role == "CLOUD_ARCHITECT"
        assert method == "RULE_CLOUD_ARCH"

    def test_machine_learning_and_ai_precedence(self):
        """Verify ML and AI Engineer take precedence over general Software Engineer and Data Scientist."""
        role, method = standardize_job_title("Senior Machine Learning Engineer")
        assert role == "MACHINE_LEARNING_ENGINEER"
        assert method == "RULE_ML_ENG"

        role, _ = standardize_job_title("MLOps Engineer (Python/Kubernetes)")
        assert role == "MACHINE_LEARNING_ENGINEER"

        role, method = standardize_job_title("Lead Generative AI Engineer")
        assert role == "AI_ENGINEER"
        assert method == "RULE_AI_ENG"

    def test_data_engineer_vs_data_analyst(self):
        """Verify Data Engineer takes precedence over Data Analyst and Software Engineer."""
        role, method = standardize_job_title("Big Data Engineer (Spark/Scala)")
        assert role == "DATA_ENGINEER"
        assert method == "RULE_DATA_ENG"

        role, _ = standardize_job_title("Senior ETL Developer")
        assert role == "DATA_ENGINEER"

        role, method = standardize_job_title("Senior Data Analyst - Marketing")
        assert role == "DATA_ANALYST"
        assert method == "RULE_DATA_ANALYST"

        role, _ = standardize_job_title("Tableau & Power BI Developer")
        assert role == "DATA_ANALYST"

    def test_data_scientist_and_dba(self):
        """Verify Data Scientist and DBA are distinctly classified."""
        role, method = standardize_job_title("Lead Data Scientist")
        assert role == "DATA_SCIENTIST"
        assert method == "RULE_DATA_SCI"

        role, method = standardize_job_title("Senior Database Administrator (PostgreSQL)")
        assert role == "DATABASE_ADMINISTRATOR"
        assert method == "RULE_DBA"

    def test_business_analyst_vs_cyber_analyst(self):
        """Verify Business Analyst and Cybersecurity Analyst distinctions."""
        role, method = standardize_job_title("Senior Business Analyst")
        assert role == "BUSINESS_ANALYST"
        assert method == "RULE_BIZ_ANALYST"

        role, method = standardize_job_title("SOC Cybersecurity Analyst - Tier 2")
        assert role == "CYBERSECURITY_ANALYST"
        assert method == "RULE_CYBER_ANALYST"

    def test_sre_devops_cloud_precedence(self):
        """Verify SRE vs Cloud DevOps vs DevOps vs Cloud Engineer."""
        role, method = standardize_job_title("Staff Site Reliability Engineer (SRE)")
        assert role == "SITE_RELIABILITY_ENGINEER"
        assert method == "RULE_SRE"

        role, method = standardize_job_title("Cloud DevOps Engineer (AWS)")
        assert role == "CLOUD_DEVOPS_ENGINEER"
        assert method == "RULE_CLOUD_DEVOPS"

        role, method = standardize_job_title("Senior DevOps Engineer")
        assert role == "DEVOPS_ENGINEER"
        assert method == "RULE_DEVOPS"

        role, method = standardize_job_title("AWS Cloud Infrastructure Engineer")
        assert role == "CLOUD_ENGINEER"
        assert method == "RULE_CLOUD_ENG"

    def test_security_and_qa_engineer(self):
        """Verify Security Engineer and QA Engineer."""
        role, method = standardize_job_title("Application Security Engineer")
        assert role == "SECURITY_ENGINEER"
        assert method == "RULE_SEC_ENG"

        role, method = standardize_job_title("Senior QA Automation Engineer (SDET)")
        assert role == "QA_ENGINEER"
        assert method == "RULE_QA_ENG"

    def test_fullstack_frontend_backend_swe(self):
        """Verify Full Stack vs Frontend vs Backend vs general Software Engineer."""
        role, method = standardize_job_title("Lead Full Stack Developer")
        assert role == "FULL_STACK_DEVELOPER"
        assert method == "RULE_FULL_STACK"

        role, method = standardize_job_title("Senior Frontend Developer (React/TypeScript)")
        assert role == "FRONTEND_DEVELOPER"
        assert method == "RULE_FRONTEND"

        role, method = standardize_job_title("Python Backend Developer")
        assert role == "BACKEND_DEVELOPER"
        assert method == "RULE_BACKEND"

        role, _ = standardize_job_title("Dot Net Developer")
        assert role == "BACKEND_DEVELOPER"

        role, method = standardize_job_title("Software Development Engineer II (SDE II)")
        assert role == "SOFTWARE_ENGINEER"
        assert method == "RULE_SWE"

        role, _ = standardize_job_title("Application Developer - Java")
        assert role == "SOFTWARE_ENGINEER"


class TestRefinedTaxonomyMatches:
    """Verifies evidence-backed taxonomy additions and positive/negative classification boundaries (Phase 7B.2)."""

    def test_data_engineering_and_platform_engineer_refinements(self):
        """Verify Data Platform Engineer, Hadoop Dev, Spark Eng, and Data Eng IC titles."""
        # Positive matches
        r1, m1 = standardize_job_title("Senior Data Platform Engineer")
        assert r1 == "DATA_ENGINEER" and m1 == "RULE_DATA_ENG"

        r2, m2 = standardize_job_title("Hadoop Developer - Big Data")
        assert r2 == "DATA_ENGINEER" and m2 == "RULE_DATA_ENG"

        r3, m3 = standardize_job_title("Python - Pyspark Engineer")
        assert r3 == "DATA_ENGINEER" and m3 == "RULE_DATA_ENG"

        r4, m4 = standardize_job_title("Future Opportunity - Data Engineering Consultant")
        assert r4 == "DATA_ENGINEER" and m4 == "RULE_DATA_ENG"

        r5, m5 = standardize_job_title("Engineer II-Data Engineering")
        assert r5 == "DATA_ENGINEER" and m5 == "RULE_DATA_ENG"

        # Negative match: Hadoop Administrator must NOT be classified as Data Engineer
        r_admin, _ = standardize_job_title("Hadoop Administrator")
        assert r_admin != "DATA_ENGINEER"

    def test_data_engineering_manager_precedence(self):
        """Verify Manager/Director/VP of Data Engineering maps to ENGINEERING_MANAGER, not DATA_ENGINEER."""
        r1, m1 = standardize_job_title("Manager, Data Engineering")
        assert r1 == "ENGINEERING_MANAGER" and m1 == "RULE_ENG_MGR"

        r2, m2 = standardize_job_title("Data Engineering Manager - Azure & PySpark")
        assert r2 == "ENGINEERING_MANAGER" and m2 == "RULE_ENG_MGR"

        r3, m3 = standardize_job_title("Director, Data Engineering & Architecture")
        assert r3 == "ENGINEERING_MANAGER" and m3 == "RULE_ENG_MGR"

        r4, m4 = standardize_job_title("VP of Data Engineering")
        assert r4 == "ENGINEERING_MANAGER" and m4 == "RULE_ENG_MGR"

    def test_data_engineering_architect_precedence(self):
        """Verify Data Engineering Architect maps to DATA_ARCHITECT, not DATA_ENGINEER."""
        r1, m1 = standardize_job_title("DATA ENGINEERING ARCHITECT")
        assert r1 == "DATA_ARCHITECT" and m1 == "RULE_DATA_ARCH"

        r2, m2 = standardize_job_title("Architect - Data Engineering")
        assert r2 == "DATA_ARCHITECT" and m2 == "RULE_DATA_ARCH"

    def test_business_intelligence_engineer_mapping(self):
        """Verify Business Intelligence Engineer maps to DATA_ANALYST per empirical BI skill signature."""
        r1, m1 = standardize_job_title("Business Intelligence Engineer")
        assert r1 == "DATA_ANALYST" and m1 == "RULE_DATA_ANALYST"

        r2, m2 = standardize_job_title("Senior Business Intelligence Engineer III")
        assert r2 == "DATA_ANALYST" and m2 == "RULE_DATA_ANALYST"

        r3, m3 = standardize_job_title("Power BI Engineer")
        assert r3 == "DATA_ANALYST" and m3 == "RULE_DATA_ANALYST"

        # Negative match: Big Data Engineer is DATA_ENGINEER, not DATA_ANALYST
        r_neg, _ = standardize_job_title("Senior Big Data Engineer")
        assert r_neg == "DATA_ENGINEER"

    def test_database_developer_refinements(self):
        """Verify PL/SQL, T-SQL, and Database Developer map to DATABASE_ADMINISTRATOR."""
        r1, m1 = standardize_job_title("Oracle PL/SQL Developer")
        assert r1 == "DATABASE_ADMINISTRATOR" and m1 == "RULE_DBA"

        r2, m2 = standardize_job_title("PLSQL Developer")
        assert r2 == "DATABASE_ADMINISTRATOR" and m2 == "RULE_DBA"

        r3, m3 = standardize_job_title("Senior T-SQL Developer")
        assert r3 == "DATABASE_ADMINISTRATOR" and m3 == "RULE_DBA"

        r4, m4 = standardize_job_title("Database Developer III")
        assert r4 == "DATABASE_ADMINISTRATOR" and m4 == "RULE_DBA"

        # Negative matches: General backend or fullstack developers must NOT map to DBA
        r_neg1, _ = standardize_job_title("Java Developer")
        assert r_neg1 == "BACKEND_DEVELOPER"

        r_neg2, _ = standardize_job_title("Full Stack Web Developer")
        assert r_neg2 == "FULL_STACK_DEVELOPER"

    def test_security_architect_not_cloud_architect(self):
        """Verify Security Architect maps to SECURITY_ENGINEER and NEVER CLOUD_ARCHITECT."""
        r1, m1 = standardize_job_title("Security Architect")
        assert r1 == "SECURITY_ENGINEER" and m1 == "RULE_SEC_ENG"
        assert r1 != "CLOUD_ARCHITECT"

        r2, m2 = standardize_job_title("Cyber Security Architect - L1")
        assert r2 == "SECURITY_ENGINEER" and m2 == "RULE_SEC_ENG"
        assert r2 != "CLOUD_ARCHITECT"

        r3, m3 = standardize_job_title("Enterprise Security Architect")
        assert r3 == "SECURITY_ENGINEER" and m3 == "RULE_SEC_ENG"
        assert r3 != "CLOUD_ARCHITECT"

        # Cloud Architect negative match: Unqualified Cloud Architect remains CLOUD_ARCHITECT
        r_cloud, m_cloud = standardize_job_title("Principal Cloud Architect")
        assert r_cloud == "CLOUD_ARCHITECT" and m_cloud == "RULE_CLOUD_ARCH"

    def test_cloud_solution_architect_refinements(self):
        """Verify Cloud/AWS/Azure Solution Architects map to CLOUD_ARCHITECT, while non-cloud do not."""
        r1, m1 = standardize_job_title("AWS Solutions Architect")
        assert r1 == "CLOUD_ARCHITECT" and m1 == "RULE_CLOUD_ARCH"

        r2, m2 = standardize_job_title("Azure Solution Architect")
        assert r2 == "CLOUD_ARCHITECT" and m2 == "RULE_CLOUD_ARCH"

        r3, m3 = standardize_job_title("Cloud Solution Architect - Enterprise")
        assert r3 == "CLOUD_ARCHITECT" and m3 == "RULE_CLOUD_ARCH"

        # Negative match: Non-cloud solution architects must NOT be classified as CLOUD_ARCHITECT
        r_sf, _ = standardize_job_title("Salesforce Solution Architect")
        assert r_sf != "CLOUD_ARCHITECT"

        r_sap, _ = standardize_job_title("SAP Solution Architect")
        assert r_sap != "CLOUD_ARCHITECT"

    def test_mlops_operations_refinement(self):
        """Verify Machine Learning Operations maps to MACHINE_LEARNING_ENGINEER."""
        r1, m1 = standardize_job_title("Machine Learning Operations – Staff Engineer")
        assert r1 == "MACHINE_LEARNING_ENGINEER" and m1 == "RULE_ML_ENG"

        r2, m2 = standardize_job_title("Lead Machine Learning Operations Engineer")
        assert r2 == "MACHINE_LEARNING_ENGINEER" and m2 == "RULE_ML_ENG"

    def test_software_developer_level_refinement(self):
        """Verify Indian IT corporate Developer L1-L5 levels map to SOFTWARE_ENGINEER."""
        r1, m1 = standardize_job_title("Developer - L3")
        assert r1 == "SOFTWARE_ENGINEER" and m1 == "RULE_SWE"

        r2, m2 = standardize_job_title("Developer - L4")
        assert r2 == "SOFTWARE_ENGINEER" and m2 == "RULE_SWE"

        r3, m3 = standardize_job_title("Developer - L2 Support")
        assert r3 == "SOFTWARE_ENGINEER" and m3 == "RULE_SWE"

    def test_application_support_engineer_negative_match(self):
        """Verify Application Support Engineer is NOT classified as SOFTWARE_ENGINEER."""
        r1, _ = standardize_job_title("Application Support Engineer")
        assert r1 != "SOFTWARE_ENGINEER", f"Application Support Engineer should not be SWE, got {r1}"

        r2, _ = standardize_job_title("Senior Application Support Engineer")
        assert r2 != "SOFTWARE_ENGINEER", f"Senior Application Support Engineer should not be SWE, got {r2}"


class TestVariationsAndFallbacks:
    """Verifies casing, whitespace, punctuation, prefixes, and safe fallback handling."""

    def test_case_insensitivity(self):
        """Verify uppercase, lowercase, and mixed case yield identical classifications."""
        t1, _ = standardize_job_title("SOFTWARE ENGINEER")
        t2, _ = standardize_job_title("software engineer")
        t3, _ = standardize_job_title("SoFtWaRe EnGiNeEr")
        assert t1 == t2 == t3 == "SOFTWARE_ENGINEER"

    def test_whitespace_and_punctuation(self):
        """Verify padded whitespace, tabs, and common punctuation."""
        t1, _ = standardize_job_title("   Data Engineer   ")
        assert t1 == "DATA_ENGINEER"

        t2, _ = standardize_job_title("Full-Stack Developer")
        assert t2 == "FULL_STACK_DEVELOPER"

        t3, _ = standardize_job_title("UI/UX Developer")
        assert t3 == "FRONTEND_DEVELOPER"

    def test_experience_level_prefixes_preserved(self):
        """Verify Junior, Senior, Staff, Principal, Intern prefixes don't break classification."""
        assert standardize_job_title("Junior Software Engineer Intern")[0] == "SOFTWARE_ENGINEER"
        assert standardize_job_title("Senior Staff Data Scientist")[0] == "DATA_SCIENTIST"
        assert standardize_job_title("Principal DevOps Engineer")[0] == "DEVOPS_ENGINEER"
        assert standardize_job_title("Lead Data Analyst - Operations")[0] == "DATA_ANALYST"

    def test_other_specialized_fallback(self):
        """Verify recognized non-tech roles classify as OTHER_SPECIALIZED."""
        roles = [
            "Registered Nurse - ICU",
            "Store Manager",
            "Shift Supervisor",
            "Sales Associate",
            "Customer Service Representative",
            "Warehouse Associate",
            "First Year Tax Professional",
            "Barista",
            "Dental Hygienist",
            "Human Resources Generalist",
        ]
        for r in roles:
            cat, method = standardize_job_title(r)
            assert cat == "OTHER_SPECIALIZED", f"Failed for {r}: got {cat}"
            assert method == "RULE_OTHER_SPECIALIZED"

    def test_unclassified_fallback(self):
        """Verify unclassifiable, unknown, or empty titles map to UNCLASSIFIED."""
        assert standardize_job_title("")[0] == "UNCLASSIFIED"
        assert standardize_job_title(None)[0] == "UNCLASSIFIED"
        assert standardize_job_title("   ")[0] == "UNCLASSIFIED"
        assert standardize_job_title("Zzzz Xxxx Yyyy 12345")[0] == "UNCLASSIFIED"


class TestSparkNativeTransformerEquivalence:
    """Verifies that the Spark-native DataFrame transformer matches pure Python outputs."""

    def test_spark_native_equivalence(self, spark: SparkSession):
        sample_data = [
            ("https://link1", "Senior Data Engineer"),
            ("https://link2", "Full Stack Software Developer"),
            ("https://link3", "Engineering Manager - Platform"),
            ("https://link4", "SOC Cybersecurity Analyst"),
            ("https://link5", "Customer Service Representative"),
            ("https://link6", "Unclassifiable Nonsense String 999"),
            ("https://link7", ""),
            ("https://link8", None),
        ]
        schema = StructType([
            StructField("job_link", StringType(), False),
            StructField("job_title_clean", StringType(), True),
        ])
        df = spark.createDataFrame(sample_data, schema)
        df_result = add_standardized_role_columns(df, title_col="job_title_clean")

        rows = df_result.collect()
        assert len(rows) == len(sample_data)

        for row in rows:
            title = row["job_title_clean"]
            expected_role, expected_method = standardize_job_title(title)
            assert row["standardized_role"] == expected_role, f"Role mismatch for '{title}'"
            assert row["role_classification_method"] == expected_method, f"Method mismatch for '{title}'"
