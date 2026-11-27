# DemandLens — Phase 6D.4 Canonical Silver Integration Execution Report

**Execution Run ID:** `canonical_run_20261126_063239_5fa26a`  
**Execution Timestamp:** `2026-11-26T12:02:39.247Z`  
**Pipeline Duration:** `745.07 seconds` (~12.4 minutes)  
**Status:** **SUCCESS** (100% Quality Gates Passed)  
**Environment:** Windows 11, PySpark 4.3.0, PyArrow 18.1.0, Java 17, Local Concurrency 4 Cores  
**Author / Orchestration:** Antigravity AI Engine

---

## 1. Executive Summary

Phase 6D.4 represents the final architectural culmination of the DemandLens Silver Layer. In this phase, source-specific Silver tables from two disparate labor market datasets:
1. **LinkedIn 2024** (North American, British, Australasian Western market: postings, normalized skills bridge, cleaned summaries)
2. **Indian Job Market 2025–2026** (Domestic Indian technology market: jobs with INR salaries and experience, normalized skills bridge)

have been unified, standardized, and promoted into the **Canonical Silver Layer** under `data/silver/canonical/`.

### Key Achievements:
- **Global Surrogate Key Stability:** Implemented prefix-based composite surrogate keys (`<source_id>#<source_job_id>`), eliminating primary key collisions across heterogeneous source identifiers (URLs vs integer IDs).
- **Decoupled Text Architecture:** Segregated heavy descriptive text into a dedicated 1:1 linked entity (`canonical_job_text`), constraining `canonical_jobs` to **130.35 MB** (1,446,133 rows), preventing a 20x I/O scan penalty for downstream analytical dashboards.
- **Strict Referential Integrity:** Pre-promotion cross-validation verified **0 orphan records** across all 27.53M skill mappings and 1.40M prose text records (`orphan_free = True`).
- **Complete Invariant Preservation:** Preserved 100% provenance, zero cross-source synthetic imputations (no fake salaries for LinkedIn, no fake calendar dates for Indian relative posts), and 1NF normalized technical skill tokens (`c++`, `c#`, `.net`, `node.js`, `sql`, `pl/sql`, `ci/cd`).
- **Regression Suite:** 62 of 62 unit, integration, and regression tests passed across all Silver modules.

---

## 2. Physical Output Verification

All canonical datasets were produced via PyArrow bounded streaming into isolated staging directories on Drive `D:`, validated, and atomically promoted:

| Table Name | Destination Path | Rows Generated | Partition Files | Disk Footprint | Format & Compression | Target Spec Compliance |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **`canonical_jobs`** | [`data/silver/canonical/jobs`](file:///d:/DemandLens/data/silver/canonical/jobs) | **1,446,133** | 8 | **130.35 MB** | Parquet / Snappy | PASSED (<= 350 MB target) |
| **`canonical_job_skills_bridge`** | [`data/silver/canonical/job_skills_bridge`](file:///d:/DemandLens/data/silver/canonical/job_skills_bridge) | **27,533,229** | 15 | **1,419.02 MB** | Parquet / Snappy | PASSED (~1.40 GB target) |
| **`canonical_job_text`** | [`data/silver/canonical/job_text`](file:///d:/DemandLens/data/silver/canonical/job_text) | **1,395,011** | 22 | **2,715.59 MB** | Parquet / Snappy | PASSED (~2.75 GB target) |
| **Canonical Metadata** | [`data/silver/canonical/_metadata`](file:///d:/DemandLens/data/silver/canonical/_metadata) | 2 manifests | - | < 1 MB | JSON | PASSED |

**Total Canonical Silver Footprint:** **4,264.96 MB** (~4.16 GiB).  
Drive `D:` remaining capacity: **> 202 GiB**.

---

## 3. Source Ingestion & Lineage Accounting

The pipeline ingested all 5 source Silver tables without data loss or source mutation:

```
                                  [Source Silver Tables]
                                             │
      ┌──────────────────────────────────────┴──────────────────────────────────────┐
      │                                                                            │
LinkedIn 2024 Silver                                                Indian Job Market 2025-2026 Silver
  ├─ job_postings:        1,348,454 rows                              ├─ jobs:               97,679 rows
  ├─ job_skills_bridge:  26,780,092 rows                              └─ job_skills_bridge: 753,137 rows
  └─ job_summary:         1,297,332 rows                                             │
      │                                                                              │
      └──────────────────────────────────────┬───────────────────────────────────────┘
                                             │
                                    [Canonical Integration]
                                             │
               ┌─────────────────────────────┼─────────────────────────────┐
               ▼                             ▼                             ▼
       canonical_jobs           canonical_job_skills_bridge        canonical_job_text
     1,446,133 total rows           27,533,229 total rows         1,395,011 total rows
     ├─ LinkedIn: 1,348,454         ├─ LinkedIn: 26,780,092       ├─ LinkedIn: 1,297,332
     └─ Indian:      97,679         └─ Indian:      753,137       └─ Indian:      97,679
```

### Source Immutability Audit:
A post-execution read-only audit confirmed that all 5 source Silver directories remain 100% unaltered:
- `data/silver/linkedin_2024/job_postings`: 1,348,454 rows (180.0 MB)
- `data/silver/linkedin_2024/job_skills_bridge`: 26,780,092 rows (1,389.62 MB)
- `data/silver/linkedin_2024/job_summary`: 1,297,332 rows (2,687.12 MB)
- `data/silver/indian_job_market_2025_2026/jobs`: 97,679 rows (40.93 MB)
- `data/silver/indian_job_market_2025_2026/job_skills_bridge`: 753,137 rows (7.92 MB)

---

## 4. Schema Conformance & Mapping Fidelity

### 4.1 `canonical_jobs` (28 Standardized Columns)

| Column Name | Data Type | Nullable | Canonical Semantics & Source Rules |
| :--- | :--- | :---: | :--- |
| `canonical_job_id` | `string` | **False** | Composite key `<source_id>#<source_job_id>`. Globally unique. |
| `source_id` | `string` | **False** | Discriminator: `'linkedin_2024'` or `'indian_job_market_2025_2026'`. |
| `source_job_id` | `string` | **False** | Original primary key: `job_link` (LinkedIn) or `job_id` (Indian). |
| `job_title_clean` | `string` | **False** | Standardized title, trimmed, casing preserved. Fallback: `'UNSPECIFIED'`. |
| `company_name_clean` | `string` | True | Standardized employer name. |
| `location_clean` | `string` | True | Standardized geographic location. |
| `country_code` | `string` | **False** | ISO-3166-1 alpha-2: `'US'`, `'GB'`, `'CA'`, `'AU'`, `'IN'`, `'OTHER'`. |
| `workplace_type` | `string` | **False** | `'REMOTE'`, `'HYBRID'`, `'ON_SITE'`, `'UNSPECIFIED'`. |
| `is_remote` | `boolean` | **False** | `True` strictly for 100% remote positions. |
| `is_hybrid` | `boolean` | **False** | `True` strictly for hybrid positions. |
| `seniority_tier` | `string` | **False** | Harmonized tier: `'ENTRY'`, `'ASSOCIATE'`, `'MID'`, `'SENIOR'`, `'LEAD'`, `'DIRECTOR'`, `'EXECUTIVE'`, `'UNSPECIFIED'`. |
| `experience_min_years` | `double` | True | Minimum numeric years. Populated for Indian, `NULL` for LinkedIn. |
| `experience_max_years` | `double` | True | Maximum numeric years. Populated for Indian, `NULL` for LinkedIn. |
| `salary_min` | `double` | True | Lower salary bound. `NULL` for LinkedIn. |
| `salary_max` | `double` | True | Upper salary bound. `NULL` for LinkedIn. |
| `salary_midpoint` | `double` | True | Calculated midpoint `(salary_min + salary_max) / 2`. |
| `salary_currency` | `string` | True | ISO currency code (`'INR'`). `NULL` for LinkedIn. |
| `salary_period` | `string` | True | Periodicity (`'ANNUAL'`, `'MONTHLY'`). `NULL` for LinkedIn. |
| `salary_disclosed` | `boolean` | **False** | Boolean indicator (`False` for all LinkedIn, `True` for Indian with wage data). |
| `posted_date` | `date` | True | Calendar post date. Populated for LinkedIn, `NULL` for Indian relative dates. |
| `freshness_category` | `string` | True | Standardized tier (`'FRESH_24H'`, `'RECENT_7D'`, etc.). |
| `skill_count` | `integer` | **False** | Count of distinct normalized skills mapped in bridge table. |
| `has_skills` | `boolean` | **False** | Derived flag: `skill_count > 0`. |
| `has_description` | `boolean` | **False** | Derived flag: `True` if record exists in `canonical_job_text`. |
| `_source_file` | `string` | **False** | Provenance lineage tracking raw origin file. |
| `_ingestion_timestamp`| `timestamp` | True | Bronze ingestion timestamp. |
| `_canonical_run_id` | `string` | **False** | Execution identifier `canonical_run_20261126_063239_5fa26a`. |
| `_canonical_timestamp`| `timestamp` | **False** | UTC processing timestamp `2026-11-26 12:02:39`. |

### 4.2 `canonical_job_skills_bridge` (5 Columns)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `canonical_job_id` | `string` | **False** | Foreign key referencing `canonical_jobs.canonical_job_id`. |
| `skill` | `string` | **False** | Normalized skill token in 1NF with technical symbols preserved. |
| `source_id` | `string` | **False** | Platform discriminator. |
| `_canonical_run_id` | `string` | **False** | Run identifier. |
| `_canonical_timestamp`| `timestamp` | **False** | UTC timestamp. |

### 4.3 `canonical_job_text` (7 Columns)

| Column Name | Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `canonical_job_id` | `string` | **False** | 1:1 Foreign key referencing `canonical_jobs.canonical_job_id`. |
| `description_clean` | `string` | **False** | Cleansed plain-text description (HTML stripped, paragraphs intact). |
| `character_count` | `integer` | **False** | Text character length. |
| `word_count` | `integer` | **False** | Word count. |
| `source_id` | `string` | **False** | Platform discriminator. |
| `_canonical_run_id` | `string` | **False** | Run identifier. |
| `_canonical_timestamp`| `timestamp` | **False** | UTC timestamp. |

---

## 5. Quality Gates & Validation Audit

Automated verification via [`data/temp_validation/validate_phase6d4_execution.py`](file:///d:/DemandLens/data/temp_validation/validate_phase6d4_execution.py) confirmed:

| Quality Gate | Validation Criterion | Observed Value | Verdict |
| :--- | :--- | :---: | :---: |
| **Row Count (Jobs)** | Exactly 1,446,133 rows | 1,446,133 | **PASSED** |
| **Row Count (Skills)** | Exactly 27,533,229 rows | 27,533,229 | **PASSED** |
| **Row Count (Text)** | Exactly 1,395,011 rows | 1,395,011 | **PASSED** |
| **Referential Integrity (Skills)** | 0 orphan skills (`skills_orphans == 0`) | 0 | **PASSED** |
| **Referential Integrity (Text)** | 0 orphan descriptions (`text_orphans == 0`) | 0 | **PASSED** |
| **Primary Key Uniqueness** | 0 duplicate canonical_job_id keys | 0 duplicates | **PASSED** |
| **Core Jobs Table Size** | Must not exceed 350 MB | 130.35 MB | **PASSED** |
| **Staging Hygiene** | 0 lingering `_staging_*` directories | 0 folders | **PASSED** |
| **Source Immutability** | All 5 source Silver tables intact | 100% matched | **PASSED** |

---

## 6. Regression Testing Summary

The full test suite covering all Silver components was executed:
- Command: `pytest tests/test_canonical_silver_integration.py tests/test_linkedin_job_summary_silver.py tests/test_linkedin_job_skills_silver.py tests/test_silver_cleaning_rules.py -v`
- Outcome: **62 passed, 0 failed, 0 skipped** (84.90s).

### Test Breakdown:
- `tests/test_canonical_silver_integration.py`: **7/7 passed** (schema structure, LinkedIn mapping, Indian mapping, union integrity, bridge grain, decoupled text, zero-orphan validation).
- `tests/test_linkedin_job_summary_silver.py`: **10/10 passed** (HTML entities, tag stripping, structural elements, paragraph preservation, technical symbols, financial figures, long text no-truncation).
- `tests/test_linkedin_job_skills_silver.py`: **6/6 passed** (symbol preservation for `C++`, `C#`, `.NET`, `Node.js`, `SQL`, `PL/SQL`, `CI/CD`, deduplication, bridge grain).
- `tests/test_silver_cleaning_rules.py`: **39/39 passed** (text cleaning, skill normalization, location parsing, experience bounds, salary parsing, freshness tiers).

---

## 7. Artifacts and Lineage References

- **Canonical Silver Pipeline Engine:** [`src/demandlens/silver/canonical_transformer.py`](file:///d:/DemandLens/src/demandlens/silver/canonical_transformer.py)
- **Canonical Silver CLI Runner:** [`scripts/run_canonical_silver_transformation.py`](file:///d:/DemandLens/scripts/run_canonical_silver_transformation.py)
- **Unit & Integration Tests:** [`tests/test_canonical_silver_integration.py`](file:///d:/DemandLens/tests/test_canonical_silver_integration.py)
- **Validation Script:** [`data/temp_validation/validate_phase6d4_execution.py`](file:///d:/DemandLens/data/temp_validation/validate_phase6d4_execution.py)
- **Canonical Manifest:** [`data/silver/canonical/_metadata/canonical_integration_manifest.json`](file:///d:/DemandLens/data/silver/canonical/_metadata/canonical_integration_manifest.json)
- **Approved Architecture Design:** [`docs/architecture/canonical_silver_integration_design.md`](file:///d:/DemandLens/docs/architecture/canonical_silver_integration_design.md)
- **Approved Preflight Report:** [`docs/reports/phase6d4_silver_integration_preflight_report.md`](file:///d:/DemandLens/docs/reports/phase6d4_silver_integration_preflight_report.md)

---

## 8. Conclusion & Milestone Sign-Off

**Phase 6D.4 is complete and verified.**

The Silver Layer of DemandLens now provides a unified, clean, type-safe, decoupled, and referentially sound foundation. The core analytical dimension (`canonical_jobs`) is compact (130 MB) and ready for lightning-fast OLAP queries in DuckDB/Spark, while the high-dimensional skill graph (27.53M edges) and prose dimension (1.40M texts) are cleanly decoupled for Phase 7 (Gold Layer dimensional modeling) and downstream RAG embedding pipelines.
