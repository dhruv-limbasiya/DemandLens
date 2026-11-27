# DemandLens — Phase 6D.4 Silver Integration Preflight Report

**Document:** Engineering Preflight & Architecture Assessment Report  
**Milestone:** Phase 6D.4 — Silver Integration Preflight & Design  
**Project Path:** `D:\DemandLens`  
**Execution Date:** 2026-11-26  
**Auditor / Lead Architect:** Data Architecture Engineering (DemandLens Team)  
**Preflight Mode:** STRICT READ-ONLY PREFLIGHT (NO EXECUTION, NO MODIFICATIONS)  
**Preflight Verdict:** **APPROVED FOR IMPLEMENTATION**  

---

## 1. Executive Summary & Preflight Objectives

Phase 6D.4 establishes the operational design and preflight validation for integrating DemandLens's two primary labor market datasets into a unified, canonical Silver layer:
1. **LinkedIn Jobs & Skills 2024:** 1,348,454 job postings, 26,780,092 normalized skill records, and 1,297,332 text summaries covering Western economies (US, GB, CA, AU).
2. **Indian Job Market Dataset 2025–2026:** 97,679 job records and 753,137 normalized skill records covering the domestic Indian IT and corporate job market.

### Core Objectives Verified in Preflight
- **Cross-Source Dimensional Schema:** Designed a 28-column canonical job schema that standardizes overlapping dimensions (title, company, country, remote status, seniority) while preserving native source identifiers and provenance.
- **Decoupled Text Architecture:** Formally prohibited denormalizing the 5.0 GB LinkedIn unstructured job summary into the main jobs table. Designed a dedicated 1:1 linked table (`canonical_job_text`) to prevent massive columnar I/O bloat in BI aggregations.
- **Defensive Analytics Guardrails:** Established strict querying protocols to prevent common cross-dataset pitfalls (e.g., averaging INR salaries across US jobs, treating missing salaries as zero, or blending disparate temporal snapshots).
- **Physical Headroom:** Verified that the estimated ~4.27 GiB canonical footprint fits safely within Drive `D:`'s **206.95 GiB** available headroom.

---

## 2. Inventory of Inspected Silver Schemas

All 5 existing Silver tables and Bronze posting baselines were inspected via Parquet metadata and distributed readers without loading full datasets into driver memory:

```
+-------------------------------------------------------------------------------------------------------+
| Physical Silver Datasets Inspected                                                                   |
+-----------------------------+------------+-------------+--------------+-------------------------------+
| Dataset Name                | Partitions | Row Count   | Size (MB)    | Primary Key / Grain           |
+-----------------------------+------------+-------------+--------------+-------------------------------+
| linkedin_postings (Bronze)* | 4 parts    | 1,348,454   | 125.78 MB    | job_link (1 row/posting)      |
| linkedin_skills_bridge      | 8 parts    | 26,780,092  | 1,389.62 MB  | (job_link, skill)             |
| linkedin_summary            | 22 parts   | 1,297,332   | 2,687.12 MB  | job_link (1 row/summary)      |
| indian_jobs                 | 2 parts    | 97,679      | 40.93 MB     | job_id (1 row/posting)        |
| indian_skills_bridge        | 2 parts    | 753,137     | 7.92 MB      | (job_id, skill)               |
+-----------------------------+------------+-------------+--------------+-------------------------------+
*Note: LinkedIn postings Silver schema was verified from src/demandlens/silver/linkedin_postings_transformer.py
```

### Key Schema Observations
1. **Primary Keys:** LinkedIn uses HTTP URLs (`job_link`), while the Indian dataset uses alphanumeric tokens (`job_id`). They cannot be directly merged without a namespace prefix.
2. **Skill Bridges:** Both datasets share identical 1NF structure (`job_key`, `skill`) and identical Phase 6B lowercase normalization with technical token preservation (`c++`, `c#`, `.net`, `node.js`, `sql`, `pl/sql`, `ci/cd`).
3. **Compensation & Experience:** 100% present in Indian jobs; 100% absent in LinkedIn jobs.
4. **Text Summaries:** Isolated as a dedicated table in LinkedIn; embedded as a raw string column (`jobDescription`) in Indian jobs.

---

## 3. Data Compatibility & Incompatibility Analysis

| Evaluation Dimension | LinkedIn Jobs 2024 | Indian Job Market 2025–2026 | Compatibility Verdict | Architectural Invariant |
| :--- | :--- | :--- | :---: | :--- |
| **Geographic Coverage** | US, GB, CA, AU, Other | 100% India (`IN`) | **ORTHOGONAL** | Standardize ISO `country_code`; mandate country filter in BI. |
| **Compensation Data** | 0% disclosed (absent) | Numeric ranges (INR) | **INCOMPATIBLE** | Populate LinkedIn salaries as `NULL`; add `salary_disclosed` boolean flag. |
| **Experience Metrics** | Categorical tiers only | Numeric years (min/max) | **PARTIALLY COMPATIBLE**| Harmonize on `seniority_tier`; leave numeric years `NULL` for LinkedIn. |
| **Workplace Arrangements** | Explicit `job_type` | Inferred from location | **COMPATIBLE** | Enforce 3-state enum (`REMOTE`, `HYBRID`, `ON_SITE`, `UNSPECIFIED`). |
| **Temporal Snapshots** | Early 2024 calendar dates| Late 2025/2026 relative tiers| **INCOMPATIBLE** | Do not construct continuous time-series across sources. |
| **Skill Representations**| 26.78M tokens in 1NF | 0.75M tokens in 1NF | **HIGHLY COMPATIBLE** | Direct union into `canonical_job_skills_bridge`. |
| **Unstructured Text** | 5.0 GB clean summary | Embedded `jobDescription` | **DECOUPLED** | Isolate text into `canonical_job_text`. |

---

## 4. Proposed Canonical Architecture & Schema

### 4.1 Canonical Schema Contracts

#### Table 1: `canonical_jobs` (`data/silver/canonical/jobs`)
- **Row Count:** 1,446,133 (1,348,454 LinkedIn + 97,679 Indian).
- **Grain:** 1 record per distinct job posting across all sources.
- **Primary Key:** `canonical_job_id` (`<source_id>#<source_job_id>`).
- **Core Columns:**
  - Identifiers: `canonical_job_id`, `source_id`, `source_job_id`.
  - Dimensions: `job_title_clean`, `company_name_clean`, `location_clean`, `country_code`.
  - Workplace: `workplace_type`, `is_remote`, `is_hybrid`.
  - Seniority & Experience: `seniority_tier`, `experience_min_years` (nullable), `experience_max_years` (nullable).
  - Compensation: `salary_min` (nullable), `salary_max` (nullable), `salary_midpoint` (nullable), `salary_currency` (nullable), `salary_period` (nullable), `salary_disclosed` (bool).
  - Temporal: `posted_date` (nullable Date), `freshness_category` (string).
  - Relational Indicators: `skill_count`, `has_skills`, `has_description`.
  - Provenance: `_source_file`, `_ingestion_timestamp`, `_canonical_run_id`, `_canonical_timestamp`.

#### Table 2: `canonical_job_skills_bridge` (`data/silver/canonical/job_skills_bridge`)
- **Row Count:** 27,533,229 (26,780,092 LinkedIn + 753,137 Indian).
- **Grain:** 1 record per unique `(canonical_job_id, skill)` pair.
- **Compound Key:** `(canonical_job_id, skill)`.
- **Columns:** `canonical_job_id`, `skill`, `source_id`, `_canonical_run_id`, `_canonical_timestamp`.

#### Table 3: `canonical_job_text` (`data/silver/canonical/job_text`)
- **Row Count:** 1,395,011 (1,297,332 LinkedIn + 97,679 Indian).
- **Grain:** 1 record per job posting with long text.
- **Primary Key:** `canonical_job_id`.
- **Columns:** `canonical_job_id`, `description_clean`, `character_count`, `word_count`, `source_id`, `_canonical_run_id`, `_canonical_timestamp`.

---

## 5. Integration Strategy & Entity Resolution Policy

### 5.1 Surrogate Key Strategy
Because different job boards use different native key formats (URLs vs. integers/hashes), the canonical layer adopts a deterministic compound surrogate key:
$$\text{canonical\_job\_id} = \text{source\_id} \parallel \text{"\#"} \parallel \text{source\_job\_id}$$
*Examples:*
- `linkedin_2024#https://www.linkedin.com/jobs/view/software-engineer-3795828993`
- `indian_job_market_2025_2026#987213`

This guarantees global uniqueness, prevents key collision, and allows downstream consumers to instantly trace any record back to its source platform.

### 5.2 Strict Prohibition of Fuzzy Cross-Source Deduplication
- **Policy:** **NO cross-source entity matching or record merging.**
- **Rationale:** The LinkedIn dataset covers Western markets during early 2024; the Indian dataset covers domestic Indian markets during 2025–2026. A posting for "Senior Java Developer" at "Accenture" in LinkedIn US is an entirely different job requisition from "Senior Java Developer" at "Accenture" in Indian job boards in 2025. Merging them based on text similarity would introduce gross statistical distortion and destroy provenance.

---

## 6. Analytics Guardrails & BI Contracts

To prevent misleading visualizations in future Dashboards and Gold layers, the following 5 guardrails are formalized:

1. **Mandatory Salary Disclosure Filter:** All compensation aggregations must enforce `WHERE salary_disclosed = True AND country_code = 'IN'`.
2. **Explicit Currency Separation:** Never pool salaries without explicit PPP normalization.
3. **Strict Remote vs. Hybrid Separation:** `is_remote = True` strictly excludes hybrid roles.
4. **Relative Skill Penetration:** Skill demand must be analyzed as relative percentages within each source, never as raw cross-platform sums.
5. **Discrete Snapshot Semantics:** Disallow merging 2024 and 2025/2026 into a single continuous trendline.

---

## 7. Storage, Partitioning & Computational Feasibility

| Canonical Dataset | Estimated Row Count | Estimated Size | Partitions | File Size Target |
| :--- | :---: | :---: | :---: | :---: |
| `canonical_jobs` | 1,446,133 | ~220 MB | 8 parts | ~27 MB / part |
| `canonical_job_skills_bridge` | 27,533,229 | ~1,400 MB (1.37 GiB) | 16 parts | ~87 MB / part |
| `canonical_job_text` | 1,395,011 | ~2,750 MB (2.69 GiB) | 22 parts | ~125 MB / part |
| **Total Canonical Layer** | **30,374,373** | **~4,370 MB (~4.27 GiB)**| **46 parts** | **Uniform Snappy** |

### Workstation Headroom Audit
- **Drive `D:` Available Space:** **206.95 GiB**
- **Projected Storage Requirement:** **~4.27 GiB**
- **Headroom Margin:** **>202.6 GiB remaining (>97.9% buffer)**
- **Memory Feasibility:** PyArrow streaming partition committer processes bounded batches of 10,000 rows (~38 MB heap footprint), well within the 6 GB driver configuration.

---

## 8. Technical & Operational Risk Assessment

| Risk Item | Impact | Severity | Mitigation Protocol |
| :--- | :--- | :---: | :--- |
| **Dimension Table Bloat** | Query slowdown on BI dashboards | HIGH | Decouple long text into `canonical_job_text`. |
| **Windows Socket Buffer Exhaustion**| JVM/Python pipe crashes | HIGH | Use pure PySpark Catalyst expressions for column projections; stream writes via PyArrow. |
| **Key Collisions** | Duplicate records in union | CRITICAL | Enforce `<source_id>#<source_job_id>` compound keys. |
| **Accidental Deduplication** | Data loss across sources | HIGH | Prohibit cross-source entity merging. |
| **Distorted Salary Analytics** | Inaccurate market reports | CRITICAL | Implement `salary_disclosed` boolean flag and guardrail rules. |
| **Storage Overflow** | Disk exhaustion during staging | LOW | Drive D: has 206.95 GiB free; pipeline uses isolated staging with automatic cleanup. |

---

## 9. Implementation Acceptance Criteria (Quality Gates)

When Phase 6E (Implementation) is initiated, the canonical pipeline must satisfy the following automated quality gates before promotion:

1. **Row Count Conservation:**
   $$\text{canonical\_jobs.count()} == 1,348,454 + 97,679 == 1,446,133$$
2. **Key Uniqueness:** Exactly 1,446,133 distinct `canonical_job_id` values (0 nulls, 0 blanks).
3. **Bridge Referential Integrity:** 100% of keys in `canonical_job_skills_bridge` must join to `canonical_jobs` (0 orphan records).
4. **Text Referential Integrity:** 100% of keys in `canonical_job_text` must join to `canonical_jobs` (0 orphan records).
5. **Decoupled Text Size:** Core `canonical_jobs` table size must not exceed **350 MB**.
6. **Immutability Invariant:** Source Silver directories (`linkedin_2024` and `indian_job_market_2025_2026`) must remain 100% byte-intact.

---

## 10. Final Preflight Verdict

### Verdict: **APPROVED FOR IMPLEMENTATION**

**Justification:**
- Forensic inspection confirms that all 5 Silver source datasets and schemas are well-structured, clean, and fully understood.
- The proposed 3-table canonical architecture (`jobs`, `skills_bridge`, `job_text`) preserves all source-specific attributes while preventing dimension table bloat.
- Rigorous analytics guardrails eliminate misleading cross-source comparisons.
- The storage footprint (~4.27 GiB) is safely bounded within Rishi's workstation capacity (>206 GiB free).

**Next Step:** Proceed to Phase 6E implementation upon stakeholder approval.

---
*Preflight design complete. Execution has halted. No transformation was performed.*
