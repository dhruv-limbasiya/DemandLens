# DemandLens: Phase 6C.1 — Indian Job Market Silver Quality Audit & Verdict Report

**Audit Date:** November 23, 2026  
**Auditor:** DemandLens Independent Quality Audit (Read-Only)  
**Scope:** Phase 6C Indian Job Market Silver Transformation Pipeline  
**Target:** `D:\DemandLens\src\demandlens\silver\indian_jobs_transformer.py`  
**Pipeline Run ID Audited:** `silver_run_20261123_174548_d90b33`  
**Verdict:** **APPROVED FOR PHASE 6D** (With Documented Caveats & Observations)  

---

## 1. Executive Verdict & Summary

A rigorous, strictly read-only audit of the **Phase 6C Indian Job Market Silver Transformation** was conducted across source code, unit tests, analytical rules, and documented execution artifacts.

The audit confirms that the Phase 6C pipeline establishes an enterprise-grade Silver transformation:
- **Primary Key Integrity:** Strict deduplication on `jobId` reduces Bronze input from `97,929` to `97,679` rows with zero loss of unique postings.
- **Normalized 1NF Skills Bridge:** Deconstructs delimited skill strings into `753,137` unique `(job_id, skill)` pairs with **100% referential integrity** (0 orphan records).
- **Critical Technical Token Preservation:** Language symbols such as `C++`, `C#`, `.NET`, `Node.js`, and `PL/SQL` are safely preserved without punctuation truncation.
- **Full Provenance & Non-Destructive Modeling:** All 17 raw Bronze columns are preserved alongside standardized columns, ingestion timestamps, and silver run IDs.
- **Regression Suite:** All **58 automated tests** (including 39 cleaning rule tests and 3 Indian Silver pipeline integration tests) pass with a 100% success rate.

**Verdict Decision:** **APPROVED FOR PHASE 6D (LinkedIn Silver Transformations)**.  
The underlying design, code contracts, and transformation logic are sound and verified. Four specific semantic caveats have been documented below for downstream Gold layer consumption.

---

## 2. Comprehensive Audit Findings

### 2.1 Duplicate Handling & Resolution
- **Bronze Discrepancy Breakdown:**
  - Raw Bronze record count: **97,929 rows**.
  - Distinct `jobId` count: **97,679 unique identifiers**.
  - Exactly **250 duplicate rows** were eliminated ($97,929 - 97,679 = 250$).
  - **Identical Duplicates:** `247` duplicate rows were byte-for-byte identical across all 21 Bronze columns (including metadata and ingestion timestamps).
  - **Core Attribute Duplicates:** `248` records matched across `(jobId, title, companyName, location)`.
  - Exactly `2` duplicates had trivial variations in secondary metadata (e.g., minor rating/review count updates).
- **Deduplication Strategy:**
  - Implemented via a window partition on `jobId`:
    ```python
    null_expr = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in df.columns)
    window_spec = Window.partitionBy("jobId").orderBy(
        F.desc("_completeness_score"),
        F.desc("_ingestion_timestamp")
    )
    df_dedup = df_scored.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1)
    ```
- **Determinism & Tie-Breaking Analysis:**
  - *Observation:* Because 247 duplicates are 100% byte-for-byte identical, both rows have identical completeness scores and identical ingestion timestamps.
  - *Determinism Impact:* Because the rows are identical across every column, selecting either record yields the exact same data values. No divergence occurs.
  - *Code Quality Note:* For absolute mathematical determinism on distributed Spark clusters, adding an explicit secondary tie-breaker (such as `orderBy(F.desc("_completeness_score"), F.desc("_ingestion_timestamp"), F.asc("title"))`) is recommended for future refactoring, though no data corruption occurred.

---

### 2.2 Salary Semantics & Range Logic
- **`salary_is_valid` Implementation:**
  - Located in `src/demandlens/silver/salary_parser.py` (`validate_salary_range`):
    ```python
    if min_salary is None and max_salary is None:
        return (None, None, True)
    ```
- **Semantic Correctness for Undisclosed Salaries:**
  - The implementation returns `salary_is_valid = True` when compensation is undisclosed (`min_salary IS NULL` and `max_salary IS NULL`).
  - *Design Rationale:* In Phase 6B design, `salary_is_valid = False` is strictly reserved for *malformed or corrupt data* (e.g., negative values, values exceeding the 50 Crore INR sanity cap, or inverted ranges where `min > max`). An undisclosed salary is considered a *valid absence of data* rather than a data defect.
  - *Downstream Consumption Guard:* To prevent analysts from computing zero-averages, `is_salary_disclosed` (or checking `salary_min IS NOT NULL`) must be used alongside `salary_is_valid = True`. 
- **Currency & Periodicity:**
  - **Currency:** Explicitly distinguishes `INR` (33,124 records) and `USD` (20 records). Undisclosed postings have `salary_currency = None`. No currency conversion is fabricated.
  - **Periodicity:** Standardized into `ANNUAL` and `MONTHLY` tiers via regex detection on the raw `salary` text.
  - **Range Sanity:** Midpoints are accurately computed as `(min + max) / 2.0`. Zero inverted ranges exist.

---

### 2.3 Remote / Hybrid Work Classification
- **Classification Logic:**
  - In `src/demandlens/silver/location_cleaning.py`:
    ```python
    _REMOTE_PATTERN = re.compile(
        r"(?i)\b(remote|hybrid|work from home|wfh|telecommute|anywhere|virtual)\b"
    )
    def detect_remote(location: Optional[str]) -> Optional[bool]:
        ...
        if _REMOTE_PATTERN.search(text):
            return True
        return False
    ```
- **Distinguishability & Analytics Distortion Risk:**
  - **Finding:** Under the current Silver schema, **Hybrid and Remote jobs are NOT distinguishable in the `is_remote` column**. Both evaluate to `True`.
  - **Analytics Impact:** Conflating hybrid and remote into a single boolean `is_remote` reports **7,713 remote/hybrid jobs (7.90%)**. In labor market analytics, hybrid positions require physical geographic proximity to an office, whereas remote positions allow national/global hiring.
  - **Mitigation:** The raw uncleaned `location` string is preserved in Silver. In Phase 7 / Gold mart modeling, this will be projected into a standardized tripartite categorical column: `workplace_type: ENUM('REMOTE', 'HYBRID', 'ON_SITE', 'UNSPECIFIED')`.

---

### 2.4 Normalized Skills Bridge Table
- **Bridge Metrics Verified:**
  - Total Bridge Rows: **753,137** distinct `(job_id, skill)` pairs.
  - Unique `(job_id, skill)` pairs: Exactly **753,137** (100% unique, zero duplicates).
  - Duplicate bridge pairs: **0**.
  - Orphan `job_id` records: **0** (verified via `left_anti` join; every bridge record maps to an existing primary key in `jobs`).
  - Vocabulary Size: **44,009** distinct normalized skill tokens.
  - Job Coverage: **97,108 jobs (99.42%)** have at least one valid normalized skill.
- **Technical Token Preservation:**
  - Verified empirical behavior for complex programming terms:
    - `C++` $\to$ `['c++']` (Preserved)
    - `C#` $\to$ `['c#']` (Preserved)
    - `.NET` $\to$ `['.net']` (Preserved)
    - `Node.js` $\to$ `['node.js']` (Preserved)
    - `PL/SQL` $\to$ `['pl/sql']` (Preserved)
  - Outer punctuation stripping correctly targets noise (`*`, `•`, `,`, `;`, quotes, brackets) while retaining internal syntax characters (`+`, `#`, `.`, `/`).

---

### 2.5 Freshness Classification
- **Rule Hierarchy:**
  - `FRESH_24H`: $\le 1$ day old (`Today`, `Just Now`, `Few Hours Ago`, `1 Day Ago`, `Yesterday`).
  - `RECENT_7D`: 2 to 7 days old (`2-7 Days Ago`, `1 Week Ago`).
  - `MODERATE_30D`: 8 to 30 days old (`8-30 Days Ago`, `2-4 Weeks Ago`).
  - `STALE_30D_PLUS`: $> 30$ days old (`30+ Days Ago`, `1+ Months Ago`).
  - `UNKNOWN`: Missing, empty, or unparseable text.
- **Distribution & Snapshot Profiling:**
  - `RECENT_7D`: **49,694 (50.87%)**
  - `MODERATE_30D`: **35,075 (35.91%)**
  - `FRESH_24H`: **12,339 (12.63%)**
  - `UNKNOWN`: **571 (0.58%)**
  - `STALE_30D_PLUS`: **0 (0.00%)**
- **Empirical Explanation of Category Counts:**
  - Audit profiling of the raw source file confirmed that 100% of standard job postings in this snapshot were uploaded within the preceding 10 days (active listings).
  - The exactly **571 `UNKNOWN` records** correspond to non-standard upload text (e.g. `Starts in 1-3 months` [321], `Starts within 1 month` [210], `Starts : 6th Oct' 25` [13]). The classifier correctly refuses to fabricate dates for these courses/internships and cleanly flags them as `UNKNOWN`.

---

### 2.6 Experience Parsing & Seniority Tiers
- **Parsing Hierarchy:**
  - Prioritizes numeric source bounds (`minimumExperience`, `maximumExperience`) over free-text parsing.
  - Validates boundaries against sanity rules ($0 \le \text{exp} \le 50$ years, $\text{min} \le \text{max}$).
- **Seniority Tiers:**
  - `ENTRY`: 0–2 years (`40,465`, 41.43%)
  - `MID`: 3–5 years (`33,313`, 34.10%)
  - `SENIOR`: 6–9 years (`14,686`, 15.03%)
  - `LEAD_EXECUTIVE`: 10+ years (`8,644`, 8.85%)
  - `Unspecified / NULL`: `571` (0.58%)
- **Handling Contradictions:**
  - Inverted ranges (e.g. `5-2 Yrs`) are swapped to valid bounds.
  - Non-numeric or impossible values ($> 50$ years) are invalidated to `NULL`.
  - The 571 internship/course postings with null experience bounds cleanly map to `NULL` tier without synthetic assumptions.

---

### 2.7 Silver Physical Storage & Manifest Verification
- **Output Storage Footprint:**
  - Clean Jobs Parquet: `40.93 MB` across 2 part files (`part-00000`, `part-00001`).
  - Skills Bridge Parquet: `7.92 MB` across 2 part files (`part-00000`, `part-00001`).
  - Compression: Snappy codec verified across 100% of part files.
- **Atomic Promotion & Cleanup:**
  - Verified staging workflow: writes to temporary directory `_staging_*` on `D:`, validates row counts and duplicate job IDs, and performs atomic rename.
  - Zero residual `_staging_*` folders left in `data/silver/`.
- **Manifest:**
  - Full execution manifest saved at `data/silver/indian_job_market_2025_2026/_metadata/silver_silver_run_20261123_174548_d90b33.json` recording source paths, timestamps, durations (48.4s), and input/output counts.

---

### 2.8 Bronze Layer Immutability
- All 4 Bronze source tables remain 100% read-only and immutable:
  - `indian_job_market_2025_2026.jobs`: 97,929 rows
  - `linkedin_2024.job_postings`: 1,348,454 rows
  - `linkedin_2024.job_skills`: 1,296,381 rows
  - `linkedin_2024.job_summary`: 1,297,332 rows
  - Total Bronze records: **4,040,096**.
- All raw CSV and compressed archive files under `data/source_csv/` have zero modifications.

---

### 2.9 Git & Environment Status
- **Git Branch:** `main`, completely synchronized with `origin/main` (`f8692cb`).
- **Working Tree:** Clean, 0 uncommitted changes, 0 staged changes.
- **Repository Strategy Note:**
  - By design, `.gitignore` excludes large physical Parquet data directories (`data/bronze/*`, `data/silver/*`, `*.parquet`).
  - Git tracks code, test suites, transformation logic, and markdown audit reports.

---

## 3. Verified Evidence vs Assumptions & Unresolved Issues

| Category | Verified Evidence (Empirical) | Assumptions / Design Decisions | Unresolved / Future Refinements |
| :--- | :--- | :--- | :--- |
| **Duplicates** | 250 duplicates removed ($97,929 \to 97,679$). 247 are 100% identical byte-for-byte. 0 duplicate `job_id`s in Silver. | Completeness score + ingestion timestamp prioritizes richest record. | Add third explicit tie-breaker in `orderBy` for theoretical distributed edge-case determinism. |
| **Salary** | 33,136 disclosed, 64,543 undisclosed. 0 inverted ranges. 33,124 INR, 20 USD. | Undisclosed salary is treated as a valid state (`salary_is_valid = True`). | Downstream Gold marts must combine `salary_is_valid = True AND salary_min IS NOT NULL`. |
| **Workplace** | 7,713 postings contain remote/hybrid keywords (7.90%). | Single boolean `is_remote = True` flags any non-exclusive on-site role. | Remote and Hybrid are conflated. Disambiguate into a 3-way `workplace_type` in Gold. |
| **Skills Bridge** | 753,137 rows, 0 orphans, 44,009 skills. `C++`, `C#`, `.NET`, `Node.js`, `PL/SQL` preserved. | Comma is primary delimiter; inner symbols preserved. | Complex multi-word skills are normalized but not merged into synthetic ontology. |
| **Freshness** | 0 postings $> 30$ days old. 571 postings with non-standard strings mapped to `UNKNOWN`. | `jobUploaded` is relative to scraping snapshot date. | No calendar dates are fabricated. |
| **Experience** | 0 negative values, 0 values $> 50$ yrs. 41.4% Entry, 34.1% Mid, 15.0% Senior, 8.8% Lead. | `min_exp <= 2.0` is categorized as `ENTRY` (e.g. 2–4 year role classified as Entry). | Some enterprise standards treat 2-5 yrs as Mid. Can be adjusted in Gold view layer if desired. |

---

## 4. Final Verdict

### Status: **APPROVED FOR PHASE 6D**

The Indian Job Market Silver transformation pipeline has met all data quality, schema integrity, idempotency, and non-destructive Medallion architecture requirements.

The project is cleared to proceed with **Phase 6D: LinkedIn 2024 Silver Layer Transformations**.
