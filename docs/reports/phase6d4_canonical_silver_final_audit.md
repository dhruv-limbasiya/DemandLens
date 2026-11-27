# DemandLens — Phase 6D.4 Canonical Silver Final Read-Only Audit

**Audit Date:** `2026-11-26`  
**Audited Run ID:** `canonical_run_20261126_063239_5fa26a`  
**Execution Runner:** [`scripts/run_canonical_silver_transformation.py`](file:///d:/DemandLens/scripts/run_canonical_silver_transformation.py)  
**Transformer Engine:** [`src/demandlens/silver/canonical_transformer.py`](file:///d:/DemandLens/src/demandlens/silver/canonical_transformer.py)  
**Audit Mode:** STRICT READ-ONLY FORENSIC AUDIT  
**Audit Lead:** Antigravity AI Forensic Inspector  

---

## 1. Scope & Evidence Inspected

This audit was conducted strictly in **read-only mode** on Drive `D:`. No transformations were rerun, no files were renamed or deleted, no source code was altered, and no cloud/AWS resources were touched.

### Canonical Physical Targets Audited:
1. `data/silver/canonical/jobs` (8 Parquet partition files)
2. `data/silver/canonical/job_skills_bridge` (15 Parquet partition files)
3. `data/silver/canonical/job_text` (22 Parquet partition files)
4. `data/silver/canonical/_metadata` (2 JSON execution manifests)

### Source Silver Datasets Audited:
1. `data/silver/linkedin_2024/job_postings` (4 Parquet partition files)
2. `data/silver/linkedin_2024/job_skills_bridge` (8 Parquet partition files)
3. `data/silver/linkedin_2024/job_summary` (22 Parquet partition files)
4. `data/silver/indian_job_market_2025_2026/jobs` (2 Parquet partition files)
5. `data/silver/indian_job_market_2025_2026/job_skills_bridge` (2 Parquet partition files)

### Documentation & Test Evidence Inspected:
- Architecture Design: [`docs/architecture/canonical_silver_integration_design.md`](file:///d:/DemandLens/docs/architecture/canonical_silver_integration_design.md)
- Preflight Report: [`docs/reports/phase6d4_silver_integration_preflight_report.md`](file:///d:/DemandLens/docs/reports/phase6d4_silver_integration_preflight_report.md)
- Execution Report: [`docs/reports/phase6d4_canonical_silver_execution_report.md`](file:///d:/DemandLens/docs/reports/phase6d4_canonical_silver_execution_report.md)
- Execution Task Logs: Task 1600 (first attempt, aborted at Stage 4) and Task 1640 (final successful promotion).
- Full Regression Test Log: Task 1651 (62 passed tests).

---

## 2. Canonical Jobs Audit

### 2.1 File Inventory & Storage Metrics
The directory [`data/silver/canonical/jobs`](file:///d:/DemandLens/data/silver/canonical/jobs) exists, is fully readable, and contains 8 snappy-compressed Parquet files:

| File Name | Parquet Rows | File Size (Bytes) | Size (MB) | Format Version | Compression | Readability |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `part-00000.parquet` | 198,702 | 17,876,432 | 17.05 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00001.parquet` | 168,731 | 16,527,105 | 15.76 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00002.parquet` | 168,695 | 16,514,642 | 15.75 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00003.parquet` | 181,497 | 17,100,539 | 16.31 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00004.parquet` | 198,444 | 17,932,156 | 17.10 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00005.parquet` | 168,551 | 16,536,290 | 15.77 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00006.parquet` | 193,325 | 17,688,574 | 16.87 MB | 2.6 | SNAPPY | VERIFIED |
| `part-00007.parquet` | 168,188 | 16,505,552 | 15.74 MB | 2.6 | SNAPPY | VERIFIED |
| **Total** | **1,446,133** | **136,681,290** | **130.35 MB** | **2.6** | **SNAPPY** | **100% OK** |

### 2.2 Row Count & Source Breakdown
- **Observed Row Count:** Exactly **1,446,133** rows.
- **Expected Row Count:** 1,348,454 (LinkedIn) + 97,679 (Indian) = **1,446,133** rows.
- **Discrepancy:** **0 rows** (100.0% exact match).
- **Source Breakdown:**
  - `linkedin_2024`: **1,348,454** rows (1,348,454 distinct `source_job_id` keys).
  - `indian_job_market_2025_2026`: **97,679** rows (97,679 distinct `source_job_id` keys).

### 2.3 Schema Conformance
The table implements all 28 canonical columns specified in the approved architecture design:
1. `canonical_job_id` (`string`, non-null)
2. `source_id` (`string`, non-null)
3. `source_job_id` (`string`, non-null)
4. `job_title_clean` (`string`, non-null)
5. `company_name_clean` (`string`, nullable)
6. `location_clean` (`string`, nullable)
7. `country_code` (`string`, non-null)
8. `workplace_type` (`string`, non-null)
9. `is_remote` (`boolean`, non-null)
10. `is_hybrid` (`boolean`, non-null)
11. `seniority_tier` (`string`, non-null)
12. `experience_min_years` (`double`, nullable)
13. `experience_max_years` (`double`, nullable)
14. `salary_min` (`double`, nullable)
15. `salary_max` (`double`, nullable)
16. `salary_midpoint` (`double`, nullable)
17. `salary_currency` (`string`, nullable)
18. `salary_period` (`string`, nullable)
19. `salary_disclosed` (`boolean`, non-null)
20. `posted_date` (`date`, nullable)
21. `freshness_category` (`string`, nullable)
22. `skill_count` (`integer`, non-null)
23. `has_skills` (`boolean`, non-null)
24. `has_description` (`boolean`, non-null)
25. `_source_file` (`string`, non-null)
26. `_ingestion_timestamp` (`timestamp`, nullable)
27. `_canonical_run_id` (`string`, non-null)
28. `_canonical_timestamp` (`timestamp`, non-null)

### 2.4 Data Semantics & Quality Checks
- **Canonical ID Uniqueness:** Total IDs = 1,446,133; Distinct IDs = 1,446,133; Null IDs = 0; Empty IDs = 0.
- **Salary Semantics:**
  - `linkedin_2024`: 0 rows have `salary_disclosed = True`. All 1,348,454 rows have `salary_min`, `salary_max`, `salary_midpoint`, `salary_currency`, and `salary_period` set to explicit `NULL`.
  - `indian_job_market_2025_2026`: Exactly 33,144 rows have `salary_disclosed = True`. 33,136 have non-null `salary_min`/`salary_max`. 64,252 rows without disclosed wage information are correctly mapped with `salary_disclosed = False`.
  - Currencies: Indian rows contain 97,659 INR and 20 USD (multinational postings). Periods: 33,169 ANNUAL, 238 MONTHLY.
- **Experience Semantics:**
  - `linkedin_2024`: Exactly 0 rows have numeric `experience_min_years`/`experience_max_years` (all `NULL`, preventing synthetic imputation).
  - `indian_job_market_2025_2026`: 97,108 rows contain numeric experience bounds.
- **Workplace Classification:**
  - Clean 3-state separation:
    - LinkedIn: `ON_SITE` = 1,337,633 (`is_remote=False, is_hybrid=False`), `REMOTE` = 4,259 (`is_remote=True, is_hybrid=False`), `HYBRID` = 6,562 (`is_remote=False, is_hybrid=True`).
    - Indian: `ON_SITE` = 89,966 (`is_remote=False, is_hybrid=False`), `REMOTE` = 7,713 (`is_remote=True, is_hybrid=False`).
  - Zero hybrid rows are falsely marked `is_remote = True`.
- **Temporal & Freshness Representation:**
  - `linkedin_2024`: 1,348,454 rows retain calendar date `posted_date` (`first_seen_date`). `freshness_category` is `'UNSPECIFIED'`.
  - `indian_job_market_2025_2026`: All 97,679 rows have `posted_date = NULL` (no fabricated calendar dates from relative offsets). Relative tiers are strictly preserved in `freshness_category`: `RECENT_7D` (49,694), `MODERATE_30D` (35,075), `FRESH_24H` (12,339), `UNKNOWN` (571).

---

## 3. Canonical ID Audit

The canonical surrogate key is deterministically defined as:
$$\text{canonical\_job\_id} = \text{source\_id} \mathbin{\Vert} \text{"\#"} \mathbin{\Vert} \text{source\_job\_id}$$

Forensic investigation of all 1,446,133 IDs revealed:
- **Prefix Integrity:**
  - 1,348,454 IDs start with `linkedin_2024#`. Bad prefix count: **0**.
  - 97,679 IDs start with `indian_job_market_2025_2026#`. Bad prefix count: **0**.
- **Reconstruction Invariant:**
  - Recalculating `concat(source_id, '#', source_job_id)` across all rows produced **0 mismatches** against `canonical_job_id`.
- **Collision Safety:**
  - LinkedIn native keys are full HTTP URLs (e.g. `https://www.linkedin.com/jobs/view/...`).
  - Indian native keys are integer strings (e.g. `99001`).
  - Because source keys belong to disjoint syntactic domains and are further segregated by the fixed prefix, collision probability is mathematically **0.0%**.
- **Delimiter Ambiguity Analysis:**
  - The separator `#` is an ASCII punctuation character. The `source_id` values (`linkedin_2024` and `indian_job_market_2025_2026`) contain strictly alphanumeric characters and underscores, never `#`.
  - Even if a LinkedIn URL contains a URI fragment `#`, splitting on the first occurrence (`split("#", 1)`) unambiguously recovers `source_id` as the head and `source_job_id` as the tail.
- **Rerun Stability:**
  - The ID generation logic in `src/demandlens/silver/canonical_transformer.py` is a pure deterministic projection without pseudo-random seeds or auto-incrementing sequences. Rerunning against identical Bronze/Silver source files guarantees identical keys.

---

## 4. Canonical Skills Bridge Audit

### 4.1 Physical Inventory & Row Count
The table [`data/silver/canonical/job_skills_bridge`](file:///d:/DemandLens/data/silver/canonical/job_skills_bridge) exists, is fully readable, and contains 15 snappy-compressed Parquet files:
- **Total Parquet Rows:** Exactly **27,533,229** rows.
- **Total File Size:** **1,419.02 MB** (1.386 GiB; 1,487,952,973 bytes).
- **Compression:** Snappy across all 15 files.
- **Null Checks:** `canonical_job_id` nulls = **0**, `skill` nulls = **0**, empty skills = **0**.

### 4.2 Source Contribution Accounting
- `linkedin_2024`: **26,780,092** skill edges across 1,294,374 distinct jobs (2,759,800 distinct skill tokens).
- `indian_job_market_2025_2026`: **753,137** skill edges across 97,108 distinct jobs (44,009 distinct skill tokens).
- Sum: $26,780,092 + 753,137 = \mathbf{27,533,229}$ rows. Exact 100% balance.

### 4.3 Grain & Edge Uniqueness
- Compound Grain: `(canonical_job_id, skill)`.
- Forensic Grouping Query:
  ```sql
  SELECT canonical_job_id, skill, COUNT(*) FROM canonical_job_skills_bridge GROUP BY 1, 2 HAVING COUNT(*) > 1
  ```
- **Observed Duplicate Edges:** **0 duplicate pairs**. The bridge table conforms strictly to First Normal Form (1NF).

### 4.4 Technical Token Preservation
Forensic sampling verified that Phase 6B/6D token normalization preserved technical punctuation without truncation:

| Skill Token | LinkedIn Occurrences | Indian Occurrences | Total Canonical Occurrences |
| :--- | :---: | :---: | :---: |
| `python` | 24,953 | 5,846 | 30,799 |
| `sql` | 21,710 | 3,983 | 25,693 |
| `java` | 13,756 | 4,088 | 17,844 |
| `aws` | 12,547 | 1,884 | 14,431 |
| `kubernetes` | 6,648 | 1,509 | 8,157 |
| `c++` | 6,634 | 1,987 | 8,621 |
| `docker` | 6,198 | 743 | 6,941 |
| `c#` | 4,707 | 2,978 | 7,685 |
| `ci/cd` | 2,926 | 1,049 | 3,975 |
| `.net` | 2,335 | 807 | 3,142 |
| `node.js` | 2,269 | 515 | 2,784 |
| `pl/sql` | 869 | 18 | 887 |

---

## 5. Canonical Text Audit

### 5.1 Physical Inventory & Row Count
The table [`data/silver/canonical/job_text`](file:///d:/DemandLens/data/silver/canonical/job_text) exists, is fully readable, and contains 22 snappy-compressed Parquet files:
- **Total Parquet Rows:** Exactly **1,395,011** rows.
- **Total File Size:** **2,715.59 MB** (2.652 GiB; 2,847,500,132 bytes).
- **Compression:** Snappy across all 22 files.

### 5.2 Source Contribution Accounting
- `linkedin_2024`: **1,297,332** text rows (clean summaries projected directly from Phase 6D.3).
- `indian_job_market_2025_2026`: **97,679** text rows (descriptions cleaned from raw `jobDescription`).
- Sum: $1,297,332 + 97,679 = \mathbf{1,395,011}$ rows. Exact 100% balance.

### 5.3 1:1 Grain & Text Quality Metrics
- **Uniqueness Check:** Distinct `canonical_job_id` count = **1,395,011** ($100.0\%$ 1:1 unique grain).
- **Null Descriptions:** **0**.
- **Empty Descriptions:** **14** (0.001% of records, corresponding to source postings with blank/whitespace-only input text).
- **Text Length Metrics:**
  - Overall: Character count ranges from 0 to 23,741 (mean: 3,586.9). Word count ranges from 0 to 3,448 (mean: 516.8).
  - LinkedIn: Mean length = 3,817.6 characters, 550.4 words.
  - Indian: Mean length = 523.2 characters, 71.2 words.
- **Structural Integrity:** 73,711 Indian descriptions and 211 LinkedIn summaries contain explicit double-newline paragraph breaks (`\n\n`), confirming that HTML paragraph tags (`<p>`, `<div>`, `<br>`) were translated into clean formatting rather than collapsed into continuous run-on sentences.

---

## 6. Referential Integrity Deep Audit

A full outer/anti-join audit was executed across all three canonical tables:

| Source Entity | Foreign Key Column | Target Entity | Primary Key Column | Orphan Count | Integrity Status |
| :--- | :--- | :--- | :--- | :---: | :---: |
| `canonical_job_skills_bridge` | `canonical_job_id` | `canonical_jobs` | `canonical_job_id` | **0** | **100% VALID** |
| `canonical_job_text` | `canonical_job_id` | `canonical_jobs` | `canonical_job_id` | **0** | **100% VALID** |

### Flag Consistency Verification:
1. `has_skills = True` vs Skills Bridge Presence:
   - Jobs flagged `has_skills = True` missing from skills bridge: **0**.
   - Jobs present in skills bridge flagged `has_skills = False`: **0**.
2. `has_description = True` vs Text Table Presence:
   - Jobs flagged `has_description = True` missing from text table: **0**.
   - Jobs present in text table flagged `has_description = False`: **0**.

Every foreign key edge resolves to a valid parent record, and every boolean indicator in the primary jobs table accurately reflects dimension membership.

---

## 7. Overwrite, Staging, & Promotion Investigation

A forensic audit of runner executions and disk events was conducted to determine whether data loss or unsafe overwrite occurred:

### 7.1 Historical Run Sequence
1. **Initial Run Attempt (`canonical_run_20261126_062128_dded70`):**
   - Transformed and wrote:
     - `_staging_jobs_72958146`: 1,446,133 rows (8 files).
     - `_staging_skills_72958146`: 27,533,229 rows (16 files).
     - `_staging_text_72958146`: 1,395,011 rows (22 files).
   - In Stage 4 (referential integrity check), Spark's Catalyst optimizer selected `BroadcastExchangeExec` to broadcast `df_stg_jobs` (1.45M rows). Local block manager memory limits caused `TaskResultLost (result lost from block manager)` on task 3 in stage 36.0.
   - **Cleanup & Rollback Behavior:** The exception handler in `CanonicalSilverPipeline.run` immediately executed `shutil.rmtree(p, ignore_errors=True)` on all three staging directories. Because promotion occurs strictly in Stage 5, the final canonical destinations were **never created or modified**. Zero partial data was published.
2. **Configuration Remediation:**
   - Disabled automatic broadcast join: `spark.sql.autoBroadcastJoinThreshold = -1`.
   - Increased shuffle partitions: `spark.sql.shuffle.partitions = 16`.
   - Increased driver max result size: `spark.driver.maxResultSize = 4g`.
3. **Successful Execution (`canonical_run_20261126_063239_5fa26a`):**
   - Staged to fresh UUID directories: `_staging_jobs_36e3c5ce`, `_staging_skills_36e3c5ce`, `_staging_text_36e3c5ce`.
   - Pre-promotion row count checks passed ($1.45M$, $27.53M$, $1.40M$).
   - Stage 4 non-broadcast SortMerge/Shuffle joins validated 0 orphans.
   - Stage 5 atomically promoted each directory to `data/silver/canonical/{jobs, job_skills_bridge, job_text}` via `shutil.move()`.
   - Manifest committed to `data/silver/canonical/_metadata/canonical_integration_manifest.json`.

### 7.2 Overwrite Safety Analysis
- When `--overwrite` was passed, the target directories did not exist prior to run 1640 because run 1600 aborted before promotion.
- Staging directory audit: **0 lingering `_staging_*` directories** were detected on disk.
- **Rollback Invariant:** If a failure occurs during staging or validation, staging directories are purged and existing canonical tables remain untouched. However, in Stage 5, `shutil.rmtree(dest)` precedes `shutil.move(stg, dest)`. While atomic on Windows NTFS within the same filesystem volume, a catastrophic power failure between `rmtree` and `move` would leave `stg` intact but `dest` absent until rerun.

---

## 8. Source Immutability Audit

### 8.1 Physical Source Baseline Comparison
The 5 source Silver tables were inspected against historical audit baselines:

| Source Dataset | Current Files | Current Rows | Current Size | Expected Baseline Rows | Row Delta | Immutability Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `data/silver/linkedin_2024/job_postings` | 4 | 1,348,454 | 180.00 MB | 1,348,454 | 0 | **UNMODIFIED** |
| `data/silver/linkedin_2024/job_skills_bridge`| 8 | 26,780,092 | 1,389.62 MB | 26,780,092 | 0 | **UNMODIFIED** |
| `data/silver/linkedin_2024/job_summary` | 22 | 1,297,332 | 2,687.12 MB | 1,297,332 | 0 | **UNMODIFIED** |
| `data/silver/indian_job_market_2025_2026/jobs`| 2 | 97,679 | 40.93 MB | 97,679 | 0 | **UNMODIFIED** |
| `data/silver/indian_job_market_2025_2026/job_skills_bridge`| 2 | 753,137 | 7.92 MB | 753,137 | 0 | **UNMODIFIED** |

### 8.2 Forensic Audit on Cryptographic Hashes
- An inspection of all JSON metadata files in:
  - `data/silver/linkedin_2024/_metadata/` (5 files)
  - `data/silver/indian_job_market_2025_2026/_metadata/` (1 file)
- **Forensic Finding:** None of the pre-existing source manifests recorded SHA-256 or MD5 cryptographic checksums of the individual Parquet partition files. Historical manifests recorded row counts, file paths, partition counts, and schemas, but omitted file-level content hashes.
- **Mandatory Audit Declaration:** Because no cryptographic hash baseline exists in the repository for source Silver Parquet files, **byte-level immutability cannot be independently established**. However, file count, file naming, byte size, schema, and row count metrics match historical records with 100% fidelity.

---

## 9. Testing Evidence

The full regression test suite was inspected from execution task 1651:
- Command: `pytest tests/test_canonical_silver_integration.py tests/test_linkedin_job_summary_silver.py tests/test_linkedin_job_skills_silver.py tests/test_silver_cleaning_rules.py -v`
- Duration: 84.90s.
- Total Tests: **62 passed, 0 failed, 0 skipped**.

### Test Suite Breakdown:
1. `tests/test_canonical_silver_integration.py` (7 tests):
   - `test_canonical_jobs_schema_structure`: PASSED
   - `test_map_linkedin_postings_to_canonical`: PASSED
   - `test_map_indian_jobs_to_canonical`: PASSED
   - `test_build_canonical_jobs_union`: PASSED
   - `test_build_canonical_skills_bridge`: PASSED
   - `test_build_canonical_job_text`: PASSED
   - `test_referential_integrity_no_orphans`: PASSED
2. `tests/test_linkedin_job_summary_silver.py` (10 tests): PASSED (cleansing rules, entity decoding, paragraph preservation, technical tokens).
3. `tests/test_linkedin_job_skills_silver.py` (6 tests): PASSED (technical token preservation, noise stripping, 1NF bridge grain).
4. `tests/test_silver_cleaning_rules.py` (39 tests): PASSED (text cleaning, skill normalization, location parsing, salary parsing, freshness classification).

---

## 10. Resource & Execution Audit

| Metric | Recorded Value | Evidence Source | Verification Status |
| :--- | :--- | :--- | :--- |
| **Pipeline Duration** | 745.07 seconds (~12.4 min) | Run Manifest & Task Log 1640 | **VERIFIED** |
| **Driver Memory Allocation** | 6 GB | Runner CLI configuration | **VERIFIED** |
| **Driver Max Result Size** | 4 GB | Runner CLI configuration | **VERIFIED** |
| **Shuffle Partitions** | 16 | Runner CLI configuration | **VERIFIED** |
| **Local Parallelism** | 4 Cores (`local[4]`) | Spark Master Configuration | **VERIFIED** |
| **Peak JVM Direct Memory Usage** | 4 GB configured | JVM `-XX:MaxDirectMemorySize=4g` | **UNVERIFIED** (Live OS telemetry not captured continuously) |
| **Staging Root Directory** | `D:\DemandLens\data\silver\canonical\_staging_*` | Pipeline code & task logs | **VERIFIED** |
| **Drive D: Free Disk Space** | 202.74 GiB (81.1% available) | OS filesystem call `shutil.disk_usage` | **VERIFIED** |
| **Canonical Jobs Footprint** | 130.35 MB (8 files) | Parquet filesystem metadata | **VERIFIED** |
| **Canonical Skills Footprint** | 1,419.02 MB (15 files) | Parquet filesystem metadata | **VERIFIED** |
| **Canonical Text Footprint** | 2,715.59 MB (22 files) | Parquet filesystem metadata | **VERIFIED** |

---

## 11. Known Architectural Limitations

1. **Geographic & Economic Incompatibility:** LinkedIn (Western 2024) has 0% salary disclosure and categorical seniority, while Indian Market (2025–2026) has explicit INR salaries and quantitative experience years. Downstream Gold models and BI reports must enforce mandatory geographic filtering (`country_code`) to prevent erroneous cross-market aggregations.
2. **Absence of Pre-Existing Hash Baselines:** Source Silver manifests lacked SHA-256 file hashes, precluding mathematical proof of byte-level source immutability (although row counts, byte sizes, and schemas match 100%).
3. **Sparse Narrative Text:** 14 records in `canonical_job_text` contain 0 characters, reflecting raw postings where descriptions were omitted by the original job poster.

---

## 12. Final Audit Verdict

Based on forensic evidence, physical file verification, row count recalculations, referential integrity proofs, and test suite execution:

### **VERDICT: APPROVED WITH LIMITATIONS**

### Justification:
1. **Mathematical Exactness:** All 1,446,133 jobs, 27,533,229 skills, and 1,395,011 text records match expected counts with zero discrepancy.
2. **Zero Referential Integrity Violations:** Both skill bridge and text dimension exhibit 0 orphan records.
3. **Decoupled Architecture Target Exceeded:** Core analytical table size is 130.35 MB, beating the 350 MB target ceiling by 62.8%.
4. **Limitations Acknowledged:** Approval carries the limitation that byte-level immutability cannot be verified cryptographically due to missing historical hash baselines, and downstream Gold transformations must respect dataset-specific semantics (compensation and seniority).

---

*Phase 6D.4 is formally concluded. In accordance with strict audit instructions, all operations have halted. No Phase 7 or Gold transformations have been initiated.*
