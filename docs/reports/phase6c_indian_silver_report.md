# DemandLens — Phase 6C: Indian Job Market Silver Transformation Report

**Date:** November 23, 2026  
**Pipeline:** Indian Job Market Dataset 2025–2026 Silver Transformation  
**Status:** COMPLETE & AUDITED  
**Execution Run ID:** `silver_run_20261123_174548_d90b33`  

---

## 1. Executive Summary

Phase 6C established and executed the production Silver transformation pipeline for the **Indian Job Market Dataset 2025–2026**. The pipeline ingests the raw Bronze table (`97,929` records), performs rigorous deterministic deduplication, cleans text, normalizes skills, parses compensation and experience, classifies posting freshness, and emits two analytics-ready, Snappy-compressed Parquet datasets:

1. **Clean Silver Jobs Dimension:** `data/silver/indian_job_market_2025_2026/jobs`  
   - Output rows: **97,679** (1 row per unique job posting)  
   - Part files: 2 files (**40.93 MB**, Snappy compression)
2. **Normalized Skills Bridge Table:** `data/silver/indian_job_market_2025_2026/job_skills_bridge`  
   - Output rows: **753,137** distinct `(job_id, skill)` pairs  
   - Part files: 2 files (**7.92 MB**, Snappy compression)

All four underlying Bronze tables remain **100% immutable and intact**. The full automated regression suite (**58 tests**) passed with **100% success rate**.

---

## 2. Ingestion & Transformation Metrics

| Metric | Bronze Input | Silver Output (Jobs) | Silver Bridge (Skills) | Variance / Delta |
| :--- | :--- | :--- | :--- | :--- |
| **Record Count** | 97,929 | 97,679 | 753,137 | -250 duplicate rows removed |
| **Unique Job IDs** | 97,679 | 97,679 | 97,108 (with skills) | 0 lost identifiers |
| **Parquet Files** | 2 | 2 | 2 | Snappy compressed |
| **Storage Size** | 38.34 MB | 40.93 MB | 7.92 MB | Net +10.51 MB |
| **Execution Time**| — | **48.4 seconds** | (combined run) | — |
| **Referential Integrity**| — | Primary Key | Foreign Key | **0 orphan records** |

---

## 3. Duplicate Investigation & Resolution

### Findings from Bronze Inspection
Profiling the raw Bronze dataset revealed:
- Total rows: `97,929`
- Distinct `jobId`: `97,679`
- Redundant duplicate rows: `250`
- **100% Exact Row Duplicates:** `247` rows were byte-for-byte identical across all 21 Bronze columns (including `_source_id` and metadata).
- **Core Attribute Duplicates:** `248` rows were identical across `(jobId, title, companyName, location)`.
- Only `2` duplicates showed slight variations in minor non-identifying attributes.

### Deterministic Deduplication Strategy
To guarantee reproducible and lossless selection:
1. Computed a row-level **completeness score** summing non-null values across all columns.
2. Formed a window partitioned by `jobId` ordered by `completeness_score DESC` and `_ingestion_timestamp DESC`.
3. Filtered for `row_number() == 1`.
4. Result: Exactly 250 redundant duplicates eliminated; exactly 97,679 unique job postings retained.

---

## 4. Column Transformation & Feature Engineering

### A. Text & Location Cleaning
- **`title_clean` & `company_name_clean`:** Whitespace stripped, repeated spaces collapsed, casing preserved for display.
- **`location_clean`:** Normalizes Indian cities and regions; cleans punctuation noise.
- **`is_remote`:** Binary boolean flag identifying remote/hybrid roles. Found **7,713 remote/hybrid jobs (7.90%)**.
- **`country`:** Explicitly set to `'India'` across the dataset for cross-source standardization with LinkedIn data.

### B. Salary Standardization
Compensation in the raw dataset is primarily expressed in Indian Rupees (LPA / monthly) or undisclosed (e.g. `0`, `Not Disclosed`, or missing):
- **Disclosed numeric compensation:** **33,136 postings (33.92%)**
- **Undisclosed compensation:** **64,543 postings (66.08%)** mapped to explicit `NULL`.
- **Validation:** `salary_is_valid = True` for 100% of disclosed salaries; zero inverted ranges (`salary_min <= salary_max`).
- **Currency breakdown:**
  - `INR`: 33,124 records
  - `USD`: 20 records (US multinational remote positions)
  - `None` (undisclosed): 64,535 records
- **Periodicity:** Standardized into `ANNUAL` and `MONTHLY` tiers.

### C. Experience Parsing & Seniority Tiers
Parsed numeric experience requirements (`experience_min_years`, `experience_max_years`) and categorized into standardized tiers:
- **ENTRY (0–2 years):** `40,465` (41.43%)
- **MID (3–5 years):** `33,313` (34.10%)
- **SENIOR (6–10 years):** `14,686` (15.03%)
- **LEAD / EXECUTIVE (11+ years):** `8,644` (8.85%)
- **Unspecified / NULL:** `571` (0.58%)

### D. Freshness Classification
Parsed relative posting timestamps (`jobUploaded`) into actionable recency tiers:
- **`RECENT_7D` (2–7 days old):** `49,694` (50.87%)
- **`MODERATE_30D` (8–30 days old):** `35,075` (35.91%)
- **`FRESH_24H` (< 24 hours old):** `12,339` (12.63%)
- **`UNKNOWN` / Null:** `571` (0.58%)

---

## 5. Skills Bridge Table Architecture

To eliminate comma-delimited string parsing for downstream analytical queries, the Silver layer extracts and normalizes the `skills` column into a first-normal-form (1NF) bridge table:

- **Destination:** `data/silver/indian_job_market_2025_2026/job_skills_bridge`
- **Total Bridge Rows:** **753,137** distinct `(job_id, skill)` pairs.
- **Coverage:** **97,108 jobs (99.42%)** have at least one valid normalized skill.
- **Vocabulary Size:** **44,009** distinct normalized skill tokens.
- **Referential Integrity:** **0 orphan records** (`left_anti` join against `jobs.job_id` returned count = 0).

### Top 10 Normalized Skills in Indian Market
| Rank | Skill | Job Postings Count | Prevalence (%) |
| :---: | :--- | :--- | :--- |
| 1 | `sales` | 9,178 | 9.40% |
| 2 | `python` | 5,846 | 5.98% |
| 3 | `project management`| 5,544 | 5.68% |
| 4 | `customer service` | 5,122 | 5.24% |
| 5 | `sap` | 5,043 | 5.16% |
| 6 | `management` | 4,848 | 4.96% |
| 7 | `css` | 4,186 | 4.29% |
| 8 | `java` | 4,088 | 4.19% |
| 9 | `sql` | 3,983 | 4.08% |
| 10 | `business development`| 3,738 | 3.83% |

---

## 6. Physical Storage & Manifest Verification

### Part Files and Compression
```
data/silver/indian_job_market_2025_2026/
├── _metadata/
│   └── silver_silver_run_20261123_174548_d90b33.json
├── jobs/
│   ├── part-00000-silver_run_20261123_174548_d90b33.snappy.parquet (20.46 MB)
│   └── part-00001-silver_run_20261123_174548_d90b33.snappy.parquet (20.47 MB)
└── job_skills_bridge/
    ├── part-00000-silver_run_20261123_174548_d90b33.snappy.parquet (3.96 MB)
    └── part-00001-silver_run_20261123_174548_d90b33.snappy.parquet (3.96 MB)
```
- **Codec:** Snappy across all 4 Parquet files.
- **Staging Cleanup:** Zero residual `_staging_*` directories.
- **Manifest:** Verified json containing record counts, partition counts, execution timestamps, source bronze path, and column counts.

---

## 7. Bronze Layer Immutability Audit

Verification confirmed that all Bronze datasets remain byte-intact with zero modifications:

| Bronze Dataset | Expected Rows | Verified Rows | Audit Status |
| :--- | :--- | :--- | :--- |
| `indian_job_market_2025_2026.jobs` | 97,929 | 97,929 | PASS (100% Match) |
| `linkedin_2024.job_postings` | 1,348,454 | 1,348,454 | PASS (100% Match) |
| `linkedin_2024.job_skills` | 1,296,381 | 1,296,381 | PASS (100% Match) |
| `linkedin_2024.job_summary` | 1,297,332 | 1,297,332 | PASS (100% Match) |
| **Total Bronze Records** | **4,040,096** | **4,040,096** | **PASS** |

- **D: Drive Available Space:** `211.13 GB`

---

## 8. Test Suite & Verification Results

The automated pytest test suite was executed against all test modules:
```
tests/test_bronze_ingestion.py .................................. [PASSED]
tests/test_dataset_inspector.py ................................. [PASSED]
tests/test_foundation.py ........................................ [PASSED]
tests/test_hybrid_parquet_writer.py ............................. [PASSED]
tests/test_indian_silver_transformation.py ...................... [PASSED]
tests/test_silver_cleaning_rules.py ............................. [PASSED]
================= 58 passed in 185.33s (0:03:05) =================
```

---

## 9. Conclusion & Readiness

Phase 6C has achieved all design criteria specified in `docs/architecture/silver_layer_design.md`:
- Deterministic deduplication removed 250 redundant records without data loss.
- Standardized text, salary, experience, and freshness attributes were populated.
- A 1NF bridge table with 753,137 rows and 100% referential integrity was materialized.
- All Bronze tables remain untouched.
- The pipeline is fully validated and ready for Phase 6D (LinkedIn Silver Transformations).
