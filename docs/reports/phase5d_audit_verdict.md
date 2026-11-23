# DemandLens: Phase 5D.1 — Final Bronze Layer Audit & Verdict Report

**Audit Date:** 2026-11-23  
**Auditor:** Antigravity (Read-Only Independent Comprehensive Audit)  
**Scope:** Phase 5D Ingestion (`jobs` table) & Complete Medallion Bronze Layer  
**Engine:** Hybrid Apache PySpark 4.3.0 + PyArrow 26.0.1  
**Project Root:** `D:\DemandLens`  

---

## 1. Executive Summary

This report documents the final, strictly read-only audit of **Phase 5D: Indian Job Market Dataset Ingestion** and evaluates the **complete Medallion Bronze layer** across both ingested data sources (`linkedin_2024` and `indian_job_market_2025_2026`).

All four planned Bronze tables are fully ingested, partitioned, compressed, and validated on the `D:` drive. A total of **4,040,096 records** now reside in the Bronze layer across **26 Parquet part files**, with zero data loss, zero destructive transformations, and verified provenance metadata.

---

## 2. Ingestion Artifacts & Manifests Inspected

| Table Name | Source ID | Run ID | Status | Validation | Duration | Manifest Location |
|---|---|---|---|---|---|---|
| `job_postings` | `linkedin_2024` | `run_20261123_131540_a6f109` | `SUCCESS` | `PASSED` | 41.35s | [job_postings_run_20261123_131540_a6f109.json](file:///D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_postings_run_20261123_131540_a6f109.json) |
| `job_skills` | `linkedin_2024` | `run_20261123_133320_0a423c` | `SUCCESS` | `PASSED` | 99.82s | [job_skills_run_20261123_133320_0a423c.json](file:///D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_skills_run_20261123_133320_0a423c.json) |
| `job_summary` | `linkedin_2024` | `run_20261123_141043_d2b718` | `SUCCESS` | `PASSED` | 148.92s | [job_summary_run_20261123_141043_d2b718.json](file:///D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_summary_run_20261123_141043_d2b718.json) |
| `jobs` | `indian_job_market_2025_2026` | `run_20261123_143008_7ab187` | `SUCCESS` | `PASSED` | 14.50s | [jobs_run_20261123_143008_7ab187.json](file:///D:/DemandLens/data/bronze/indian_job_market_2025_2026/_metadata/jobs_run_20261123_143008_7ab187.json) |

All 4 manifest JSON files were verified on disk; each records status `SUCCESS`, validation status `PASSED`, and matches output directory row counts and columns exactly.

---

## 3. Phase 5D Specific Verification (Indian Job Market Dataset)

### 3.1 Row Count & File Verification
- **Source File:** `D:\DemandLens\data\source_csv\indian_job_market_2025_2026\extracted\indian-job-market-dataset-2025.csv` (83,363,503 bytes / 79.50 MB).
- **Source Row Count:** **97,929 rows**.
- **Bronze Row Count:** **97,929 rows** (100% exact match).
- **Parquet Part Files:** **2 files** (`part-00000.parquet`: 19.16 MB / 48,964 rows; `part-00001.parquet`: 19.17 MB / 48,965 rows).
- **Total Parquet Size:** **38.34 MB (40,197,404 bytes)**.
- **Compression Codec:** **Snappy** verified on all column chunks across both part files.

### 3.2 Schema & Provenance Preservation
The output contains **21 total columns**:
- **17 Original Columns (100% Preserved):**
  `title`, `jobId`, `currency`, `jobUploaded`, `companyName`, `tagsAndSkills`, `experience`, `salary`, `location`, `companyId`, `ReviewsCount`, `AggregateRating`, `jobDescription`, `minimumSalary`, `maximumSalary`, `minimumExperience`, `maximumExperience`.
- **4 Standard Provenance Columns:**
  `_source_id` (`indian_job_market_2025_2026`), `_source_file` (`indian-job-market-dataset-2025.csv`), `_ingestion_timestamp` (UTC microsecond), `_ingestion_run_id` (`run_20261123_143008_7ab187`).

### 3.3 Full-Record Equivalence Evidence
A bidirectional distributed set-difference check (`exceptAll`) across all 17 original columns and all 97,929 rows was executed in `verify_phase5d_jobs.py`:
- `Source exceptAll Bronze`: **0 rows difference**.
- `Bronze exceptAll Source`: **0 rows difference**.
- **Evidence Assessment:** Full-record equivalence is **empirically proven**. Zero records were modified, dropped, or corrupted.

### 3.4 Null-Count Parity
Null counts in Bronze match the raw CSV source profiling across all 21 columns:
- Primary key `jobId`: **0 nulls (0.00%)**.
- `tagsAndSkills`: **571 nulls (0.58%)**.
- `experience`: **2,105 nulls (2.15%)**.
- `ReviewsCount` & `AggregateRating`: **35,252 nulls (36.00%)**.
- Salary / Experience brackets: **571 nulls (0.58%)**.
- Metadata columns: **0 nulls (0.00%)**.

---

## 4. Complete Bronze Layer Verification

### 4.1 Master Table Inventory & Footprint

| Platform / Source | Table Name | Part Files | Bronze Rows | Output Size (MB) | Codec | Ingestion Run ID |
|---|---|---|---|---|---|---|
| `linkedin_2024` | `job_postings` | 4 | 1,348,454 | 125.78 MB | SNAPPY | `run_20261123_131540_a6f109` |
| `linkedin_2024` | `job_skills` | 4 | 1,296,381 | 320.02 MB | SNAPPY | `run_20261123_133320_0a423c` |
| `linkedin_2024` | `job_summary` | 16 | 1,297,332 | 2,682.73 MB | SNAPPY | `run_20261123_141043_d2b718` |
| `indian_job_market_2025_2026` | `jobs` | 2 | 97,929 | 38.34 MB | SNAPPY | `run_20261123_143008_7ab187` |
| **MEDALLION BRONZE TOTAL** | **4 Tables** | **26 Files** | **4,040,096** | **3,166.87 MB (~3.09 GiB)** | **SNAPPY** | **ALL VALIDATED** |

### 4.2 Cross-Dataset Isolation & Integrity
- All 3 previously ingested LinkedIn 2024 tables were audited and verified intact:
  - `job_postings`: 1,348,454 rows (no change).
  - `job_skills`: 1,296,381 rows (no change).
  - `job_summary`: 1,297,332 rows (no change).
- Neither LinkedIn data nor metadata was modified during Phase 5D execution.
- Staging and backup cleanup verified:
  - Zero leftover staging directories in `data/bronze/indian_job_market_2025_2026/`.
  - Zero leftover staging directories in `data/bronze/linkedin_2024/`.
  - Note: An empty test folder `data/bronze/_staging_test_p2` remains from Phase 3 writer unit testing; it contains 0 bytes and does not impact data integrity.

### 4.3 D: Drive Resource Audit
- **Current Available Free Space on D:** **211.18 GB**.
- Total Bronze Parquet storage consumed across all 4 tables: **~3.09 GB** (from 6.22 GB raw source files, providing ~49.0% compression reduction).
- Storage headroom is excellent (>60x).

---

## 5. Git & Workspace Audit

### 5.1 Modified Tracked Files (4 files, +153 / -6 lines):
- `scripts/run_bronze_ingestion.py`: Added `--table` CLI parameter and explicit D: temp directories.
- `src/demandlens/ingestion/bronze_ingestor.py`: Implemented partition-based hybrid PyArrow Parquet writer, atomic staging promotion, and exception safety.
- `src/demandlens/ingestion/dataset_inspector.py`: Configured `SPARK_LOCAL_DIRS` and `TEMP`/`TMP` on `D:`.
- `tests/test_bronze_ingestion.py`: Updated test fixtures for D: temp storage.

### 5.2 Untracked Files:
- Test files: `tests/test_hybrid_parquet_writer.py`.
- Validation scripts: `data/temp_validation/run_phase4_validation.py`, `verify_phase5a_postings.py`, `verify_phase5b_skills.py`, `preflight_5c.py`, `verify_phase5c_summary.py`, `profile_indian_source.py`, `verify_phase5d_jobs.py`.
- Reports: `docs/reports/*.md`.
- No datasets, no virtual environment binaries, and no large Parquet files are tracked by Git.

### 5.3 Git Head Status:
- Head commit remains `616ff36 ready for bronze layer`.
- Working tree has zero staged commits and zero pushes.

---

## 6. Audit Checklist Summary

| Verification Check | Target Requirement | Audit Finding | Verdict |
|---|---|---|---|
| **Indian Source & Bronze Count** | Exact match (97,929 rows) | 97,929 == 97,929 | ✅ PASS |
| **Full-Record Comparison** | Bidirectional distributed `exceptAll` | 0 differences across all 17 columns | ✅ PASS |
| **No Destructive Transformations** | Raw types and values preserved | Zero normalization/casting applied | ✅ PASS |
| **Schema & Metadata Columns** | 17 original + 4 metadata columns | All 21 columns present & populated | ✅ PASS |
| **Null-Count Parity** | 100% fidelity to raw source | Exactly matches raw source profiling | ✅ PASS |
| **Parquet File Count & Size** | 2 part files, ~38.3 MB | 2 files, 38.34 MB | ✅ PASS |
| **Snappy Compression** | Validated on all column chunks | 100% Snappy across all files | ✅ PASS |
| **Manifest Correspondence** | Matches disk output | `jobs_run_20261123_143008_7ab187.json` matches | ✅ PASS |
| **Staging Cleanup** | No leftover staging artifacts | 0 staging folders in source dirs | ✅ PASS |
| **LinkedIn Tables Intact** | 3 tables (3,942,167 rows) | 100% verified intact | ✅ PASS |
| **Total Bronze Records** | 4,040,096 across 4 tables | Exactly 4,040,096 rows | ✅ PASS |
| **Storage & Free Space** | Operations on D:, >200 GB free | 211.18 GB free on D: | ✅ PASS |
| **Git Discipline** | No commits, no secrets/data tracked | Clean untracked state | ✅ PASS |

---

# ✅ FINAL AUDIT VERDICT: APPROVED

The Medallion Bronze layer is **100% complete, fully verified, and architecturally sound**.  
The platform is **APPROVED** to proceed to **Phase 6: Silver Layer Standardisation, Deduplication & Transformation Planning**.
