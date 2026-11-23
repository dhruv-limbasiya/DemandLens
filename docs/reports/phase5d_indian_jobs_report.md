# DemandLens: Phase 5D — Indian Job Market Dataset Bronze Ingestion Report

**Report Date:** 2026-11-23 14:35:00 UTC  
**Target Table:** `jobs` (Source: `indian_job_market_2025_2026` / Indian Job Market Dataset 2025–2026)  
**Status:** **SUCCESS (Validation PASSED — 100% Full-Record Fidelity)**  
**Run ID:** `run_20261123_143008_7ab187`  

---

## 1. Executive Summary

This report documents the execution and post-ingestion verification of **Phase 5D: Bronze Ingestion of the Indian Job Market Dataset 2025–2026** (`indian-job-market-dataset-2025.csv`).

Ingestion was executed using the verified hybrid PySpark + PyArrow ingestion engine, streaming records partition-by-partition in bounded batches without materializing the complete dataset in memory or using `toPandas()`.

A **100% full-dataset record comparison** was conducted using distributed set difference (`exceptAll`) across all 17 original columns and all 97,929 rows, confirming zero data loss, zero record modification, and 100% schema and value fidelity.

All 4 Bronze tables across both core platforms (`linkedin_2024` and `indian_job_market_2025_2026`) are now fully ingested into the Medallion Bronze layer on the `D:` drive.

---

## 2. Ingestion Parameters & Metrics

| Metric | Measured Value |
|---|---|
| **Source File** | `D:\DemandLens\data\source_csv\indian_job_market_2025_2026\extracted\indian-job-market-dataset-2025.csv` |
| **Source File Size** | **83,363,503 bytes (79.50 MB)** |
| **Source Record Count** | **97,929 rows** |
| **Bronze Record Count** | **97,929 rows** (100% Match) |
| **Bronze Output Directory** | [D:/DemandLens/data/bronze/indian_job_market_2025_2026/jobs](file:///D:/DemandLens/data/bronze/indian_job_market_2025_2026/jobs) |
| **Ingestion Run ID** | `run_20261123_143008_7ab187` |
| **Target Partitions** | **2 partitions** (configured in `config/sources/indian_job_market_2025_2026.json`) |
| **Parquet Part Files Written** | **2 files** (`part-00000.parquet`, `part-00001.parquet`) |
| **Total Bronze Parquet Footprint** | **38.34 MB (40,197,404 bytes)** |
| **Compression Codec** | **Snappy** (Verified on all column chunks across both part files) |
| **Compression Space Savings** | **~51.78% reduction** from raw CSV |
| **Ingestion Duration** | **14.50 seconds** |
| **Throughput** | **~6,754 rows / second** |
| **D: Drive Free Space Before** | **211.22 GB** |
| **D: Drive Free Space After** | **211.18 GB** (~40 MB net consumed) |
| **Run Metadata Manifest** | [D:/DemandLens/data/bronze/indian_job_market_2025_2026/_metadata/jobs_run_20261123_143008_7ab187.json](file:///D:/DemandLens/data/bronze/indian_job_market_2025_2026/_metadata/jobs_run_20261123_143008_7ab187.json) |

---

## 3. Parquet Part Files Breakdown

| Part File Name | Size (MB) | Row Count | Row Groups | Compression |
|---|---|---|---|---|
| `part-00000.parquet` | 19.16 MB | 48,964 | 5 | SNAPPY |
| `part-00001.parquet` | 19.17 MB | 48,965 | 5 | SNAPPY |
| **Total** | **38.34 MB** | **97,929** | **10** | **SNAPPY** |

---

## 4. Post-Ingestion Validation Matrix

A distributed verification script was executed via Apache Spark and PyArrow ([verify_phase5d_jobs.py](file:///D:/DemandLens/data/temp_validation/verify_phase5d_jobs.py)):

### 4.1 Row Count Fidelity
* **Source CSV Rows:** `97,929`
* **Bronze Readback Rows:** `97,929`
* **Mismatch:** **0 (100% preservation)**

### 4.2 Schema & Provenance Columns (21 Columns Total)
* **Preserved Original Source Columns (17):**
  * `title` (`StringType`)
  * `jobId` (`StringType`) — Source primary key
  * `currency` (`StringType`)
  * `jobUploaded` (`StringType`)
  * `companyName` (`StringType`)
  * `tagsAndSkills` (`StringType`)
  * `experience` (`StringType`)
  * `salary` (`StringType`)
  * `location` (`StringType`)
  * `companyId` (`StringType`)
  * `ReviewsCount` (`StringType`)
  * `AggregateRating` (`StringType`)
  * `jobDescription` (`StringType`)
  * `minimumSalary` (`StringType`)
  * `maximumSalary` (`StringType`)
  * `minimumExperience` (`StringType`)
  * `maximumExperience` (`StringType`)
* **Standard Ingestion Provenance Columns (4):**
  * `_source_id`: Fixed literal `"indian_job_market_2025_2026"` across all records
  * `_source_file`: Fixed literal `"indian-job-market-dataset-2025.csv"` across all records
  * `_ingestion_timestamp`: UTC microsecond timestamp
  * `_ingestion_run_id`: Run identifier `"run_20261123_143008_7ab187"`

### 4.3 Null-Value Preservation Audit
Null counts in Bronze match the raw source CSV profiling across all 21 columns with 100% fidelity:

| Column Name | Source Nulls | Bronze Nulls | Null % | Match Status |
|---|---|---|---|---|
| `title` | 0 | 0 | 0.00% | ✅ MATCH |
| `jobId` | 0 | 0 | 0.00% | ✅ MATCH |
| `currency` | 0 | 0 | 0.00% | ✅ MATCH |
| `jobUploaded` | 0 | 0 | 0.00% | ✅ MATCH |
| `companyName` | 0 | 0 | 0.00% | ✅ MATCH |
| `tagsAndSkills` | 571 | 571 | 0.58% | ✅ MATCH |
| `experience` | 2,105 | 2,105 | 2.15% | ✅ MATCH |
| `salary` | 0 | 0 | 0.00% | ✅ MATCH |
| `location` | 0 | 0 | 0.00% | ✅ MATCH |
| `companyId` | 0 | 0 | 0.00% | ✅ MATCH |
| `ReviewsCount` | 35,252 | 35,252 | 36.00% | ✅ MATCH |
| `AggregateRating` | 35,252 | 35,252 | 36.00% | ✅ MATCH |
| `jobDescription` | 0 | 0 | 0.00% | ✅ MATCH |
| `minimumSalary` | 571 | 571 | 0.58% | ✅ MATCH |
| `maximumSalary` | 571 | 571 | 0.58% | ✅ MATCH |
| `minimumExperience` | 571 | 571 | 0.58% | ✅ MATCH |
| `maximumExperience` | 571 | 571 | 0.58% | ✅ MATCH |
| `_source_id` | N/A | 0 | 0.00% | ✅ MATCH |
| `_source_file` | N/A | 0 | 0.00% | ✅ MATCH |
| `_ingestion_timestamp` | N/A | 0 | 0.00% | ✅ MATCH |
| `_ingestion_run_id` | N/A | 0 | 0.00% | ✅ MATCH |

### 4.4 Full-Record Comparison Evidence (100% Completed)
Unlike large multi-gigabyte text datasets where only sample comparisons were feasible, a complete distributed bidirectional set-difference (`exceptAll`) was performed across all 97,929 rows and 17 original columns:
* **Source exceptAll Bronze:** `0` records difference.
* **Bronze exceptAll Source:** `0` records difference.
* **Duration:** 9.36 seconds.
* **Result:** **100% full-record equivalence confirmed across the entire dataset.**

### 4.5 Staging & Atomic Swap Safeguards
* Ingestion wrote to staging directory `_staging_jobs_8955a12b` on `D:`.
* Staging output was read back and verified at `97,929` rows before promotion.
* Staging directory was atomically promoted to `data/bronze/indian_job_market_2025_2026/jobs`.
* Zero leftover staging or backup folders remain.

### 4.6 Prior LinkedIn Bronze Datasets Verification
All three LinkedIn 2024 tables were read back and verified completely intact:
* `data/bronze/linkedin_2024/job_postings`: **1,348,454 rows** (4 part files, 125.78 MB).
* `data/bronze/linkedin_2024/job_skills`: **1,296,381 rows** (4 part files, 320.02 MB).
* `data/bronze/linkedin_2024/job_summary`: **1,297,332 rows** (16 part files, 2,682.73 MB).
* Zero modification, corruption, or cross-dataset pollution.

---

## 5. Medallion Bronze Layer Completion Status

With Phase 5D complete, **all tables across both core datasets are 100% ingested into the Medallion Bronze layer**:

| Source Dataset | Table | Source Size | Bronze Size | Partitions | Rows Ingested | Status |
|---|---|---|---|---|---|---|
| **LinkedIn Jobs & Skills 2024** | `job_postings` | 396.60 MB | 125.78 MB | 4 | 1,348,454 | ✅ **PASSED (Phase 5A)** |
| **LinkedIn Jobs & Skills 2024** | `job_skills` | 641.55 MB | 320.02 MB | 4 | 1,296,381 | ✅ **PASSED (Phase 5B)** |
| **LinkedIn Jobs & Skills 2024** | `job_summary` | 5,102.02 MB | 2,682.73 MB | 16 | 1,297,332 | ✅ **PASSED (Phase 5C)** |
| **Indian Job Market 2025–2026** | `jobs` | 83.36 MB | 38.34 MB | 2 | 97,929 | ✅ **PASSED (Phase 5D)** |
| **GRAND TOTAL** | **4 Tables** | **6.22 GB** | **3.17 GB** | **26 Part Files** | **4,040,096 Rows** | **100% COMPLETE** |

---

## 6. Errors, Warnings & Observations

* **Errors:** **Zero.**
* **Warnings:**
  * Standard PySpark notice (`FutureWarning: PySpark does not yet fully support pandas >= 3.0.0...`). Non-impactful as the hybrid writer uses PyArrow directly.
  * Standard Spark warning during staging validation (`DataSource: All paths were ignored`). Benign path-globbing message on Windows local filesystem; staging readback confirmed 97,929 rows.
* **Storage Footprint:** Total Bronze Parquet storage across all 4 tables is **3.17 GB**, compressed from 6.22 GB raw source files (~49.0% overall space reduction). D: drive free space is healthy at **211.18 GB**.

---
*Report generated following Phase 5D execution and 100% full-record distributed verification.*
