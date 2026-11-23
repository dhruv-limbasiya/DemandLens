# DemandLens: Phase 5C — LinkedIn Job Summary Bronze Ingestion Report

**Report Date:** 2026-11-23 14:15:00 UTC  
**Target Table:** `job_summary` (Source: `linkedin_2024` / LinkedIn Jobs & Skills 2024)  
**Status:** **SUCCESS (Validation PASSED — 100% Data Fidelity)**  
**Run ID:** `run_20261123_141043_d2b718`  

---

## 1. Executive Summary

This report documents the successful execution and post-ingestion verification of **Phase 5C: Full Bronze Ingestion of LinkedIn Job Summary** (`job_summary.csv`).

The `job_summary.csv` dataset is the largest table in the LinkedIn 2024 source collection (5.10 GB raw text). Ingestion was conducted using the verified hybrid PySpark + PyArrow engine, streaming records in bounded 10,000-row batches directly to partitioned Parquet files with Snappy compression, entirely eliminating full-dataset in-memory materialization or `toPandas()` usage.

All three tables for the LinkedIn 2024 dataset (`job_postings`, `job_skills`, and `job_summary`) now reside completely and verified in the Medallion Bronze layer on the `D:` drive.

---

## 2. Ingestion Parameters & Metrics

| Metric | Measured Value |
|---|---|
| **Source File** | `D:\DemandLens\data\source_csv\linkedin_2024\extracted\job_summary.csv` |
| **Source File Size** | **5,102,016,275 bytes (5.10 GB / 4.7516 GiB)** |
| **Source Record Count** | **1,297,332 rows** |
| **Bronze Record Count** | **1,297,332 rows** (100% Match) |
| **Bronze Output Directory** | [D:/DemandLens/data/bronze/linkedin_2024/job_summary](file:///D:/DemandLens/data/bronze/linkedin_2024/job_summary) |
| **Ingestion Run ID** | `run_20261123_141043_d2b718` |
| **Target Partitions** | **16 partitions** (configured in `config/sources/linkedin_2024.json`) |
| **Parquet Part Files Written** | **16 files** (`part-00000.parquet` to `part-00015.parquet`) |
| **Total Bronze Parquet Footprint** | **2,682.73 MB (2.6199 GiB / 2,813,016,846 bytes)** |
| **Compression Codec** | **Snappy** (Verified on all column chunks across all 16 part files) |
| **Compression Space Savings** | **~47.42% reduction** from raw CSV text |
| **Ingestion Execution Duration** | **148.92 seconds (~2.48 minutes)** |
| **Throughput** | **~8,711 rows / second** |
| **D: Drive Available Space Before** | **213.84 GB** |
| **D: Drive Available Space After** | **211.22 GB** (~2.62 GB net consumed) |
| **Run Metadata Manifest** | [D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_summary_run_20261123_141043_d2b718.json](file:///D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_summary_run_20261123_141043_d2b718.json) |

---

## 3. Parquet Part Files Breakdown

| Part File Name | Size (MB) | Row Count | Row Groups | Compression |
|---|---|---|---|---|
| `part-00000.parquet` | 167.81 MB | 81,083 | 9 | SNAPPY |
| `part-00001.parquet` | 167.81 MB | 81,083 | 9 | SNAPPY |
| `part-00002.parquet` | 167.49 MB | 81,083 | 9 | SNAPPY |
| `part-00003.parquet` | 167.78 MB | 81,083 | 9 | SNAPPY |
| `part-00004.parquet` | 167.97 MB | 81,084 | 9 | SNAPPY |
| `part-00005.parquet` | 167.95 MB | 81,084 | 9 | SNAPPY |
| `part-00006.parquet` | 167.08 MB | 81,084 | 9 | SNAPPY |
| `part-00007.parquet` | 167.70 MB | 81,084 | 9 | SNAPPY |
| `part-00008.parquet` | 167.59 MB | 81,083 | 9 | SNAPPY |
| `part-00009.parquet` | 167.65 MB | 81,083 | 9 | SNAPPY |
| `part-00010.parquet` | 167.30 MB | 81,083 | 9 | SNAPPY |
| `part-00011.parquet` | 167.86 MB | 81,083 | 9 | SNAPPY |
| `part-00012.parquet` | 167.70 MB | 81,083 | 9 | SNAPPY |
| `part-00013.parquet` | 168.16 MB | 81,083 | 9 | SNAPPY |
| `part-00014.parquet` | 167.30 MB | 81,083 | 9 | SNAPPY |
| `part-00015.parquet` | 167.59 MB | 81,083 | 9 | SNAPPY |
| **Total** | **2,682.73 MB** | **1,297,332** | **144** | **SNAPPY** |

---

## 4. Post-Ingestion Validation Matrix

A distributed verification script was executed via Apache Spark and PyArrow ([verify_phase5c_summary.py](file:///D:/DemandLens/data/temp_validation/verify_phase5c_summary.py)):

### 4.1 Row Count Fidelity
* **Source CSV Rows:** `1,297,332`
* **Bronze Readback Rows:** `1,297,332`
* **Mismatch:** **0 (100% preservation)**

### 4.2 Schema & Provenance Columns (6 Columns Total)
* **Preserved Original Source Columns (2):**
  * `job_link` (`StringType`): Primary key reference
  * `job_summary` (`StringType`): Unstructured full text job description
* **Standard Ingestion Provenance Columns (4):**
  * `_source_id` (`StringType`): Exactly `"linkedin_2024"` across all records
  * `_source_file` (`StringType`): Exactly `"job_summary.csv"` across all records
  * `_ingestion_timestamp` (`TimestampType`): UTC microsecond timestamp
  * `_ingestion_run_id` (`StringType`): Exactly `"run_20261123_141043_d2b718"`

### 4.3 Null-Value Audit
* `job_link`: **0 nulls (0.0000%)** — primary key integrity confirmed.
* `job_summary`: **0 nulls (0.0000%)** — 100% non-null text records.
* `_source_id`: **0 nulls (0.0000%)**.
* `_source_file`: **0 nulls (0.0000%)**.
* `_ingestion_timestamp`: **0 nulls (0.0000%)**.
* `_ingestion_run_id`: **0 nulls (0.0000%)**.

### 4.4 Multiline Text Formatting Verification
* **Multiline Descriptions Preserved:** `1,297,332` records contain embedded newline (`\n`) characters (**100.00%**).
* **Sample Record Verification:** Sample records were retrieved from both raw CSV and Bronze Parquet by `job_link` and compared character-for-character:
  * Verified 100% character equivalence between raw source text and Bronze Parquet text.
  * No truncation, no escaped quote artifacts, and no loss of paragraph structures.

### 4.5 Staging & Atomic Swap Safeguards
* Table was initially written to staging directory `_staging_job_summary_92172f8d` on `D:`.
* Staging output was read back and validated at `1,297,332` rows before promotion.
* Staging directory was atomically promoted to `data/bronze/linkedin_2024/job_summary`.
* Staging directory was cleaned up with zero leftover temporary files.

### 4.6 Regression Check on Previously Ingested Bronze Tables
* `data/bronze/linkedin_2024/job_postings`: **1,348,454 rows** (Intact, 4 part files).
* `data/bronze/linkedin_2024/job_skills`: **1,296,381 rows** (Intact, 4 part files).
* No cross-table pollution or unintended file modifications occurred.

---

## 5. Errors, Warnings & Observations

* **Errors:** **Zero.**
* **Warnings:**
  * Standard PySpark notice (`FutureWarning: PySpark does not yet fully support pandas >= 3.0.0...`). Non-impactful as the hybrid writer uses PyArrow and avoids Pandas completely.
  * Standard Spark warning during staging validation (`DataSource: All paths were ignored`). Benign path-globbing message on Windows local filesystem; readback verified 1,297,332 rows successfully.
* **Resource Impact:** Net D: drive consumption was ~2.62 GB, leaving 211.22 GB available.

---

## 6. Cumulative Bronze Layer Status

All 3 tables of the **LinkedIn Jobs & Skills 2024** source are now fully ingested into Bronze:

| Table Name | Source Size | Bronze Size | Partitions | Total Rows | Ingestion Status |
|---|---|---|---|---|---|
| `job_postings` | 396.60 MB | 125.78 MB | 4 | 1,348,454 | **SUCCESS (PASSED)** |
| `job_skills` | 641.55 MB | 320.02 MB | 4 | 1,296,381 | **SUCCESS (PASSED)** |
| `job_summary` | 5,102.02 MB | 2,682.73 MB | 16 | 1,297,332 | **SUCCESS (PASSED)** |
| **LinkedIn 2024 Total** | **6.14 GB** | **3.13 GB** | **24** | **3,942,167** | **ALL PASSED** |

Remaining Bronze table:
* Source B: `indian_job_market_2025_2026` (`jobs` table: 79.5 MB, ~97.9K rows) scheduled for Phase 5D.

---
*Report generated following Phase 5C execution and distributed validation.*
