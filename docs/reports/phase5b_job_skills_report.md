# DemandLens: Phase 5B — LinkedIn Job Skills Bronze Ingestion Report

**Report Date:** 2026-11-23 13:35:00 UTC  
**Target Table:** `job_skills` (Source: LinkedIn Jobs & Skills 2024)  
**Status:** **SUCCESS (Validation PASSED)**

---

## 1. Executive Summary

This report documents the execution of **Phase 5B: Full Bronze Ingestion of LinkedIn Job Skills** (`job_skills.csv`). Ingestion was executed using the verified hybrid PySpark + PyArrow engine, enforcing bounded-memory streaming without full-dataset collection.

Both `job_postings` (ingested in Phase 5A) and `job_skills` (ingested in Phase 5B) now reside completely in the Medallion Bronze layer on the `D:` drive.

---

## 2. Ingestion Parameters & Metrics

| Metric | Measured Value |
|---|---|
| **Source File** | `D:\DemandLens\data\source_csv\linkedin_2024\extracted\job_skills.csv` |
| **Source File Size** | 641.55 MB (672,718,092 bytes) |
| **Source Record Count** | **1,296,381 rows** |
| **Bronze Record Count** | **1,296,381 rows** (100% Match) |
| **Bronze Output Directory** | [D:/DemandLens/data/bronze/linkedin_2024/job_skills](file:///D:/DemandLens/data/bronze/linkedin_2024/job_skills) |
| **Ingestion Run ID** | `run_20261123_133320_0a423c` |
| **Target Partitions** | **4 partitions** |
| **Parquet Part Files Written** | **4 files** |
| **Total Bronze Parquet Footprint** | **320.02 MB (335,567,958 bytes)** |
| **Compression Format** | **Snappy** (Verified on all column chunks) |
| **Compression Space Savings** | **~50.11% reduction** from raw CSV text |
| **Ingestion Execution Duration** | **99.82 seconds** |
| **Throughput** | **~13,000 rows / second** |
| **D: Drive Available Space Before** | **214.16 GB** |
| **D: Drive Available Space After** | **213.84 GB** (~320 MB net consumed) |
| **Run Metadata Manifest** | [D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_skills_run_20261123_133320_0a423c.json](file:///D:/DemandLens/data/bronze/linkedin_2024/_metadata/job_skills_run_20261123_133320_0a423c.json) |

---

## 3. Generated Parquet Files Breakdown

| File Name | File Size | Row Count | Row Groups | Compression |
|---|---|---|---|---|
| `part-00000.parquet` | 79.96 MB | 324,095 | 33 | SNAPPY |
| `part-00001.parquet` | 79.93 MB | 324,096 | 33 | SNAPPY |
| `part-00002.parquet` | 80.08 MB | 324,095 | 33 | SNAPPY |
| `part-00003.parquet` | 80.05 MB | 324,095 | 33 | SNAPPY |
| **Total** | **320.02 MB** | **1,296,381** | **132** | **SNAPPY** |

---

## 4. Post-Ingestion Validation Matrix

A distributed verification was conducted via Apache Spark and PyArrow ([verify_phase5b_skills.py](file:///D:/DemandLens/data/temp_validation/verify_phase5b_skills.py)) without driver collection:

1. **Row Count Fidelity:**
   * Source row count: `1,296,381`
   * Bronze readback row count: `1,296,381`
   * Mismatch: **0** (100% preservation).

2. **Schema & Columns (6 Columns Total):**
   * **Original 2 Source Columns (100% Preserved):**
     * `job_link` (StringType)
     * `job_skills` (StringType)
   * **Added Ingestion Provenance Columns (4):**
     * `_source_id`: Exactly `"linkedin_2024"`
     * `_source_file`: Exactly `"job_skills.csv"`
     * `_ingestion_timestamp`: UTC microsecond timestamp
     * `_ingestion_run_id`: `"run_20261123_133320_0a423c"`

3. **Null-Value Preservation Audit:**
   * `job_link`: **0 nulls (0.000%)** — primary key integrity confirmed.
   * `job_skills`: **2,007 nulls (0.155%)** — exactly matches the raw source CSV null count (`2,007`). Zero unexpected null conversions.
   * `_source_id`: **0 nulls**.
   * `_ingestion_timestamp`: **0 nulls**.

4. **Staging & Finalization Safeguards:**
   * The table was written to an isolated staging directory (`_staging_job_skills_6eecabdb`).
   * Staging Parquet files were read back and verified (`1,296,381` rows).
   * Staging was promoted atomically to `data/bronze/linkedin_2024/job_skills`.

---

## 5. Errors, Warnings & Observations
* **Errors:** **Zero.**
* **Warnings:** Standard PySpark Pandas 3.0 notice (`FutureWarning: PySpark does not yet fully support pandas >= 3.0.0...`). This warning is non-impactful as the hybrid writer streams records via PyArrow and avoids Pandas entirely.
* **Integrity of Previously Ingested Tables:** `data/bronze/linkedin_2024/job_postings` remains intact with its 4 Parquet part files (`1,348,454` rows).

---

*Report generated automatically for Phase 5B validation.*
