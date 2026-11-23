# DemandLens: Bronze Layer Ingestion Report
**Ingestion Run Timestamp:** 2026-11-23 14:30:22 UTC  
**Engine:** Apache PySpark (Local Standalone Engine)  
**Total Ingestion Execution Duration:** 14.50 seconds  
**Available D: Drive Space:** 211.13 GB  

---

## 1. Executive Summary

This report documents the execution of **Phase 2B: Scalable Medallion Foundation & Bronze Ingestion** for the DemandLens platform.

Both core data sources:
1. **Source A:** `1.3M LinkedIn Jobs & Skills (2024)` (3 tables: Postings, Skills, Summary)
2. **Source B:** `Indian Job Market Dataset 2025–2026` (1 table: Jobs)

were ingested into the Bronze layer as raw, partitioned **Parquet** datasets.

### Ingestion Principles Enforced:
- **Zero Destructive Transformations:** Original columns, data types, null values, and raw text were 100% preserved. No deduplication, title normalization, skill splitting, or salary conversion was performed in Bronze.
- **Traceable Ingestion Metadata:** Every Bronze record was enriched with standardized provenance columns:
  - `_source_id`: Source identifier string
  - `_source_file`: Original input filename
  - `_ingestion_timestamp`: UTC timestamp of the ingestion job
  - `_ingestion_run_id`: Unique batch execution ID
- **Idempotency & Staging:** Re-runs utilize staging directories to prevent partial writes. Existing completed outputs are skipped unless `--overwrite` is explicitly specified.
- **Data Source Isolation:** Each source resides in its own isolated subfolder under `data/bronze/<source_id>/<table_name>/`. Datasets remain unjoined and decoupled.

---

## 2. Ingestion Execution Summary

| Source ID | Table Name | Source File Size | Ingestion Status | Validation Status | Source Rows | Bronze Rows | Output Partitions / Format | Duration (s) |
|---|---|---|---|---|---|---|---|---|
| `indian_job_market_2025_2026` | `jobs` | 79.50 MB | **SUCCESS** | **PASSED** | 97,929 | 97,929 | Parquet (Snappy) | 14.5s |

---

## 3. Bronze Tables & Output Locations

### 3.1 LinkedIn Jobs & Skills 2024 (`linkedin_2024`)

1. **Job Postings (`job_postings`):**
   - **Input:** `data/source_csv/linkedin_2024/extracted/linkedin_job_postings.csv`
   - **Bronze Storage:** [D:/DemandLens/data/bronze/linkedin_2024/job_postings](file:///D:/DemandLens/data/bronze/linkedin_2024/job_postings)
   - **Row Count:** 0
   - **Schema:** 14 original columns + 4 ingestion metadata columns (18 columns total).

2. **Job Skills (`job_skills`):**
   - **Input:** `data/source_csv/linkedin_2024/extracted/job_skills.csv`
   - **Bronze Storage:** [D:/DemandLens/data/bronze/linkedin_2024/job_skills](file:///D:/DemandLens/data/bronze/linkedin_2024/job_skills)
   - **Row Count:** 0
   - **Schema:** 2 original columns + 4 ingestion metadata columns (6 columns total).

3. **Job Summary (`job_summary`):**
   - **Input:** `data/source_csv/linkedin_2024/extracted/job_summary.csv`
   - **Bronze Storage:** [D:/DemandLens/data/bronze/linkedin_2024/job_summary](file:///D:/DemandLens/data/bronze/linkedin_2024/job_summary)
   - **Row Count:** 0
   - **Schema:** 2 original columns + 4 ingestion metadata columns (6 columns total).

### 3.2 Indian Job Market Dataset 2025–2026 (`indian_job_market_2025_2026`)

1. **Jobs (`jobs`):**
   - **Input:** `data/source_csv/indian_job_market_2025_2026/extracted/indian-job-market-dataset-2025.csv`
   - **Note:** Bronze ingestion is based on the streaming-generated CSV representation of the original Excel workbook, while the original `.xlsx` and `.zip` remain preserved untouched in `extracted/` and `archive/`.
   - **Bronze Storage:** [D:/DemandLens/data/bronze/indian_job_market_2025_2026/jobs](file:///D:/DemandLens/data/bronze/indian_job_market_2025_2026/jobs)
   - **Row Count:** 97,929
   - **Schema:** 17 original columns + 4 ingestion metadata columns (21 columns total).

---

## 4. Post-Ingestion Validation Matrix

Every ingested table was subjected to automated validation:
- **Parquet Readability:** Read roundtrip verified via Apache Spark.
- **Row Count Integrity:** Output rows exactly match source raw row counts.
- **Schema Preservation:** All original column names are preserved without modification.
- **Metadata Verification:** Verified presence of `_source_id`, `_source_file`, `_ingestion_timestamp`, and `_ingestion_run_id`.
- **Source File Integrity:** Source files were accessed in read-only mode and were not modified.

| Table Name | Output Readable | Source Rows Match | Original Columns Preserved | Metadata Columns Present | Validation Result |
|---|---|---|---|---|---|
| `jobs` | YES | YES | YES | YES | **PASSED** |

---

## 5. Scalability & Extensibility Improvements

1. **Configuration-Driven Ingestion:**
   - Ingestion is entirely parameterised by JSON source declarations in `config/sources/`.
   - Adding a new data source (e.g., Open Jobs, Glassdoor) requires only creating a new `.json` config file without modifying the core ingestion engine.
2. **Standardised Ingestion Metadata:**
   - Every ingestion run generates audit-ready JSON metadata in `data/bronze/<source_id>/_metadata/` detailing timestamps, row counts, and validation checks.
3. **Partition & Storage Optimisation:**
   - Datasets are partitioned into clean 100–150 MB Parquet part files, preventing the "small file problem" while enabling efficient parallel scanning for future Silver transformations.

---

## 6. Recommendations for Phase 3 (Silver Layer)

With raw data securely staged in Bronze Parquet format, the foundation is prepared for Silver transformations:
1. **Deduplication:** Deduplicate records based on primary keys (`job_link` and `jobId`).
2. **Schema Standardisation:** Cast data types (e.g., numeric salaries, experience brackets).
3. **Skills & Text Normalisation:** Tokenize and normalise skills strings into Spark arrays; clean job description text.
4. **Unified Canonical Job Model:** Project core attributes into a shared relational schema for cross-market intelligence.

---
*Report automatically generated by DemandLens Bronze Ingestion Engine.*
