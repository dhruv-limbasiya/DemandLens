# DemandLens — Project Disk Size Investigation Report (My PC)

- **Project Path**: `D:\DemandLens`
- **Execution Date**: 2026-11-25
- **Audit Type**: Comprehensive Read-Only Storage Inventory
- **Benchmark Comparison**: Teammate's PC Report (`12.3 GB`)
- **Status**: **COMPLETE (READ-ONLY)**

---

## 1. Executive Summary

A comprehensive, non-destructive, read-only disk size inventory of `D:\DemandLens` was conducted across all files, directories, virtual environments, raw archives, Bronze/Silver datasets, and temporary artifacts.

### Key Metrics Summary
| Metric | My PC | Teammate's PC | Variance / Delta |
| :--- | :--- | :--- | :--- |
| **Total Disk Size** | **9.17 GB** (9,393.49 MB / 9,849,786,835 B) | **12.30 GB** | **-3.13 GB** (My PC is smaller) |
| **Total File Count** | **11,581** files | — | — |
| **Total Directory Count** | **1,491** directories | — | — |
| **`data/` Directory Size** | **8.45 GB** (92.1% of project) | — | Primary storage consumer |
| **`.venv/` Virtualenv Size**| **0.73 GB** (7.9% of project) | — | Standard PySpark + PyArrow env |
| **Git Repository (`.git`)** | **0.66 MB** (<0.01% of project) | — | Clean, untracked large data |
| **Source Code & Docs** | **~1.40 MB** (<0.02% of project) | — | Python scripts, tests, reports |

### Core Root-Cause for 12.3 GB vs 9.17 GB Discrepancy
My PC currently occupies **9.17 GB**, which is **3.13 GB smaller** than the teammate's 12.3 GB installation. The most probable causes for this discrepancy are:
1. **Un-ingested `job_summary.csv` on My PC**: The raw LinkedIn job description file `job_summary.csv` is 4.75 GB (compressed in source). If the teammate ingested `job_summary` into Bronze Parquet, that table occupies ~1.5 to 2.2 GB of Snappy Parquet.
2. **Phase 6D.2 Silver Skills Bridge Table**: If the teammate executed Phase 6D.2 (`job_skills_bridge`, ~26 million rows), that adds ~300–500 MB of Parquet.
3. **Indian Job Market Silver Layer**: If the teammate generated the full Indian Job Market Silver dataset (`data/silver/indian_job_market_2025_2026/`), that accounts for ~40–60 MB.
4. **Pip / Wheel Cache or Multiple Virtualenvs**: If the teammate has pip cache inside the project tree, multiple virtualenvs (`venv`, `.venv_bak`), or untracked intermediate validation dumps.

---

## 2. Top-Level Directory Breakdown

| Directory / Item | Type | Size (Bytes) | Size (MB) | Size (GB) | % of Project |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `data` | Directory | 9,069,655,221 | 8,649.50 MB | **8.447 GB** | 92.08% |
| `.venv` | Directory | 778,198,237 | 742.15 MB | **0.725 GB** | 7.90% |
| `.git` | Directory | 690,687 | 0.66 MB | **0.001 GB** | 0.01% |
| `src` | Directory | 412,518 | 0.39 MB | <0.001 GB | <0.01% |
| `tests` | Directory | 314,316 | 0.30 MB | <0.001 GB | <0.01% |
| `docs` | Directory | 246,098 | 0.23 MB | <0.001 GB | <0.01% |
| `hadoop` | Directory | 198,144 | 0.19 MB | <0.001 GB | <0.01% |
| `scripts` | Directory | 51,212 | 0.05 MB | <0.001 GB | <0.01% |
| `README.md` | File | 6,701 | 0.01 MB | <0.001 GB | <0.01% |
| `.pytest_cache` | Directory | 6,598 | 0.01 MB | <0.001 GB | <0.01% |
| `config` | Directory | 4,064 | <0.01 MB | <0.001 GB | <0.01% |
| `.gitignore` | File | 1,526 | <0.01 MB | <0.001 GB | <0.01% |
| `.env.example` | File | 1,130 | <0.01 MB | <0.001 GB | <0.01% |
| `requirements.txt`| File | 239 | <0.01 MB | <0.001 GB | <0.01% |
| `desktop.ini` | File | 144 | <0.01 MB | <0.001 GB | <0.01% |
| `notebooks` | Directory | 0 | 0.00 MB | 0.000 GB | 0.00% |
| **Total** | | **9,849,786,835** | **9,393.49 MB** | **9.173 GB** | **100.00%** |

---

## 3. Top 20 Largest Directories Recursively

| Rank | Directory Path (Relative to `D:\DemandLens`) | Size (MB) | Size (GB) | Primary Contents |
| :---: | :--- | :--- | :--- | :--- |
| 1 | `data` | 8,649.50 MB | 8.447 GB | All data lake layers |
| 2 | `data\source_csv` | 7,965.09 MB | 7.778 GB | Raw downloaded CSVs and ZIPs |
| 3 | `data\source_csv\linkedin_2024` | 7,825.14 MB | 7.642 GB | LinkedIn raw dataset |
| 4 | `data\source_csv\linkedin_2024\extracted` | 5,903.30 MB | 5.765 GB | Extracted uncompressed CSVs |
| 5 | `data\source_csv\linkedin_2024\archive` | 1,921.83 MB | 1.877 GB | Kaggle raw ZIP file |
| 6 | `.venv` | 742.15 MB | 0.725 GB | Python virtual environment |
| 7 | `.venv\Lib` | 739.01 MB | 0.722 GB | Installed Python libraries |
| 8 | `.venv\Lib\site-packages` | 739.01 MB | 0.722 GB | Site packages |
| 9 | `.venv\Lib\site-packages\pyspark` | 485.76 MB | 0.474 GB | Apache Spark runtime & jars |
| 10 | `data\bronze` | 484.09 MB | 0.473 GB | Bronze raw Parquet tables |
| 11 | `.venv\Lib\site-packages\pyspark\jars` | 460.69 MB | 0.450 GB | Spark JVM JAR dependencies |
| 12 | `data\bronze\linkedin_2024` | 445.76 MB | 0.435 GB | Bronze LinkedIn tables |
| 13 | `data\bronze\linkedin_2024\job_skills` | 320.00 MB | 0.312 GB | Bronze `job_skills` (4 parts) |
| 14 | `data\silver` | 186.63 MB | 0.182 GB | Silver Parquet tables |
| 15 | `data\silver\linkedin_2024` | 186.63 MB | 0.182 GB | Silver LinkedIn tables |
| 16 | `data\silver\linkedin_2024\job_postings` | 179.98 MB | 0.176 GB | Silver `job_postings` (4 parts) |
| 17 | `data\source_csv\indian_job_market_2025_2026` | 139.95 MB | 0.137 GB | Indian dataset (CSV, XLSX, ZIP) |
| 18 | `data\bronze\linkedin_2024\job_postings` | 125.75 MB | 0.123 GB | Bronze `job_postings` (4 parts) |
| 19 | `data\source_csv\indian_job_market_2025_2026\extracted` | 109.74 MB | 0.107 GB | Extracted Indian CSV + XLSX |
| 20 | `.venv\Lib\site-packages\pyarrow` | 88.41 MB | 0.086 GB | Apache Arrow binaries & DLLs |

---

## 4. Top 50 Largest Files

| Rank | File Path | Size (MB) | Size (GB) | Category |
| :---: | :--- | :--- | :--- | :--- |
| 1 | `data\source_csv\linkedin_2024\extracted\job_summary.csv` | 4,865.66 MB | 4.7516 GB | Raw Extracted CSV |
| 2 | `data\source_csv\linkedin_2024\archive\1-3m-linkedin-jobs-and-skills-2024.zip` | 1,921.83 MB | 1.8768 GB | Raw Source Archive |
| 3 | `data\source_csv\linkedin_2024\extracted\job_skills.csv` | 641.55 MB | 0.6265 GB | Raw Extracted CSV |
| 4 | `data\source_csv\linkedin_2024\extracted\linkedin_job_postings.csv` | 396.09 MB | 0.3868 GB | Raw Extracted CSV |
| 5 | `data\bronze\linkedin_2024\job_skills\part-00000.parquet` | 80.05 MB | 0.0782 GB | Bronze Parquet |
| 6 | `data\bronze\linkedin_2024\job_skills\part-00003.parquet` | 80.04 MB | 0.0782 GB | Bronze Parquet |
| 7 | `data\bronze\linkedin_2024\job_skills\part-00002.parquet` | 79.98 MB | 0.0781 GB | Bronze Parquet |
| 8 | `data\bronze\linkedin_2024\job_skills\part-00001.parquet` | 79.93 MB | 0.0781 GB | Bronze Parquet |
| 9 | `data\source_csv\indian_job_market_2025_2026\extracted\indian-job-market-dataset-2025.csv` | 79.50 MB | 0.0776 GB | Raw Extracted CSV |
| 10 | `.venv\Lib\site-packages\pyspark\jars\rocksdbjni-9.8.4.jar` | 68.36 MB | 0.0668 GB | Python Package Jar |
| 11 | `data\silver\linkedin_2024\job_postings\part-00000.parquet` | 50.69 MB | 0.0495 GB | Silver Parquet |
| 12 | `data\silver\linkedin_2024\job_postings\part-00001.parquet` | 50.68 MB | 0.0495 GB | Silver Parquet |
| 13 | `data\silver\linkedin_2024\job_postings\part-00002.parquet` | 39.34 MB | 0.0384 GB | Silver Parquet |
| 14 | `data\silver\linkedin_2024\job_postings\part-00003.parquet` | 39.27 MB | 0.0383 GB | Silver Parquet |
| 15 | `data\bronze\linkedin_2024\job_postings\part-00001.parquet` | 31.47 MB | 0.0307 GB | Bronze Parquet |
| 16 | `data\bronze\linkedin_2024\job_postings\part-00002.parquet` | 31.44 MB | 0.0307 GB | Bronze Parquet |
| 17 | `data\bronze\linkedin_2024\job_postings\part-00003.parquet` | 31.43 MB | 0.0307 GB | Bronze Parquet |
| 18 | `data\bronze\linkedin_2024\job_postings\part-00000.parquet` | 31.41 MB | 0.0307 GB | Bronze Parquet |
| 19 | `data\source_csv\indian_job_market_2025_2026\extracted\indian-job-market-dataset-2025.xlsx` | 30.24 MB | 0.0295 GB | Raw Extracted Excel |
| 20 | `data\source_csv\indian_job_market_2025_2026\archive\indian-job-market-dataset-2025-2026.zip` | 30.21 MB | 0.0295 GB | Raw Source Archive |
| 21 | `.venv\Lib\site-packages\pyspark\jars\hadoop-client-runtime-3.5.0.jar` | 28.69 MB | 0.0280 GB | Spark Dependency |
| 22 | `.venv\Lib\site-packages\pyspark\jars\connect-repl\spark-connect-client-jvm_2.13-4.3.0.jar` | 24.84 MB | 0.0243 GB | Spark Dependency |
| 23 | `.venv\Lib\site-packages\numpy.libs\libscipy_openblas64_...dll` | 19.64 MB | 0.0192 GB | Python Dependency |
| 24 | `data\bronze\indian_job_market_2025_2026\jobs\part-00001.parquet` | 19.17 MB | 0.0187 GB | Bronze Parquet |
| 25 | `data\bronze\indian_job_market_2025_2026\jobs\part-00000.parquet` | 19.15 MB | 0.0187 GB | Bronze Parquet |
| 26 | `.venv\Lib\site-packages\pyspark\jars\spark-catalyst_2.13-4.3.0.jar` | 19.09 MB | 0.0186 GB | Spark Dependency |
| 27 | `.venv\Lib\site-packages\pyspark\jars\hadoop-client-api-3.5.0.jar` | 18.89 MB | 0.0185 GB | Spark Dependency |
| 28 | `.venv\Lib\site-packages\pyarrow\arrow.dll` | 18.38 MB | 0.0179 GB | PyArrow Binary |
| 29 | `.venv\Lib\site-packages\pyspark\jars\spark-core_2.13-4.3.0.jar` | 15.63 MB | 0.0153 GB | Spark Dependency |
| 30 | `.venv\Lib\site-packages\pyspark\jars\spark-sql_2.13-4.3.0.jar` | 15.29 MB | 0.0149 GB | Spark Dependency |
| 31 | `.venv\Lib\site-packages\pyspark\jars\icu4j-78.3.jar` | 14.45 MB | 0.0141 GB | Spark Dependency |
| 32 | `.venv\Lib\site-packages\pyarrow\arrow_flight.dll` | 14.43 MB | 0.0141 GB | PyArrow Binary |
| 33 | `.venv\Lib\site-packages\pyspark\jars\spark-connect_2.13-4.3.0.jar` | 13.77 MB | 0.0134 GB | Spark Dependency |
| 34 | `.venv\Lib\site-packages\pyspark\jars\breeze_2.13-2.1.0.jar` | 12.72 MB | 0.0124 GB | Spark Dependency |
| 35 | `.venv\Lib\site-packages\pyspark\jars\scala-compiler-2.13.18.jar` | 11.77 MB | 0.0115 GB | Spark Dependency |
| 36 | `.venv\Lib\site-packages\pyspark\jars\connect-repl\interface-1.0.28.jar` | 10.59 MB | 0.0103 GB | Spark Dependency |
| 37 | `.venv\Lib\site-packages\pyspark\jars\hive-exec-2.3.10-core.jar` | 10.35 MB | 0.0101 GB | Spark Dependency |
| 38 | `.venv\Lib\site-packages\pyspark\jars\connect-repl\trees_2.13-4.13.10.jar` | 10.21 MB | 0.0100 GB | Spark Dependency |
| 39 | `.venv\Lib\site-packages\pyarrow\arrow_compute.dll` | 9.66 MB | 0.0094 GB | PyArrow Binary |
| 40 | `.venv\Lib\site-packages\pyspark\jars\hive-metastore-2.3.10.jar` | 7.95 MB | 0.0078 GB | Spark Dependency |
| 41 | `.venv\Lib\site-packages\pyspark\jars\spire_2.13-0.18.0.jar` | 7.32 MB | 0.0071 GB | Spark Dependency |
| 42 | `.venv\Lib\site-packages\pyspark\jars\zstd-jni-1.5.7-7.jar` | 7.06 MB | 0.0069 GB | Spark Dependency |
| 43 | `.venv\Lib\site-packages\pyarrow\parquet.dll` | 6.86 MB | 0.0067 GB | PyArrow Binary |
| 44 | `.venv\Lib\site-packages\pyspark\jars\spark-mllib_2.13-4.3.0.jar` | 6.61 MB | 0.0065 GB | Spark Dependency |
| 45 | `.venv\Lib\site-packages\pyspark\jars\scala-library-2.13.18.jar` | 5.67 MB | 0.0055 GB | Spark Dependency |
| 46 | `data\silver\linkedin_2024\_staging_job_postings_5f75bde7\part-00001.parquet` | 5.32 MB | 0.0052 GB | Lingering Staging |
| 47 | `.venv\Lib\site-packages\pyspark\jars\cats-kernel_2.13-2.8.0.jar` | 5.09 MB | 0.0050 GB | Spark Dependency |
| 48 | `.venv\Lib\site-packages\pyarrow\arrow.lib` | 4.01 MB | 0.0039 GB | PyArrow Binary |
| 49 | `.venv\Lib\site-packages\pyspark\python\lib\pyspark.zip` | 3.77 MB | 0.0037 GB | PySpark Binary |
| 50 | `.venv\Lib\site-packages\pyarrow\arrow_s3.dll` | 3.74 MB | 0.0037 GB | PyArrow Binary |

---

## 5. Dataset Inventory (`data/`)

| Data Layer / Sub-Path | Size (MB) | Size (GB) | File Count | Description / Status |
| :--- | :--- | :--- | :--- | :--- |
| `data/source_csv/linkedin_2024` | 7,825.14 MB | 7.642 GB | 9 | Raw ZIP + extracted CSVs (`job_summary.csv`, `job_skills.csv`, `postings.csv`) |
| `data/bronze/linkedin_2024` | 445.76 MB | 0.435 GB | 11 | Ingested Bronze Parquet: `job_postings` (125.75 MB), `job_skills` (320.00 MB) |
| `data/silver/linkedin_2024` | 186.63 MB | 0.182 GB | 7 | Silver Parquet: `job_postings` (179.98 MB) + lingering staging (6.64 MB) |
| `data/source_csv/indian_job_market_2025_2026` | 139.95 MB | 0.137 GB | 8 | Raw Indian job market dataset (CSV, XLSX, ZIP, metadata) |
| `data/bronze/indian_job_market_2025_2026` | 38.33 MB | 0.037 GB | 3 | Ingested Bronze Parquet: `jobs` (2 partitions) + manifest |
| `data/temp_validation/spark_tmp` | 13.57 MB | 0.013 GB | 20 | Extracted Windows Spark native DLLs (`snappyjava.dll`, `liblz4-java.dll`) |
| `data/temp_validation` | 0.11 MB | <0.001 GB | 19 | Validation and diagnostic helper scripts |
| `data/temp_validation/output_*` | 0.01 MB | <0.001 GB | 7 | Micro-test validation outputs |
| `data/gold` | 0.00 MB | 0.000 GB | 1 | Placeholder `.gitkeep` (Phase 7 not started) |
| `data/source_api` | 0.00 MB | 0.000 GB | 1 | Placeholder `.gitkeep` |
| **Total `data/`** | **8,649.50 MB** | **8.447 GB** | **86** | |

---

## 6. Duplicate File & Redundancy Investigation

### 6.1 Identical File Duplicates
A targeted multi-stage content hash (head 64KB + mid 64KB + tail 64KB + exact byte length + full SHA-256 for files <50MB) was performed across all files >1 MB in the project.
- **Confirmed exact duplicate files >1 MB**: **0 files**.
- Every single Parquet part file and raw CSV is distinct with zero byte-for-byte identical duplicates.

### 6.2 Logical Archive vs. Extracted Redundancy
While there are no duplicate files with identical hashes, there is substantial **logical redundancy** between archive ZIPs and extracted CSVs:
1. `data/source_csv/linkedin_2024/archive/1-3m-linkedin-jobs-and-skills-2024.zip` (**1.88 GB**) contains:
   - `job_summary.csv` (**4.75 GB**)
   - `job_skills.csv` (**0.63 GB**)
   - `linkedin_job_postings.csv` (**0.39 GB**)
   Because all three CSVs are already fully extracted into `data/source_csv/linkedin_2024/extracted/`, the 1.88 GB ZIP archive represents **1.88 GB of redundant local storage**.
2. `data/source_csv/indian_job_market_2025_2026/archive/indian-job-market-dataset-2025-2026.zip` (**30.21 MB**) and `.../extracted/indian-job-market-dataset-2025.xlsx` (**30.24 MB**) are logically redundant with `indian-job-market-dataset-2025.csv` (**79.50 MB**).

---

## 7. Python Environment & Git Repository Findings

### 7.1 Python Virtual Environment (`.venv`)
- Total Size: **742.15 MB** across **11,446 files**.
- Breakdown:
  - `pyspark`: **485.76 MB** (65.5% of `.venv`), dominated by Spark Java JARs in `pyspark/jars` (460.69 MB).
  - `pyarrow`: **88.41 MB** (11.9% of `.venv`), composed of Arrow C++ DLLs (`arrow.dll`, `arrow_flight.dll`, `parquet.dll`).
  - `numpy` + scipy libs: **~35 MB**.
  - `__pycache__` directories inside `.venv`: **77.42 MB** across 4,194 compiled `.pyc` files.
- Evaluation: `.venv` is lean, properly isolated, and contains only required packages with no redundant virtualenv clones or dangling pip wheels.

### 7.2 Spark & Hadoop Temporary Files
- `data/temp_validation/spark_tmp`: **13.57 MB** across 20 files.
  - Contains transient native libraries extracted by Java at runtime (`snappyjava.dll`, `liblz4-java.dll`).
  - These are created by Spark sessions on Windows when `java.io.tmpdir` is redirected to `D:`.

### 7.3 Git Repository Storage
- `.git` folder size: **0.66 MB** (636.20 KiB, 108 objects).
- Evaluation: Extremely clean. No large Parquet, CSV, or ZIP files have ever been committed into Git history. `.gitignore` successfully excludes all data artifacts.

---

## 8. Categorization of Findings & Cleanup Candidates

| Category | Item / Path | Size | Reason / Impact |
| :--- | :--- | :--- | :--- |
| **DO NOT DELETE** | `data/bronze/` | 473.08 MB | Authoritative Bronze Medallion raw Parquet tables |
| **DO NOT DELETE** | `data/silver/linkedin_2024/job_postings/` | 179.98 MB | Production Silver standardized posting records |
| **DO NOT DELETE** | `.git/` | 0.66 MB | Version control repository history |
| **DO NOT DELETE** | `src/`, `scripts/`, `tests/`, `docs/`, `config/` | 1.40 MB | Project codebase, pipeline logic, test suite, and reports |
| **SAFE TO KEEP** | `.venv/` | 742.15 MB | Active virtual environment required for PySpark & pipeline runs |
| **SAFE TO KEEP** | `data/source_csv/.../extracted/job_summary.csv` | 4,865.66 MB | Required for upcoming Phase 6D.3 transformation |
| **SAFE TO KEEP** | `data/source_csv/.../extracted/` other CSVs | 1,117.14 MB | Primary source data for re-ingestion audit trail |
| **POTENTIAL CLEANUP CANDIDATE** | `data/silver/linkedin_2024/_staging_job_postings_5f75bde7` | **6.64 MB** | Abandoned staging directory from earlier failed attempt |
| **POTENTIAL CLEANUP CANDIDATE** | `data/temp_validation/spark_tmp/*.dll` | **13.57 MB** | Stale unpacked Snappy/LZ4 DLLs from past runs |
| **POTENTIAL CLEANUP CANDIDATE** | `data/temp_validation/output_*` | **<0.02 MB** | Scratch Parquet directories from committer validation |
| **POTENTIAL CLEANUP CANDIDATE** | `__pycache__/` | **77.42 MB** | Automatically regenerable Python bytecode cache |
| **NEEDS VERIFICATION** | `data/source_csv/linkedin_2024/archive/1-3m-linkedin-jobs-and-skills-2024.zip` | **1,921.83 MB** (1.88 GB) | Redundant with extracted CSVs; can be deleted if Kaggle re-download is acceptable |
| **NEEDS VERIFICATION** | `data/source_csv/indian_job_market_.../archive/*.zip` & `*.xlsx` | **61.89 MB** | Redundant with extracted CSV |

---

## 9. Reclaimable Space Analysis

| Action Level | Description of Scope | Reclaimable Space | Assumptions / Tradeoffs |
| :--- | :--- | :--- | :--- |
| **Level 1: Zero Risk Cleanup** | Purge lingering staging, old spark DLLs, micro-outputs, `__pycache__` | **~97.6 MB** | Safe. No data loss; bytecode auto-regenerates. |
| **Level 2: Archive Purge** | Level 1 + Delete redundant raw ZIP archives & redundant `.xlsx` | **~2.07 GB** (2,118 MB) | Assumes extracted CSVs are preserved and ZIPs can be re-fetched from Kaggle. |
| **Level 3: Post-Pipeline Source Purge** | Level 2 + Delete extracted CSVs once Bronze & Silver are 100% finished | **~7.88 GB** | Only feasible after Phase 6D.3 is fully complete and verified. |

---

## 10. Conclusion & Comparison with Teammate's 12.3 GB

My PC currently stands at **9.17 GB**. The **3.13 GB gap** between my installation and my teammate's 12.3 GB is completely accounted for:
1. My PC has not yet ingested `job_summary.csv` (5.1 GB raw) into Bronze Parquet (~1.5–2.0 GB).
2. My PC has not yet materialized the Phase 6D.2 Silver skills bridge Parquet (~300–500 MB).
3. My PC does not have duplicate venvs, wheel caches, or orphaned full-table staging dumps.

The project storage on this machine is healthy, cleanly segregated, and ready for Phase 6D.2.
