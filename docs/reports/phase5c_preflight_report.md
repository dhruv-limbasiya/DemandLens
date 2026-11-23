# DemandLens: Phase 5C Preflight Assessment Report

**Assessment Date:** 2026-11-23  
**Target Table:** `job_summary` (Source: `linkedin_2024`)  
**Scope:** Read-Only Preflight Assessment for Full Bronze Ingestion  
**Engine:** Hybrid PySpark 4.3.0 + PyArrow 26.0.1  

---

## 1. Environment Verification

| Component | Verified Specification | Status |
|---|---|---|
| **Project Root** | `D:\DemandLens` | ✅ Verified |
| **Python Interpreter** | `D:\DemandLens\.venv\Scripts\python.exe` (v3.12.10) | ✅ Verified |
| **PySpark** | Version 4.3.0 | ✅ Verified |
| **PyArrow** | Version 26.0.1 | ✅ Verified |
| **Java Runtime** | Java 17 (`D:\jdk-17.0.18.8-hotspot`) | ✅ Verified |
| **Hadoop Windows Binaries** | `D:\DemandLens\hadoop\bin` (`winutils.exe`, `hadoop.dll`) | ✅ Verified |
| **Spark Temp Storage** | `D:\DemandLens\data\temp_validation\spark_tmp` | ✅ Verified on `D:` |
| **D: Drive Available Space** | **213.84 GB** free | ✅ Verified |

---

## 2. Source File & Row Count Evidence

| Metric | Measured Value |
|---|---|
| **Source File Location** | `D:\DemandLens\data\source_csv\linkedin_2024\extracted\job_summary.csv` |
| **Source File Size** | **5,102,016,275 bytes (4.7516 GB / ~4.75 GB)** |
| **CSV Read Options** | `header="true"`, `multiLine="true"`, `escape="\""` |
| **Exact Row Count** | **1,297,332 records** |
| **PySpark Scan Duration** | 75.54 seconds (full read-only count) |
| **Average Record Size** | ~3,932.7 bytes / row |
| **Source Cleanliness** | Validated: Embedded multiline newlines and quotes are properly parsed |

---

## 3. Source Schema & Null Profile

### 3.1 Schema
The source CSV contains exactly **2 columns**:
1. `job_link` (`string`): Primary key referencing the job posting URL.
2. `job_summary` (`string`): Full unstructured text description of the job posting.

### 3.2 Null Profile
- `job_link`: **0 nulls (0.0000%)** — primary key integrity confirmed across all 1,297,332 records.
- `job_summary`: **0 nulls (0.0000%)** — every record contains a non-null description string.

---

## 4. D: Drive Free Space Analysis

- **Current Free Space:** 213.84 GB.
- **Estimated Ingestion Consumption:**
  - Raw uncompressed text: 4.75 GB.
  - Estimated Snappy Parquet footprint: **~1.2 GB to ~1.6 GB** (due to ~65–75% compression of English text).
  - Staging directory peak storage: ~1.5 GB.
  - Spark shuffle/spill temporary space: ~2.0 GB.
  - Total peak disk requirement: **~3.5 GB**.
- **Margin of Safety:** Available disk space (213.84 GB) exceeds peak requirement by **>60x**. No disk exhaustion risk.

---

## 5. Existing Target Status

- **Target Directory:** `D:\DemandLens\data\bronze\linkedin_2024\job_summary`
- **Target Exists:** **`False`** (directory does not exist).
- **Target Integrity:** Clean slate. No preexisting files, no risk of collision or accidental overwrite.

---

## 6. Writer Compatibility & Pipeline Analysis

### 6.1 Hybrid PyArrow Writer Compatibility
- The hybrid PyArrow writer (`_write_partition_to_parquet_worker`) operates at partition level with bounded in-memory row batches:
  - Default `batch_size = 10,000` rows.
  - Peak memory per batch = 10,000 rows × ~3,933 bytes ≈ **~39.3 MB**.
  - PyArrow converts the batch into an Arrow Table and writes a Parquet row group, then immediately clears `batch_rows`.
  - Memory consumption is strictly bounded and will not cause OutOfMemory (OOM) errors even for a 4.75 GB source dataset.

### 6.2 Spark Partitioning Strategy
- Because the input CSV uses `multiLine="true"`, Apache Spark initially reads the single CSV file with **1 partition**.
- The table declaration in `config/sources/linkedin_2024.json` specifies:
  `"target_partitions": 16`
- The ingestion engine (`bronze_ingestor.py`) automatically detects `current_parts < target_partitions` (1 < 16) and applies:
  `df_bronze = df_bronze.repartition(16)`
- This partitions the 1,297,332 records into 16 balanced partitions:
  - **~81,083 rows per partition**.
  - **~8 row groups per Parquet part file** (at 10,000 rows/batch).
  - Each part file will be approximately **75 MB to 100 MB**, matching the Medallion 100–150 MB optimal Parquet file size guideline.

### 6.3 Staging, Validation & Promotion Architecture
1. **Isolated Staging:** Writes output to `_staging_job_summary_<uuid>/` on `D:`.
2. **Pre-Swap Staging Validation:** Reads back staging files with Spark to verify exact record count (`1,297,332`).
3. **Atomic Directory Swap:** Promotes staging directory to `D:\DemandLens\data\bronze\linkedin_2024\job_summary`.
4. **Post-Swap Verification:** Validates readability, metadata columns, and schema preservation.
5. **Metadata Manifest:** Generates audit JSON in `data/bronze/linkedin_2024/_metadata/`.

---

## 7. Risks & Mitigations

| Risk | Level | Mitigation |
|---|---|---|
| **Multiline CSV Parsing Inconsistency** | Low | Verified: Read-only scan succeeded using `multiLine="true"`, `escape="\""`, finding 0 nulls across all 1.297M rows. |
| **Worker OOM during Parquet Write** | Low | Verified: Bounded batching of 10,000 rows limits batch memory to ~39.3 MB per partition worker. |
| **Driver OOM / High Collect Memory** | Low | Verified: Worker function yields only partition row counts `[row_count]`; driver never collects data rows. |
| **Spark Shuffle Temp Spill** | Very Low | `spark.local.dir` and `java.io.tmpdir` point to `D:\DemandLens\data\temp_validation\spark_tmp` with >213 GB free. |
| **Windows Socket Permission Error (WinError 10013)** | Addressed | Ingestion commands must be run outside the sandbox (`BypassSandbox: true`). |
| **Long Execution Duration** | Anticipated | Estimated execution time is **3 to 6 minutes** due to reading 4.75 GB multiline text, shuffling 16 partitions, and writing Parquet. Tool timeout should be configured appropriately. |

---

## 8. Recommended Ingestion Settings

- **CLI Ingestion Command:**
  ```powershell
  & "D:\DemandLens\.venv\Scripts\python.exe" scripts/run_bronze_ingestion.py --source linkedin_2024 --table job_summary
  ```
- **Driver Memory:** 6 GB (`spark.driver.memory=6g`)
- **Max Result Size:** 2 GB (`spark.driver.maxResultSize=2g`)
- **Target Partitions:** 16 (defined in `config/sources/linkedin_2024.json`)
- **Batch Size:** 10,000 rows / batch
- **Compression:** Snappy

---

## 9. Required Validation Plan

Upon ingestion completion, execute the following validation steps:
1. **File Footprint Check:** Confirm 16 Parquet part files (`part-00000.parquet` to `part-00015.parquet`) with non-zero sizes in `D:\DemandLens\data\bronze\linkedin_2024\job_summary`.
2. **Compression Format:** Verify `SNAPPY` codec on Parquet metadata column chunks.
3. **Row Count Fidelity:** Verify readback row count is **exactly 1,297,332** (0 row loss).
4. **Schema Preservation:**
   - Original columns: `job_link`, `job_summary`.
   - Metadata columns: `_source_id`, `_source_file`, `_ingestion_timestamp`, `_ingestion_run_id`.
5. **Null Count Audit:** Verify 0 nulls for `job_link`, `job_summary`, and `_source_id`.
6. **Previous Bronze Datasets:** Confirm `job_postings` and `job_skills` remain untouched and intact.
7. **Metadata Manifest:** Verify presence of JSON audit file in `_metadata/`.

---

## 10. Final Preflight Verdict

# ✅ READY

All preflight checks have passed without errors:
- Environment is fully configured on `D:`.
- Source file is intact, readable, and verified at **1,297,332 rows**.
- Pipeline architecture is memory-safe and ready for the 4.75 GB dataset.
- Target directory does not exist and is ready for initial creation.
- Awaiting user approval to initiate Phase 5C full ingestion.
