# DemandLens — Phase 6D.1 Engineering Report: LinkedIn Job Postings Silver Transformation

- **Dataset**: LinkedIn Job Postings 2024 (`job_postings`)
- **Pipeline Module**: `src/demandlens/silver/linkedin_postings_transformer.py`
- **Execution Script**: `scripts/run_linkedin_silver_transformation.py`
- **Test Suite**: `tests/test_linkedin_silver_transformation.py`
- **Source Path**: `data/bronze/linkedin_2024/job_postings`
- **Destination Path**: `data/silver/linkedin_2024/job_postings`
- **Manifest**: `data/silver/linkedin_2024/_metadata/silver_job_postings_silver_run_20261125_071532_8b5fdf.json`
- **Execution Date**: 2026-11-25
- **Status**: **SUCCESS**

---

## 1. Executive Summary

Phase 6D.1 successfully implemented and executed the production Silver transformation for the **LinkedIn 2024 Job Postings** dataset. 

Out of **1,348,454** Bronze records ingested, exactly **1,348,454** records were transformed, standardized, validated, and atomically promoted to the Silver Parquet destination with zero data loss and zero record duplication. 

The pipeline completed in **30.5 seconds** across 4 partitioned Snappy Parquet files (~180 MB total) by utilizing pure native Apache Spark SQL Catalyst optimization expressions coupled with a bounded-memory PyArrow committer, completely circumventing Windows JVM-to-Python IPC socket exhaustion.

### Key Pipeline Metrics

| Metric | Bronze Input | Silver Output | Delta / Notes |
| :--- | :--- | :--- | :--- |
| **Row Count** | 1,348,454 | 1,348,454 | 0 rows dropped (100.0% retention) |
| **Distinct Primary Keys (`job_link`)** | 1,348,454 | 1,348,454 | 100.0% unique primary keys |
| **Duplicates Removed** | — | 0 | No synthetic or hash deduplication needed |
| **Parquet Files** | 4 parts | 4 parts | Snappy compressed (179.98 MB) |
| **Pipeline Execution Time** | — | 30.51 s | End-to-end execution |
| **Remote Workplace Records** | — | 4,259 (0.32%) | `workplace_type = 'REMOTE'`, `is_remote = True`, `is_hybrid = False` |
| **Hybrid Workplace Records** | — | 6,562 (0.49%) | `workplace_type = 'HYBRID'`, `is_remote = False`, `is_hybrid = True` |
| **On-Site Workplace Records** | — | 1,337,633 (99.20%) | `workplace_type = 'ON_SITE'`, `is_remote = False`, `is_hybrid = False` |
| **Hybrid Contamination of Remote** | — | **0** | Strict separation invariant verified |

---

## 2. Architecture & Design Decisions

### 2.1 Pure Spark SQL Catalyst Transformation Engine
In earlier iterations on large Windows datasets, chaining Python row-by-row UDFs across 1.35 million records triggered Windows socket buffer aborts (`java.io.IOException: An established connection was aborted by the software in your host machine`).

To resolve this at the architectural level:
- Replaced all row-by-row Python UDF invocations with **pure native Spark SQL Catalyst expressions** (`pyspark.sql.functions`: `when`, `regexp_replace`, `trim`, `to_date`, `to_timestamp`).
- The entire transformation compiles into JVM bytecode via Janino code generation, maintaining high throughput and zero Python worker IPC serialization overhead.

### 2.2 Hybrid PyArrow Parquet Committer & Staging Isolation
Due to Windows filesystem semantics where Spark's default Hadoop `FileOutputCommitter` encounters `AccessDeniedException` during directory listing and atomic renames:
1. Transformations stream partition-by-partition to a temporary staging path: `data/silver/linkedin_2024/_staging_job_postings_<token>`.
2. PyArrow writes each partition with bounded memory consumption and Snappy compression.
3. Pre-promotion data quality gates evaluate row counts, schema conformity, and partition counts.
4. An atomic directory swap promotes the staging directory to `data/silver/linkedin_2024/job_postings`.
5. An audit-compliant JSON manifest is persisted to `data/silver/linkedin_2024/_metadata/`.

### 2.3 Strict Workplace Classification Invariant
A key governance mandate was preventing hybrid work arrangements from distorting remote work analytics:
- **`Remote`** $\rightarrow$ `workplace_type = 'REMOTE'`, `is_remote = True`, `is_hybrid = False`
- **`Hybrid`** $\rightarrow$ `workplace_type = 'HYBRID'`, `is_remote = False`, `is_hybrid = True`
- **`Onsite`** $\rightarrow$ `workplace_type = 'ON_SITE'`, `is_remote = False`, `is_hybrid = False`
- **Unrecognized / NULL** $\rightarrow$ `workplace_type = 'UNSPECIFIED'`, `is_remote = False`, `is_hybrid = False`

Automated assertions proved that **0** hybrid postings have `is_remote = True`.

---

## 3. Schema & Field Standardization

The resulting Silver schema retains all source information while prefixing standardized analytics attributes:

| Column | Type | Nullable | Source / Logic |
| :--- | :--- | :--- | :--- |
| `job_link` | string | False | Source primary key (100% unique) |
| `title_clean` | string | True | Trimmed, normalized whitespace, preserved special characters (C++, .NET) |
| `company_clean` | string | True | Trimmed, normalized whitespace (11 genuine nulls preserved) |
| `location_clean` | string | True | Trimmed, normalized whitespace (19 genuine nulls preserved) |
| `country_code` | string | True | Standardized ISO 3166-1 alpha-2 code (`US`, `GB`, `CA`, `AU`) |
| `workplace_type` | string | False | Enumerated: `REMOTE`, `HYBRID`, `ON_SITE`, `UNSPECIFIED` |
| `is_remote` | boolean | False | Boolean indicator strictly for 100% remote positions |
| `is_hybrid` | boolean | False | Boolean indicator strictly for hybrid positions |
| `seniority_tier` | string | True | Harmonized tier: `MID_SENIOR`, `ASSOCIATE` |
| `first_seen_date` | date | True | Parsed `yyyy-MM-dd` date |
| `last_processed_timestamp`| timestamp | True | Parsed ISO 8601 UTC timestamp |
| *(Source Columns)* | string | True | `job_title`, `company`, `job_location`, `first_seen`, `last_processed_time`, `search_city`, `search_country`, `search_position`, `job_level`, `job_type`, `got_summary`, `got_ner`, `is_being_worked` |
| *(Audit Metadata)* | string/timestamp| False | `_source_id`, `_source_file`, `_ingestion_timestamp`, `_ingestion_run_id`, `_silver_run_id`, `_silver_timestamp` |

---

## 4. Distribution & Statistical Profiling

### 4.1 Workplace Classification
| Workplace Type | Record Count | Percentage | `is_remote` | `is_hybrid` |
| :--- | :--- | :--- | :--- | :--- |
| **ON_SITE** | 1,337,633 | 99.20% | `False` | `False` |
| **HYBRID** | 6,562 | 0.49% | `False` | `True` |
| **REMOTE** | 4,259 | 0.32% | `True` | `False` |
| **Total** | 1,348,454 | 100.00% | — | — |

### 4.2 Geographic Distribution (`country_code`)
| Country | Code | Count | Percentage |
| :--- | :--- | :--- | :--- |
| United States | `US` | 1,149,342 | 85.23% |
| United Kingdom | `GB` | 113,421 | 8.41% |
| Canada | `CA` | 55,972 | 4.15% |
| Australia | `AU` | 29,719 | 2.20% |
| **Total** | | **1,348,454** | **100.00%** |

### 4.3 Seniority Harmonization (`seniority_tier`)
| Source `job_level` | Mapped `seniority_tier` | Count | Percentage |
| :--- | :--- | :--- | :--- |
| `Mid-Senior level` | `MID_SENIOR` | 1,204,445 | 89.32% |
| `Associate` | `ASSOCIATE` | 144,009 | 10.68% |
| **Total** | | **1,348,454** | **100.00%** |

### 4.4 Missingness & Fidelity
- `company_clean` retains exactly **11** genuine NULLs present in source.
- `location_clean` retains exactly **19** genuine NULLs present in source.
- Zero records fabricated, hallucinated, or imputed.
- Bronze source files verified 100% immutable and intact.

---

## 5. Verification & Automated Quality Assurance

### 5.1 Parquet Dataset Validation
Execution of `data/temp_validation/validate_phase6d1.py` confirmed:
1. `table.num_rows == 1348454` $\rightarrow$ Passed.
2. `len(unique(job_links)) == 1348454` $\rightarrow$ Passed.
3. `hybrid_table.filter(is_remote == True).count() == 0` $\rightarrow$ Passed.
4. `remote_table.filter(is_remote == True).count() == 4259` $\rightarrow$ Passed.
5. `bronze_table.num_rows == 1348454` $\rightarrow$ Passed.

### 5.2 Unit & Regression Test Suite
- `pytest tests/test_linkedin_silver_transformation.py tests/test_silver_cleaning_rules.py -v`:
  - **48 passed** in 20.13s (9 dedicated LinkedIn Silver tests, 39 core cleaning tests).
- `pytest tests/test_indian_silver_transformation.py -v`:
  - **3 passed** in 89.06s (Indian job market pipeline verified regression-free).

---

### 5.3 Memory & Execution Boundary Verification
- Spark driver memory was strictly protected: zero `toPandas()` or full-driver `collect()` calls were issued on the 1,348,454-row dataset.
- PyArrow streaming wrote each partition directly to disk in bounded chunk batches.
- Temporary staging directory was placed strictly on the `D:` drive (`D:\DemandLens\data\silver\linkedin_2024\_staging_job_postings_*`) avoiding local C: drive capacity limits.
- Staging directory validation verified file existence, partition count (4), non-empty files, and row-count exactness before atomic directory promotion.

---

## 6. Disclosed Limitations & Non-Performed Checks

In compliance with data governance and scoping guidelines:
1. **Salary Processing Intentionally Deferred**:
   - The LinkedIn `job_postings` source contains no salary fields. Empty salary columns were **not** fabricated, nor was heuristic salary parsing applied. Cross-dataset salary schema alignment is deferred to Gold modeling.
2. **Skills and Summary Joins Deferred**:
   - Foreign-key integrity validation against `job_skills` and `job_summary` was not performed in this phase; it is scheduled as part of Phase 6D.2 and Phase 6D.3.
3. **No Unsupervised NLP Classification**:
   - Workplace type classification was derived strictly and deterministically from `job_type`. No unverified heuristic or text-mining classification was used to guess workplace type for missing values.
4. **Timezone Semantics**:
   - `last_processed_time` was parsed to UTC `TimestampType` using ISO 8601 parsing (`yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][XXX]`). Postings without valid timestamps remain `NULL`.

---

## 7. Recommendations & Next Steps

1. **Phase 6D.2 Scope**: LinkedIn Skills Transformation (`job_skills`)
   - Standardize skills tokens using the existing `SkillNormalizer`.
   - Maintain referential integrity against `job_link`.
2. **Phase 6D.3 Scope**: LinkedIn Job Summary (`job_summary`)
   - Text cleaning and HTML entity removal for multiline job descriptions.
3. **Gold Layer Aggregation Preparation**:
   - The unified `workplace_type`, `country_code`, and `seniority_tier` now directly support cross-market benchmarking against Indian Job Market Silver data.

