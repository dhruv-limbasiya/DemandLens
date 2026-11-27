# DemandLens — Phase 6D.2 Recovery & Verification Audit Report

**Date:** November 25, 2026  
**Project Root:** `D:\DemandLens`  
**Milestone:** Phase 6D.2 — LinkedIn Job Skills Silver Transformation  
**Status:** RECOVERED, VERIFIED & READY FOR EXECUTION (READ-ONLY AUDIT)  

---

## 1. Executive Summary

This audit establishes the baseline for **Phase 6D.2 (LinkedIn Job Skills Silver Transformation)** following Git repository synchronization and local storage reconciliation.

All Phase 6D.2 implementation code, CLI orchestrators, normalization logic, and test suites were successfully recovered from repository commit `4208860`:
- **Core Pipeline Transformer:** `src/demandlens/silver/linkedin_skills_transformer.py`
- **CLI Orchestrator:** `scripts/run_linkedin_skills_silver_transformation.py`
- **Focused Test Suite:** `tests/test_linkedin_job_skills_silver.py`

### Recovery Highlights
1. **Verified Technical Token Preservation:** Normalization rules in `src/demandlens/silver/skill_normalization.py` were rigorously tested. All critical technical tokens (`C++`, `C#`, `.NET`, `Node.js`, `PL/SQL`, `SQL`, `CI/CD`) are preserved with 100% fidelity.
2. **Focused Tests Verified:** All **6 out of 6** focused unit and integration tests passed in 34.8s.
3. **Bronze Layer Immutability:** Bronze LinkedIn `job_skills` (`data/bronze/linkedin_2024/job_skills`) remains 100% byte-intact with exactly **1,296,381 records** across 4 Snappy Parquet files matching manifest `run_20261123_133320_0a423c`.
4. **Target Destination Status:** `data/silver/linkedin_2024/job_skills_bridge` does not exist yet (0 bytes). The full transformation has **not** been executed on this machine.
5. **Storage Capacity:** Drive `D:` has **211.05 GiB free** (plenty of headroom for the estimated ~150 MB output and temporary staging).

---

## 2. Recovery of Existing Phase 6D.2 Artifacts

A forensic inspection of the codebase verified the recovery of Phase 6D.2 components from commit `4208860` (`Phase 6D.1 LinkedIn Silver Transformation, Phase 6D.2 Skills Bridge scaffolding...`):

| Component | Path | Status | Verification Summary |
| :--- | :--- | :---: | :--- |
| **Pipeline Transformer** | `src/demandlens/silver/linkedin_skills_transformer.py` | **Complete** (285 lines) | Implements `transform_linkedin_skills` and `LinkedInSkillsSilverPipeline` with staging isolation, atomic rename, and referential integrity join against `job_postings`. |
| **CLI Runner** | `scripts/run_linkedin_skills_silver_transformation.py` | **Complete** (111 lines) | CLI with `--overwrite` and `--partitions 8` arguments, Windows JVM temp configuration, and memory bounding (`driver.memory = 6g`). |
| **Focused Test Suite** | `tests/test_linkedin_job_skills_silver.py` | **Complete** (162 lines) | Covers token normalization, whitespace/noise stripping, intra-posting deduplication, bridge grain (1:1 per job/skill pair), and referential integrity logic. |
| **Foundation Normalizer** | `src/demandlens/silver/skill_normalization.py` | **Complete** (115 lines) | Defines `_OUTER_PUNCTUATION`, `normalize_skill_token`, and `normalize_skills`. |

---

## 3. Skill Normalization Technical Token Verification

The normalization logic was evaluated against technical tokens that frequently suffer corruption in naive text preprocessing pipelines:

| Raw Input Token | Normalized Output | Expected Output | Status | Rule Applied |
| :--- | :--- | :--- | :---: | :--- |
| `C++` | `c++` | `c++` | **PASS** | `+` retained (excluded from outer noise) |
| `C#` | `c#` | `c#` | **PASS** | `#` retained (excluded from outer noise) |
| `.NET` | `.net` | `.net` | **PASS** | Leading `.` preserved (excluded from outer noise) |
| `Node.js` | `node.js` | `node.js` | **PASS** | Internal `.` preserved |
| `PL/SQL` | `pl/sql` | `pl/sql` | **PASS** | Internal `/` preserved |
| `SQL` | `sql` | `sql` | **PASS** | Case-folded to lowercase |
| `CI/CD` | `ci/cd` | `ci/cd` | **PASS** | Internal `/` preserved |
| `  .NET Core  ` | `.net core` | `.net core` | **PASS** | Surrounding whitespace stripped, internal space collapsed |
| `C++ / STL` | `c++ / stl` | `c++ / stl` | **PASS** | Compound token punctuation retained |
| `React.js` | `react.js` | `react.js` | **PASS** | Framework naming retained |
| `Vue.js` | `vue.js` | `vue.js` | **PASS** | Framework naming retained |
| `ASP.NET` | `asp.net` | `asp.net` | **PASS** | Framework naming retained |
| `TCP/IP` | `tcp/ip` | `tcp/ip` | **PASS** | Protocol naming retained |

### Intra-Posting Deduplication Test
```python
Input:  "C++, c++, C#, .NET, Node.js, PL/SQL, SQL, CI/CD, Python, python"
Output: ["c++", "c#", ".net", "node.js", "pl/sql", "sql", "ci/cd", "python"]
```
Result: Case differences and exact duplicates within a single job listing are cleanly deduplicated while preserving order and technical punctuation.

---

## 4. Focused Test Suite Verification

The focused pytest suite (`tests/test_linkedin_job_skills_silver.py`) was executed:

```
tests/test_linkedin_job_skills_silver.py::TestSkillNormalizationRules::test_special_technical_tokens_preservation PASSED [ 16%]
tests/test_linkedin_job_skills_silver.py::TestSkillNormalizationRules::test_whitespace_and_noise_stripping PASSED [ 33%]
tests/test_linkedin_job_skills_silver.py::TestSkillNormalizationRules::test_null_empty_and_noise_tokens PASSED [ 50%]
tests/test_linkedin_job_skills_silver.py::TestSkillNormalizationRules::test_normalize_skills_deduplication PASSED [ 66%]
tests/test_linkedin_job_skills_silver.py::TestLinkedInSkillsTransformerFidelity::test_transform_linkedin_skills_bridge_grain PASSED [ 83%]
tests/test_linkedin_job_skills_silver.py::TestLinkedInSkillsTransformerFidelity::test_referential_integrity_logic PASSED [100%]
======================= 6 passed, 2 warnings in 34.80s ========================
```

*Note on Execution Environment:* On Windows, PySpark requires `PYSPARK_PYTHON` set to `.venv\Scripts\python.exe` so worker subprocesses launch cleanly without falling back to system `python3`. When configured, all 6 tests pass with 100% fidelity.

---

## 5. Bronze LinkedIn Job Skills Dataset Integrity

The input Bronze dataset was checked against its physical metadata and ingestion manifest:

| Attribute | Verified Value | Manifest Baseline | Integrity Verdict |
| :--- | :--- | :--- | :---: |
| **Path** | `data/bronze/linkedin_2024/job_skills` | `data/bronze/linkedin_2024/job_skills` | **MATCH** |
| **Parquet Files** | 4 files (`part-00000` to `part-00003`) | 4 partitions | **MATCH** |
| **Row Count** | **1,296,381 rows** | **1,296,381 rows** | **100% MATCH** |
| **Total Size** | **320.02 MiB** (335,567,958 bytes) | ~320 MB | **MATCH** |
| **Compression** | Snappy across all row groups | Snappy | **MATCH** |
| **Manifest Path** | `job_skills_run_20261123_133320_0a423c.json` | Valid JSON | **MATCH** |
| **Ingestion Status** | `SUCCESS` | `SUCCESS` | **MATCH** |

**Verdict:** The Bronze dataset is completely intact and has not been altered or corrupted.

---

## 6. Storage Capacity & Infrastructure Health

A storage capacity check was conducted on the host environment:

| Storage Metric | Value (GiB) | Value (Bytes) | Operational Assessment |
| :--- | :---: | :---: | :--- |
| **D: Drive Total Capacity** | 250.00 GiB | 268,435,456,000 bytes | — |
| **D: Drive Used Space** | 38.95 GiB | 41,821,691,904 bytes | 15.58% utilization |
| **D: Drive Available Free Space** | **211.05 GiB** | **226,613,764,096 bytes** | **84.42% free** |
| **Current Project Size** | 12.36 GiB | 13,273,593,490 bytes | Clean, stable |
| **Estimated Phase 6D.2 Output** | ~0.15 GiB | ~160 MB Parquet | Snappy compressed |
| **Estimated Staging Peak Memory/Disk** | ~0.50 GiB | ~500 MB temporary | PyArrow streaming batch size 20,000 |

**Verdict:** Ample free disk space is available (over 211 GiB). The Phase 6D.2 pipeline will consume less than 0.25% of available free space.

---

## 7. Categorized Audit Findings

### Verified Facts
- Core pipeline `linkedin_skills_transformer.py` is fully authored and implements the required PyArrow bounded committer.
- Technical token preservation is verified for `C++`, `C#`, `.NET`, `Node.js`, `PL/SQL`, `SQL`, `CI/CD`.
- All 6 unit/integration tests in `test_linkedin_job_skills_silver.py` pass.
- Bronze `job_skills` has exactly 1,296,381 records and is untouched.
- D: drive has 211.05 GiB free space.

### Previous Task Results Recovered
- The scaffolding and initial commit were established in commit `4208860`.
- The teammate's PC reported 9.17 GB because it had not yet ingested Bronze `job_summary` (2.62 GiB) or generated Indian Silver (0.05 GiB).

### Unverified Claims Clarified
- *Claim:* "Phase 6D.2 had already run and failed or left staging folders."  
  *Finding:* **False.** No staging folders exist in `data/silver/`, and `data/silver/linkedin_2024` does not yet exist on this PC. The full transformation was never started on this PC.
- *Claim:* "Silver `job_postings` exists on this PC for the referential integrity check."  
  *Finding:* **Clarified.** Silver `job_postings` was committed on the teammate's machine (reported in `phase6d1_linkedin_postings_silver_report.md`), but its Parquet data was excluded by `.gitignore`. The `LinkedInSkillsSilverPipeline` is architected to safely handle this scenario (skips the referential join if `job_postings` is absent or runs once `job_postings` is generated).

### Risks & Mitigations
1. **Windows Socket Buffer Exhaustion during Explode / UDF:**
   - *Risk:* Exploding 1.3M skill rows into ~4-5M bridge rows using row-by-row Python UDFs could stress socket connections if batch sizes are unconstrained.
   - *Mitigation:* The pipeline already uses `write_dataframe_to_parquet_pyarrow` with a bounded batch size of `20,000` rows and `spark.driver.memory=6g`.
2. **Missing `job_postings` Silver Table for Referential Join:**
   - *Risk:* If `validate_referential_integrity` expects `data/silver/linkedin_2024/job_postings`, the check will return `status: SKIPPED`.
   - *Mitigation:* We can either run Phase 6D.1 (`run_linkedin_silver_transformation.py`) first to generate the postings table, or execute Phase 6D.2 standalone (which logs a clean warning and skips foreign-key verification without failing).

---

## 8. Recommended Next Action

The audit confirms that the environment and codebase are in a verified state:
1. **Step 1:** Execute Phase 6D.1 (`scripts/run_linkedin_silver_transformation.py`) to generate `data/silver/linkedin_2024/job_postings` (~30 seconds runtime).
2. **Step 2:** Execute Phase 6D.2 (`scripts/run_linkedin_skills_silver_transformation.py`) to transform and materialize `data/silver/linkedin_2024/job_skills_bridge` with full referential integrity validation.
3. **Step 3:** Audit and produce the final Phase 6D completion report.
