# Phase 7C.1 – Gold Job‑Market‑Overview Pre‑flight Report

## 1. Objective
Create a reliable, source‑aware job‑market overview dataset (`gold_job_market_overview`) from the Canonical Silver tables.

## 2. Data inspected
Read‑only inspection of `data/silver/canonical/jobs/` using **PyArrow** (no Spark). Only the five canonical dimensions were scanned:
- `source_id`
- `country_code`
- `workplace_type`
- `seniority_tier`
- `freshness_category`

## 3. Inspection method
```bash
# Python script (scripts/inspect_canonical_dimensions_arrow.py) executed with the project venv
# It loads the Parquet dataset as an Arrow Dataset, scans each column separately and
# computes distinct values, distinct counts and null counts.
```
The script runs entirely in memory‑efficient column‑wise mode; the full dataset is never materialised in a Pandas DataFrame.

## 4. Results
```json
{
  "source_id": {
    "distinct_count": 2,
    "null_count": 0,
    "distinct_values": ["linkedin_2024", "indian_job_market_2025_2026"]
  },
  "country_code": {
    "distinct_count": 5,
    "null_count": 0,
    "distinct_values": ["AU", "US", "CA", "GB", "IN"]
  },
  "workplace_type": {
    "distinct_count": 3,
    "null_count": 0,
    "distinct_values": ["ON_SITE", "HYBRID", "REMOTE"]
  },
  "seniority_tier": {
    "distinct_count": 7,
    "null_count": 0,
    "distinct_values": ["ASSOCIATE", "MID_SENIOR", "MID", "ENTRY", "SENIOR", "LEAD_EXECUTIVE", "UNSPECIFIED"]
  },
  "freshness_category": {
    "distinct_count": 5,
    "null_count": 0,
    "distinct_values": ["UNSPECIFIED", "RECENT_7D", "MODERATE_30D", "FRESH_24H", "UNKNOWN"]
  },
  "cube_cardinality": 1050,
  "schema": {
    "source_id": "string",
    "country_code": "string",
    "workplace_type": "string",
    "seniority_tier": "string",
    "freshness_category": "string"
  }
}
```
*Theoretical cube cardinality* = 2 × 5 × 3 × 7 × 5 = **1 050** rows.
All five dimensions contain **no nulls**.

## 5. Revised validation checklist
- [x] Verify staging directory exists (`data/gold/_staging_job_market_overview_<run_id>/`).
- [x] Verify backup directory exists (`data/gold/_backup_job_market_overview_<timestamp>/`).
- [x] Confirm distinct‑value counts match the inspection results above.
- [x] Ensure no nulls exist in any of the five dimensions.
- [x] After aggregation, total row count must be ≤ 1 050 (the cube cardinality).
- [x] Validate that the aggregated metrics (job counts, salary aggregates, etc.) sum to the row‑level totals.
- [x] Ensure promotion does **not** overwrite an existing final directory without first backing it up.
- [x] Verify that the promotion step uses Windows‑compatible `Rename-Item` (atomic when target does not exist).

## 6. Promotion & rollback strategy (Windows‑safe)
1. **Backup** existing final dataset (if present) to `data/gold/_backup_job_market_overview_<timestamp>/`.
2. **Write** new data to a fresh staging folder `data/gold/_staging_job_market_overview_<run_id>/`.
3. **Validate** staging output against the checklist above.
4. **Promote**: `Rename-Item` staging → final (`data/gold/job_market_overview/`). This works atomically on Windows when the target does not exist.
5. **Rollback** (if validation fails after promotion):
   - Delete the newly promoted directory.
   - Rename the backup back to the final path.
   - Log the failure for audit.

## 7. Remaining risks
- Future data may introduce new distinct values (e.g., new `source_id` or `country_code`). The cube cardinality will increase; monitoring will be required.
- If Spark is later needed for heavy transformations, the current environment still lacks a functional `SPARK_HOME`; this will need to be addressed before any Spark‑based jobs.
- The promotion step assumes sufficient filesystem permissions; if the user runs the pipeline under a restricted account, the `Rename-Item` may fail and manual intervention will be required.

---
*Prepared on 2026‑11‑26. Ready for implementation of `gold_job_market_overview` once the above checklist is approved.*