# DemandLens — Gold Layer Architecture & Analytical Design Specification

**Document Version:** 1.0.0  
**Phase:** 7A (Gold Analytics Design & Preflight)  
**Status:** APPROVED DESIGN  
**Date:** 2026-11-26  
**Project:** `D:\DemandLens`  
**Pipeline Stage:** Canonical Silver (`data/silver/canonical/`) → Gold (`data/gold/`) → Analytics Dashboard / RAG  

---

## 1. Executive Overview & Design Principles

The DemandLens Gold Layer serves as the optimized, business-ready analytical data plane. While the Canonical Silver Layer standardized schemas, normalized technical tokens, and decoupled text across 1,446,133 job records, the Gold Layer transforms these transactional dimensions into aggregated, query-optimized OLAP models.

The primary consumers of the Gold Layer are:
1. **DemandLens Executive Analytics Dashboard:** Sub-second interactive slicing across labor demand, skills, compensation, and workplace trends.
2. **Career Intelligence & Student Advisory Portal:** Actionable pathways mapping technical skills to market demand and entry-level viability.
3. **Future RAG Embeddings & Context Retrieval:** Semantic indexing of curated role profiles and skill combinations.

### Core Architectural Principles:
- **No Direct Raw Cross-Source Aggregation:** Western (LinkedIn 2024) and Indian (2025–2026) labor pools represent orthogonal geographies, time periods, and industries. Gold tables must enforce strict `source_id` and `country_code` partitioning or grouping.
- **Relative Prevalence Over Absolute Counts:** Skills intelligence must present percentage prevalence ($\frac{\text{postings with skill}}{\text{total postings in segment}}$) to eliminate false conclusions driven by unequal sample sizes ($1.35\text{M}$ vs $98\text{K}$).
- **Compensation Integrity:** Salary metrics must strictly filter `salary_disclosed = True`, exclude missing/zero values, and isolate currency/periodicity to avoid mixing Indian Rupees with US Dollars or monthly stipends with annual compensation.
- **Bounded Cardinality & Dimensional Decoupling:** Long-tail skill strings (>2.7M tokens) and raw job titles (>600K strings) must be structured via standardized role families and frequency thresholds to prevent combinatorial explosion.

---

## 2. Source Canonical Silver Baseline

The Gold Layer is built exclusively from the three verified Canonical Silver tables:

| Source Entity | Physical Path | Actual Rows | Size | Grain | Primary / Composite Keys |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **`canonical_jobs`** | `data/silver/canonical/jobs` | 1,446,133 | 130.35 MB | 1 row per job posting | `canonical_job_id` |
| **`canonical_job_skills_bridge`** | `data/silver/canonical/job_skills_bridge` | 27,533,229 | 1,419.02 MB | 1 row per `(canonical_job_id, skill)` edge | `(canonical_job_id, skill)` |
| **`canonical_job_text`** | `data/silver/canonical/job_text` | 1,395,011 | 2,715.59 MB | 1 row per job with text description | `canonical_job_id` |

---

## 3. Standardized Role Taxonomy & Taxonomy Normalization

With **583,998 unique job titles in LinkedIn** and **54,997 in the Indian dataset**, direct grouping by raw title leads to extreme data sparsity. To enable robust comparative analytics, Phase 7 establishes a deterministic **Standardized Role Taxonomy**:

```
                                  Standardized Role Families
                                               │
      ┌─────────────────┬──────────────────────┼──────────────────────┬─────────────────┐
      │                 │                      │                      │                 │
Data & AI         Software Eng        Cloud & DevOps          Business/Sales      Operations/Other
- Data Engineer   - Full Stack Dev    - Cloud Architect       - Account Exec      - Store Manager
- Data Scientist  - Backend Dev       - DevOps Engineer       - Sales Lead        - Shift Lead
- ML Engineer     - Frontend Dev      - SRE                   - BDM               - Nurse
- BI Analyst      - Mobile Dev (iOS/And)- Sysadmin            - Marketing Spec    - Customer Rep
```

### Deterministic Keyword Mapping Rules:
- **`DATA_ENGINEER`**: Title matches `%(data engineer|etl|big data|data pipeline)%`.
- **`DATA_SCIENTIST_ML`**: Title matches `%(data scientist|machine learning|ml engineer|ai engineer|deep learning)%`.
- **`DATA_ANALYST_BI`**: Title matches `%(data analyst|business intelligence|bi developer|tableau developer|power bi)%`.
- **`SOFTWARE_ENGINEER`**: Title matches `%(software engineer|software developer|sde|application developer|java developer|python developer|c\+\+ developer)%`.
- **`FRONTEND_DEVELOPER`**: Title matches `%(frontend|front end|ui developer|react developer|angular developer|vue developer)%`.
- **`BACKEND_DEVELOPER`**: Title matches `%(backend|back end|node developer|django developer|spring boot developer)%`.
- **`FULLSTACK_DEVELOPER`**: Title matches `%(full stack|fullstack|full-stack)%`.
- **`CLOUD_DEVOPS_ENGINEER`**: Title matches `%(devops|cloud engineer|platform engineer|site reliability|sre|aws engineer|azure engineer)%`.
- **`PRODUCT_PROJECT_MANAGER`**: Title matches `%(product manager|technical program manager|project manager|scrum master)%`.
- **`SALES_BUSINESS_DEV`**: Title matches `%(sales|business development|account executive|account manager)%`.
- **`CUSTOMER_SERVICE_SUPPORT`**: Title matches `%(customer service|customer support|call center|help desk)%`.
- **`OPERATIONS_MANAGEMENT`**: Title matches `%(operations manager|general manager|shift manager|store manager|assistant manager)%`.
- **`OTHER_SPECIALIZED`**: Fallback for titles not captured by major tech/business patterns.

---

## 4. Proposed Gold Dataset Specifications

Eight targeted Gold models are designed to answer specific analytical questions without data duplication.

```
data/gold/
├── job_market_overview/             # Macro KPI cube (source, country, workplace, seniority)
├── job_demand_by_location/          # Geographic labor distribution & remote adoption
├── job_demand_by_company/           # Corporate hiring rankings & workplace policies
├── skill_demand_overall/            # Global/Country skill rankings & relative prevalence
├── role_skill_matrix/               # Competency requirements by standardized role
├── skill_cooccurrence/              # Technical skill bundles & pairing affinities
├── salary_analytics_india/          # Indian compensation benchmarks & experience curves
├── career_pathways_entry_level/     # Student & entry-level job-seeker market intelligence
└── _metadata/                       # Manifests, lineage records, and quality audit logs
```

---

### 4.1 Dataset 1: `gold_job_market_overview`
- **Purpose:** High-level executive KPI cube powering the dashboard's summary cards, dynamic filters, and macro trend cards.
- **Grain:** `(source_id, country_code, workplace_type, seniority_tier, freshness_category)`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `country_code` (`string`, PK component)
  - `workplace_type` (`string`, PK component)
  - `seniority_tier` (`string`, PK component)
  - `freshness_category` (`string`, PK component)
  - `total_postings` (`integer`)
  - `remote_postings` (`integer`)
  - `hybrid_postings` (`integer`)
  - `onsite_postings` (`integer`)
  - `postings_with_salary_disclosed` (`integer`)
  - `postings_with_skills` (`integer`)
  - `postings_with_description` (`integer`)
  - `avg_skills_per_posting` (`double`)
  - `_gold_timestamp` (`timestamp`)
- **Source:** `canonical_jobs`
- **Aggregation:** Direct group-by aggregation across primary categorical dimensions.
- **Filtering:** None (full universe representation).
- **Expected Volume:** ~2,500 rows (< 200 KB).
- **Partitioning:** Unpartitioned (single Parquet file).

---

### 4.2 Dataset 2: `gold_job_demand_by_location`
- **Purpose:** Regional labor demand mapping, top hiring metropolitan areas, and local remote-work penetration.
- **Grain:** `(source_id, country_code, location_clean)`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `country_code` (`string`, PK component)
  - `location_clean` (`string`, PK component)
  - `posting_count` (`integer`)
  - `pct_of_country_postings` (`double`)
  - `remote_posting_count` (`integer`)
  - `remote_penetration_pct` (`double`)
  - `hybrid_posting_count` (`integer`)
  - `hybrid_penetration_pct` (`double`)
  - `top_seniority_tier` (`string`)
  - `_gold_timestamp` (`timestamp`)
- **Source:** `canonical_jobs`
- **Filtering:** `location_clean IS NOT NULL` and `posting_count >= 5`.
- **Expected Volume:** ~15,000 rows (~500 KB).
- **Partitioning:** Partitioned by `country_code`.

---

### 4.3 Dataset 3: `gold_job_demand_by_company`
- **Purpose:** Corporate hiring rankings, employer hiring concentration, and employer-specific workplace flexibility.
- **Grain:** `(source_id, country_code, company_name_clean)`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `country_code` (`string`, PK component)
  - `company_name_clean` (`string`, PK component)
  - `total_job_postings` (`integer`)
  - `remote_jobs_count` (`integer`)
  - `hybrid_jobs_count` (`integer`)
  - `onsite_jobs_count` (`integer`)
  - `top_role_family` (`string`)
  - `top_location` (`string`)
  - `_gold_timestamp` (`timestamp`)
- **Source:** `canonical_jobs` (joined with role classification)
- **Filtering:** `company_name_clean IS NOT NULL` and `total_job_postings >= 3`.
- **Expected Volume:** ~40,000 rows (~1.2 MB).
- **Partitioning:** Partitioned by `source_id`.

---

### 4.4 Dataset 4: `gold_skill_demand_overall`
- **Purpose:** Definitive skills intelligence ranking. Provides skill demand counts, frequency rank, and relative prevalence within each platform/country market.
- **Grain:** `(source_id, country_code, skill)`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `country_code` (`string`, PK component)
  - `skill` (`string`, PK component)
  - `posting_count` (`integer`)
  - `total_market_postings` (`integer`)
  - `prevalence_pct` (`double`, $\frac{\text{posting\_count}}{\text{total\_market\_postings}} \times 100$)
  - `market_skill_rank` (`integer`)
  - `_gold_timestamp` (`timestamp`)
- **Sources:** `canonical_job_skills_bridge` joined with pre-aggregated segment totals from `canonical_jobs`.
- **Filtering:** `posting_count >= 10` (eliminates singular unstandardized strings).
- **Expected Volume:** ~75,000 rows (~2.5 MB).
- **Partitioning:** Partitioned by `country_code`.

---

### 4.5 Dataset 5: `gold_role_skill_matrix`
- **Purpose:** Core career intelligence engine. Maps standardized roles to top required technical skills, identifying must-have vs optional competencies.
- **Grain:** `(source_id, standardized_role, skill)`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `standardized_role` (`string`, PK component)
  - `skill` (`string`, PK component)
  - `skill_occurrences` (`integer`)
  - `total_role_postings` (`integer`)
  - `requirement_frequency_pct` (`double`, $\frac{\text{occurrences}}{\text{role\_postings}} \times 100$)
  - `role_skill_rank` (`integer`)
  - `_gold_timestamp` (`timestamp`)
- **Sources:** `canonical_jobs` (with role classification) joined to `canonical_job_skills_bridge`.
- **Filtering:** `skill_occurrences >= 5` within each standardized role.
- **Expected Volume:** ~50,000 rows (~1.8 MB).
- **Partitioning:** Partitioned by `source_id`.

---

### 4.6 Dataset 6: `gold_skill_cooccurrence`
- **Purpose:** Skill bundling and technology ecosystem analysis. Informs students and professionals which tools are complementary (e.g. Python + SQL vs Python + PyTorch).
- **Grain:** `(source_id, skill_a, skill_b)` where `skill_a < skill_b`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `skill_a` (`string`, PK component)
  - `skill_b` (`string`, PK component)
  - `cooccurrence_count` (`integer`)
  - `skill_a_total` (`integer`)
  - `skill_b_total` (`integer`)
  - `jaccard_similarity` (`double`, $\frac{\text{cooccurrence}}{\text{skill\_a\_total} + \text{skill\_b\_total} - \text{cooccurrence}}$)
  - `_gold_timestamp` (`timestamp`)
- **Sources:** Self-join of `canonical_job_skills_bridge` on `canonical_job_id` with `skill_a < skill_b`.
- **Filtering:** Top 300 technical skills per source; `cooccurrence_count >= 15`. Prevents $O(N^2)$ combinatorial explosion across 2.7M tokens.
- **Expected Volume:** ~45,000 rows (~1.5 MB).
- **Partitioning:** Partitioned by `source_id`.

---

### 4.7 Dataset 7: `gold_salary_analytics_india`
- **Purpose:** Compensation intelligence across job roles, experience levels, and technology skills. Strictly constrained to valid Indian market data.
- **Grain:** `(standardized_role, seniority_tier, location_clean)`
- **Schema:**
  - `standardized_role` (`string`, PK component)
  - `seniority_tier` (`string`, PK component)
  - `location_clean` (`string`, PK component)
  - `sample_size` (`integer`)
  - `avg_experience_min_years` (`double`)
  - `avg_experience_max_years` (`double`)
  - `salary_min_p25_inr` (`double`)
  - `salary_min_median_inr` (`double`)
  - `salary_mid_mean_inr` (`double`)
  - `salary_mid_median_inr` (`double`)
  - `salary_max_median_inr` (`double`)
  - `salary_max_p75_inr` (`double`)
  - `salary_currency` (`string`, constant `'INR'`)
  - `salary_period` (`string`, constant `'ANNUAL'`)
  - `_gold_timestamp` (`timestamp`)
- **Source:** `canonical_jobs`
- **Filtering:**
  ```sql
  WHERE source_id = 'indian_job_market_2025_2026'
    AND salary_disclosed = True
    AND salary_midpoint IS NOT NULL
    AND salary_currency = 'INR'
    AND salary_period = 'ANNUAL'
    AND sample_size >= 3
  ```
- **Expected Volume:** ~3,500 rows (~250 KB).
- **Partitioning:** Unpartitioned (single Parquet file).

---

### 4.8 Dataset 8: `gold_career_pathways_entry_level`
- **Purpose:** Actionable student and career switcher portal. Highlights entry-level hiring hubs, high-yield starter skills, and realistic starting roles.
- **Grain:** `(source_id, country_code, standardized_role, skill)`
- **Schema:**
  - `source_id` (`string`, PK component)
  - `country_code` (`string`, PK component)
  - `standardized_role` (`string`, PK component)
  - `skill` (`string`, PK component)
  - `entry_posting_count` (`integer`)
  - `role_entry_total` (`integer`)
  - `skill_entry_prevalence_pct` (`double`)
  - `remote_entry_pct` (`double`)
  - `entry_skill_rank` (`integer`)
  - `_gold_timestamp` (`timestamp`)
- **Sources:** Filtered join of `canonical_jobs` and `canonical_job_skills_bridge`.
- **Filtering:**
  - LinkedIn: `seniority_tier IN ('ENTRY', 'ASSOCIATE')`.
  - Indian: `seniority_tier = 'ENTRY' OR experience_min_years <= 2.0`.
  - `entry_posting_count >= 5`.
- **Expected Volume:** ~20,000 rows (~800 KB).
- **Partitioning:** Partitioned by `country_code`.

---

## 5. Cross-Source Comparability & Analytical Guardrails

### 5.1 Incomparability Matrix

| Dimension | LinkedIn 2024 (Western) | Indian Job Market 2025–2026 | Analytical Guardrail & Gold Directive |
| :--- | :--- | :--- | :--- |
| **Temporal Horizon** | Early 2024 scrape | Late 2025 / Early 2026 scrape | **Temporal Asymmetry:** Never combine in time-series trend lines. Treat as separate market periods. |
| **Geographic Scope** | US (85.2%), GB (8.4%), CA (4.1%), AU (2.2%) | 100% India (`IN`) | **Geographic Orthogonality:** Mandatory `country_code` filtering on all UI visualizations. |
| **Industry Breadth** | Omni-economy (retail, nursing, sales, hospitality, tech) | Heavily skewed towards IT, software development, IT services | **Sampling Skew:** Do not compare global job volumes. Highlight tech role concentrations separately. |
| **Salary Data** | 0% disclosed (100% NULL) | 34.0% disclosed in INR | **Compensation Guardrail:** LinkedIn excluded from all salary aggregations. Indian salaries never converted using static forex without clear caveat. |
| **Seniority vs Exp** | Categorical tiers only (`job_level`) | Quantitative years (`experience_min/max_years`) | **Experience Guardrail:** Numeric experience queries apply strictly to Indian records. Seniority queries use harmonized categorical tier. |
| **Workplace Mode** | Strict 3-way split: Remote (0.3%), Hybrid (0.5%), On-Site (99.2%) | Binary: Remote (7.9%), On-Site (92.1%) | **Workplace Guardrail:** Hybrid metrics are zero for Indian data; remote metrics are comparable as pure remote percentage. |
| **Posting Freshness**| Calendar date (`first_seen_date`) | Categorical tier (`freshness_category`) | **Freshness Guardrail:** Freshness queries rely on tier categories (`FRESH_24H`, `RECENT_7D`, etc.). |

### 5.2 The Relative Prevalence Mandate
Because LinkedIn contains 1,348,454 jobs and the Indian dataset contains 97,679 jobs, summing raw counts creates a **13.8:1 distortion**. 
Any cross-source comparison must utilize **Relative Prevalence**:
$$\text{Relative Prevalence}_{\text{skill}, \text{segment}} = \frac{\text{Postings requiring skill in segment}}{\text{Total postings in segment}} \times 100\%$$
This allows fair comparisons such as: *"Python appears in 12.3% of Indian tech postings vs 10.8% of US tech postings."*

---

## 6. Resolution of Salary Classification Discrepancy

### 6.1 Forensic Root Cause
The Phase 6D.4 Final Audit noted:
- 33,144 salary-disclosed Indian records.
- 64,252 salary-undisclosed Indian records.
- Sum: $33,144 + 64,252 = 97,396$, leaving **283 records** unaccounted for out of the 97,679 total Indian jobs.

Forensic examination of `canonical_jobs` and the raw source Silver data revealed:
1. `salary_disclosed` is a boolean indicator defined in Catalyst as:
   `salary_disclosed = (salary_min IS NOT NULL OR salary_max IS NOT NULL)`
2. Full physical scan of all 97,679 Indian records in `canonical_jobs`:
   - `salary_disclosed == True`: **33,144** records.
   - `salary_disclosed == False`: **64,535** records.
   - Sum: $33,144 + 64,535 = \mathbf{97,679}$ ($100.0\%$ match).
3. **The 283-record composition:**
   - 238 records have `salary_currency = 'INR'` and `salary_period = 'MONTHLY'`, but `salary_min` and `salary_max` are `NULL`.
   - 45 records have `salary_currency = 'INR'` and `salary_period = 'ANNUAL'`, but `salary_min` and `salary_max` are `NULL`.
   - Sum: $238 + 45 = \mathbf{283}$.
   - In the source Silver parser, raw strings such as `'5,000/month'`, `'10,000/month'` (internship stipends) matched the keyword regex for periodicity (`'MONTHLY'`), but failed to parse into standardized numeric min/max Lacs ranges.
   - The previous audit misidentified `64,252` (the count of records where `salary_period IS NULL`) as the total undisclosed count, overlooking the 283 records with parsed periodicity but null numeric bounds.
4. **Resolution Status:** **FULLY RESOLVED**.
   - Zero records are unclassified.
   - Every single record in `canonical_jobs` has a non-null boolean `salary_disclosed`.
   - The 283 records are properly, safely, and correctly classified as `salary_disclosed = False`.

---

## 7. Gold Layer Architecture & Operational Framework

### 7.1 Physical Organization
```
data/gold/
├── job_market_overview/             # ~2,500 rows, 1 part file (~150 KB)
├── job_demand_by_location/          # ~15,000 rows, 5 part files (~500 KB)
├── job_demand_by_company/           # ~40,000 rows, 2 part files (~1.2 MB)
├── skill_demand_overall/            # ~75,000 rows, 5 part files (~2.5 MB)
├── role_skill_matrix/               # ~50,000 rows, 2 part files (~1.8 MB)
├── skill_cooccurrence/              # ~45,000 rows, 2 part files (~1.5 MB)
├── salary_analytics_india/          # ~3,500 rows, 1 part file (~250 KB)
├── career_pathways_entry_level/     # ~20,000 rows, 5 part files (~800 KB)
└── _metadata/                       # Manifests, lineage records, and quality audit logs
```

### 7.2 Storage Format & Optimization
- **Format:** Apache Parquet with Snappy compression.
- **Row Groups:** Sized to match target partition sizes (single row group per file).
- **Dictionary Encoding:** Enabled for all categorical columns (`source_id`, `country_code`, `standardized_role`, `seniority_tier`, `workplace_type`).
- **Statistics:** Full min/max and null-count statistics computed on all columns to enable Parquet predicate pushdown in DuckDB and Spark.

### 7.3 Staging & Promotion Strategy
Following the battle-tested Phase 6D pattern:
1. Write each Gold table to an isolated temporary staging directory: `data/gold/_staging_<dataset>_<uuid>/`.
2. Execute automated pre-promotion validation (row count non-zero, schema conformance, primary key uniqueness, null threshold checks).
3. If valid, atomically promote staging to destination: `shutil.rmtree(dest); shutil.move(stg, dest)`.
4. If invalid, purge staging directory and abort without modifying existing target.
5. Commit run manifest to `data/gold/_metadata/gold_execution_manifest_<run_id>.json`.

---

## 8. Resource & Capacity Planning

| Dataset Name | Input Dependencies | Projected Rows | Projected Partitions | Estimated Disk Footprint | Peak Driver RAM | Target Processing Duration |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| `job_market_overview` | `canonical_jobs` | 2,500 | 1 | 150 KB | < 1.0 GB | ~15s |
| `job_demand_by_location` | `canonical_jobs` | 15,000 | 5 | 500 KB | < 1.0 GB | ~20s |
| `job_demand_by_company` | `canonical_jobs` | 40,000 | 2 | 1.2 MB | < 1.2 GB | ~30s |
| `skill_demand_overall` | `canonical_jobs`, `canonical_job_skills_bridge` | 75,000 | 5 | 2.5 MB | < 2.5 GB | ~90s |
| `role_skill_matrix` | `canonical_jobs`, `canonical_job_skills_bridge` | 50,000 | 2 | 1.8 MB | < 2.5 GB | ~75s |
| `skill_cooccurrence` | `canonical_job_skills_bridge` | 45,000 | 2 | 1.5 MB | < 3.0 GB | ~120s |
| `salary_analytics_india` | `canonical_jobs` | 3,500 | 1 | 250 KB | < 1.0 GB | ~20s |
| `career_pathways_entry_level` | `canonical_jobs`, `canonical_job_skills_bridge` | 20,000 | 5 | 800 KB | < 2.0 GB | ~60s |
| **Total Gold Layer** | - | **~251,000** | **23** | **~8.7 MB** | **< 3.5 GB** | **~7.2 min** |

**Total Gold Storage Overhead:** Less than **15 MB** on disk, representing negligible footprint against Drive `D:`'s **202.74 GiB** available space.

---

## 9. Gold Dependency Graph

```
[Canonical Silver Layer]
   │
   ├────────────────────────────────────────────────────────────────────────┐
   ▼                                                                        ▼
canonical_jobs (1.45M rows)                              canonical_job_skills_bridge (27.53M rows)
   │                                                                        │
   ├──────────────────────────────┬──────────────────────────────┐          │
   │                              │                              │          │
   ▼                              ▼                              ▼          ▼
[Phase 7B: Foundation Layer]      │                              │          │
gold_role_classified_jobs (View)  │                              │          │
   │                              │                              │          │
   ├──────────────────────────────┼──────────────────────────────┼──────────┤
   │                              │                              │          │
   ▼                              ▼                              │          │
[Phase 7C: Job Market]     [Phase 7E: Salary]                    │          │
- gold_job_market_overview - gold_salary_analytics_india         │          │
- gold_job_demand_by_loc                                         │          │
- gold_job_demand_by_comp                                        │          │
   │                                                             │          │
   ├─────────────────────────────────────────────────────────────┴──────────┤
   ▼                                                                        ▼
[Phase 7D: Skills Intelligence]                                   [Phase 7D: Bundling]
- gold_skill_demand_overall                                       - gold_skill_cooccurrence
- gold_role_skill_matrix                                                    │
   │                                                                        │
   └──────────────────────────────────────┬─────────────────────────────────┘
                                          ▼
                             [Phase 7F: Career Intelligence]
                             - gold_career_pathways_entry_level
                                          │
                                          ▼
                             [DemandLens Streamlit Dashboard & RAG]
```

---

## 10. Phased Implementation Roadmap

1. **Phase 7A: Gold Analytics Design & Preflight** *(Current Phase — Complete & Approved)*
2. **Phase 7B: Gold Foundation & Role Standardization Engine**
   - Implement `RoleStandardizer` UDF and role family classification rules.
   - Build base staging pipeline and shared utilities.
3. **Phase 7C: Job Market & Macro Analytics Implementation**
   - Build and promote: `gold_job_market_overview`, `gold_job_demand_by_location`, `gold_job_demand_by_company`.
4. **Phase 7D: Skills Intelligence & Co-occurrence Matrix Implementation**
   - Build and promote: `gold_skill_demand_overall`, `gold_role_skill_matrix`, `gold_skill_cooccurrence`.
5. **Phase 7E: Compensation Analytics (India Focus)**
   - Build and promote: `gold_salary_analytics_india` with strict currency/periodicity guardrails.
6. **Phase 7F: Career Intelligence & Student Pathways**
   - Build and promote: `gold_career_pathways_entry_level`.
7. **Phase 7G: End-to-End Gold Layer Validation & Forensic Audit**
   - Full acceptance criteria verification, cross-table audit, and dashboard query benchmarking.
