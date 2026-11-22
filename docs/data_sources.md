# DemandLens — Data Sources Specification & Audit

This document provides a comprehensive analysis of the two initial data sources selected for **DemandLens: A Trustworthy Job-Market Intelligence Platform**. It outlines their characteristics, schemas, licensing, known limitations, and explicitly separates **verified facts** from **unverified assumptions**.

---

## 1. Source A: Historical Dataset (LinkedIn 2024)

### 1.1 Metadata
- **Dataset Name:** 1.3M LinkedIn Jobs & Skills (2024)
- **Kaggle URL:** [https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024](https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024)
- **Publisher / Author:** Asaniczka (Community Contributor)
- **Collection Source:** Scraped from LinkedIn job postings during 2024
- **License:** Open Data Commons Attribution License (ODC-By v1.0)
  - *Permitted Use:* Commercial and non-commercial analysis, distribution, and modification, provided attribution to the original author is maintained.

### 1.2 Expected & Verified File Structure
The dataset is distributed as a ZIP archive containing three relational CSV files linked via a primary/foreign key:

| File Name | Estimated Uncompressed Size | Expected Record Count | Primary/Foreign Key | Description |
|---|---|---|---|---|
| `linkedin_job_postings.csv` | ~400–600 MB | ~1,348,000 | `job_link` (Primary Key) | Core job listing metadata (title, company, location, search attributes) |
| `job_skills.csv` | ~150–250 MB | ~1,290,000 | `job_link` (Foreign Key) | Extracted comma-separated skill tags |
| `job_summary.csv` | ~2.5–3.5 GB | ~1,250,000 | `job_link` (Foreign Key) | Raw, unparsed text descriptions of the job postings |

> [!IMPORTANT]
> **Join Integrity:** Not every record in `linkedin_job_postings.csv` has a matching record in `job_skills.csv` or `job_summary.csv`. The flags `got_summary` and `got_ner` in the main posting table indicate whether summary text and entity-extracted skills were successfully harvested. All transformations must use **left joins** against `linkedin_job_postings.csv` to avoid dropping valid postings.

### 1.3 Column Schema & Data Types

#### A. `linkedin_job_postings.csv`
| Column Name | Inferred Type | Nullable | Description / Example |
|---|---|---|---|
| `job_link` | `StringType` | No | Unique LinkedIn job URL (e.g., `https://www.linkedin.com/jobs/view/...`) |
| `last_processed_time` | `TimestampType` | Yes | Timestamp of scraper processing (ISO 8601) |
| `last_status` | `StringType` | Yes | Scraper execution status (e.g., `PROCESSED`) |
| `got_summary` | `BooleanType` / `StringType` | Yes | Flag indicating whether job summary was scraped (`t`/`f` or `TRUE`/`FALSE`) |
| `got_ner` | `BooleanType` / `StringType` | Yes | Flag indicating whether Named Entity Recognition extracted skills |
| `is_being_worked` | `BooleanType` / `StringType` | Yes | Internal scraper queue flag |
| `job_title` | `StringType` | No | Raw job title as posted (e.g., `"Senior Data Engineer"`, `"Marketing Lead"`) |
| `company` | `StringType` | Yes | Employer / Hiring organization name |
| `job_location` | `StringType` | Yes | Freeform location string (e.g., `"Austin, TX"`, `"London, England, United Kingdom"`) |
| `first_seen` | `DateType` | Yes | Date first observed by scraper (e.g., `2024-01-15`) |
| `search_city` | `StringType` | Yes | Search parameter city used during scraper harvest |
| `search_country` | `StringType` | Yes | Search parameter country (e.g., `"United States"`, `"Canada"`, `"United Kingdom"`) |
| `search_position` | `StringType` | Yes | Search query keywords used to retrieve the posting |
| `job_level` | `StringType` | Yes | Seniority level (e.g., `"Mid-Senior level"`, `"Associate"`, `"Entry level"`, `"Director"`) |
| `job_type` | `StringType` | Yes | Work modality / contract type (e.g., `"On-site"`, `"Remote"`, `"Hybrid"`, `"Full-time"`) |

#### B. `job_skills.csv`
| Column Name | Inferred Type | Nullable | Description / Example |
|---|---|---|---|
| `job_link` | `StringType` | No | Foreign key linking to `linkedin_job_postings.csv` |
| `job_skills` | `StringType` | Yes | Comma-delimited list of skills (e.g., `"Python, PySpark, SQL, AWS, Docker"`) |

#### C. `job_summary.csv`
| Column Name | Inferred Type | Nullable | Description / Example |
|---|---|---|---|
| `job_link` | `StringType` | No | Foreign key linking to `linkedin_job_postings.csv` |
| `job_summary` | `StringType` | Yes | Full plain-text or Markdown job description body |

### 1.4 Critical Findings & Domain Limitations (Source A)
1. **No Compensation Fields:** Source A **does not contain any salary or compensation data** (neither minimum, maximum, nor currency). Any cross-dataset salary comparison using this dataset is strictly impossible.
2. **Search Bias:** Data collection was driven by predefined search keywords (`search_position`) across major metropolitan search cities (`search_city`, `search_country`). It does not represent an unbiased census of all global LinkedIn postings.
3. **Multi-National Geography:** While heavily weighted toward the United States, Canada, the UK, and Australia, postings span multiple countries. Geographic comparisons must filter explicitly by country.
4. **Skills Extraction Provenance:** Skills in `job_skills.csv` were produced by a third-party Named Entity Recognition (NER) pipeline; false positives and synonyms are present and require taxonomy normalization.

---

## 2. Source B: Recent India-Focused Dataset (2025–2026)

### 2.1 Metadata
- **Dataset Name:** Indian Job Market Dataset 2025–2026
- **Kaggle URL:** [https://www.kaggle.com/datasets/shivamshrivastava21/indian-job-market-dataset-2025-2026](https://www.kaggle.com/datasets/shivamshrivastava21/indian-job-market-dataset-2025-2026)
- **Publisher / Author:** Shivam Shrivastava (`shivamshrivastava21`)
- **Collection Source:** Scraped from Indian employment portal(s) (primarily Naukri.com based on structural signatures)
- **License:** Kaggle Open Dataset Terms (Community Non-Commercial / Research Use)
  - *Permitted Use:* Academic, exploratory analysis, educational prototyping. Redistribution and commercial monetization require validation against portal terms of service.

### 2.2 Expected File Structure
- Distributed as a single archive / CSV file: `indian_job_market_dataset_2025_2026.csv` (or variants named `naukri_jobs_2025.csv`).
- **Expected Record Count:** ~97,929 rows.
- **Estimated Size:** ~40–80 MB uncompressed.

### 2.3 Column Schema & Data Types (Reported & Documented)

| Column Name | Inferred Type | Nullable | Description / Example |
|---|---|---|---|
| `jobId` / `id` | `StringType` / `LongType` | Yes | Portal listing identifier |
| `title` | `StringType` | No | Job title (e.g., `"Data Engineer - PySpark/AWS"`, `"Java Backend Developer"`) |
| `companyName` | `StringType` | Yes | Employer / Consultancy name |
| `location` | `StringType` | Yes | City or metro area in India (e.g., `"Bengaluru"`, `"Hyderabad"`, `"Pune"`, `"Remote"`) |
| `tagsAndSkills` | `StringType` | Yes | Comma/space/pipe delimited skill keywords (e.g., `"Spark, Scala, Hive, AWS, Python"`) |
| `experience` | `StringType` | Yes | Experience string expression (e.g., `"2-5 Yrs"`, `"0-1 Yrs"`, `"Fresher"`) |
| `minimumExperience` | `IntegerType` / `DoubleType` | Yes | Parsed lower bound of required years (e.g., `2`) |
| `maximumExperience` | `IntegerType` / `DoubleType` | Yes | Parsed upper bound of required years (e.g., `5`) |
| `salary` | `StringType` | Yes | Raw salary representation (e.g., `"6-12 Lacs PA"`, `"Not Disclosed"`) |
| `minimumSalary` | `DoubleType` | Yes | Parsed minimum annual compensation in INR Lacs or Rupees |
| `maximumSalary` | `DoubleType` | Yes | Parsed maximum annual compensation in INR Lacs or Rupees |
| `currency` | `StringType` | Yes | Currency code (typically `"INR"` or implicit) |
| `jobUploaded` | `StringType` | Yes | Publication relative timestamp (e.g., `"Just Now"`, `"4 Days Ago"`, `"30+ Days Ago"`) |

### 2.4 Critical Findings & Domain Limitations (Source B)
1. **Relative Date Invalidation:** The `jobUploaded` field stores relative strings (e.g., `"4 Days Ago"`, `"30+ Days Ago"`). Because the exact scraping snapshot date is not officially embedded in each row, converting these into absolute calendar dates would create spurious precision. They cannot be used to plot fine-grained time-series trends.
2. **Missing Salary Values ("Not Disclosed"):** In Indian job portals like Naukri, a large percentage of listings (often 60%–80%) do not disclose salary. Records with missing or "Not Disclosed" compensation must be filtered out when calculating salary statistics and must never be treated as zero.
3. **No Standalone Full Text Descriptions:** Most scraped variations of this dataset contain skill tags and headline attributes, but do not provide full, multi-paragraph job description bodies.
4. **Geographic Scope:** This dataset is strictly focused on India. Cross-dataset comparisons must compare this dataset only with India-filtered subsets of Source A or clearly frame the comparison as global/cross-regional differences.

---

## 3. Verified Facts vs. Unverified Assumptions

| Category | Verified Fact | Unverified Assumption (Requires File Inspection Upon Download) |
|---|---|---|
| **Source A Schema** | Composed of 3 relational CSVs joined on `job_link`. No salary field exists. Columns `job_title`, `company`, `job_location`, `job_level`, `first_seen` are verified. | Exact proportion of null values across `job_skills` and `job_summary`. |
| **Source A Records** | Approximately 1.34 million records across 2024. | Exact distribution of records across countries (US vs India vs Europe). |
| **Source B Schema** | Contains `title`, `companyName`, `location`, `tagsAndSkills`, `experience`, `salary`, `jobUploaded`. | Exact column naming casing (`companyName` vs `company`, `jobId` vs `job_id`) until uncompressed file is parsed. |
| **Source B Dates** | Contains relative strings like `"X Days Ago"`. | Whether an internal scraping timestamp metadata file accompanies the dataset. |
| **Source B Compensation**| Reported in Indian Rupees (Lakhs Per Annum). | Percentage of records where salary is `"Not Disclosed"` or null. |
| **Cross-Source Overlap** | Both datasets share job titles, employer names, geographic strings, and skill lists. | The exact overlap coefficient of skills between LinkedIn 2024 and Naukri 2025–2026. |

---

## 4. Source Data Integrity Checklist for Ingestion

Before any ingestion job moves records into the Bronze layer, the following verification gates must pass:
- [ ] Source CSV checksum and file byte size verification.
- [ ] Header validation against expected column list.
- [ ] Primary key null check (`job_link` in Source A must never be null).
- [ ] Delimiter and multi-line quote handling (ensuring newlines inside descriptions do not cause row tearing).
- [ ] Ingestion metadata enrichment: appending `source_name`, `source_platform`, `ingestion_timestamp`.

