# DemandLens: A Trustworthy Job-Market Intelligence Platform

DemandLens is an end-to-end data platform designed to process large-scale job posting datasets, enforce strict data quality standards, uncover emerging skill and compensation trends, and deliver trustworthy market intelligence through an interactive dashboard and an AI-driven retrieval-augmented generation (RAG) assistant.

---

## 🎯 Purpose & Goals

Modern job market datasets are often noisy, duplicated, and unstructured. DemandLens provides a verifiable, trustworthy pipeline that:
1. **Ingests & Normalizes:** Collects and ingests raw job market data into a structured **Medallion Architecture** (Bronze, Silver, Gold).
2. **Ensures Quality:** Implements rigorous data quality validation gates at every stage before publishing metrics.
3. **Analyzes Trends:** Identifies high-demand tech skills, emerging roles, salary dynamics, and regional hiring demands.
4. **Enables AI Intelligence:** Surfaces answers to complex market questions with citations via a pgvector-backed RAG assistant.
5. **Interactive Visualization:** Presents actionable insights through an executive-ready Streamlit dashboard.

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Language Runtime** | Python 3.11 | Core application and pipeline scripting |
| **Big Data Engine** | Apache Spark / PySpark | Distributed data processing and transformation |
| **Data Exploration** | Pandas | Lightweight exploratory data analysis and sampling |
| **Storage Format** | Apache Parquet | Columnar storage for Bronze, Silver, and Gold layers |
| **Relational / Vector DB** | PostgreSQL + pgvector | Analytics serving and high-dimensional semantic search |
| **Frontend UI** | Streamlit | Interactive dashboards and search interface |
| **AI / Embeddings** | OpenAI API | Vector embeddings and conversational RAG assistant |
| **Testing** | pytest | Automated unit and integration testing |
| **Version Control** | Git & GitHub | Source control and collaboration |
| **Cloud Storage** | AWS S3 *(Future)* | Object storage for large-scale data lake |
| **Disaster Recovery** | Google Drive *(Future)* | Secondary backup only |

---

## 🏗️ Medallion Architecture

- **Bronze Layer (`data/bronze/`):** Raw, unprocessed job postings stored in original structure for provenance and auditing.
- **Silver Layer (`data/silver/`):** Cleaned, deduplicated, schema-validated, and enriched data (standardized titles, normalized skill names, extracted salary bands).
- **Gold Layer (`data/gold/`):** Aggregated metrics, dimensional models, and curated analytical datasets ready for SQL querying and dashboards.

---

## 🗺️ Planned Roadmap & Phases

- [x] **Phase 0: Project Foundation (Current)**
  - Local environment verification (Python, Java, Git, storage).
  - Directory layout and dependency specifications.
  - PySpark local verification and baseline health check.
- [ ] **Phase 1: Ingestion & Raw Data Staging**
  - Schema definition and batch ingestion into Bronze Parquet storage.
  - Basic file integrity checks and metadata logging.
- [ ] **Phase 2: Cleaning, Transformation & Quality Gates**
  - Deduplication and null-value handling.
  - Skill extraction and title normalization.
  - Data validation rules and automated quality checks.
- [ ] **Phase 3: Analytics Layer & PostgreSQL Storage**
  - Curated Gold tables.
  - Database loading pipeline into PostgreSQL.
  - Analytical queries for hiring trends, top skills, and compensation benchmarks.
- [ ] **Phase 4: RAG Engine with pgvector & LLM**
  - Text chunking and embedding generation using OpenAI embeddings.
  - Vector indexing in PostgreSQL via pgvector.
  - Contextual retrieval pipeline with grounding and source citations.
- [ ] **Phase 5: Streamlit Interactive Dashboard**
  - Visual charts, filter controls, and interactive queries.
  - Integrated chat interface for the RAG assistant.
- [ ] **Phase 6: Cloud Scaling (AWS S3 & Databricks)**
  - Migrating storage to S3 buckets.
  - Databricks / EMR cluster processing for large-scale execution.

---

## 📁 Project Structure

```
DemandLens/
├── data/
│   ├── bronze/                   # Raw ingested job posting data
│   ├── silver/                   # Cleaned, deduplicated, and validated data
│   └── gold/                     # Curated aggregates and analytics-ready datasets
├── notebooks/
│   └── exploration/              # Jupyter notebooks for ad-hoc exploration
├── src/
│   └── demandlens/
│       ├── ingestion/            # Raw data loading and staging pipelines
│       ├── transformations/      # Data cleaning, normalization, enrichment
│       ├── quality/              # Data quality rules and validation checks
│       ├── analytics/            # Aggregations, metric calculations, and reporting
│       ├── rag/                  # Embeddings, vector retrieval, and LLM orchestration
│       └── app/                  # Streamlit dashboard and UI components
├── tests/                        # Unit, integration, and data quality test suites
├── scripts/                      # Utility and operational scripts (e.g., test runners)
├── config/                       # Pipeline configuration files (YAML / JSON)
├── docs/                         # Architecture documentation and specs
├── requirements.txt              # Production and development dependencies
├── .gitignore                    # Git ignore rules for virtualenv, data, and secrets
├── .env.example                  # Environment configuration template
└── README.md                     # Project documentation
```

---

## 🚀 Getting Started

### 1. Prerequisites
- **Git:** 2.40+
- **Python:** 3.11 recommended (compatible with Spark 3.5.x)
- **Java:** JDK 17 recommended for Apache Spark local runtime (or compatible JDK with `JAVA_HOME` configured)

### 2. Environment Setup
```powershell
# Clone the repository
git clone <repo-url>
cd DemandLens

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Install initial dependencies
pip install -r requirements.txt

# Copy environment variables template
copy .env.example .env
```

### 3. Verify PySpark Installation
Run the minimal PySpark verification script:
```powershell
python scripts/test_pyspark.py
```
If configured properly, the script initializes a local Spark session, generates a sample DataFrame of job postings, writes it as Parquet to `data/bronze/test_sample.parquet`, reads it back, and displays the contents.

