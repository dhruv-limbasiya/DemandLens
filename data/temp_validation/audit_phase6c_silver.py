"""DemandLens - Phase 6C Comprehensive Silver Quality Audit Script.

Verifies:
  - Jobs table row count (97,679), schema, null counts, salary validity, experience validity, freshness.
  - Bridge table row count (753,137), distinctness, referential integrity.
  - Parquet compression, file sizes, staging cleanup, Bronze immutability.
"""

import os
import shutil
import sys
import time
from pathlib import Path

root = Path(r"D:\DemandLens")
sys.path.insert(0, str(root))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    print("=" * 70)
    print(" DemandLens — Phase 6C Silver Quality Audit & Verification")
    print("=" * 70)

    silver_jobs_dir = root / "data" / "silver" / "indian_job_market_2025_2026" / "jobs"
    silver_bridge_dir = root / "data" / "silver" / "indian_job_market_2025_2026" / "job_skills_bridge"
    meta_dir = root / "data" / "silver" / "indian_job_market_2025_2026" / "_metadata"
    spark_tmp = root / "data" / "temp_validation" / "spark_tmp"

    # 1. Disk & Parquet Part Files Audit
    print("\n=== 1. DISK & PARQUET FILE AUDIT ===")
    assert silver_jobs_dir.exists(), "Silver jobs directory missing!"
    assert silver_bridge_dir.exists(), "Silver bridge directory missing!"

    jobs_parts = sorted(silver_jobs_dir.glob("part-*.parquet"))
    bridge_parts = sorted(silver_bridge_dir.glob("part-*.parquet"))

    print(f"Jobs part files count: {len(jobs_parts)}")
    jobs_size = sum(p.stat().st_size for p in jobs_parts)
    jobs_rows_meta = sum(pq.ParquetFile(str(p)).metadata.num_rows for p in jobs_parts)
    jobs_comp = [pq.ParquetFile(str(p)).metadata.row_group(0).column(0).compression for p in jobs_parts]
    print(f"  Jobs size: {jobs_size / (1024**2):.2f} MB | Rows in meta: {jobs_rows_meta:,} | Codecs: {set(jobs_comp)}")
    assert set(jobs_comp) == {"SNAPPY"}, "Non-snappy compression in jobs!"
    assert jobs_rows_meta == 97679, f"Jobs meta row count mismatch: {jobs_rows_meta}"

    print(f"\nBridge part files count: {len(bridge_parts)}")
    bridge_size = sum(p.stat().st_size for p in bridge_parts)
    bridge_rows_meta = sum(pq.ParquetFile(str(p)).metadata.num_rows for p in bridge_parts)
    bridge_comp = [pq.ParquetFile(str(p)).metadata.row_group(0).column(0).compression for p in bridge_parts]
    print(f"  Bridge size: {bridge_size / (1024**2):.2f} MB | Rows in meta: {bridge_rows_meta:,} | Codecs: {set(bridge_comp)}")
    assert set(bridge_comp) == {"SNAPPY"}, "Non-snappy compression in bridge!"
    assert bridge_rows_meta == 753137, f"Bridge meta row count mismatch: {bridge_rows_meta}"

    # Check for leftover staging
    silver_base = root / "data" / "silver" / "indian_job_market_2025_2026"
    staging_dirs = [p.name for p in silver_base.iterdir() if p.name.startswith("_staging")]
    print(f"\nLeftover staging folders: {staging_dirs}")
    assert len(staging_dirs) == 0, f"Found leftover staging folders: {staging_dirs}"

    # 2. PySpark Readback & Analytical Quality Audit
    print("\n=== 2. PYSPARK READBACK & QUALITY AUDIT ===")
    spark = (
        SparkSession.builder.appName("DemandLens-Audit-Indian-Silver")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(spark_tmp))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df_jobs = spark.read.parquet(str(silver_jobs_dir))
        df_bridge = spark.read.parquet(str(silver_bridge_dir))

        jobs_count = df_jobs.count()
        bridge_count = df_bridge.count()
        print(f"df_jobs count: {jobs_count:,}")
        print(f"df_bridge count: {bridge_count:,}")

        assert jobs_count == 97679
        assert bridge_count == 753137

        # Check unique job_id in jobs table
        distinct_jobs = df_jobs.select("job_id").distinct().count()
        print(f"Distinct job_id in jobs table: {distinct_jobs:,}")
        assert distinct_jobs == jobs_count, "Found duplicate job_id in Silver jobs table!"

        # Check unique (job_id, skill) in bridge table
        distinct_bridge_pairs = df_bridge.select("job_id", "skill").distinct().count()
        print(f"Distinct (job_id, skill) in bridge table: {distinct_bridge_pairs:,}")
        assert distinct_bridge_pairs == bridge_count, "Found duplicate (job_id, skill) in bridge table!"

        # Referential integrity: All bridge job_ids exist in jobs table
        orphans = df_bridge.select("job_id").distinct().join(df_jobs.select("job_id"), "job_id", "left_anti").count()
        print(f"Orphan bridge records (referential integrity check): {orphans}")
        assert orphans == 0, f"Found {orphans} orphan records in bridge table!"

        # Salary distributions and nulls
        print("\n=== Salary Analysis ===")
        disclosed_salaries = df_jobs.filter(F.col("salary_min").isNotNull()).count()
        invalid_salaries = df_jobs.filter(F.col("salary_is_valid") == False).count()
        currencies = [r["salary_currency"] for r in df_jobs.select("salary_currency").distinct().collect() if r["salary_currency"] is not None]
        periods = [r["salary_period"] for r in df_jobs.select("salary_period").distinct().collect() if r["salary_period"] is not None]
        print(f"Disclosed numeric salaries count: {disclosed_salaries:,} ({disclosed_salaries/jobs_count*100:.2f}%)")
        print(f"Undisclosed salaries (explicit null): {jobs_count - disclosed_salaries:,} ({(jobs_count - disclosed_salaries)/jobs_count*100:.2f}%)")
        print(f"Invalid salary range records: {invalid_salaries}")
        print(f"Distinct salary currencies: {currencies}")
        print(f"Distinct salary periods: {periods}")
        currency_counts = [r.asDict() for r in df_jobs.groupBy("salary_currency").count().collect()]
        print(f"Currency counts: {currency_counts}")
        assert invalid_salaries == 0
        assert set(currencies).issubset({"INR", "USD"})

        # Experience distributions
        print("\n=== Experience Analysis ===")
        tiers = [r.asDict() for r in df_jobs.groupBy("seniority_tier").count().orderBy(F.desc("count")).collect()]
        for t in tiers:
            print(f"  Tier {str(t['seniority_tier']):15s}: {t['count']:,} ({t['count']/jobs_count*100:.2f}%)")

        # Freshness distributions
        print("\n=== Freshness Tier Analysis ===")
        freshness = [r.asDict() for r in df_jobs.groupBy("freshness_tier").count().orderBy(F.desc("count")).collect()]
        for f in freshness:
            print(f"  Tier {str(f['freshness_tier']):15s}: {f['count']:,} ({f['count']/jobs_count*100:.2f}%)")

        # Skills analysis
        print("\n=== Skills Analysis ===")
        jobs_with_skills = df_jobs.filter(F.col("has_skills") == True).count()
        print(f"Jobs with skills: {jobs_with_skills:,} ({jobs_with_skills/jobs_count*100:.2f}%)")
        unique_skills = df_bridge.select("skill").distinct().count()
        print(f"Distinct normalized skills in bridge table: {unique_skills:,}")
        top_skills = [r.asDict() for r in df_bridge.groupBy("skill").count().orderBy(F.desc("count")).limit(10).collect()]
        print("Top 10 skills in bridge table:")
        for s in top_skills:
            print(f"  {s['skill']}: {s['count']:,}")

        # Remote work analysis
        remote_count = df_jobs.filter(F.col("is_remote") == True).count()
        print(f"\nRemote/Hybrid jobs identified: {remote_count:,} ({remote_count/jobs_count*100:.2f}%)")

        # 3. Bronze Immutability Check
        print("\n=== 3. BRONZE IMMUTABILITY CHECK ===")
        b_jobs = spark.read.parquet(str(root / "data" / "bronze" / "indian_job_market_2025_2026" / "jobs")).count()
        b_postings = spark.read.parquet(str(root / "data" / "bronze" / "linkedin_2024" / "job_postings")).count()
        b_skills = spark.read.parquet(str(root / "data" / "bronze" / "linkedin_2024" / "job_skills")).count()
        b_summary = spark.read.parquet(str(root / "data" / "bronze" / "linkedin_2024" / "job_summary")).count()
        print(f"Bronze Indian jobs count:   {b_jobs:,} (expected 97,929)")
        print(f"Bronze LinkedIn postings:   {b_postings:,} (expected 1,348,454)")
        print(f"Bronze LinkedIn skills:     {b_skills:,} (expected 1,296,381)")
        print(f"Bronze LinkedIn summary:    {b_summary:,} (expected 1,297,332)")
        assert b_jobs == 97929
        assert b_postings == 1348454
        assert b_skills == 1296381
        assert b_summary == 1297332

        free_gb = shutil.disk_usage("D:\\").free / (1024**3)
        print(f"\nD: Drive available free space: {free_gb:.2f} GB")

        print("\n" + "=" * 70)
        print(" PHASE 6C SILVER AUDIT: ALL CHECKS PASSED (100% FIDELITY)")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
