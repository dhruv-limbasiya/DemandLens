"""Investigate duplicates in Indian Job Market Bronze Dataset."""

import sys
from pathlib import Path

root = Path(r"D:\DemandLens")
sys.path.insert(0, str(root))

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = (
        SparkSession.builder.appName("DemandLens-Inspect-Indian-Bronze")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        bronze_path = str(root / "data" / "bronze" / "indian_job_market_2025_2026" / "jobs")
        df = spark.read.parquet(bronze_path)
        total_count = df.count()
        print(f"Total Bronze Indian rows: {total_count:,}")
        print(f"Columns: {df.columns}")

        # Check jobId nulls & duplicates
        null_job_ids = df.filter(F.col("jobId").isNull() | (F.trim(F.col("jobId")) == "")).count()
        distinct_job_ids = df.select("jobId").distinct().count()
        print(f"Null/Empty jobId: {null_job_ids}")
        print(f"Distinct jobId: {distinct_job_ids:,}")
        dupe_count = total_count - distinct_job_ids
        print(f"Duplicate jobId count: {dupe_count:,}")

        if dupe_count > 0:
            print("\nTop 10 duplicate jobIds:")
            dupes = df.groupBy("jobId").count().filter("count > 1").orderBy(F.desc("count")).limit(10).collect()
            for d in dupes:
                print(f"  jobId={d['jobId']}, count={d['count']}")

            # Inspect one duplicate group in detail
            sample_dupe_id = dupes[0]["jobId"]
            print(f"\nDetailed rows for sample duplicate jobId={sample_dupe_id}:")
            sample_rows = df.filter(F.col("jobId") == sample_dupe_id).collect()
            for idx, r in enumerate(sample_rows):
                print(f"  Row {idx+1}: title={r['title']}, company={r['companyName']}, salary={r['salary']}, location={r['location']}, jobUploaded={r['jobUploaded']}")

        # Check exact row duplicates across all 21 columns
        distinct_rows = df.dropDuplicates().count()
        print(f"\nDistinct entire rows: {distinct_rows:,}")
        print(f"Exact duplicate rows: {total_count - distinct_rows:,}")

        # Check duplicate on content: (title, companyName, location, jobId)
        content_dupes = total_count - df.dropDuplicates(["jobId", "title", "companyName", "location"]).count()
        print(f"Duplicate on (jobId, title, companyName, location): {content_dupes:,}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
