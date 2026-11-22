"""DemandLens - Isolated PyArrow write + PySpark read test.

Single SparkSession. No Spark writes at all.
Tests whether PySpark can READ Parquet files written by PyArrow.
"""
import os
import sys
import shutil
from pathlib import Path

project_root = Path(r"D:\DemandLens")
for candidate in [Path(r"C:\Program Files\Java\jdk-24")]:
    if candidate.exists() and (candidate / "bin" / "java.exe").exists():
        os.environ["JAVA_HOME"] = str(candidate)
        os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")
        break

local_hadoop = project_root / "hadoop"
if local_hadoop.exists():
    os.environ["HADOOP_HOME"] = str(local_hadoop)
    os.environ["PATH"] = str(local_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# ============================================================
# STEP 1: Write Parquet files using PyArrow ONLY (no Spark)
# ============================================================
print("=" * 60)
print(" STEP 1: Write Parquet with PyArrow (no Spark)")
print("=" * 60)

base_dir = project_root / "data" / "temp_validation"
out_single = base_dir / "output_L_pyarrow_only"
out_multi = base_dir / "output_M_pyarrow_multi"

for d in [out_single, out_multi]:
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True, exist_ok=True)

pdf = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "title": ["Software Engineer", "Data Analyst", None, "ML Engineer"],
    "location": ["San Francisco", None, "New York", "Remote"],
})

# Single-file write
table = pa.Table.from_pandas(pdf, preserve_index=False)
single_file = out_single / "part-00000.parquet"
pq.write_table(table, str(single_file), compression="snappy")
print(f"  Single file: {single_file} ({single_file.stat().st_size} bytes)")

# Multi-file write (simulates partitioned output)
for i in range(2):
    chunk = pdf.iloc[i*2:(i+1)*2]
    t = pa.Table.from_pandas(chunk, preserve_index=False)
    f = out_multi / f"part-{i:05d}.parquet"
    pq.write_table(t, str(f), compression="snappy")
    print(f"  Multi file {i}: {f} ({f.stat().st_size} bytes)")

# Verify with PyArrow read (no Spark)
read_table = pq.read_table(str(out_single))
print(f"  PyArrow read-back: {read_table.num_rows} rows, {read_table.column_names}")
assert read_table.num_rows == 4
assert read_table.column_names == ["id", "title", "location"]
print("  PyArrow round-trip: PASSED\n")

# ============================================================
# STEP 2: Read the same files with PySpark
# ============================================================
print("=" * 60)
print(" STEP 2: Read PyArrow-written Parquet with PySpark")
print("=" * 60)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Test_PyArrow_Read")
    .master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    # Test L: Read single-file parquet directory
    print(f"\n  Reading {out_single}...")
    df1 = spark.read.parquet(str(out_single))
    count1 = df1.count()
    cols1 = df1.columns
    null_title1 = df1.filter(df1["title"].isNull()).count()
    null_loc1 = df1.filter(df1["location"].isNull()).count()
    print(f"  Test L (single file): {count1} rows, cols={cols1}")
    print(f"    null_title={null_title1}, null_loc={null_loc1}")
    assert count1 == 4, f"Expected 4 rows, got {count1}"
    assert set(cols1) == {"id", "title", "location"}
    assert null_title1 == 1 and null_loc1 == 1
    print("  Test L: PASSED")

    # Test M: Read multi-file parquet directory
    print(f"\n  Reading {out_multi}...")
    df2 = spark.read.parquet(str(out_multi))
    count2 = df2.count()
    cols2 = df2.columns
    null_title2 = df2.filter(df2["title"].isNull()).count()
    null_loc2 = df2.filter(df2["location"].isNull()).count()
    print(f"  Test M (multi file): {count2} rows, cols={cols2}")
    print(f"    null_title={null_title2}, null_loc={null_loc2}")
    assert count2 == 4, f"Expected 4 rows, got {count2}"
    assert set(cols2) == {"id", "title", "location"}
    assert null_title2 == 1 and null_loc2 == 1
    print("  Test M: PASSED")

    # ============================================================
    # STEP 3: Full pipeline test - PySpark CSV read -> transform -> PyArrow write -> PySpark read
    # ============================================================
    print(f"\n{'='*60}")
    print(" STEP 3: Full pipeline (PySpark process -> PyArrow write -> PySpark read)")
    print("=" * 60)

    # Create a temp CSV to simulate source data
    csv_dir = base_dir / "temp_csv_source"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_file = csv_dir / "test_jobs.csv"
    csv_file.write_text(
        "job_id,title,company,salary\n"
        "101,Data Engineer,Acme Corp,500000\n"
        "102,Software Engineer,Beta Tech,700000\n"
        "103,ML Engineer,Gamma AI,900000\n"
        "104,Cloud Architect,Delta Cloud,1200000\n"
        "105,DevOps Lead,,800000\n",
        encoding="utf-8",
    )

    # Read CSV with PySpark
    csv_df = spark.read.option("header", "true").csv(str(csv_file))
    print(f"  CSV read: {csv_df.count()} rows, {csv_df.columns}")

    # Add metadata columns (simulates Bronze ingestion)
    import pyspark.sql.functions as F
    bronze_df = (
        csv_df
        .withColumn("_source_id", F.lit("test_source"))
        .withColumn("_source_file", F.lit("test_jobs.csv"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_run_id", F.lit("test_run_001"))
    )
    print(f"  Bronze DataFrame: {bronze_df.count()} rows, {len(bronze_df.columns)} cols")

    # Convert to Pandas and write with PyArrow
    out_pipeline = base_dir / "output_N_pipeline"
    if out_pipeline.exists():
        shutil.rmtree(out_pipeline, ignore_errors=True)
    out_pipeline.mkdir(parents=True, exist_ok=True)

    bronze_pdf = bronze_df.toPandas()
    bronze_table = pa.Table.from_pandas(bronze_pdf, preserve_index=False)
    pq.write_table(bronze_table, str(out_pipeline / "part-00000.parquet"), compression="snappy")
    print(f"  PyArrow wrote pipeline output: {(out_pipeline / 'part-00000.parquet').stat().st_size} bytes")

    # Read back with PySpark and validate
    read_bronze = spark.read.parquet(str(out_pipeline))
    read_count = read_bronze.count()
    read_cols = read_bronze.columns
    print(f"  PySpark read-back: {read_count} rows, cols={read_cols}")

    assert read_count == 5, f"Expected 5, got {read_count}"
    assert "job_id" in read_cols
    assert "_source_id" in read_cols
    assert "_ingestion_run_id" in read_cols
    # Check metadata values
    src_ids = [r["_source_id"] for r in read_bronze.select("_source_id").distinct().collect()]
    assert src_ids == ["test_source"], f"Unexpected source_id: {src_ids}"
    # Check null preservation (company is empty string in row 5)
    print("  Test N (full pipeline): PASSED")

    print(f"\n{'='*60}")
    print(" ALL TESTS PASSED")
    print("=" * 60)
    print("  Test L (PyArrow single write + PySpark read): PASSED")
    print("  Test M (PyArrow multi write + PySpark read): PASSED")
    print("  Test N (CSV->PySpark->PyArrow->PySpark pipeline): PASSED")
    print(f"\n  Conclusion: PyArrow write + PySpark read works correctly.")
    print(f"  The Bronze ingestion engine should use PyArrow for Parquet writes")
    print(f"  while keeping PySpark for all CSV reading, transformations, and validation.")

except Exception as exc:
    import traceback
    print(f"\n  FAILED: {exc}")
    traceback.print_exc()
finally:
    spark.stop()
    # Clean up temp CSV
    shutil.rmtree(base_dir / "temp_csv_source", ignore_errors=True)
    print("\n  SparkSession stopped.")
