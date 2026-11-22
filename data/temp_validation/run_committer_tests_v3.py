"""DemandLens - Committer test round 3.

Tests:
  H: Disable Hadoop native IO (io.native.lib.available=false)
  I: Disable native IO + RawLocalFS + disable cache
  J: Bypass Hadoop committer entirely - write Parquet via PyArrow
"""
import os
import sys
import shutil
import time
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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

base_dir = project_root / "data" / "temp_validation"
base_dir.mkdir(parents=True, exist_ok=True)

test_data = [
    (1, "Software Engineer", "San Francisco"),
    (2, "Data Analyst", None),
    (3, "ML Engineer", "New York"),
    (4, None, "Remote"),
]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("location", StringType(), True),
])

results = []

def validate_parquet(spark, target_dir, test_name):
    """Read back and validate the written Parquet."""
    read_df = spark.read.parquet(str(target_dir))
    rows = read_df.collect()
    read_count = len(rows)
    read_cols = read_df.columns
    null_title = sum(1 for r in rows if r["title"] is None)
    null_loc = sum(1 for r in rows if r["location"] is None)

    checks = []
    if read_count != 4: checks.append(f"row count {read_count}!=4")
    if set(read_cols) != {"id", "title", "location"}: checks.append(f"cols {read_cols}")
    if null_title != 1: checks.append(f"null_title {null_title}!=1")
    if null_loc != 1: checks.append(f"null_loc {null_loc}!=1")

    if checks:
        return False, "; ".join(checks), {}
    return True, None, {"rows": read_count, "cols": read_cols,
                         "null_title": null_title, "null_loc": null_loc}

# ============================================================
# TEST H: Disable native IO
# ============================================================
test_name = "H_no_native_io"
target_dir = base_dir / f"output_{test_name}"
if target_dir.exists():
    shutil.rmtree(target_dir, ignore_errors=True)
print(f"\n{'='*60}\n {test_name}: io.native.lib.available=false\n{'='*60}")
t0 = time.time()
spark = None
try:
    spark = (
        SparkSession.builder.appName(f"Test_{test_name}")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("io.native.lib.available", "false")

    df = spark.createDataFrame(data=test_data, schema=schema).coalesce(1)
    df.write.mode("overwrite").parquet(str(target_dir))
    ok, err, val = validate_parquet(spark, target_dir, test_name)
    status = "SUCCESS" if ok else "VALIDATION_FAILED"
    print(f"  {status}: {val if ok else err}")
    results.append({"name": test_name, "status": status, "error": err,
                     "validation": val, "elapsed": round(time.time()-t0, 1)})
except Exception as exc:
    emsg = str(exc).split("\n")[0][:200]
    print(f"  FAILED: {emsg}")
    results.append({"name": test_name, "status": "FAILED", "error": emsg,
                     "validation": {}, "elapsed": round(time.time()-t0, 1)})
finally:
    if spark: spark.stop()

# ============================================================
# TEST I: No native IO + RawLocalFS + disable cache
# ============================================================
test_name = "I_no_native_rawfs_nocache"
target_dir = base_dir / f"output_{test_name}"
if target_dir.exists():
    shutil.rmtree(target_dir, ignore_errors=True)
print(f"\n{'='*60}\n {test_name}: no native + RawLocalFS + disable cache\n{'='*60}")
t0 = time.time()
spark = None
try:
    spark = (
        SparkSession.builder.appName(f"Test_{test_name}")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("io.native.lib.available", "false")
    hconf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    hconf.set("fs.file.impl.disable.cache", "true")

    df = spark.createDataFrame(data=test_data, schema=schema).coalesce(1)
    df.write.mode("overwrite").parquet(str(target_dir))
    ok, err, val = validate_parquet(spark, target_dir, test_name)
    status = "SUCCESS" if ok else "VALIDATION_FAILED"
    print(f"  {status}: {val if ok else err}")
    results.append({"name": test_name, "status": status, "error": err,
                     "validation": val, "elapsed": round(time.time()-t0, 1)})
except Exception as exc:
    emsg = str(exc).split("\n")[0][:200]
    print(f"  FAILED: {emsg}")
    results.append({"name": test_name, "status": "FAILED", "error": emsg,
                     "validation": {}, "elapsed": round(time.time()-t0, 1)})
finally:
    if spark: spark.stop()

# ============================================================
# TEST J: PyArrow direct Parquet write (bypass Hadoop committer)
# ============================================================
test_name = "J_pyarrow_write"
target_dir = base_dir / f"output_{test_name}"
if target_dir.exists():
    shutil.rmtree(target_dir, ignore_errors=True)
target_dir.mkdir(parents=True, exist_ok=True)
print(f"\n{'='*60}\n {test_name}: PySpark read -> toPandas -> PyArrow write\n{'='*60}")
t0 = time.time()
spark = None
try:
    spark = (
        SparkSession.builder.appName(f"Test_{test_name}")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.createDataFrame(data=test_data, schema=schema)
    print(f"  PySpark DataFrame created: {df.count()} rows, {len(df.columns)} cols")

    # Convert to pandas and write via PyArrow
    import pyarrow as pa
    import pyarrow.parquet as pq

    pdf = df.toPandas()
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    output_file = target_dir / "part-00000.parquet"
    pq.write_table(table, str(output_file), compression="snappy")
    print(f"  PyArrow wrote {output_file.stat().st_size} bytes to {output_file}")

    # Validate by reading back with PySpark
    ok, err, val = validate_parquet(spark, target_dir, test_name)
    status = "SUCCESS" if ok else "VALIDATION_FAILED"
    print(f"  {status}: {val if ok else err}")
    results.append({"name": test_name, "status": status, "error": err,
                     "validation": val, "elapsed": round(time.time()-t0, 1)})
except Exception as exc:
    emsg = str(exc).split("\n")[0][:200]
    print(f"  FAILED: {emsg}")
    results.append({"name": test_name, "status": "FAILED", "error": emsg,
                     "validation": {}, "elapsed": round(time.time()-t0, 1)})
finally:
    if spark: spark.stop()

# ============================================================
# TEST K: PyArrow write in batches (simulates large dataset pattern)
# ============================================================
test_name = "K_pyarrow_batched"
target_dir = base_dir / f"output_{test_name}"
if target_dir.exists():
    shutil.rmtree(target_dir, ignore_errors=True)
target_dir.mkdir(parents=True, exist_ok=True)
print(f"\n{'='*60}\n {test_name}: PySpark partitions -> PyArrow batch writes\n{'='*60}")
t0 = time.time()
spark = None
try:
    spark = (
        SparkSession.builder.appName(f"Test_{test_name}")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    import pyarrow as pa
    import pyarrow.parquet as pq

    # Create a slightly larger dataset with 2 target partitions
    larger_data = test_data * 25  # 100 rows
    df = spark.createDataFrame(data=larger_data, schema=schema).repartition(2)
    total_rows = df.count()
    print(f"  PySpark DataFrame: {total_rows} rows, 2 partitions")

    # Collect per-partition and write separate parquet files
    # This simulates what the ingestion engine would do for large datasets
    spark_schema = df.schema
    arrow_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=True),
        pa.field("title", pa.string(), nullable=True),
        pa.field("location", pa.string(), nullable=True),
    ])

    pdf = df.toPandas()
    # Split into chunks to simulate partitioned writes
    chunk_size = len(pdf) // 2
    for i, start in enumerate(range(0, len(pdf), chunk_size)):
        chunk = pdf.iloc[start:start+chunk_size]
        table = pa.Table.from_pandas(chunk, schema=arrow_schema, preserve_index=False)
        out_file = target_dir / f"part-{i:05d}.parquet"
        pq.write_table(table, str(out_file), compression="snappy")
        print(f"    Wrote partition {i}: {len(chunk)} rows -> {out_file.name}")

    # Validate with PySpark
    read_df = spark.read.parquet(str(target_dir))
    read_count = read_df.count()
    read_cols = read_df.columns
    null_title = read_df.filter(read_df["title"].isNull()).count()
    null_loc = read_df.filter(read_df["location"].isNull()).count()

    checks = []
    if read_count != total_rows: checks.append(f"row count {read_count}!={total_rows}")
    if set(read_cols) != {"id", "title", "location"}: checks.append(f"cols {read_cols}")
    if null_title != 25: checks.append(f"null_title {null_title}!=25")
    if null_loc != 25: checks.append(f"null_loc {null_loc}!=25")

    if checks:
        status = "VALIDATION_FAILED"
        err = "; ".join(checks)
        val = {}
    else:
        status = "SUCCESS"
        err = None
        val = {"rows": read_count, "cols": read_cols,
               "null_title": null_title, "null_loc": null_loc}

    print(f"  {status}: {val if status == 'SUCCESS' else err}")
    results.append({"name": test_name, "status": status, "error": err,
                     "validation": val, "elapsed": round(time.time()-t0, 1)})
except Exception as exc:
    emsg = str(exc).split("\n")[0][:200]
    print(f"  FAILED: {emsg}")
    results.append({"name": test_name, "status": "FAILED", "error": emsg,
                     "validation": {}, "elapsed": round(time.time()-t0, 1)})
finally:
    if spark: spark.stop()

# ============================================================
# SUMMARY
# ============================================================
print(f"\n{'='*60}")
print(f" RESULTS SUMMARY - ROUND 3")
print(f"{'='*60}")
for r in results:
    marker = "OK" if r["status"] == "SUCCESS" else "XX"
    print(f"  [{marker}] {r['name']:40s} {r['status']:20s} {r['elapsed']}s")
    if r["status"] == "SUCCESS":
        print(f"       Validation: {r['validation']}")
    else:
        print(f"       Error: {r['error']}")
