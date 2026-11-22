"""DemandLens - Targeted Parquet committer test round 2.

Tests single-threaded write (local[1] + coalesce(1)) to eliminate
concurrent task directory contention on Windows NTFS.
"""
import os
import sys
import shutil
import time
from pathlib import Path

# --- Environment setup ---
project_root = Path(r"D:\DemandLens")
for candidate in [Path(r"C:\Program Files\Java\jdk-24"), Path(r"C:\Program Files\Java\jdk-21")]:
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

TESTS = {
    "D_local1_coalesce1": {
        "master": "local[1]",
        "coalesce": 1,
        "configs": {},
    },
    "E_local1_coalesce1_rawfs": {
        "master": "local[1]",
        "coalesce": 1,
        "configs": {
            "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
        },
    },
    "F_local2_coalesce1": {
        "master": "local[2]",
        "coalesce": 1,
        "configs": {},
    },
    "G_local_star_coalesce1": {
        "master": "local[*]",
        "coalesce": 1,
        "configs": {},
    },
}

results = []
for test_name, spec in TESTS.items():
    target_dir = base_dir / f"output_{test_name}"
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)

    print(f"\n{'='*60}")
    print(f" {test_name}  master={spec['master']}  coalesce={spec['coalesce']}")
    print(f" configs={spec['configs']}")
    print(f"{'='*60}")

    builder = (
        SparkSession.builder.appName(f"Test_{test_name}")
        .master(spec["master"])
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
    )
    for k, v in spec["configs"].items():
        builder = builder.config(k, v)

    spark = None
    status = "FAILED"
    error_msg = None
    validation = {}
    t0 = time.time()

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # Also push Hadoop configs through the JVM side
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        for k, v in spec["configs"].items():
            hconf.set(k.replace("spark.hadoop.", ""), str(v))

        df = spark.createDataFrame(data=test_data, schema=schema)
        if spec["coalesce"]:
            df = df.coalesce(spec["coalesce"])

        df.write.mode("overwrite").parquet(str(target_dir))
        print(f"  Write succeeded in {time.time()-t0:.1f}s")

        # Validate
        read_df = spark.read.parquet(str(target_dir))
        rows = read_df.collect()
        read_count = len(rows)
        read_cols = read_df.columns
        null_title = sum(1 for r in rows if r["title"] is None)
        null_loc   = sum(1 for r in rows if r["location"] is None)

        ok = True
        checks = []
        if read_count != 4:
            ok = False; checks.append(f"row count {read_count}!=4")
        if read_cols != ["id", "title", "location"]:
            ok = False; checks.append(f"cols {read_cols}")
        if null_title != 1:
            ok = False; checks.append(f"null_title {null_title}!=1")
        if null_loc != 1:
            ok = False; checks.append(f"null_loc {null_loc}!=1")

        if ok:
            status = "SUCCESS"
            validation = {"rows": read_count, "cols": read_cols,
                          "null_title": null_title, "null_loc": null_loc}
            print(f"  Validation PASSED: {validation}")
        else:
            error_msg = "; ".join(checks)
            print(f"  Validation FAILED: {error_msg}")

    except Exception as exc:
        error_msg = str(exc).split("\n")[0][:200]
        print(f"  FAILED: {error_msg}")
    finally:
        if spark:
            spark.stop()

    elapsed = round(time.time() - t0, 1)
    results.append({"name": test_name, "status": status, "elapsed": elapsed,
                     "error": error_msg, "validation": validation})

# --- Summary ---
print(f"\n{'='*60}")
print(f" RESULTS SUMMARY")
print(f"{'='*60}")
for r in results:
    print(f"  {r['name']:35s}  {r['status']:8s}  {r['elapsed']}s")
    if r["status"] == "SUCCESS":
        print(f"    {r['validation']}")
    else:
        print(f"    Error: {r['error']}")
