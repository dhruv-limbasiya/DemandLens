import os
import sys
import shutil
from pathlib import Path

# Setup environment variables for Windows execution
project_root = Path(r"D:\DemandLens")
if "JAVA_HOME" not in os.environ:
    for candidate in [Path(r"C:\Program Files\Java\jdk-24"), Path(r"C:\Program Files\Java\jdk-21")]:
        if candidate.exists() and (candidate / "bin" / "java.exe").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")
            break

local_hadoop = project_root / "hadoop"
if "HADOOP_HOME" not in os.environ and local_hadoop.exists():
    os.environ["HADOOP_HOME"] = str(local_hadoop)
    os.environ["PATH"] = str(local_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

base_test_dir = project_root / "data" / "temp_validation"
base_test_dir.mkdir(parents=True, exist_ok=True)

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

def run_single_test(test_name: str, hadoop_configs: dict) -> dict:
    print(f"\n==================================================")
    print(f" STARTING: {test_name}")
    print(f" Configs: {hadoop_configs}")
    print(f"==================================================")
    
    target_dir = base_test_dir / f"output_{test_name.lower().replace(' ', '_')}"
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
        
    builder = (
        SparkSession.builder.appName(f"Test_{test_name}")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
    )
    
    for k, v in hadoop_configs.items():
        builder = builder.config(k, v)
        
    spark = None
    result = {
        "test_name": test_name,
        "configs": hadoop_configs,
        "target_dir": str(target_dir),
        "status": "FAILED",
        "error": None,
        "validation": {},
    }
    
    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        # Apply hadoop config to jsc as well to be certain
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        for k, v in hadoop_configs.items():
            clean_k = k.replace("spark.hadoop.", "")
            hadoop_conf.set(clean_k, str(v))
            
        df = spark.createDataFrame(data=test_data, schema=schema)
        
        # Parquet write
        df.write.mode("overwrite").parquet(str(target_dir))
        
        # Read back
        read_df = spark.read.parquet(str(target_dir))
        read_rows = read_df.collect()
        read_count = len(read_rows)
        read_cols = read_df.columns
        null_counts = {col: read_df.filter(read_df[col].isNull()).count() for col in read_cols}
        
        assert read_count == len(test_data), f"Count mismatch: expected {len(test_data)}, got {read_count}"
        assert read_cols == ["id", "title", "location"], f"Columns mismatch: {read_cols}"
        assert null_counts["title"] == 1, "Expected 1 null title"
        assert null_counts["location"] == 1, "Expected 1 null location"
        
        result["status"] = "SUCCESS"
        result["validation"] = {
            "row_count": read_count,
            "columns": read_cols,
            "null_counts": null_counts,
            "sample_first_row": read_rows[0].asDict() if read_rows else None,
        }
        print(f"[SUCCESS] {test_name} passed completely!")
        print(f"Validation: {result['validation']}")
        
    except Exception as exc:
        result["status"] = "FAILED"
        result["error"] = str(exc)
        print(f"[FAILED] {test_name} failed: {exc}")
        
    finally:
        if spark is not None:
            spark.stop()
            
    return result

# 1. Baseline Test
baseline_res = run_single_test("Baseline", {})

# 2. Test A: Algorithm Version 2
test_a_res = run_single_test("Test_A_AlgoVersion2", {
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
})

# 3. Test B: RawLocalFileSystem
test_b_res = run_single_test("Test_B_RawLocalFS", {
    "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem"
})

# 4. Test C: Both combined
test_c_res = run_single_test("Test_C_Combined", {
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
})

print("\n" + "=" * 60)
print(" SUMMARY OF COMMUTER TESTS")
print("=" * 60)
for r in [baseline_res, test_a_res, test_b_res, test_c_res]:
    print(f"- {r['test_name']}: {r['status']}")
    if r['status'] == 'FAILED':
        err_msg = r['error'].split('\n')[0] if r['error'] else "Unknown"
        print(f"    Error: {err_msg[:120]}")
    else:
        print(f"    Validation: {r['validation']}")
