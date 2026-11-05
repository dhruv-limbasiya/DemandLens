"""DemandLens - Minimal PySpark Foundation Test Script.

This script verifies that Apache Spark / PySpark is correctly installed,
can initialize a local SparkSession, create a DataFrame, write it to
a local Parquet dataset, read it back, and cleanly shut down.
"""

import os
import sys
from pathlib import Path

# Auto-detect JAVA_HOME if not already set in the current environment
if "JAVA_HOME" not in os.environ:
    # Common standard Java installation paths on Windows
    candidate_paths = [
        Path(r"C:\Program Files\Java\jdk-24"),
        Path(r"C:\Program Files\Java\jdk-21"),
        Path(r"C:\Program Files\Java\jdk-17"),
        Path(r"C:\Program Files\Java\jdk-11"),
        Path(r"C:\Program Files\Eclipse Adoptium\jdk-17"),
        Path(r"C:\Program Files\Amazon Corretto\jdk17"),
    ]
    for candidate in candidate_paths:
        if candidate.exists() and (candidate / "bin" / "java.exe").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            # Prepend Java bin directory to PATH for this process
            java_bin = str(candidate / "bin")
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
            print(f"[INFO] Auto-detected JAVA_HOME: {candidate}")
            break

# Auto-detect HADOOP_HOME for Windows support (winutils.exe)
project_root = Path(__file__).resolve().parent.parent
local_hadoop = project_root / "hadoop"
if "HADOOP_HOME" not in os.environ and local_hadoop.exists():
    os.environ["HADOOP_HOME"] = str(local_hadoop)
    hadoop_bin = str(local_hadoop / "bin")
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    print(f"[INFO] Configured project HADOOP_HOME: {local_hadoop}")

# Ensure PySpark uses the current virtual environment Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    print("=" * 60)
    print(" DemandLens: PySpark Environment Verification")
    print("=" * 60)
    print(f"[INFO] Python executable : {sys.executable}")
    print(f"[INFO] Python version    : {sys.version.split()[0]}")
    print(f"[INFO] JAVA_HOME         : {os.environ.get('JAVA_HOME', 'NOT SET')}")
    print(f"[INFO] HADOOP_HOME       : {os.environ.get('HADOOP_HOME', 'NOT SET')}")

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError as e:
        print(f"\n[ERROR] PySpark is not installed in this environment: {e}")
        print("Please activate your virtual environment and run:")
        print("    pip install -r requirements.txt")
        sys.exit(1)

    output_dir = Path("data/bronze/test_verification_parquet")

    # Build local SparkSession
    print("\n[STEP 1/4] Initializing local PySpark session...")
    builder = (
        SparkSession.builder.appName("DemandLens-Foundation-Verification")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )

    # Enable native access for Java 21+ runtimes to suppress JVM warnings
    builder = builder.config(
        "spark.driver.extraJavaOptions",
        "--enable-native-access=ALL-UNNAMED"
    )

    spark = None
    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        print(f"[SUCCESS] SparkSession initialized (Spark version: {spark.version})")

        # Step 2: Create a sample DataFrame
        print("\n[STEP 2/4] Creating test job-postings DataFrame...")
        schema = StructType([
            StructField("job_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("company", StringType(), False),
            StructField("location", StringType(), True),
            StructField("salary_min", DoubleType(), True),
            StructField("salary_max", DoubleType(), True),
        ])

        sample_data = [
            (101, "Data Engineer", "TechCorp", "Remote", 110000.0, 140000.0),
            (102, "Data Scientist", "InsightWorks", "San Francisco, CA", 125000.0, 160000.0),
            (103, "Analytics Engineer", "CloudScale", "New York, NY", 105000.0, 135000.0),
        ]

        df = spark.createDataFrame(data=sample_data, schema=schema)
        print("[INFO] DataFrame schema:")
        df.printSchema()
        print("[INFO] Sample records:")
        df.show(truncate=False)

        # Step 3: Write DataFrame to Parquet
        print(f"[STEP 3/4] Writing test DataFrame to Parquet at: {output_dir}...")
        df.write.mode("overwrite").parquet(str(output_dir))
        print("[SUCCESS] Parquet files written successfully.")

        # Step 4: Read back and verify Parquet dataset
        print(f"\n[STEP 4/4] Reading back Parquet dataset from: {output_dir}...")
        read_back_df = spark.read.parquet(str(output_dir))
        read_count = read_back_df.count()
        print(f"[INFO] Rows successfully read back: {read_count}")

        assert read_count == len(sample_data), f"Expected {len(sample_data)} rows, got {read_count}"
        print("[SUCCESS] Row count matches! Round-trip Parquet verification passed.")

        print("\n" + "=" * 60)
        print(" [RESULT] PySpark Foundation Test: PASSED")
        print("=" * 60)

    except Exception as exc:
        print(f"\n[FAIL] PySpark verification encountered an error: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()
            print("[INFO] SparkSession stopped cleanly.")

if __name__ == "__main__":
    main()

