import os
import sys
from pathlib import Path

# Auto-detect JAVA_HOME and HADOOP_HOME
if "JAVA_HOME" not in os.environ:
    for c in [Path(r"C:\Program Files\Java\jdk-24"), Path(r"C:\Program Files\Java\jdk-21"), Path(r"C:\Program Files\Java\jdk-17")]:
        if c.exists() and (c / "bin" / "java.exe").exists():
            os.environ["JAVA_HOME"] = str(c)
            os.environ["PATH"] = str(c / "bin") + os.pathsep + os.environ.get("PATH", "")
            break

local_hadoop = Path(r"D:\DemandLens\hadoop")
if local_hadoop.exists():
    os.environ["HADOOP_HOME"] = str(local_hadoop)
    os.environ["PATH"] = str(local_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
import shutil

configs_to_test = [
    ("AlgoVersion2", {
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
    }),
    ("RawLocalFS", {
        "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
    }),
    ("NoMarkSuccessfulJobs", {
        "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
    }),
    ("ParquetOutputCommitter", {
        "spark.sql.parquet.output.committer.class": "org.apache.parquet.hadoop.ParquetOutputCommitter",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
    }),
]

for name, confs in configs_to_test:
    print(f"\n--- Testing config: {name} ---")
    builder = SparkSession.builder.appName(f"Test-{name}").master("local[2]")
    builder = builder.config("spark.ui.enabled", "false")
    builder = builder.config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED")
    for k, v in confs.items():
        builder = builder.config(k, v)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    target = Path(f"D:/DemandLens/data/tmp_test_{name}")
    shutil.rmtree(target, ignore_errors=True)
    try:
        df.write.mode("overwrite").parquet(str(target))
        read_df = spark.read.parquet(str(target))
        cnt = read_df.count()
        print(f"SUCCESS: {name} wrote and read {cnt} rows!")
        spark.stop()
        shutil.rmtree(target, ignore_errors=True)
        break
    except Exception as e:
        print(f"FAILED: {name} with error:\n{e}")
        spark.stop()
        shutil.rmtree(target, ignore_errors=True)
