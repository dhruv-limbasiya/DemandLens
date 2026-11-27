import os
os.environ["HADOOP_HOME"] = r"D:\\DemandLens\\hadoop"
os.environ["PYSPARK_PYTHON"] = r"D:\\DemandLens\\.venv\\Scripts\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\\DemandLens\\.venv\\Scripts\\python.exe"
os.environ["TEMP"] = r"D:\\DemandLens\\tmp_spark"
os.environ["TMP"] = r"D:\\DemandLens\\tmp_spark"

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def main():
    spark = SparkSession.builder \
        .appName("InspectCanonicalDimensions") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    path = "data/silver/canonical/jobs/"
    df = spark.read.parquet(path)

    dims = ["source_id", "country_code", "workplace_type", "seniority_tier", "freshness_category"]
    results = {}
    for d in dims:
        distinct_vals = df.select(d).distinct().orderBy(d).rdd.map(lambda row: row[0]).collect()
        null_count = df.filter(col(d).isNull()).count()
        results[d] = {
            "distinct_count": len(distinct_vals),
            "null_count": null_count,
            "distinct_values": distinct_vals
        }
    # Compute theoretical cube cardinality (product of distinct counts)
    cardinality = 1
    for info in results.values():
        cardinality *= max(info["distinct_count"], 1)  # avoid zero
    results["cube_cardinality"] = cardinality

    print(json.dumps(results, indent=2, default=str))
    spark.stop()

if __name__ == "__main__":
    main()
