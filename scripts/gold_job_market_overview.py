import os
import shutil

import sys
import uuid
from datetime import datetime

# Process‑scoped environment for Spark (set before Spark import)
os.environ['HADOOP_HOME'] = r'D:\\DemandLens\\hadoop'
os.environ['PYSPARK_PYTHON'] = r'D:\\DemandLens\\.venv\\Scripts\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\\DemandLens\\.venv\\Scripts\\python.exe'
os.environ['TEMP'] = r'D:\\DemandLens\\tmp_spark'
os.environ['TMP'] = r'D:\\DemandLens\\tmp_spark'
os.environ['SPARK_LOCAL_DIRS'] = r'D:\\DemandLens\\tmp_spark'

# Spark imports after environment variables are set
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def safe_col(df, name, default_type='boolean'):
    """Return a column if it exists, otherwise a literal default.
    Args:
        df: Spark DataFrame
        name: column name
        default_type: 'boolean', 'integer', or other
    """
    if name in df.columns:
        return F.col(name)
    if default_type == 'boolean':
        return F.lit(False)
    if default_type == 'integer':
        return F.lit(0)
    return F.lit(None)


def run_aggregation(spark, data_path):
    """Read canonical silver jobs and compute gold metrics.
    Returns tuple (silver_df, result_df) where result_df includes the _gold_timestamp.
    """
    try:
        silver_df = spark.read.parquet(data_path)
    except Exception as read_err:
        # Fallback: use PyArrow/Pandas to read parquet when Hadoop native libraries fail (Windows)
        import pandas as pd
        import pyarrow.parquet as pq
        import pyarrow as pa
        # Collect all parquet files under the directory
        import os as _os
        files = [_os.path.join(data_path, f) for f in _os.listdir(data_path) if f.endswith('.parquet')]
        tables = [pq.read_table(f) for f in files]
        if tables:
            combined = pa.concat_tables(tables)
        else:
            combined = pa.Table.from_pandas(pd.DataFrame())
        pdf = combined.to_pandas()
        silver_df = spark.createDataFrame(pdf)
    dims = ["source_id", "country_code", "workplace_type", "seniority_tier", "freshness_category"]
    agg_df = silver_df.groupBy(*dims).agg(
        F.sum(F.lit(1)).alias('total_postings'),
        F.sum(F.when(F.col('workplace_type') == 'REMOTE', 1).otherwise(0)).alias('remote_postings'),
        F.sum(F.when(F.col('workplace_type') == 'HYBRID', 1).otherwise(0)).alias('hybrid_postings'),
        F.sum(F.when(F.col('workplace_type') == 'ON_SITE', 1).otherwise(0)).alias('onsite_postings'),
        F.sum(F.when(F.col('workplace_type').isin('UNSPECIFIED', ''), 1).otherwise(0)).alias('unspecified_postings'),
        F.sum(F.when(safe_col(silver_df, 'salary_disclosed', 'boolean') == True, 1).otherwise(0)).alias('salary_disclosed_postings'),
        F.sum(F.when(safe_col(silver_df, 'has_skills', 'boolean') == True, 1).otherwise(0)).alias('postings_with_skills'),
        F.sum(F.when(safe_col(silver_df, 'has_description', 'boolean') == True, 1).otherwise(0)).alias('postings_with_description'),
        F.avg(safe_col(silver_df, 'skill_count', 'integer')).alias('avg_skills_per_posting')
    )
    gold_ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    result_df = agg_df.withColumn('_gold_timestamp', F.lit(gold_ts))
    return silver_df, result_df


def validate_metrics(result_df):
    """Lightweight validation of metric values.
    Returns a list of error strings (empty if no errors).
    """
    errors = []
    summary = result_df.select(
        F.sum('total_postings').alias('sum_total'),
        F.sum('remote_postings').alias('sum_remote'),
        F.sum('hybrid_postings').alias('sum_hybrid'),
        F.sum('onsite_postings').alias('sum_onsite'),
        F.sum('unspecified_postings').alias('sum_unspecified')
    ).collect()[0]
    if (summary['sum_remote'] + summary['sum_hybrid'] + summary['sum_onsite'] + summary['sum_unspecified']) != summary['sum_total']:
        errors.append('Workplace counts do not sum to total postings')
    negative_count = result_df.filter(
        (F.col('total_postings') < 0) |
        (F.col('remote_postings') < 0) |
        (F.col('hybrid_postings') < 0) |
        (F.col('onsite_postings') < 0) |
        (F.col('unspecified_postings') < 0) |
        (F.col('salary_disclosed_postings') < 0) |
        (F.col('postings_with_skills') < 0) |
        (F.col('postings_with_description') < 0) |
        (F.col('avg_skills_per_posting') < 0)
    ).limit(1).count()
    if negative_count > 0:
        errors.append('Found negative metric values')
    return errors


def reconcile_source(silver_df, gold_df):
    """Compare source‑wise totals between silver input and gold aggregation.
    Returns a list of mismatch description strings.
    """
    silver_totals = silver_df.groupBy('source_id').agg(F.count('*').alias('src_total'))
    gold_totals = gold_df.groupBy('source_id').agg(F.sum('total_postings').alias('agg_total'))
    diff = silver_totals.join(gold_totals, on='source_id', how='full_outer') \
        .withColumn('src_total', F.coalesce('src_total', F.lit(0))) \
        .withColumn('agg_total', F.coalesce('agg_total', F.lit(0))) \
        .filter(F.col('src_total') != F.col('agg_total'))
    mismatches = []
    for row in diff.collect():
        mismatches.append(f"Source {row['source_id']}: silver {row['src_total']} != gold {row['agg_total']}")
    return mismatches


def promote_and_cleanup(staging_dir, final_dir):
    """Move staging dir to final location with backup and rollback protection.
    Returns the backup directory path (or None) on success.
    Raises RuntimeError on failure.
    """
    backup_dir = None
    try:
        if os.path.isdir(final_dir):
            ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            backup_dir = os.path.abspath(os.path.join(os.path.dirname(final_dir), f'_backup_job_market_overview_{ts}'))
            os.rename(final_dir, backup_dir)
        os.rename(staging_dir, final_dir)
    except Exception as promo_err:
        rollback_err = None
        if backup_dir and os.path.isdir(backup_dir):
            try:
                os.rename(backup_dir, final_dir)
            except Exception as rb_err:
                rollback_err = rb_err
        msg = f"PROMOTION FAILED: {promo_err}"
        if rollback_err:
            msg += f" | ROLLBACK FAILED: {rollback_err}"
        else:
            msg += " | ROLLBACK SUCCESSFUL"
        raise RuntimeError(msg)
    return backup_dir


def main():
    spark = SparkSession.builder \
        .appName('GoldJobMarketOverview') \
        .master('local[*]') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .getOrCreate()

    data_path = os.path.abspath(os.path.join('data', 'silver', 'canonical', 'jobs'))
    silver_df, result_df = run_aggregation(spark, data_path)

    # Validation
    metric_errors = validate_metrics(result_df)
    source_errors = reconcile_source(silver_df, result_df)
    all_errors = metric_errors + source_errors
    if all_errors:
        print('VALIDATION FAILED:', all_errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # Write to staging directory
    run_id = uuid.uuid4().hex[:8]
    staging_dir = os.path.abspath(os.path.join('data', 'gold', f'_staging_job_market_overview_{run_id}'))
    os.makedirs(staging_dir, exist_ok=True)
    result_df.write.mode('overwrite').parquet(staging_dir)

    # Promotion with rollback protection
    final_dir = os.path.abspath(os.path.join('data', 'gold', 'job_market_overview'))
    try:
        backup_dir = promote_and_cleanup(staging_dir, final_dir)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        spark.stop()
        sys.exit(1)

    print('Gold job market overview dataset written to', final_dir)
    if backup_dir:
        print('Previous version backed up at', backup_dir)

    spark.stop()

if __name__ == '__main__':
    main()
