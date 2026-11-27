import os
import shutil
import sys
import uuid
from datetime import datetime

from pathlib import Path

# Define project root for robust path resolution
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Ensure no leftover PySpark gateway env vars that may cause issues
for var in ['PYSPARK_GATEWAY_PORT', 'PYSPARK_GATEWAY_SECRET', 'PYSPARK_GATEWAY_ADDRESS']:
    os.environ.pop(var, None)

from src.demandlens.ingestion.dataset_inspector import configure_windows_environment
configure_windows_environment()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.demandlens.gold.role_standardizer import add_standardized_role_columns

# Reuse helpers from gold_job_market_overview
import importlib.util, os as _os
from pathlib import Path
_SCRIPT_PATH = str(Path(__file__).resolve().parent / 'gold_job_market_overview.py')
_spec = importlib.util.spec_from_file_location('gold_job_market_overview', _SCRIPT_PATH)
_gold_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_gold_mod)
# safe_col, reconcile_source, promote_and_cleanup are now available
safe_col = _gold_mod.safe_col
reconcile_source = _gold_mod.reconcile_source
promote_and_cleanup = _gold_mod.promote_and_cleanup


def run_aggregation_company(spark, data_path):
    """Read canonical jobs and aggregate by company.
    Returns (silver_df, result_df).
    """
    silver_df = spark.read.parquet(data_path)
    # Exclude rows without a company name
    silver_df = silver_df.filter(F.col('company_name_clean').isNotNull() & (F.trim(F.col('company_name_clean')) != ''))
    # Derive standardized_role from job_title_clean using Phase 7B taxonomy rules
    silver_df = add_standardized_role_columns(silver_df, title_col='job_title_clean')
    if 'standardized_role' not in silver_df.columns:
        raise RuntimeError(
            f"standardized_role missing before aggregation; columns present: {silver_df.columns}"
        )
    dims = ["source_id", "country_code", "company_name_clean"]
    agg_df = silver_df.groupBy(*dims).agg(
        F.count('*').alias('total_postings'),
        F.sum(F.when(F.col('workplace_type') == 'REMOTE', 1).otherwise(0)).alias('remote_postings'),
        F.sum(F.when(F.col('workplace_type') == 'HYBRID', 1).otherwise(0)).alias('hybrid_postings'),
        F.sum(F.when(F.col('workplace_type') == 'ON_SITE', 1).otherwise(0)).alias('onsite_postings'),
        F.sum(F.when(safe_col(silver_df, 'has_skills', 'boolean') == True, 1).otherwise(0)).alias('postings_with_skills'),
        F.sum(F.when(safe_col(silver_df, 'has_description', 'boolean') == True, 1).otherwise(0)).alias('postings_with_description'),
        F.countDistinct('standardized_role').alias('distinct_roles'),
        F.countDistinct('location_clean').alias('distinct_locations')
    )
    gold_ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    result_df = agg_df.withColumn('_gold_timestamp', F.lit(gold_ts))
    return silver_df, result_df


def validate_company_metrics(df):
    """Validate metric sanity for company aggregation.
    Returns list of error strings (empty if OK)."""
    errors = []
    # Workplace counts must sum to total
    summary = df.select(
        F.sum('total_postings').alias('sum_total'),
        F.sum('remote_postings').alias('sum_remote'),
        F.sum('hybrid_postings').alias('sum_hybrid'),
        F.sum('onsite_postings').alias('sum_onsite')
    ).collect()[0]
    if (summary['sum_remote'] + summary['sum_hybrid'] + summary['sum_onsite']) != summary['sum_total']:
        errors.append('Workplace counts do not sum to total postings')
    # No negative values
    neg = df.filter(
        (F.col('total_postings') < 0) |
        (F.col('remote_postings') < 0) |
        (F.col('hybrid_postings') < 0) |
        (F.col('onsite_postings') < 0) |
        (F.col('postings_with_skills') < 0) |
        (F.col('postings_with_description') < 0) |
        (F.col('distinct_roles') < 0) |
        (F.col('distinct_locations') < 0)
    ).limit(1).count()
    if neg > 0:
        errors.append('Found negative metric values')
    return errors


def main():
    spark = SparkSession.builder \
        .appName('GoldJobDemandByCompany') \
        .master('local[*]') \
        .config('spark.driver.memory', '4g') \
        .config('spark.sql.shuffle.partitions', '16') \
        .config('spark.ui.enabled', 'false') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .getOrCreate()
    data_path = str(PROJECT_ROOT / 'data' / 'silver' / 'canonical' / 'jobs')
    silver_df, result_df = run_aggregation_company(spark, data_path)
    # Validation
    metric_errors = validate_company_metrics(result_df)
    source_errors = reconcile_source(silver_df, result_df)
    all_errors = metric_errors + source_errors
    if all_errors:
        print('VALIDATION FAILED:', all_errors, file=sys.stderr)
        spark.stop()
        sys.exit(1)
    # Write staging
    run_id = uuid.uuid4().hex[:8]
    staging_dir = str(PROJECT_ROOT / 'data' / 'gold' / f'_staging_job_demand_by_company_{run_id}')
    os.makedirs(staging_dir, exist_ok=True)
    result_df.write.mode('overwrite').parquet(staging_dir)
    # Promotion
    final_dir = str(PROJECT_ROOT / 'data' / 'gold' / 'job_demand_by_company')
    try:
        backup_dir = promote_and_cleanup(staging_dir, final_dir)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        spark.stop()
        sys.exit(1)
    print('Gold job demand by company dataset written to', final_dir)
    if backup_dir:
        print('Previous version backed up at', backup_dir)
    spark.stop()

if __name__ == '__main__':
    main()

