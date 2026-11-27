import os
import shutil
import sys
import unittest
import importlib.util

# Load the script as a module
SCRIPT_PATH = os.path.abspath(os.path.join('scripts', 'gold_job_market_overview.py'))
spec = importlib.util.spec_from_file_location('gold_job_market_overview', SCRIPT_PATH)
gold_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gold_mod)

from pyspark.sql import SparkSession

class GoldJobMarketOverviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        hadoop_bin = os.path.abspath(os.path.join('hadoop', 'bin'))
        if os.path.isdir(hadoop_bin) and hadoop_bin not in os.environ.get('PATH', ''):
            os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
        cls.spark = SparkSession.builder \
            .master('local[*]') \
            .appName('GoldJobMarketTest') \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _write_silver_data(self, base_path, rows, cols):
        df = self.spark.createDataFrame(rows, schema=cols)
        df.write.mode('overwrite').parquet(base_path)
        return df

    def test_aggregation_and_validation(self):
        tmp_dir = self._testMethodName
        silver_path = os.path.join('tmp', tmp_dir, 'silver')
        os.makedirs(silver_path, exist_ok=True)
        rows = [
            ('src1', 'US', 'REMOTE', 'SENIOR', 'RECENT', True, True, True, 5),
            ('src1', 'US', 'HYBRID', 'JUNIOR', 'OLD', False, True, False, 3),
            ('src2', 'CA', 'ON_SITE', 'MID', 'RECENT', True, False, True, 2),
        ]
        cols = ['source_id','country_code','workplace_type','seniority_tier','freshness_category',
                'salary_disclosed','has_skills','has_description','skill_count']
        # Write synthetic silver data using pandas/pyarrow to avoid Spark's native Hadoop write issues
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        pdf = pd.DataFrame(rows, columns=cols)
        table = pa.Table.from_pandas(pdf)
        pq.write_table(table, os.path.join(silver_path, 'data.parquet'))
        # Load the parquet with Spark for source reconciliation
        silver_df = self.spark.read.parquet(silver_path)
        silver_df2, result_df = gold_mod.run_aggregation(self.spark, silver_path)
        # Expect three distinct dimension groups
        self.assertEqual(result_df.count(), 3)
        # Metrics validation should pass
        self.assertEqual(gold_mod.validate_metrics(result_df), [])
        # Source reconciliation should pass
        self.assertEqual(gold_mod.reconcile_source(silver_df2, result_df), [])

    def test_promotion_success(self):
        tmp_dir = self._testMethodName
        base = os.path.join('tmp', tmp_dir)
        shutil.rmtree(base, ignore_errors=True)
        os.makedirs(base, exist_ok=True)
        staging = os.path.join(base, 'staging')
        final = os.path.join(base, 'final')
        os.makedirs(staging)
        with open(os.path.join(staging, 'data.txt'), 'w') as f:
            f.write('staging')
        os.makedirs(final)
        with open(os.path.join(final, 'data.txt'), 'w') as f:
            f.write('final')
        backup = gold_mod.promote_and_cleanup(staging, final)
        self.assertFalse(os.path.isdir(staging))
        self.assertTrue(os.path.isdir(final))
        with open(os.path.join(final, 'data.txt')) as f:
            self.assertEqual(f.read(), 'staging')
        self.assertIsNotNone(backup)
        self.assertTrue(os.path.isdir(backup))
        with open(os.path.join(backup, 'data.txt')) as f:
            self.assertEqual(f.read(), 'final')

    def test_promotion_failure_and_rollback_success(self):
        tmp_dir = self._testMethodName
        base = os.path.join('tmp', tmp_dir)
        shutil.rmtree(base, ignore_errors=True)
        os.makedirs(base, exist_ok=True)
        staging = os.path.join(base, 'staging')
        final = os.path.join(base, 'final')
        os.makedirs(staging)
        os.makedirs(final)
        # Monkey‑patch os.rename to force failure on the second rename
        original_rename = os.rename
        def fake_rename(src, dst):
            # First rename (final -> backup) should succeed
            if src == final:
                original_rename(src, dst)
                return
            # During rollback, src will be the backup directory (contains '_backup_job_market_overview_')
            if '_backup_job_market_overview_' in src:
                # Simulate successful rollback rename
                original_rename(src, dst)
                return
            # All other rename attempts (staging -> final) fail
            raise OSError('forced rename failure')
        os.rename = fake_rename
        try:
            with self.assertRaises(RuntimeError) as ctx:
                gold_mod.promote_and_cleanup(staging, final)
            self.assertIn('ROLLBACK SUCCESSFUL', str(ctx.exception))
            self.assertTrue(os.path.isdir(final))
        finally:
            os.rename = original_rename

    def test_promotion_failure_and_rollback_failure(self):
        tmp_dir = self._testMethodName
        base = os.path.join('tmp', tmp_dir)
        shutil.rmtree(base, ignore_errors=True)
        os.makedirs(base, exist_ok=True)
        staging = os.path.join(base, 'staging')
        final = os.path.join(base, 'final')
        os.makedirs(staging)
        os.makedirs(final)
        # Both renames fail: backup rename succeeds, staging rename fails, rollback fails
        def fail_rename(src, dst):
            # Backup rename (final -> backup) succeeds
            if src == final and dst != staging:
                original_rename(src, dst)
                return
            # Rollback rename (backup -> final) fails
            if '_backup_job_market_overview_' in src:
                raise OSError('forced rollback failure')
            # Staging to final rename fails
            raise OSError('forced promotion failure')
        original_rename = os.rename
        os.rename = fail_rename
        try:
            with self.assertRaises(RuntimeError) as ctx:
                gold_mod.promote_and_cleanup(staging, final)
            self.assertIn('ROLLBACK FAILED', str(ctx.exception))
            # After failed rollback, final was moved to backup and could not be restored
            self.assertFalse(os.path.isdir(final))
        finally:
            os.rename = original_rename

if __name__ == '__main__':
    unittest.main()
