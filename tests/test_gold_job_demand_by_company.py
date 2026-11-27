import os
import shutil
import sys
import unittest
import importlib.util

# Load the new script as a module
SCRIPT_PATH = os.path.abspath(os.path.join('scripts', 'gold_job_demand_by_company.py'))
spec = importlib.util.spec_from_file_location('gold_job_demand_by_company', SCRIPT_PATH)
gold_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gold_mod)

from pyspark.sql import SparkSession

class GoldJobDemandByCompanyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master('local[*]') \
            .appName('GoldJobDemandByCompanyTest') \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _write_silver_data(self, base_path, rows, cols):
        # Write synthetic data using pandas + pyarrow (no Spark write)
        import pandas as pd, pyarrow as pa, pyarrow.parquet as pq
        pdf = pd.DataFrame(rows, columns=cols)
        os.makedirs(base_path, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(pdf), os.path.join(base_path, 'data.parquet'))
        return pdf

    def test_aggregation_and_validation(self):
        tmp_dir = self._testMethodName
        silver_path = os.path.join('tmp', tmp_dir, 'silver')
        shutil.rmtree(os.path.join('tmp', tmp_dir), ignore_errors=True)
        rows = [
            ('src1', 'US', 'Acme Corp', 'REMOTE', True, True, 'Software Engineer', 'San Francisco, CA'),
            ('src1', 'US', 'Acme Corp', 'HYBRID', False, True, 'Data Scientist', 'New York, NY'),
            ('src2', 'CA', 'Maple Ltd', 'ON_SITE', True, False, 'Product Manager', 'Toronto'),
            ('src2', 'CA', None, 'REMOTE', True, True, 'Software Engineer', 'Vancouver'),
        ]
        cols = ['source_id','country_code','company_name_clean','workplace_type','has_skills','has_description','job_title_clean','location_clean']
        self._write_silver_data(silver_path, rows, cols)
        # Run aggregation
        silver_df, result_df = gold_mod.run_aggregation_company(self.spark, silver_path)
        # Expected groups: (src1,US,Acme Corp) and (src2,CA,Maple Ltd)
        self.assertEqual(result_df.count(), 2)
        # Validate metrics via script's validator
        self.assertEqual(gold_mod.validate_company_metrics(result_df), [])
        # Source reconciliation should be empty (each source rows match)
        self.assertEqual(gold_mod.reconcile_source(silver_df, result_df), [])

    def test_null_company_exclusion(self):
        tmp_dir = self._testMethodName
        silver_path = os.path.join('tmp', tmp_dir, 'silver')
        shutil.rmtree(os.path.join('tmp', tmp_dir), ignore_errors=True)
        rows = [
            ('src1', 'US', None, 'REMOTE', True, True, 'Software Engineer', 'Seattle'),
            ('src1', 'US', '', 'HYBRID', False, False, 'Data Analyst', 'Portland'),
            ('src1', 'US', '   ', 'ON_SITE', False, False, 'Product Manager', 'Austin'),
        ]
        cols = ['source_id','country_code','company_name_clean','workplace_type','has_skills','has_description','job_title_clean','location_clean']
        self._write_silver_data(silver_path, rows, cols)
        silver_df, result_df = gold_mod.run_aggregation_company(self.spark, silver_path)
        # All rows should be filtered out – no output rows
        self.assertEqual(result_df.count(), 0)

    def test_role_standardisation_in_company(self):
        tmp_dir = self._testMethodName
        silver_path = os.path.join('tmp', tmp_dir, 'silver')
        shutil.rmtree(os.path.join('tmp', tmp_dir), ignore_errors=True)
        rows = [
            ('src1', 'US', 'TechCorp', 'REMOTE', True, True, 'Senior Software Engineer', 'NYC'),
            ('src1', 'US', 'TechCorp', 'HYBRID', True, True, 'Engineering Manager', 'NYC'),
            ('src1', 'US', 'TechCorp', 'ON_SITE', True, True, 'Data Scientist', 'NYC'),
            ('src1', 'US', 'TechCorp', 'ON_SITE', False, True, 'Registered Nurse', 'NYC'),
        ]
        cols = ['source_id','country_code','company_name_clean','workplace_type','has_skills','has_description','job_title_clean','location_clean']
        self._write_silver_data(silver_path, rows, cols)
        silver_df, result_df = gold_mod.run_aggregation_company(self.spark, silver_path)
        self.assertIn('standardized_role', silver_df.columns)
        row = result_df.collect()[0]
        self.assertEqual(row['total_postings'], 4)
        self.assertEqual(row['distinct_roles'], 4)

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

    def test_rollback_success(self):
        tmp_dir = self._testMethodName
        base = os.path.join('tmp', tmp_dir)
        shutil.rmtree(base, ignore_errors=True)
        os.makedirs(base, exist_ok=True)
        staging = os.path.join(base, 'staging')
        final = os.path.join(base, 'final')
        os.makedirs(staging)
        os.makedirs(final)
        # Monkey‑patch rename: backup succeeds, promotion fails, rollback succeeds
        original_rename = os.rename
        def fake_rename(src, dst):
            if src == final:
                original_rename(src, dst)   # backup move
                return
            if '_backup_' in src:
                original_rename(src, dst)   # rollback move
                return
            raise OSError('forced rename failure')
        os.rename = fake_rename
        try:
            with self.assertRaises(RuntimeError) as ctx:
                gold_mod.promote_and_cleanup(staging, final)
            self.assertIn('ROLLBACK SUCCESSFUL', str(ctx.exception))
            self.assertTrue(os.path.isdir(final))
        finally:
            os.rename = original_rename

    def test_rollback_failure(self):
        tmp_dir = self._testMethodName
        base = os.path.join('tmp', tmp_dir)
        shutil.rmtree(base, ignore_errors=True)
        os.makedirs(base, exist_ok=True)
        staging = os.path.join(base, 'staging')
        final = os.path.join(base, 'final')
        os.makedirs(staging)
        os.makedirs(final)
        # Both promotion and rollback fail; backup succeed
        original_rename = os.rename
        def fail_rename(src, dst):
            if src == final and dst != staging:
                original_rename(src, dst)   # backup move succeeds
                return
            raise OSError('forced failure')
        os.rename = fail_rename
        try:
            with self.assertRaises(RuntimeError) as ctx:
                gold_mod.promote_and_cleanup(staging, final)
            self.assertIn('ROLLBACK FAILED', str(ctx.exception))
            self.assertFalse(os.path.isdir(final))
        finally:
            os.rename = original_rename

if __name__ == '__main__':
    unittest.main()
