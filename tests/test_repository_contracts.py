"""Static regression tests for the CareFlow360 Databricks bundle.

These run without Databricks credentials. Integration validation still requires
running the bundle in a real workspace.
"""

import ast
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]


def read(path):
    return (ROOT / path).read_text(encoding="utf-8")


class BundleContractTests(unittest.TestCase):
    def test_all_source_files_compile(self):
        for path in sorted((ROOT / "src").rglob("*.py")):
            with self.subTest(path=path.relative_to(ROOT)):
                compile(path.read_text(encoding="utf-8"), str(path), "exec")

    def test_yaml_files_parse(self):
        for path in sorted(ROOT.glob("**/*.yml")):
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertIsInstance(yaml.safe_load(path.read_text(encoding="utf-8")), dict)

    def test_run_aware_patient_ids_are_distinct(self):
        tree = ast.parse(read("src/notebooks/01_generate_sample_data.py"))
        fn = next(
            n for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "make_patient_ids"
        )
        namespace = {}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<patient_id_test>", "exec"), namespace)
        patient_ids = namespace["make_patient_ids"]
        first = patient_ids("RUN_A", 100)
        second = patient_ids("RUN_B", 100)
        self.assertEqual(len(first), 100)
        self.assertEqual(len(set(first)), 100)
        self.assertFalse(set(first).intersection(second))

    def test_incremental_job_does_not_reset_landing(self):
        jobs = yaml.safe_load(read("resources/03_jobs.yml"))["resources"]["jobs"]
        tasks = {t["task_key"]: t for t in jobs["careflow360_job"]["tasks"]}
        params = tasks["generate_sample_events"]["notebook_task"]["base_parameters"]
        self.assertEqual(params["reset_events"], "false")
        self.assertEqual(params["num_patients"], "100")
        self.assertEqual(params["num_batches"], "2")
        self.assertIn("refresh_lakehouse", tasks)
        self.assertIn("validate_project", tasks)
        self.assertEqual(
            tasks["train_triage_model"]["notebook_task"]["base_parameters"]["schema"],
            "$" + "{resources.schemas.gold_schema.name}",
        )

    def test_ml_uses_gold_presentation_views(self):
        src = read("src/notebooks/04_train_triage_model.py")
        self.assertIn('source_table = f"{catalog}.{schema}.triage_training"', src)
        self.assertIn('f"{catalog}.{schema}.triage_predictions"', src)
        self.assertNotIn('f"{catalog}.{schema}.gold_triage_training"', src)

    def test_quarantine_can_observe_invalid_triage_identifiers(self):
        bronze = read("src/pipelines/01_bronze.py")
        silver = read("src/pipelines/02_silver.py")
        self.assertIn('@dp.expect("triage_has_ids"', bronze)
        self.assertIn('@dp.expect_or_drop("triage_valid_ids"', silver)
        self.assertIn('name="silver_triage_quarantine"', silver)

    def test_patient_360_joins_are_encounter_scoped(self):
        source = read("src/pipelines/03_gold.py")
        self.assertIn(
            'Window.partitionBy("patient_id", "encounter_id").orderBy',
            source,
        )
        for table in ("vitals", "complaints", "location"):
            self.assertIn(
                f'.join({table}, ["patient_id", "encounter_id"], "left")',
                source,
            )
        self.assertIn(
            'latest.join(admitted, ["patient_id", "encounter_id"], "inner")',
            source,
        )
        self.assertIn('F.col("is_currently_admitted") == F.lit(True)', source)

    def test_validation_rejects_stale_alert_encounters(self):
        source = read("src/notebooks/06_validate.py")
        self.assertIn('invalid_alerts =', source)
        self.assertIn('["patient_id", "encounter_id"], "left"', source)
        self.assertIn('without an active encounter', source)

    def test_validation_targets_presentation_assets(self):
        src = read("src/notebooks/06_validate.py")
        for name in ("bronze_schema", "silver_schema", "gold_schema"):
            self.assertIn(name, src)
        for name in ("triage_training", "triage_predictions", "patient_360"):
            self.assertIn(name, src)


if __name__ == "__main__":
    unittest.main()
