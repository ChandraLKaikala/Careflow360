"""Offline executable tests for CareFlow360 inputs, outputs, and DAB wiring.

No Spark cluster is involved; live pipeline and model runs require Databricks.
"""
from __future__ import annotations

import contextlib
import io
import json
import pathlib
import types
import unittest
from datetime import datetime, timezone

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "src/notebooks/01_generate_sample_data.py"
VIEWS = ROOT / "src/notebooks/03_create_views.py"
POLICY = ROOT / "src/notebooks/05_create_policy_docs.py"


class FakeWidgets:
    def __init__(self, values):
        self.values = dict(values)

    def text(self, name, default, label=None):
        self.values.setdefault(name, str(default))

    def get(self, name):
        return self.values[name]


class FakeFs:
    def __init__(self):
        self.files = {}
        self.rm_called = False
        self.dirs = set()

    def mkdirs(self, path):
        self.dirs.add(path)
        return True

    def ls(self, path):
        if path not in self.dirs:
            raise FileNotFoundError(path)
        prefix = path.rstrip("/") + "/"
        return [
            types.SimpleNamespace(name=p.removeprefix(prefix))
            for p in sorted(self.files)
            if p.startswith(prefix) and "/" not in p[len(prefix):]
        ]

    def put(self, path, content, overwrite=False):
        if path in self.files and not overwrite:
            raise FileExistsError(path)
        self.files[path] = content
        return True

    def rm(self, *args, **kwargs):
        self.rm_called = True
        raise AssertionError("Generated data must never be deleted by a daily run")


class FakeDbutils:
    def __init__(self, values, fs=None):
        self.widgets = FakeWidgets(values)
        self.fs = fs or FakeFs()


class FakeSparkResult:
    def first(self):
        return {"c": "workspace"}


class FakeSpark:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        return FakeSparkResult()


def execute_code(path, spark, dbutils):
    code = path.read_text(encoding="utf-8")
    namespace = {"spark": spark, "dbutils": dbutils}
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(code, str(path), "exec"), namespace)
    return namespace


def event_rows(fs, stream):
    rows = []
    for path, payload in fs.files.items():
        if f"/events/{stream}/" in path:
            rows.extend(json.loads(line) for line in payload.splitlines() if line.strip())
    return rows


class GeneratorOutputTests(unittest.TestCase):
    def settings(self, run_id, reset="false", n="50", batches="2"):
        return {
            "catalog": "workspace", "schema": "careflow360", "volume": "landing",
            "num_patients": n, "num_batches": batches, "reset_events": reset,
            "seconds_between_batches": "0", "run_id": run_id,
        }

    def test_generated_records_and_append(self):
        fs = FakeFs()
        for run_id in ("smoke01", "smoke02"):
            execute_code(GENERATOR, FakeSpark(), FakeDbutils(self.settings(run_id), fs))

        triage = event_rows(fs, "triage")
        vitals = event_rows(fs, "vitals")
        adt = event_rows(fs, "adt")
        chief = event_rows(fs, "chief_complaints")
        self.assertEqual(len(triage), 100)
        self.assertEqual(len(chief), 100)
        self.assertGreaterEqual(len(vitals), 300)
        self.assertGreaterEqual(len(adt), 100)
        self.assertEqual(len({r["patient_id"] for r in triage}), 100)
        self.assertEqual(len({r["event_id"] for r in triage}), len(triage))
        self.assertEqual(len({r["event_id"] for r in vitals}), len(vitals))
        self.assertEqual(len({r["event_id"] for r in adt}), len(adt))
        self.assertEqual({r["event_type"] for r in adt} - {"A01_ADMIT", "A02_TRANSFER", "A03_DISCHARGE"}, set())
        self.assertTrue(all(1 <= r["triage_acuity"] <= 5 for r in triage))
        self.assertTrue(all(75 <= r["spo2"] <= 100 for r in vitals))
        self.assertTrue(all(0 < r["age"] <= 120 for r in triage))
        self.assertTrue(all(r["source"] == "careflow360_synthetic" for r in triage))
        future_tolerance = datetime.now(timezone.utc).timestamp() + 60
        for collection in (triage, vitals, adt, chief):
            self.assertTrue(all(datetime.fromisoformat(r["event_ts"].replace("Z", "+00:00")).timestamp() <= future_tolerance for r in collection))
        manifests = [k for k in fs.files if "/manifests/" in k]
        self.assertEqual(len(manifests), 2)
        for path in manifests:
            manifest = json.loads(fs.files[path])
            self.assertEqual(manifest["num_patients"], 50)
            self.assertEqual(manifest["rows"]["triage"], 50)
            self.assertEqual(manifest["rows"]["chief_complaints"], 50)
            self.assertIn("Not clinically validated", manifest["disclaimer"])

    def test_duplicate_run_id_is_rejected_without_overwrite(self):
        fs = FakeFs()
        dbutils = FakeDbutils(self.settings("same01"), fs)
        execute_code(GENERATOR, FakeSpark(), dbutils)
        before = dict(fs.files)
        with self.assertRaisesRegex(ValueError, "already exists"):
            execute_code(GENERATOR, FakeSpark(), FakeDbutils(self.settings("same01"), fs))
        self.assertEqual(before, fs.files)

    def test_destructive_reset_is_rejected(self):
        fs = FakeFs()
        with self.assertRaisesRegex(ValueError, "disabled"):
            execute_code(GENERATOR, FakeSpark(), FakeDbutils(self.settings("reset01", reset="true"), fs))
        self.assertFalse(fs.rm_called)
        self.assertEqual(fs.files, {})


class ViewOutputTests(unittest.TestCase):
    def test_presentation_views_target_expected_tables(self):
        spark = FakeSpark()
        values = {"catalog": "workspace", "pipeline_schema": "careflow360",
                  "bronze_schema": "careflow360_bronze",
                  "silver_schema": "careflow360_silver",
                  "gold_schema": "careflow360_gold"}
        execute_code(VIEWS, spark, FakeDbutils(values))
        statements = [x for x in spark.statements if "CREATE OR REPLACE VIEW" in x]
        self.assertEqual(len(statements), 15)
        self.assertEqual(len(set(statements)), 15)
        self.assertTrue(any("careflow360_gold.patient_360" in sql and
                            "careflow360.gold_patient_360" in sql for sql in statements))
        self.assertTrue(any("careflow360_gold.triage_training" in sql and
                            "careflow360.gold_triage_training" in sql for sql in statements))
        self.assertTrue(any("careflow360_silver.patient_location_history" in sql
                            for sql in statements))

    def test_invalid_sql_identifier_rejected(self):
        bad = FakeDbutils({"catalog": "workspace;DROP", "pipeline_schema": "careflow360"})
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            execute_code(VIEWS, FakeSpark(), bad)


class BundleContractTests(unittest.TestCase):
    def test_dab_dependencies_paths_and_daily_schedule(self):
        cfg = yaml.safe_load((ROOT / "resources/03_jobs.yml").read_text())
        job = cfg["resources"]["jobs"]["careflow360_job"]
        self.assertEqual(job["schedule"]["quartz_cron_expression"], "0 0 2 * * ?")
        self.assertEqual(job["schedule"]["timezone_id"], "UTC")
        self.assertEqual(job["schedule"]["pause_status"], "UNPAUSED")
        self.assertEqual(job["max_concurrent_runs"], 1)
        tasks = job["tasks"]
        self.assertEqual(len(tasks), 6)
        by_key = {t["task_key"]: t for t in tasks}
        self.assertEqual(len(by_key), len(tasks))
        self.assertEqual(set(by_key), {
            "generate_sample_events", "refresh_lakehouse", "create_presentation_views",
            "create_policy_docs", "train_triage_model", "validate_project",
        })
        for key, task in by_key.items():
            for dep in task.get("depends_on", []):
                self.assertIn(dep["task_key"], by_key)
                self.assertNotEqual(dep["task_key"], key)
            if "notebook_task" in task:
                path = (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve()
                self.assertTrue(path.is_file(), f"Notebook missing for {key}: {path}")
            if "pipeline_task" in task:
                self.assertEqual(task["pipeline_task"].get("full_refresh"), False)
        params = by_key["generate_sample_events"]["notebook_task"]["base_parameters"]
        self.assertEqual(params["reset_events"], "false")
        self.assertEqual(params["num_patients"], "100")

    def test_pipeline_glob_and_source_paths(self):
        cfg = yaml.safe_load((ROOT / "resources/02_pipeline.yml").read_text())
        pipeline = cfg["resources"]["pipelines"]["hospital_pipeline"]
        self.assertTrue(pipeline["serverless"])
        self.assertFalse(pipeline["continuous"])
        self.assertEqual(pipeline["channel"], "CURRENT")
        self.assertTrue(all((ROOT / "src/pipelines" / f).is_file()
                            for f in ["01_bronze.py", "02_silver.py", "03_gold.py"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
