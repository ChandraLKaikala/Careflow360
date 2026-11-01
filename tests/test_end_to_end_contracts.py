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

    def test_partial_run_files_cannot_be_overwritten(self):
        fs = FakeFs()
        base = "/Volumes/workspace/careflow360/landing/events/triage"
        fs.mkdirs(base)
        saved = f"{base}/batch_partial01_0000.json"
        fs.put(saved, '{"event_id":"existing"}\\n', False)
        with self.assertRaisesRegex(ValueError, "Partial run"):
            execute_code(GENERATOR, FakeSpark(), FakeDbutils(self.settings("partial01"), fs))
        self.assertEqual(fs.files[saved], '{"event_id":"existing"}\\n')

    def test_larger_microbatch_replay_never_future_dates_events(self):
        fs = FakeFs()
        execute_code(GENERATOR, FakeSpark(),
                     FakeDbutils(self.settings("large01", n="1200", batches="20"), fs))
        triage = event_rows(fs, "triage")
        adt = event_rows(fs, "adt")
        self.assertEqual(len(triage), 1200)
        self.assertEqual(len({r["patient_id"] for r in triage}), 1200)
        self.assertTrue(all(datetime.fromisoformat(r["event_ts"].replace("Z", "+00:00"))
                            <= datetime.now(timezone.utc) for r in adt))


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


class PolicyOutputTests(unittest.TestCase):
    def test_policy_table_contains_only_synthetic_demo_documents(self):
        class Writer:
            def __init__(self, parent):
                self.parent = parent
                self.options = {}

            def mode(self, name):
                self.options["mode"] = name
                return self

            def option(self, key, value):
                self.options[key] = value
                return self

            def saveAsTable(self, name):
                self.parent.saved_table = name

        class Frame:
            def __init__(self, rows, columns):
                self.rows = rows
                self.columns = columns
                self.write = Writer(self)
                self.saved_table = None

            def count(self):
                return len(self.rows)

        class PolicySpark(FakeSpark):
            def createDataFrame(self, rows, columns):
                self.frame = Frame(rows, columns)
                return self.frame

        spark = PolicySpark()
        dbutils = FakeDbutils({"catalog": "workspace", "schema": "careflow360_gold"})
        execute_code(POLICY, spark, dbutils)
        frame = spark.frame
        self.assertEqual(frame.saved_table, "workspace.careflow360_gold.policy_chunks")
        self.assertGreaterEqual(frame.count(), 6)
        self.assertEqual(len(frame.rows), len({r[0] for r in frame.rows}))
        self.assertEqual({r[4] for r in frame.rows}, {"synthetic_demo_policy"})
        self.assertTrue(all(r[3].strip() for r in frame.rows))
        self.assertTrue(any("delta.enableChangeDataFeed" in query for query in spark.statements))
        # The standalone unified-schema quickstart must retain its legacy name.
        unified = PolicySpark()
        execute_code(POLICY, unified, FakeDbutils({
            "catalog": "workspace", "schema": "careflow360_dev"
        }))
        self.assertEqual(unified.frame.saved_table, "workspace.careflow360_dev.gold_policy_chunks")
        # An explicit override must work for arbitrary presentation schema names.
        custom = PolicySpark()
        execute_code(POLICY, custom, FakeDbutils({
            "catalog": "workspace", "schema": "my_presentation",
            "policy_table_name": "policy_chunks"
        }))
        self.assertEqual(custom.frame.saved_table, "workspace.my_presentation.policy_chunks")
        with self.assertRaisesRegex(ValueError, "Unsafe policy_table_name"):
            execute_code(POLICY, PolicySpark(), FakeDbutils({
                "catalog": "workspace", "schema": "careflow360_gold",
                "policy_table_name": "bad;DROP"
            }))


class OptionalExtensionContractTests(unittest.TestCase):
    def test_app_and_genie_use_actual_gold_views(self):
        ext = ROOT / "CareFlow360_Databricks_Free_Edition/extensions"
        for extension in ("genie", "app"):
            bundle = yaml.safe_load((ext / extension / "databricks.yml").read_text())
            self.assertEqual(bundle["variables"]["schema"]["default"], "careflow360_gold")
            self.assertEqual(bundle["variables"]["table_prefix"]["default"], "")
            path = ext / extension / "resources" / f"{extension}.yml"
            text = path.read_text(encoding="utf-8")
            self.assertIn("${var.table_prefix}patient_360", text)
            self.assertNotIn("${var.schema}.gold_patient_360", text)
        for extension in ("ai_search", "model_serving"):
            bundle = yaml.safe_load((ext / extension / "databricks.yml").read_text())
            self.assertEqual(bundle["variables"]["schema"]["default"], "careflow360_gold")
        search_bundle = yaml.safe_load((ext / "ai_search/databricks.yml").read_text())
        self.assertEqual(search_bundle["variables"]["table_prefix"]["default"], "")
        search_resource = (ext / "ai_search/resources/ai_search.yml").read_text()
        self.assertIn("${var.schema}.${var.table_prefix}policy_chunks", search_resource)
        self.assertNotIn("${var.schema}.gold_policy_chunks", search_resource)
        genie = yaml.safe_load((ext / "genie/resources/genie.yml").read_text())
        serialized = json.loads(genie["resources"]["genie_spaces"]["careflow_genie"]["serialized_space"])
        self.assertEqual(len(serialized["data_sources"]["tables"]), 5)
        instructions = " ".join(
            " ".join(x["content"]) for x in serialized["instructions"]["text_instructions"]
        )
        self.assertIn("never show patient identifiers", instructions)

    def test_dashboard_sql_pack_is_aggregate_only(self):
        sql = (ROOT / "CareFlow360_Databricks_Free_Edition/core/sql/dashboard_queries.sql").read_text()
        self.assertIn("ORDER BY snapshot_ts DESC", sql)
        self.assertNotIn("SELECT patient_id", sql)
        self.assertIn("GROUP BY DATE_TRUNC('HOUR', event_ts), source", sql)


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
        policy_params = by_key["create_policy_docs"]["notebook_task"]["base_parameters"]
        self.assertEqual(policy_params["policy_table_name"], "policy_chunks")
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
