"""Guarded update plan for the existing scheduled Databricks job.

Offline unit tests only: these do not modify Databricks jobs or volumes.
"""
import ast
import copy
import pathlib
import posixpath
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
HELPER = ROOT / "src/notebooks/08_upgrade_existing_daily_job.py"
PIPELINE_ID = "dde7c33b-8715-4666-8f6b-ad2af0848966"


def load_helper_function(name):
    tree = ast.parse(HELPER.read_text(encoding="utf-8"))
    target = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    global_vars = {
        "copy": copy, "posixpath": posixpath, "PIPELINE_ID": PIPELINE_ID,
        "JOB_NAME": "CareFlow360 Daily Incremental Pipeline",
        "CRON": "0 0 2 * * ?",
        "SCHEMA": "careflow360",
        "GOLD": "careflow360_gold",
    }
    exec(compile(ast.Module(body=[target], type_ignores=[]), str(HELPER), "exec"), global_vars)
    return global_vars[name]


def old_tasks():
    return [
        {
            "task_key": "generate_daily_patients", "timeout_seconds": 1800,
            "notebook_task": {"notebook_path": "/old/generator",
                              "base_parameters": {"num_patients": "100"}},
        },
        {
            "task_key": "refresh_pipeline",
            "pipeline_task": {"pipeline_id": PIPELINE_ID, "full_refresh": True},
            "depends_on": [{"task_key": "generate_daily_patients"}],
        },
        {
            "task_key": "create_presentation_views", "timeout_seconds": 600,
            "depends_on": [{"task_key": "refresh_pipeline"}],
            "notebook_task": {"notebook_path": "/old/views", "base_parameters": {}},
        },
        {
            "task_key": "train_ml_model", "timeout_seconds": 1200,
            "depends_on": [{"task_key": "create_presentation_views"}],
            "notebook_task": {
                "notebook_path": "/old/ml",
                "base_parameters": {
                    "experiment_path": "/Users/demo/careflow360/triage_experiment",
                    "model_name": "triage_acuity_model",
                },
            },
        },
    ]


def paths():
    return {stem + ".py": "/Workspace/Users/demo/Careflow360/src/notebooks/" + stem
            for stem in ["01_generate_sample_data", "03_create_views",
                         "04_train_triage_model", "05_create_policy_docs",
                         "06_validate"]}


class LiveJobUpgradePlanTests(unittest.TestCase):
    def test_4_to_6_tasks_preserve_pipeline_schedule_inputs_and_idempotency(self):
        planner = load_helper_function("make_plan")
        original = old_tasks()
        plan = planner(original, paths())
        self.assertEqual(len(original), 4)
        self.assertEqual(original[1]["pipeline_task"]["full_refresh"], True,
                         "The pure planner must not mutate existing settings")
        self.assertEqual(len(plan), 6)
        tasks = {t["task_key"]: t for t in plan}
        self.assertEqual(set(tasks), {
            "generate_daily_patients", "refresh_pipeline", "create_presentation_views",
            "create_policy_docs", "train_ml_model", "validate_project",
        })
        self.assertEqual(tasks["refresh_pipeline"]["pipeline_task"], {
            "pipeline_id": PIPELINE_ID, "full_refresh": False,
        })
        self.assertEqual(tasks["generate_daily_patients"]["timeout_seconds"], 1800)
        params = tasks["generate_daily_patients"]["notebook_task"]["base_parameters"]
        self.assertEqual(params["run_id"], "{{job.run_id}}")
        self.assertEqual(params["num_patients"], "random")
        self.assertEqual(params["reset_events"], "false")
        self.assertEqual(params["schema"], "careflow360")
        self.assertEqual(tasks["train_ml_model"]["notebook_task"]["base_parameters"]["experiment_path"],
                         "/Users/demo/careflow360/triage_experiment")
        self.assertEqual(tasks["create_policy_docs"]["notebook_task"]["base_parameters"]["policy_table_name"],
                         "policy_chunks")
        self.assertEqual(tasks["validate_project"]["depends_on"],
                         [{"task_key": "create_policy_docs"}, {"task_key": "train_ml_model"}])
        vp = tasks["validate_project"]["notebook_task"]["base_parameters"]
        self.assertEqual(vp["expected_run_id"],
                         "{{tasks.generate_daily_patients.values.run_id}}")
        self.assertEqual(vp["expected_new_patients"],
                         "{{tasks.generate_daily_patients.values.num_patients}}")
        self.assertEqual(vp["schema"], "careflow360")
        self.assertEqual(vp["landing_volume"], "landing")
        self.assertEqual(planner(plan, paths()), plan, "Rerunning the upgrade must not add tasks")

    def test_unknown_tasks_or_different_pipeline_are_rejected(self):
        planner = load_helper_function("make_plan")
        foreign = old_tasks()
        foreign[1]["pipeline_task"]["pipeline_id"] = "another-pipeline-id"
        with self.assertRaisesRegex(RuntimeError, "Unexpected pipeline ID"):
            planner(foreign, paths())
        foreign = old_tasks()
        foreign[2]["task_key"] = "unknown_task"
        with self.assertRaisesRegex(RuntimeError, "Unexpected live task keys"):
            planner(foreign, paths())
        foreign = old_tasks()
        foreign[1]["depends_on"] = []
        with self.assertRaisesRegex(RuntimeError, "Unexpected pipeline task dependencies"):
            planner(foreign, paths())

    def test_schedule_and_job_identity_guard(self):
        guard = load_helper_function("check_job")
        class Schedule:
            def __init__(self, pause="UNPAUSED"):
                self.pause = pause

            def as_dict(self):
                return {"quartz_cron_expression": "0 0 2 * * ?",
                        "timezone_id": "UTC", "pause_status": self.pause}

        settings = types.SimpleNamespace(
            name="CareFlow360 Daily Incremental Pipeline",
            git_source=None, schedule=Schedule(),
        )
        self.assertEqual(guard(settings)["pause_status"], "UNPAUSED")
        settings.schedule = Schedule("PAUSED")
        with self.assertRaisesRegex(RuntimeError, "Unexpected/paused schedule"):
            guard(settings)
        settings.schedule = Schedule()
        settings.git_source = object()
        with self.assertRaisesRegex(RuntimeError, "Wrong job name"):
            guard(settings)

    def test_pipeline_libraries_must_point_at_pulled_git_sources(self):
        guard = load_helper_function("pipeline_source_paths")
        root = "/Workspace/Users/demo/Careflow360/src/notebooks"
        base = "/Workspace/Users/demo/Careflow360/src/pipelines/"
        def spec(paths, schema="careflow360", continuous=False):
            return types.SimpleNamespace(
                catalog="workspace", schema=schema, target=None,
                continuous=continuous, root_path=None,
                libraries=[
                    types.SimpleNamespace(as_dict=lambda path=p: {"file": {"path": path}})
                    for p in paths
                ],
            )
        all_paths = [base + x for x in ("01_bronze.py", "02_silver.py", "03_gold.py")]
        self.assertEqual(len(guard(spec(all_paths), root)), 3)
        with self.assertRaisesRegex(RuntimeError, "different folder"):
            guard(spec(["/Users/demo/old/01_bronze.py"] + all_paths[1:]), root)
        with self.assertRaisesRegex(RuntimeError, "missing source files"):
            guard(spec(all_paths[:2]), root)
        with self.assertRaisesRegex(RuntimeError, "catalog/schema differs"):
            guard(spec(all_paths, schema="wrong_schema"), root)
        with self.assertRaisesRegex(RuntimeError, "continuous"):
            guard(spec(all_paths, continuous=True), root)
        glob_spec = types.SimpleNamespace(
            catalog="workspace", schema="careflow360", target=None,
            continuous=False, root_path=None,
            libraries=[types.SimpleNamespace(
                as_dict=lambda: {"glob": {"include": base + "**"}}
            )],
        )
        self.assertEqual(len(guard(glob_spec, root)), 1)

    def test_script_is_dry_run_first(self):
        source = HELPER.read_text(encoding="utf-8")
        self.assertIn('dbutils.widgets.dropdown("apply_changes", "false"', source)
        self.assertIn('dbutils.widgets.dropdown("verify_with_new_run", "false"', source)
        self.assertIn("client.jobs.update(", source)
        self.assertNotIn("client.jobs.create(", source)
        self.assertNotIn("client.jobs.reset(", source)
        self.assertNotIn("client.jobs.delete(", source)
        self.assertIn("save_backup(client, current.settings)", source)
        self.assertIn("checked_notebooks(client, root)", source)
        self.assertIn("verify_pipeline_sources(client, root)", source)
        self.assertIn("client.jobs.list_runs(job_id=JOB_ID, active_only=True, limit=1)", source)
        self.assertIn('"expected_stream_counts_from_manifest"', source)
        self.assertIn('"upsert_policy_chunks"', source)
        # Task values are NOT readable via jobs.get_run_output in this SDK
        # (RunOutput has no task_values field): the immutable generator
        # manifest is the supported mechanism instead.
        self.assertIn(
            'manifest_path = f"/Volumes/workspace/{SCHEMA}/landing/manifests/{result.run_id}.json"',
            source,
        )
        self.assertIn('expected_new = int(manifest["num_patients"])', source)
        # The generator manifest stores run_id as a STRING while the SDK's
        # result.run_id is an int: the comparison must coerce via str() or
        # every verification run falsely reports a manifest mismatch.
        self.assertIn('manifest.get("run_id") != str(result.run_id)', source)
        self.assertNotIn('manifest.get("run_id") != result.run_id', source)
        self.assertNotIn("generate_output.task_values", source)
        self.assertIn("after_count - before_count != expected_new", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
