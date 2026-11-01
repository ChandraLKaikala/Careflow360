# Databricks notebook source
# MAGIC %md
# MAGIC # One-time CareFlow360 existing-job upgrade
# MAGIC Pull GitHub main into this Databricks Git folder first. This notebook
# MAGIC defaults to DRY RUN. It never creates a second job or pipeline.
# MAGIC No full refresh, data deletion, checkpoint reset, or real patient data.

# COMMAND ----------

import base64
import copy
import json
import posixpath
import uuid
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace

JOB_ID = 484367296292508
PIPELINE_ID = "dde7c33b-8715-4666-8f6b-ad2af0848966"
JOB_NAME = "CareFlow360 Daily Incremental Pipeline"
CRON = "0 0 2 * * ?"
SCHEMA = "careflow360"
GOLD = "careflow360_gold"

def make_plan(tasks, paths):
    """Preserve existing 4 task settings, add policy + per-run validation."""
    original = {t["task_key"]: copy.deepcopy(t) for t in tasks}
    four = {"generate_daily_patients", "refresh_pipeline",
            "create_presentation_views", "train_ml_model"}
    six = four | {"create_policy_docs", "validate_project"}
    if len(original) != len(tasks) or set(original) not in (four, six):
        raise RuntimeError(f"Unexpected live task keys: {sorted(original)}")
    generator = original["generate_daily_patients"]
    pipeline = original["refresh_pipeline"]
    views = original["create_presentation_views"]
    model = original["train_ml_model"]
    if any("notebook_task" not in t for t in (generator, views, model)):
        raise RuntimeError("Expected notebook tasks are missing")
    if pipeline.get("pipeline_task", {}).get("pipeline_id") != PIPELINE_ID:
        raise RuntimeError("Unexpected pipeline ID; refusing to patch job")
    if pipeline.get("depends_on") != [{"task_key": "generate_daily_patients"}]:
        raise RuntimeError("Unexpected pipeline task dependencies")

    def notebook(task, filename, params):
        task["notebook_task"] = {
            "notebook_path": paths[filename], "source": "WORKSPACE",
            "base_parameters": params,
        }

    notebook(generator, "01_generate_sample_data.py", {
        "catalog": "workspace", "schema": SCHEMA, "volume": "landing",
        "num_patients": "100", "num_batches": "5",
        "run_id": "{{job.run_id}}", "reset_events": "false",
        "seconds_between_batches": "0",
    })
    pipeline["pipeline_task"]["full_refresh"] = False
    notebook(views, "03_create_views.py", {
        "catalog": "workspace", "pipeline_schema": SCHEMA,
        "bronze_schema": "careflow360_bronze",
        "silver_schema": "careflow360_silver",
        "gold_schema": GOLD,
    })
    old_parameters = model["notebook_task"].get("base_parameters") or {}
    model_parameters = {
        "catalog": "workspace", "schema": GOLD,
        "model_name": old_parameters.get("model_name", "triage_acuity_model"),
        "max_local_rows": str(min(4000, int(old_parameters.get("max_local_rows", "4000")))),
    }
    if old_parameters.get("experiment_path"):
        model_parameters["experiment_path"] = old_parameters["experiment_path"]
    notebook(model, "04_train_triage_model.py", model_parameters)

    policy = original.get("create_policy_docs") or copy.deepcopy(views)
    policy["task_key"] = "create_policy_docs"
    policy["depends_on"] = [{"task_key": "create_presentation_views"}]
    notebook(policy, "05_create_policy_docs.py", {
        "catalog": "workspace", "schema": GOLD,
        "policy_table_name": "policy_chunks",
    })
    validate = original.get("validate_project") or copy.deepcopy(views)
    validate["task_key"] = "validate_project"
    validate["depends_on"] = [
        {"task_key": "create_policy_docs"},
        {"task_key": "train_ml_model"},
    ]
    notebook(validate, "06_validate.py", {
        "catalog": "workspace", "schema": SCHEMA, "landing_volume": "landing",
        "layout": "split",
        "bronze_schema": "careflow360_bronze",
        "silver_schema": "careflow360_silver",
        "gold_schema": GOLD, "max_snapshot_age_hours": "36",
        "expected_run_id": "{{tasks.generate_daily_patients.values.run_id}}",
        "expected_new_patients": "{{tasks.generate_daily_patients.values.num_patients}}",
    })
    return [generator, pipeline, views, policy, model, validate]


def check_job(settings):
    if settings.name != JOB_NAME or settings.git_source:
        raise RuntimeError("Wrong job name or Git-source mode; refusing to modify")
    if not settings.schedule:
        raise RuntimeError("Job has no existing schedule")
    schedule = settings.schedule.as_dict()
    if (schedule.get("quartz_cron_expression") != CRON
            or schedule.get("timezone_id") != "UTC"
            or schedule.get("pause_status") != "UNPAUSED"):
        raise RuntimeError(f"Unexpected/paused schedule: {schedule}")
    return schedule


def checked_notebooks(client, root):
    expected = {
        "01_generate_sample_data.py": [
            'f"P{run_id}-{i:06d}"', 'dbutils.jobs.taskValues.set(key="run_id"',
            "dbutils.fs.put(path, payload, False)",
        ],
        "03_create_views.py": ["CREATE OR REPLACE VIEW"],
        "04_train_triage_model.py": [
            "insert_new_patient_predictions",
            "whenNotMatchedInsertAll",
            "split_patient_holdout",
            'new_keys = current_keys.join(existing_ids, "patient_id", "left_anti")',
        ],
        "05_create_policy_docs.py": [
            "policy_table_name", "upsert_policy_chunks", "whenNotMatchedInsertAll"
        ],
        "06_validate.py": [
            "verify_new_patient_inserts", "expected_new_patients",
            "expected_stream_counts_from_manifest", "landing_volume",
        ],
    }
    resolved = {}
    for filename, markers in expected.items():
        for candidate in (root + "/" + filename[:-3], root + "/" + filename):
            try:
                status = client.workspace.get_status(path=candidate)
            except Exception as exc:
                if "RESOURCE_DOES_NOT_EXIST" in str(exc):
                    continue
                raise
            if str(status.object_type).split(".")[-1].upper() != "NOTEBOOK":
                continue
            response = client.workspace.export_(
                path=candidate, format=workspace.ExportFormat.SOURCE
            )
            source = base64.b64decode(response.content or "").decode("utf-8")
            if any(marker not in source for marker in markers):
                raise RuntimeError(f"Outdated notebook: {candidate}; pull GitHub main first")
            resolved[filename] = candidate
            break
        else:
            raise RuntimeError(f"Missing executable notebook {filename} under {root}")
    return resolved


def save_backup(client, settings):
    owner = client.current_user.me().user_name
    folder = f"/Users/{owner}/careflow360-job-backups"
    client.workspace.mkdirs(path=folder)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{folder}/{JOB_ID}-{ts}-{uuid.uuid4().hex[:8]}.json"
    data = base64.b64encode(json.dumps(
        settings.as_dict(), indent=2, default=str
    ).encode("utf-8")).decode("ascii")
    client.workspace.import_(
        path=filename, format=workspace.ImportFormat.RAW,
        content=data, overwrite=False
    )
    return filename

# COMMAND ----------

dbutils.widgets.text("notebook_root", "")
dbutils.widgets.dropdown("apply_changes", "false", ["false", "true"])
dbutils.widgets.dropdown("verify_with_new_run", "false", ["false", "true"])
apply_changes = dbutils.widgets.get("apply_changes") == "true"
verify_now = dbutils.widgets.get("verify_with_new_run") == "true"
if verify_now and not apply_changes:
    raise ValueError("Set apply_changes=true to enable verify_with_new_run")
root = dbutils.widgets.get("notebook_root").rstrip("/")
if not root:
    current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    root = posixpath.dirname(current_path)
if not root.startswith(("/Users/", "/Workspace/Users/", "/Shared/", "/Workspace/Shared/")):
    raise ValueError("notebook_root must be a Databricks Workspace Git folder path")

client = WorkspaceClient()
current = client.jobs.get(job_id=JOB_ID)
if not current.settings or not current.settings.tasks:
    raise RuntimeError("Existing job settings could not be read")
schedule_before = check_job(current.settings)
# Never rewrite task definitions while another scheduled/manual run is active.
# This is read-only and keeps the updater safe around the 02:00 UTC trigger.
active_runs = list(client.jobs.list_runs(job_id=JOB_ID, active_only=True, limit=1))
if active_runs:
    raise RuntimeError(
        f"Existing job {JOB_ID} has an active run "
        f"({active_runs[0].run_id}); retry during a quiet window"
    )
if len(current.settings.tasks) not in (4, 6):
    raise RuntimeError("Unexpected task count; review manually")
paths = checked_notebooks(client, root)
plan = make_plan([task.as_dict() for task in current.settings.tasks], paths)
print("Existing job:", JOB_ID, JOB_NAME)
print("Preserved cron:", schedule_before)
print("Preserved pipeline:", PIPELINE_ID)
print("Proposed tasks and notebook paths:")
for task in plan:
    print("  ", task["task_key"],
          task.get("notebook_task", {}).get("notebook_path", PIPELINE_ID))

if not apply_changes:
    print("DRY RUN: No changes applied. Inspect the plan, then select apply_changes=true.")
else:
    backup_path = save_backup(client, current.settings)
    print("Saved private job-settings backup:", backup_path)
    client.jobs.update(
        job_id=JOB_ID,
        new_settings=jobs.JobSettings(
            tasks=[jobs.Task.from_dict(task) for task in plan],
            max_concurrent_runs=1,
        ),
    )
    after = client.jobs.get(job_id=JOB_ID)
    if check_job(after.settings) != schedule_before:
        raise RuntimeError("Job schedule changed unexpectedly")
    actual = {task.task_key: task for task in (after.settings.tasks or [])}
    if set(actual) != {t["task_key"] for t in plan}:
        raise RuntimeError("Updated job task keys differ from planned tasks")
    if actual["refresh_pipeline"].pipeline_task.pipeline_id != PIPELINE_ID:
        raise RuntimeError("Pipeline ID changed unexpectedly")
    print("UPDATED EXISTING JOB IN PLACE:", JOB_ID)
    if verify_now:
        before_count = spark.sql(
            f"SELECT COUNT(*) AS n FROM workspace.{GOLD}.patient_360"
        ).first()["n"]
        print("Patient 360 before run:", before_count)
        result = client.jobs.run_now(job_id=JOB_ID).result()
        latest = client.jobs.get_run(run_id=result.run_id)
        status = str(latest.state.result_state) if latest.state else ""
        if not status.endswith("SUCCESS"):
            raise RuntimeError(f"Job failed: run_id={result.run_id}, result_state={status}")
        after_count = spark.sql(
            f"SELECT COUNT(*) AS n FROM workspace.{GOLD}.patient_360"
        ).first()["n"]
        print("Patient 360 after run:", after_count)
        if after_count - before_count != 100:
            raise RuntimeError(f"Expected +100 synthetic patients, observed {after_count-before_count}")
        print("PASS: end-to-end live Patient 360 count increased by 100")
