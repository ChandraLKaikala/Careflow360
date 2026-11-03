# CareFlow360 — safe GitHub → existing Databricks workspace sync

This is a deployment runbook, **not a claim that GitHub has updated the Databricks workspace**.
Use only synthetic or public non-PHI data in Databricks Free Edition.

## Live assets confirmed through Databricks system tables (October 31, 2026)

- Existing daily scheduled job ID: `484367296292508`, name: **CareFlow360 Daily Incremental Pipeline**
- Job schedule: `0 0 2 * * ?` in `UTC` (02:00 UTC daily), `paused=false` at last inspection
- Existing pipeline ID: `dde7c33b-8715-4666-8f6b-ad2af0848966`
- Pipeline: serverless Lakeflow, triggered via a job task
- Daily job task chain: `generate_daily_patients` → `refresh_pipeline` → `create_presentation_views` → `train_ml_model`
- Missing execution dates: October 23–26 and October 31, 2026. The cause is not confirmed. Do not claim an automatic refresh guarantee.
- Published AI/BI dashboard ID: `01f1beeb451e10439c0b4bf6aa8eeb70`.

**Do not run `databricks bundle deploy` against this workspace without first reconciling the existing resources.**
The repository DAB currently describes a *different logical job name* and a six-task DAG, whereas the existing
workspace job has four tasks. An unbound deploy could create duplicate pipeline/job resources and exceed Free Edition
quotas. The DAB now declares the **same 02:00 UTC cron schedule** with a single-run concurrency cap, but **Git pull alone
will not update the existing job or activate that schedule**.

## Step 1 — Merge only after the pull request checks pass

Review the diff and CI. The repository keeps a single canonical source tree (`src/`, `resources/`); the former
`CareFlow360_Databricks_Free_Edition/core/` mirror was removed from the repository on 2026-11-03.

## Step 2 — Sync code, not live data

Open the workspace Git folder pointing to the Careflow360 repository. Use **Git operations → Pull**
after the branch is merged (or check out the PR branch for isolated testing).
Inspect the actual notebook paths referenced by **Jobs & Pipelines → CareFlow360 Daily Incremental Pipeline → Tasks**.
A Git pull updates only files in that Git folder; it does not automatically replace notebooks deployed at unrelated paths,
update existing job definitions, or run a pipeline.

Before changing any job task, save a copy of the current job settings and the pipeline settings.
Never reset, truncate, full refresh, or delete the landing volume merely to apply this patch.

## Recommended guarded in-place update for the existing scheduled job

After pulling the latest GitHub `main` into your Databricks Git folder, open
[`src/notebooks/08_upgrade_existing_daily_job.py`](../src/notebooks/08_upgrade_existing_daily_job.py)
**inside that Git folder** and run all its cells.

1. First leave the widgets at **`apply_changes=false`** and
   **`verify_with_new_run=false`**. This is a read-only dry run: it checks
   job ID `484367296292508`, the exact existing daily UTC schedule, pipeline
   ID `dde7c33b-8715-4666-8f6b-ad2af0848966`, existing four task keys,
   no other active job run, and the contents of all five latest source notebooks.
   It also inspects the **existing Lakeflow pipeline source libraries** and
   refuses an upgrade unless they point to the **three pipeline files inside
   the freshly pulled Git folder** (`01_bronze.py`, `02_silver.py`,
   `03_gold.py`) or a matching `src/pipelines/**` glob. It checks the catalog,
   schema and triggered (noncontinuous) pipeline mode. If notebook paths cannot
   be resolved automatically, supply the absolute workspace folder containing
   the notebooks via `notebook_root`.
   
   **If source-library preflight fails:** inspect the existing pipeline's
   Settings → Source code, and update the library paths to the corresponding
   `src/pipelines/` files in your pulled Git folder, **in the same existing
   pipeline ID**. Save the pipeline settings without creating a new pipeline,
   clearing its checkpoints, or requesting full refresh. Re-run the dry run to
   confirm the library paths before applying any job changes.
2. Review the six planned tasks. Switch **`apply_changes=true`** and
   **`verify_with_new_run=true`**, then run all cells again. This writes a
   job-settings backup to the authenticated user's private Databricks workspace
   folder, updates only the **existing** job (using `jobs.update`), preserves the
   original schedule/pipeline ID, and triggers one supervised synthetic
   patient run (random 50–150).
3. The final job task reads the **immutable run manifest**, checks its expected event counts and requires the run's **actual new-patient count (published via task values) across all four event streams in both Bronze and Silver**, plus Gold Patient 360 and predictions (10 checks). The helper also
   compares the before/after Patient 360 counts and fails if the increase does
   not match the run's expected count. (The November 3, 2026 in-place upgrade
   run verified a random 50–150 insert against the live baseline of 1,000
   patients.)
4. If any preflight check fails, the helper must not update the job. If a
   post-update or live data check fails, inspect the logged job/task outputs,
   the saved backup, and pipeline errors. Never reset or full-refresh the
   pipeline to force success.

This notebook has already been applied (2026-11-03): the existing job now runs the same *functional*
six-task graph as GitHub, while retaining the live task keys
(`generate_daily_patients`, `refresh_pipeline`,
`create_presentation_views`, `train_ml_model`) and having added
`create_policy_docs` and `validate_project`. Existing run histories,
permissions, and the cron remain attached to the same job ID. The helper
requires an authenticated user with permission to edit and run that job;
no personal access token should be pasted into source code or chat.
Its dry-run and offline tests are **not** an executed Databricks deployment.

## Step 3 — Preview/compare the bundle rather than blindly creating resources

In a Databricks-enabled CLI session, from the repository root:

```bash
databricks bundle validate -t dev --var="catalog=workspace"
databricks bundle plan -t dev --var="catalog=workspace"
```

Compare the proposed DAB resource keys `careflow360_job` and `hospital_pipeline` against
existing live IDs above. If they are already managed by a DAB deployment, use that same deployment
state, target, and configuration rather than creating another deployment.
If they are *not* bound, a qualified workspace owner can use
`databricks bundle deployment bind` to link them **only after** checking the job/pipeline settings
and reviewing the deployment diff. Binding causes future deploys to update those exact resources.
A fresh unbound deploy is not a safe substitute.

For manual sync, inspect the four live task notebook paths, update their intended files in place,
and explicitly preserve the 02:00 UTC schedule. Add the validation notebook only after tests pass.

## Step 4 — Test a controlled append and prove the row counts change

Use `01_generate_sample_data.py` on serverless with `num_patients=50`, `num_batches=2`,
`reset_events=false`, and a new `run_id`. Check the per-run manifest in
`/Volumes/<catalog>/<pipeline_schema>/landing/manifests/`.
Then trigger the **existing** Lakeflow pipeline and update the presentation views.
Read patient counts before and after in `<catalog>.careflow360_gold.patient_360`:
the count should grow by 50 **only if all 50 new run-scoped IDs are ingested and the refresh succeeds**.
Do not claim success without confirming the actual result. Previous observed 600 patients would
therefore become **650** after one successful controlled 50-patient append, not 600.

The updated DAB daily generator receives `run_id: "{{job.run_id}}"` and publishes both
`run_id` and `num_patients` as Databricks Job task values. The final validation task
receives those exact values using dynamic references, then verifies the current run contributes
the manifest-declared event counts for **triage, vitals, ADT and complaints** across Bronze and Silver, plus 50 (or 100 in scheduled production runs) new Patient 360 records and predictions. **Missing event streams or zero new patients cause a job failure**,
not a green success. This requires deploying the newer six-task job (not just Git pulling).

Then run the ML notebook on the Gold presentation schema, followed by `06_validate.py` with
`layout=split`, `gold_schema=careflow360_gold`, `silver_schema=careflow360_silver`,
`bronze_schema=careflow360_bronze`, `max_snapshot_age_hours=36`,
`schema=careflow360`, `landing_volume=landing`, `expected_run_id=<run_id from generated manifest>`, `expected_new_patients=50`. The validator then reads the immutable manifest to check every event stream.
The prediction table must be `triage_predictions`, not `gold_triage_predictions`.

The updated ML scorer uses an **insert-only Delta MERGE** by `patient_id` so old prediction
rows are retained and brand-new patient IDs insert fresh rows. The existing predictions
table is configured with the `appendOnly` Delta feature, so matched updates are *not*
performed without a deliberate separate change to its table properties. This is
deliberately different from Patient 360 (a materialized latest-state view) and
location-history SCD2 (a change-tracking table).

## Step 5 — Verify scheduling and visibility

Check the existing job's next run time, last runs, and any disabled/quota/permission problems.
Databricks Free Edition has **no SLA** and can stop compute when fair-usage quotas are reached.
The recorded gap on October 23–26 and October 31 requires investigating run history and workspace notices.

The AI/BI dashboard has a separate dataset refresh schedule from the Lakeflow pipeline.
Check the dashboard schedule in the UI and set it **after** the successful pipeline window
(e.g. 02:30 UTC), avoiding duplicate schedules. Display data freshness prominently.
For streaming demonstrations, replay micro-batches using the existing triggered pipeline
or a short, supervised continuous demo; never present yesterday's events as live telemetry.

## Rollback

Keep the old job/pipeline settings and notebook revisions. If a test fails, restore the
previous notebook revisions or deployment and rerun the current non-destructive pipeline.
Do not attempt to 'fix' a failure by resetting Auto Loader input files or full-refreshing
the pipeline without understanding checkpoint/table effects.

## Verification boundaries

- GitHub CI now verifies Python source/cells, YAML, synthetic JSON event counts/unique IDs/timestamps, modeled ML feature/prediction shape, notebook output contracts and job dependencies, **not** actual Spark/Lakeflow or MLflow registry execution.
- Current ChatGPT Databricks connection supports Genie read-only analysis and AI/BI dashboard
  APIs; it cannot update Jobs or workspace notebooks or initiate pipeline runs.
- Full validation requires a successful controlled workspace run and comparison of resulting tables.

Official references:
- https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources
- https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- https://docs.databricks.com/aws/en/repos/git-operations-with-repos/
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
