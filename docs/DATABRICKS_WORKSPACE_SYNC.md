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
The repository DAB currently describes a *different logical job name*, and an unbound deploy could create a duplicate
pipeline or job, violating Free Edition quotas. The 02:00 UTC schedule in the existing job is not encoded as a schedule
in the current DAB file; Git pull alone does not change it.

## Step 1 — Merge only after the pull request checks pass

Review the diff and CI. The repository contains both root `src/`, `resources/` and a packaged
`CareFlow360_Databricks_Free_Edition/core/` mirror; the integrity test checks these stay identical.

## Step 2 — Sync code, not live data

Open the workspace Git folder pointing to the Careflow360 repository. Use **Git operations → Pull**
after the branch is merged (or check out the PR branch for isolated testing).
Inspect the actual notebook paths referenced by **Jobs & Pipelines → CareFlow360 Daily Incremental Pipeline → Tasks**.
A Git pull updates only files in that Git folder; it does not automatically replace notebooks deployed at unrelated paths,
update existing job definitions, or run a pipeline.

Before changing any job task, save a copy of the current job settings and the pipeline settings.
Never reset, truncate, full refresh, or delete the landing volume merely to apply this patch.

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
the count should grow by 50 **if no rows are dropped and the run succeeds**.
Do not claim success without confirming that actual result.

Then run the ML notebook on the Gold presentation schema, followed by `06_validate.py` with
`layout=split`, `gold_schema=careflow360_gold`, `silver_schema=careflow360_silver`,
`bronze_schema=careflow360_bronze` and `max_snapshot_age_hours=36`.
Confirm the generated predictions table is `triage_predictions`, not a second `gold_triage_predictions`.

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

- GitHub CI verifies syntax and repository consistency, **not** Spark runtime behavior.
- Current ChatGPT Databricks connection supports Genie read-only analysis and AI/BI dashboard
  APIs; it cannot update Jobs or workspace notebooks or initiate pipeline runs.
- Full validation requires a successful controlled workspace run and comparison of resulting tables.

Official references:
- https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources
- https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- https://docs.databricks.com/aws/en/repos/git-operations-with-repos/
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
