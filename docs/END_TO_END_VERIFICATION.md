# CareFlow360 end-to-end verification and output acceptance criteria

This page distinguishes **offline source checks** from **actual Databricks workspace execution**. A passing GitHub workflow must never be described as an executed Lakeflow pipeline.

## System-under-test

- Six-task source DAB DAG: generate synthetic events → refresh Lakeflow → create presentation views → (create policy docs + train triage model) → validate project.
- One serverless Lakeflow ETL pipeline; Bronze, Silver, and Gold.
- Physical outputs in the deployed Gold presentation schema: `triage_predictions`, `policy_chunks`; five Gold presentation views. In the unified quickstart schema the notebook outputs are named `gold_triage_predictions` and `gold_policy_chunks`.
- Scheduled source DAB job: **02:00 UTC**, `max_concurrent_runs: 1`, triggered refresh (not continuous streaming).
- Synthetic-only operational dashboard; no clinical use.

## GitHub CI / offline acceptance tests

Run:

```bash
python CareFlow360_Databricks_Free_Edition/tests/static_validate.py
python -m unittest discover -s tests -p 'test_*.py' -v
```

Required test categories:

1. Syntax: all Python scripts and individual Databricks Python cells parse; notebook JSON and YAML are valid.
2. Package parity: root DAB and Free Edition/core mirrored files are identical; the `.ipynb` cell matches the presentation-view script.
3. Generator outputs: 2 append runs of 50 synthetic patients give 100 distinct triage/patient identifiers, matching chief-complaint counts, admissible acuities, expected ADT types, valid vitals, historical timestamps, and separate manifests.
4. Generator safety: destructive reset is rejected; duplicate run IDs and interrupted-run files cannot be overwritten; a 1,200-patient/20-batch replay produces no future-dated ADT events.
5. Presentation views: expected 15 views target the correct pipeline source names, with invalid Unity Catalog identifiers rejected.
6. Gold pipeline graph: all table dependencies exist; exactly 15 declared datasets including CDC SCD Type 2.
7. Policy documents: only fabricated synthetic policy chunks with unique chunk IDs; table write and Delta CDF property are attempted.
8. Model algorithm: synthetic feature schema matches the ML notebook; a local scikit-learn model fits and emits valid 1–5 predictions on held-out examples, and current-patient scoring rejects duplicate IDs, missing encounter keys, and incomplete coverage. This is not MLflow registry testing.
9. Job and extensions: six task keys, valid dependency references, 02:00 UTC cron, single-run concurrency, no full refresh; optional app/Genie/Search/Serving default to deployed Gold presentation schema; dashboard query examples are aggregated.

**Not proven by CI:** actual Databricks SQL parsing and output values for every query; Auto Loader checkpoint behavior; live CDC/SCD2 history; MLflow registration and Champion alias; permissions; production job scheduling; dashboard visual rendering; account compute quota.

## Required live Databricks output test — existing resources only

The existing workspace has a different four-task job from the new six-task source DAB.
Do **not** create another job or pipeline to test the upgrade. Backup the original settings,
compare the bundle plan and bind resources only after reviewing differences.
Follow [DATABRICKS_WORKSPACE_SYNC.md](DATABRICKS_WORKSPACE_SYNC.md).

### Baseline counts and timestamps

Run **before** a controlled append (queries return aggregates only):

```sql
SELECT COUNT(*) AS patient_rows, COUNT(DISTINCT patient_id) AS unique_patients
FROM workspace.careflow360_gold.patient_360;

SELECT COUNT(*) AS prediction_rows,
       COUNT(DISTINCT patient_id) AS predicted_patients
FROM workspace.careflow360_gold.triage_predictions;

SELECT active_patients, ed_patients, icu_patients,
       operational_alert_patients, snapshot_ts
FROM workspace.careflow360_gold.hospital_command_center
ORDER BY snapshot_ts DESC LIMIT 1;
```

Record these results and the exact UTC query time.

### Controlled replay and pipeline run

1. In a test Git folder, run the source `01_generate_sample_data.py` with `num_patients=50`, `num_batches=2`, `reset_events=false`, and a **new unique** `run_id`.
2. Verify its per-run JSON manifest and the expected Bronze JSON event file counts.
3. Run **the existing** serverless pipeline with an incremental refresh, **not full refresh**.
4. Run `03_create_views.py`, `05_create_policy_docs.py`, `04_train_triage_model.py` with `schema=careflow360_gold`, then `06_validate.py` with `layout=split` and a 36-hour freshness threshold.
5. Rerun the baseline queries and verify new distinct Patient 360 records increased by 50 if all 50 new patients pass pipeline quality checks; if not, inspect quarantines and data-quality metrics.
6. Check the Gold snapshot timestamp moved forward. Training is encounter-level, but the batch predictions table must contain **one latest-encounter prediction per current Patient 360 patient**; all predicted acuities must be integers 1–5. Preserve historical training examples, not duplicate published current-patient predictions.

### Post-run query acceptance checks

```sql
SELECT COUNT(*) AS scored_rows,
       COUNT(DISTINCT patient_id) AS distinct_scored_patients,
       COUNT(DISTINCT encounter_id) AS distinct_scored_encounters
FROM workspace.careflow360_gold.triage_predictions;

SELECT COUNT(*) AS training_encounter_rows,
       COUNT(DISTINCT patient_id) AS historical_patients
FROM workspace.careflow360_gold.triage_training;

SELECT COUNT(*) AS invalid_labels
FROM workspace.careflow360_gold.triage_predictions
WHERE predicted_triage_acuity IS NULL
   OR predicted_triage_acuity NOT BETWEEN 1 AND 5;

SELECT COUNT(*) AS patient_rows,
       COUNT(DISTINCT patient_id) AS unique_patients
FROM workspace.careflow360_gold.patient_360;

SELECT COUNT(*) AS admitted_without_department
FROM workspace.careflow360_gold.patient_360
WHERE is_currently_admitted AND current_department IS NULL;
```

Expected: zero invalid labels, **scored_rows = distinct_scored_patients = Patient 360 patient_rows**, one Patient 360 row per distinct patient, zero admitted patients without a department. Training rows can legitimately exceed the current-patient count because they cover historical encounters. Any unexpected outcome is a failure to investigate, not a reason to reset existing data.

### Scheduled refresh and dashboard

- Confirm the existing job remains unpaused, is scheduled daily at `0 0 2 * * ?` UTC, and its next two scheduled occurrences actually complete. Historical gaps on October 23–26 and October 31, 2026 were observed previously; their cause was not established.
- Confirm all job tasks and the pipeline refresh report success for each scheduled run, **and** the newest source data timestamp advances.
- Check the AI/BI dashboard's **separate** dataset refresh schedule. Schedule after the job (for example 02:30 UTC) without duplicating schedules.
- Open the published dashboard and verify each of its 3 pages visibly renders, all KPI tiles match fresh SQL results, hourly flows show ADT event counts, and aggregate synthetic alert panels display no individual IDs.
- Verify the freshness warning turns on when the snapshot exceeds 30 hours and off after a successful refresh; verify missing-day coverage indicators honestly flag skipped days.
- Report timestamps in UTC and identify daily batch analytics as **daily refreshed**, not real-time telemetry.

## Release gate and rollback

Only mark **end-to-end PASS** when the controlled append, pipeline, MLflow outputs, SQL checks, next scheduled runs, and published dashboard visuals all pass in the workspace. Keep prior notebook and job revisions for rollback; never reset Auto Loader files or checkpoints to make tests pass.

Databricks Free Edition is quota-limited, serverless-only and has no service-level agreement. Perfect unattended reliability cannot be guaranteed on that tier.

References:
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources
