# CareFlow360 end-to-end verification and output acceptance criteria

This page distinguishes **offline source checks** from **actual Databricks workspace execution**. A passing GitHub workflow must never be described as an executed Lakeflow pipeline.

## System-under-test

- Six-task source DAB DAG: generate synthetic events → refresh Lakeflow → create presentation views → (create policy docs + train triage model) → validate project.
- One serverless Lakeflow ETL pipeline; Bronze, Silver, and Gold.
- Physical outputs in the deployed Gold presentation schema: `triage_predictions`, `policy_chunks`; five Gold presentation views. In the unified quickstart schema the notebook outputs are named `gold_triage_predictions` and `gold_policy_chunks`.
- Scheduled source DAB job: **02:00 UTC**, `max_concurrent_runs: 1`, triggered refresh (not continuous streaming).
- **Insert-new-patients contract:** `01_generate_sample_data.py` uses a distinct `{{job.run_id}}` per job execution. Patient 360 is a **Lakeflow materialized view**, not a manually merged Delta table: it gains one row per new `patient_id` when the pipeline refreshes, while still showing the latest state for existing patients.
- **Predictions contract:** The ML scorer uses insert-only Delta `MERGE` keyed by `patient_id`; existing matches are left unchanged and new patient IDs are added. The checked live Delta table lists `appendOnly` as a **supported feature** but did not set the enforcement property `delta.appendOnly=true`. Its current column schema matches the new scorer; runtime MERGE must still be tested after deployment.
- Synthetic-only operational dashboard; no clinical use.

## GitHub CI / offline acceptance tests

Run:

```bash
python tests/static_validate.py
python -m unittest discover -s tests -p 'test_*.py' -v
```

Required test categories:

1. Syntax: all Python scripts and individual Databricks Python cells parse; notebook JSON and YAML are valid.
2. Source integrity: the single `src/` tree is the canonical source; the former root/package mirror check was retired when the duplicate Free Edition copy was removed (2026-11-03).
3. Generator outputs: 2 append runs of 50 synthetic patients give 100 distinct triage/patient identifiers, matching chief-complaint counts, admissible acuities, expected ADT types, valid vitals, historical timestamps, and separate manifests.
4. Generator safety: destructive reset is rejected; event files cannot be overwritten; duplicate run IDs and interrupted-run files cannot be replayed; a 1,200-patient/20-batch replay produces no future-dated ADT events.
5. Presentation views: expected 16 views target the correct pipeline source names, with invalid Unity Catalog identifiers rejected.
6. Gold pipeline graph: all table dependencies exist; exactly 15 declared datasets including CDC SCD Type 2.
7. Policy documents: only fabricated synthetic policy chunks with unique IDs; first creation uses an explicit Delta write and subsequent refreshes MERGE only new/changed chunks, preserving the Delta table's identity and Change Data Feed history.
8. Model algorithm: feature schema and hyperparameters match the notebook; a local scikit-learn model fits and emits valid 1–5 predictions; **GroupShuffleSplit holds out whole patient IDs** with no train/test overlap; a stable-hash, capped sample prevents unbounded pandas collection; logging includes a dummy-classifier baseline and balanced accuracy; only newly unscored current patients are collected. This does not test the Databricks MLflow registry.
9. Job and extensions: six task keys, unique run IDs, exact immutable-manifest row counts across triage/vitals/ADT/complaints at both Bronze and Silver plus Gold patients/predictions, valid dependencies, 02:00 UTC cron, single-run concurrency, no full refresh; optional app/Genie/Search/Serving default to deployed Gold presentation schema; dashboard queries are aggregated.

**Not proven by CI:** actual Databricks SQL parsing and output values for every query; Auto Loader checkpoint behavior; live CDC/SCD2 history; MLflow registration and Champion alias; permissions; production job scheduling; dashboard visual rendering; account compute quota.

**Current-encounter Gold safety:** After the new pipeline code is deployed and refreshed, Patient 360 must never attach a vital timestamp belonging to a different encounter. Run this read-only check; the expected count is zero. Historical patient IDs may legitimately have multiple encounters.

```sql
SELECT COUNT(*) AS inconsistent_current_vitals
FROM workspace.careflow360_gold.patient_360 p
WHERE p.latest_vital_ts IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM workspace.careflow360_silver.vitals v
    WHERE v.patient_id = p.patient_id
      AND v.encounter_id = p.encounter_id
      AND v.event_ts = p.latest_vital_ts
  );
```

**Optional CPU benchmark:** `09_compare_free_edition_models.py` runs only when explicitly opened, never as a job task. It tests Logistic Regression, Random Forest and histogram gradient boosting (plus a dummy baseline) on three non-overlapping patient groups. Model selection is by validation macro-F1, then held-out test metrics and CPU time are reported. The daily `Champion` model remains Random Forest until a measured alternative is intentionally reviewed, approved and separately deployed; this experiment cannot guarantee the best model.

**Free Edition ML release checks:** The default historical training sample is capped at 4,000 rows and the Random Forest uses 96 trees with at most two parallel workers. Group-held-out metrics are not comparable with historical random-encounter-split accuracy claims. Inspect the real MLflow run for `train_unique_patients`, `test_unique_patients`, `training_sample_capped`, `balanced_accuracy`, `f1_macro`, and `dummy_f1_macro`. There must be no patient overlap between train and test; `f1_macro` should be compared with its dummy baseline. Existing prediction rows are not rescored by the append-only table contract; the SQL row count should grow by the number of previously unseen current patient IDs.

## Required live Databricks output test — existing resources only

The existing workspace job was upgraded in place (2026-11-03) to the six-task graph; live task keys may differ from the DAB source keys (`generate_sample_events` vs `generate_daily_patients`, `refresh_lakehouse` vs `refresh_pipeline`, `train_triage_model` vs `train_ml_model`).
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
4. Run `03_create_views.py`, `05_create_policy_docs.py`, `04_train_triage_model.py` with `schema=careflow360_gold`, then `06_validate.py` with `schema=careflow360`, `landing_volume=landing`, `layout=split`, `max_snapshot_age_hours=36`, `expected_run_id=<exact generated run_id>`, and `expected_new_patients=50`. Validation reads the immutable manifest and checks its event counts for **all four streams in Bronze and Silver**, plus **50 distinct run-scoped patients in Gold Patient 360 and predictions**.
5. Rerun the baseline queries and verify new distinct Patient 360 records increased by 50 if all 50 new patients pass pipeline quality checks; if not, inspect quarantines and data-quality metrics.
6. Check the Gold snapshot timestamp moved forward. Training remains encounter-level, while the batch predictions table inserts one row **for each new current patient ID** using insert-only `MERGE`, preserving prior predictions. The total predictions count should grow by the same number of new unique patients; all predicted acuities must be integers 1–5.

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
