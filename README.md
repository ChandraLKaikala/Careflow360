# CareFlow360 — Hospital Operations Analytics Platform

> **Deployment notice (November 2026):** The source-code repair branch contains safer append-only replay,
> split-schema ML references, richer validation, and static CI. These are **GitHub changes, not yet deployed**
> to the existing Databricks job. The existing 02:00 UTC job has missing historical run dates.
> Before deploying, follow [the workspace sync and validation runbook](docs/DATABRICKS_WORKSPACE_SYNC.md);
> an unbound DAB deploy can create duplicate jobs/pipelines in quota-limited Free Edition.
> Keep this demo synthetic/public only — never load real patient PHI.

**End-to-end output acceptance checklist:** [Live workspace verification and CI scope](docs/END_TO_END_VERIFICATION.md).

**Update the existing scheduled Databricks job safely:** After pulling the newest GitHub `main` in your Databricks Git folder, run [`08_upgrade_existing_daily_job.py`](src/notebooks/08_upgrade_existing_daily_job.py) first in dry-run mode, then use the explicit apply + live-verification widgets. It updates job `484367296292508` in place, not by creating a duplicate. See the [step-by-step guarded job-update instructions](docs/DATABRICKS_WORKSPACE_SYNC.md#recommended-guarded-in-place-update-for-the-existing-scheduled-job). This has **not** been executed in the workspace from GitHub CI.


## Overview
CareFlow360 is a **synthetic hospital operations analytics platform** built on Databricks, demonstrating a medallion lakehouse architecture with ML predictions, optional AI/Genie analytics, and a **daily refreshed** command center dashboard. The core job is triggered, not a continuously running real-time hospital telemetry system.

> **⚠️ Disclaimer:** All data is synthetic. This is a portfolio/demo project, NOT a clinical system.

## Architecture — Medallion Lakehouse

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Unity Catalog                      │
├──────────────────┬──────────────────┬───────────────────────────┤
│  Bronze Schema   │  Silver Schema   │  Gold Schema              │
│  (Raw)           │  (Cleansed)      │  (Analytics)              │
│                  │                  │                           │
│  4 views         │  6 views         │  5 views + 2 tables       │
│  - triage        │  - triage        │  - patient_360            │
│  - vitals        │  - triage_quar.  │  - hospital_command_ctr    │
│  - adt           │  - vitals        │  - department_hourly       │
│  - chief_compl.  │  - adt           │  - live_alerts             │
│                  │  - chief_compl.  │  - triage_training        │
│                  │  - patient_loc.  │  - triage_predictions (T) │
│                  │                  │  - policy_chunks (T)      │
├──────────────────┴──────────────────┴───────────────────────────┤
│  Schema: 15 SDP datasets + 2 notebook-managed physical tables   │
└─────────────────────────────────────────────────────────────────┘
```

## Optional SQL Command Center App

The [Databricks Streamlit App](extensions/app/README.md)
provides an aggregate-only operations overview with validated Unity Catalog
resource bindings, a 2-minute result cache, and 7-day/100-group bounded
alert and ADT flow queries. This extension is **not deployed by GitHub CI**,
and it must use only synthetic/public data, never patient PHI.

## Components

### 1. SDP Pipeline (Spark Declarative Pipelines)
- **Bronze layer** (`01_bronze.py`): Auto-ingests schema-enforced JSON from the landing volume
- **Silver layer** (`02_silver.py`): Cleanses, deduplicates, computes derived columns (shock_index, pulse_pressure), quarantines bad records
- **Gold layer** (`03_gold.py`): Aggregates patient 360, command center KPIs, hourly department flow, live alerts, training features

### 2. ML Model — Triage Acuity Predictor
- **Algorithm**: scikit-learn RandomForestClassifier (96 trees, depth 12, at most 2 CPU workers)
- **Training**: deterministic, feature-projected sampling capped at 4,000 synthetic encounters; one replay capped at 2,000 patients and 50 batches to respect Free Edition quotas
- **Evaluation**: held-out **patient groups**, not random encounters; logs accuracy, balanced accuracy, macro-F1, and majority-class baselines to MLflow. A retrain must beat the dummy macro-F1 and not underperform its balanced accuracy before registration/promotion; otherwise the task fails closed.
- **Features**: Vital signs, demographics, arrival info, GCS, pain score, shock index
- **Target**: Synthetic acuity classes 1–5. No accuracy percentage is assumed: inspect the current MLflow run's measured held-out metrics.
- **Registration**: Unity Catalog model under the configured Gold schema, typically `workspace.careflow360_gold.triage_acuity_model` with `Champion` alias
- **Batch scoring**: the already-fitted/evaluated CPU pipeline scores only previously unseen current patient IDs and inserts them via a non-destructive Delta MERGE, avoiding a redundant registry model reload. Existing predictions are retained.
- **Optional model comparison**: [`09_compare_free_edition_models.py`](src/notebooks/09_compare_free_edition_models.py) benchmarks majority-class, Logistic Regression, Random Forest, Extra Trees, and histogram gradient boosting on distinct train/validation/test **patient groups**. It is a manual CPU-only experiment (2,000 rows by default), **does not register or promote models**, and is **not run by the daily job**. Compare validation macro-F1, held-out test macro-F1, and fit time before considering a model replacement. A larger model is not inherently better on synthetic tabular data.

### 3. Analytics and optional AI (`07_ai_functions.py`)
- **Default, Free Edition-safe:** standard Spark SQL KPI queries, a clearly labeled moving-average baseline and aggregate complaint-keyword groups; no foundation-model endpoint required.
- **Optional:** AI_CLASSIFY, AI_EXTRACT, AI_FORECAST and AI_QUERY may be explored in a separate notebook only when workspace features and model access permit. Their results are **not** required by the daily core job.
- These are synthetic portfolio demonstrations, not clinical classifiers or validated patient forecasts.

### 4. RAG Policy Documents
- 6 synthetic hospital policy documents chunked for semantic search
- Topics: ICU escalation, ED surge, bed assignment, discharge, data quality, synthetic data use

### 5. Genie Space — Natural Language Q&A
- **7 gold tables** accessible via natural language
- **8 text instructions** (triage scale, departments, alert thresholds, event types, ML, shock index, time zone, policies)
- **5 SQL example queries** for common question patterns
- **10 knowledge snippets** (3 joins, 4 filters, 3 measures)
- **15 column descriptions** with clinical context
- **Entity matching** on 14 categorical columns
- **10 starter questions** + **8 benchmark questions** (8/8 passing)

### 6. Dashboard — Hospital Command Center
- Current published dashboard also includes daily job-run gaps, pipeline/data freshness, aggregated synthetic alerts and ML quality. Dashboard revisions are managed inside Databricks, not by this DAB source.
- Published with embedded credentials

## Engineering review and Free Edition limitations

See [the notebook-by-notebook Free Edition audit](docs/FREE_EDITION_NOTEBOOK_AUDIT.md) for the source/cell inventory, CPU-model rationale, current offline validation coverage, operational caveats, and safe deployment runbook. GitHub CI does **not** validate live Databricks runtime, MLflow registry, streaming checkpoint migrations, or existing job state.

## DAB Configuration
- `databricks.yml`: Main bundle config (schema=careflow360)
- `resources/01_uc.yml`: Unity Catalog schemas (4 schemas)
- `resources/02_pipeline.yml`: SDP pipeline definition
- `resources/03_jobs.yml`: Job orchestration (view creation, ML training, policy docs)

## UC Governance
- Tags: `domain=healthcare`, `layer=bronze/silver/gold`, `classification=raw/cleansed/analytics`

## File Structure
```
Careflow360/
├── databricks.yml              # Main DAB config
├── QUICKSTART.md               # Unified-quickstart walkthrough (careflow360_dev)
├── resources/                  # DAB resources (UC schemas, pipeline, 6-task job)
├── src/
│   ├── pipelines/              # 01_bronze / 02_silver / 03_gold pipeline sources
│   └── notebooks/              # 00_preflight … 09_compare_free_edition_models
├── sql/                        # Unified-quickstart SQL (setup, dashboard queries, Genie benchmarks, validation)
├── tests/                      # Offline contract tests + static_validate.py (CI)
├── extensions/app/             # Optional Streamlit command center app
├── docs/                       # Architecture, audits, verification, interview prep
└── presentations/              # Portfolio presentation notebooks
```

## Technologies
- Databricks (Free Edition)
- Unity Catalog
- Spark Declarative Pipelines (SDP)
- MLflow
- AI/BI Dashboard (Lakeview)
- AI/BI Genie Space
- Declarative Automation Bundles (DAB)
- SQL AI Functions (ai_classify, ai_extract, ai_forecast)

## License
Portfolio/demo project. All data is synthetic.

## 🔄 Daily Incremental Pipeline (Added 2026-10-03)

### Automated Data Growth (after deploying updated GitHub code)

- **Schedule defined in GitHub:** Daily at 2:00 AM UTC
- **Existing Databricks job ID:** `484367296292508` — upgraded in place to the six-task configuration (verified 2026-11-03)
- **New patient count per scheduled run:** a random 50–150 patients (`num_patients: "random"`; the generator notebook resolves it within its unchanged 50–2000 guard and publishes the actual count through the run manifest and job task values)
- **Unique keys:** `P<job_run_id>-<patient_number>` for every new batch, preventing reuse of legacy patient IDs
- **Ingestion:** append-only JSON landing files; Bronze and Silver retain events; Patient 360 materialized view gains a row for every unseen patient ID
- **ML predictions:** insert-only Delta MERGE, preserving existing patient predictions and inserting one for each new patient
- **Validation:** the six-task job reads the immutable per-run manifest, verifies every event stream's row counts through Bronze and Silver, and requires exactly the run's actual new-patient count in Gold Patient 360 and predictions

**Live status (verified 2026-11-03):** the workspace job was upgraded in place to six tasks and validated end to end; Gold Patient 360 and predictions both reached 1,000 patients across 600→700→800→900→1000 daily increments, all insert-only (zero updates or deletes).

The notebook sources in this repository are the GitHub source of truth; the live workspace job runs mirrored copies under `/Users/<user>/CareFlow360_Databricks_Free_Edition`. See [workspace synchronization](docs/DATABRICKS_WORKSPACE_SYNC.md) and [end-to-end acceptance checks](docs/END_TO_END_VERIFICATION.md).

---

For detailed changes, see [CHANGELOG.md](CHANGELOG.md)
