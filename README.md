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
CareFlow360 is a **synthetic hospital operations analytics platform** built on Databricks, demonstrating an industry-ready medallion lakehouse architecture with ML predictions, AI functions, Genie natural-language Q&A, and a real-time command center dashboard.

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
│  Pipeline Schema: workspace.careflow360 (17 SDP tables)         │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. SDP Pipeline (Spark Declarative Pipelines)
- **Bronze layer** (`01_bronze.py`): Auto-ingests CSV/JSON from landing volume
- **Silver layer** (`02_silver.py`): Cleanses, deduplicates, computes derived columns (shock_index, pulse_pressure), quarantines bad records
- **Gold layer** (`03_gold.py`): Aggregates patient 360, command center KPIs, hourly department flow, live alerts, training features

### 2. ML Model — Triage Acuity Predictor
- **Algorithm**: RandomForestClassifier (160 trees, max_depth=12)
- **Training data**: 600 synthetic patient records
- **Features**: Vital signs, demographics, arrival info, GCS, pain score, shock index
- **Target**: Triage acuity level 1-5 (1=critical, 5=non-urgent)
- **Accuracy**: ~87%
- **MLflow**: Registered as `workspace.careflow360.triage_acuity_model` with Champion alias

### 3. AI Functions Notebook (`07_ai_functions.py`)
- `ai_classify`: Classifies chief complaints into clinical categories (Cardiac, Respiratory, GI, Neuro, Trauma)
- `ai_extract`: Extracts body_part, symptom, severity from complaint text (v2.1)
- `ai_forecast`: Predicts patient admission volume (requires preview enablement)
- Direct SQL: Natural-language queries over hospital data

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
├── resources/
│   ├── 01_uc.yml              # UC schema definitions
│   ├── 02_pipeline.yml        # SDP pipeline config
│   └── 03_jobs.yml            # Job orchestration
└── src/
    ├── pipelines/
    │   ├── 01_bronze.py       # Bronze ingestion
    │   ├── 02_silver.py        # Silver cleansing
    │   └── 03_gold.py          # Gold aggregation
    └── notebooks/
        ├── 00_preflight.py
        ├── 01_generate_sample_data.py
        ├── 02_kaggle_normalize.py
        ├── 03_create_views.py
        ├── 04_train_triage_model.py
        ├── 05_create_policy_docs.py
        ├── 06_validate.py
        └── 07_ai_functions.py
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
- **Existing Databricks job ID:** `484367296292508` (still using an older 4-task configuration until synchronized)
- **New patient count per scheduled run:** 100 *if* all records are accepted and the pipeline completes
- **Unique keys:** `P<job_run_id>-<patient_number>` for every new batch, preventing reuse of legacy patient IDs
- **Ingestion:** append-only JSON landing files; Bronze and Silver retain events; Patient 360 materialized view gains a row for every unseen patient ID
- **ML predictions:** insert-only Delta MERGE, preserving existing patient predictions and inserting one for each new patient
- **Validation:** the six-task GitHub job checks its own run ID and fails unless 100 new patients appear in Bronze/Silver/Gold and the predictions table

**Known live baseline:** 600 unique patients as measured on November 1, 2026. A properly deployed and validated 100-patient scheduled run should produce **700 patients**, then **800 after a second distinct run**. This is an *expected acceptance test*, not a claim that the existing workspace has been upgraded or that each scheduled run always fires.

The live job presently reuses legacy patient IDs and has missed scheduled dates. GitHub CI tests cannot fix that until the new notebook code and task definition are deployed safely to the existing workspace. See [workspace synchronization](docs/DATABRICKS_WORKSPACE_SYNC.md) and [end-to-end acceptance checks](docs/END_TO_END_VERIFICATION.md).

---

For detailed changes, see [CHANGELOG.md](CHANGELOG.md)
