# CareFlow360 — Hospital Operations Analytics Platform

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
│  Pipeline Schema: workspace.careflow360 (15 SDP tables; 2 more Gold tables created by notebooks)         │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. SDP Pipeline (Spark Declarative Pipelines)
- **Bronze layer** (`01_bronze.py`): Auto-ingests CSV/JSON from landing volume
- **Silver layer** (`02_silver.py`): Cleanses, deduplicates, computes derived columns (shock_index, pulse_pressure), quarantines bad records
- **Gold layer** (`03_gold.py`): Aggregates patient 360, command center KPIs, hourly department flow, live alerts, training features

### 2. ML Model — Triage Acuity Predictor
- **Algorithm**: RandomForestClassifier (160 trees, max_depth=12)
- **Training data**: Synthetic patient records; the number grows with successful incremental runs
- **Features**: Vital signs, demographics, arrival info, GCS, pain score, shock index
- **Target**: Triage acuity level 1-5 (1=critical, 5=non-urgent)
- **Accuracy**: Run-specific metrics logged to MLflow; no clinical validity is implied
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
- **7 gold presentation assets** intended for natural-language access (workspace configuration not versioned here)
- **8 text instructions** (triage scale, departments, alert thresholds, event types, ML, shock index, time zone, policies)
- **5 SQL example queries** for common question patterns
- **10 knowledge snippets** (3 joins, 4 filters, 3 measures)
- **15 column descriptions** with clinical context
- **Entity matching** on 14 categorical columns
- **10 starter questions** + **8 benchmark questions** (8/8 passing)

### 6. Dashboard — Hospital Command Center
- 10 widgets: KPI counters, department census, triage distribution, patient flow, live alerts, ML predictions
- Workspace-managed dashboard; its exported definition is not included in this repository

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

## 🔄 Incremental pipeline and daily scheduling

The [2026-10-03 changelog](CHANGELOG.md) documents a workspace-level daily job. **The current GitHub bundle does not define a schedule**, so confirm the job's actual schedule and state in Databricks rather than treating a historical job ID as proof of a running deployment.

The bundle's `generate_sample_events` task now emits **100 new synthetic patients per invocation** in **2 micro-batches**, with `reset_events=false` explicitly set. Each run preserves its source manifest in `manifests/source_manifest_<run_id>.json` for traceability. Each run generates a unique run ID and uses it in patient and encounter identifiers, preventing new runs from overwriting landing data or reusing patient IDs. The pipeline refreshes data, recreates consumer views, updates policy documents, trains a demonstration model, and validates outputs.

**Important operational distinctions:**
- Bronze/Silver events and relevant Gold aggregations can increase or change after successful incremental runs. A one-row command-center snapshot does not append daily, and static policy chunks do not grow.
- Historical source files created before this update may still use non-unique patient IDs. This fix prevents reuse in *new* runs; it does not rewrite historical Delta tables.
- Patient 360 joins triage, vitals, complaint, and ADT records by **patient + encounter**, so multiple admissions cannot mix their attributes. Live alerts include **currently admitted encounters only**.
- ML evaluation uses a patient-grouped holdout split to reduce evaluation leakage; reported quality metrics still measure synthetic data only.
- This is a synthetic, non-clinical demo. Do not load real patient data or treat triage predictions as medical advice.
- The pipeline requires serverless capacity and supported features in the target Free Edition workspace. The repository alone cannot verify a live Databricks deployment.

### Validate before deploying

1. Run `python -m compileall -q src` and `python -m unittest discover -s tests -v` locally (install `PyYAML` for the contract tests).
2. Run `databricks bundle validate -t dev` using your configured workspace credentials and current Databricks CLI.
3. Review the changes with `databricks bundle plan -t dev` before deployment.
4. Deploy to a development target, run `careflow360_job`, and inspect the validation job task and data quality metrics.
5. For daily updates, configure or confirm exactly one schedule in the workspace. Be mindful of Free Edition fair-usage limits.

The GitHub Actions workflow performs **static checks only**. It does not execute Databricks Spark jobs or validate clinical or model performance.

---

For earlier project changes, see [CHANGELOG.md](CHANGELOG.md).
