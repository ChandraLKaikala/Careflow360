# Databricks notebook source
# DBTITLE 1,CareFlow360 — Complete Project Presentation
# MAGIC %md
# MAGIC # 🏥 CareFlow360 — Hospital Operations Analytics Platform
# MAGIC ## Complete Project Presentation
# MAGIC
# MAGIC **Project**: CareFlow360 Hospital Analytics
# MAGIC **Owner**: YOUR_WORKSPACE_USER
# MAGIC **Workspace**: dbc-11dcf304-ed66.cloud.databricks.com
# MAGIC **Edition**: Databricks Free Edition
# MAGIC **Last Updated**: November 3, 2026
# MAGIC **Status**: ✅ FULLY OPERATIONAL — All tasks passing, daily schedule active
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Table of Contents
# MAGIC
# MAGIC 1. [Executive Summary](#executive-summary)
# MAGIC 2. [Architecture Overview](#architecture-overview)
# MAGIC 3. [Medallion Architecture Deep Dive](#medallion-architecture)
# MAGIC 4. [SDP Pipeline (Bronze → Silver → Gold)](#sdp-pipeline)
# MAGIC 5. [Lakeflow Job — Daily Incremental Pipeline](#lakeflow-job)
# MAGIC 6. [ML/AI Capabilities](#ml-ai-capabilities)
# MAGIC 7. [AI/BI Dashboard](#ai-bi-dashboard)
# MAGIC 8. [Genie Space — Hospital Operations Agent](#genie-space)
# MAGIC 9. [DAB (Declarative Automation Bundles) Deployment](#dab-deployment)
# MAGIC 10. [UC Governance & Security](#uc-governance)
# MAGIC 11. [AI Functions & RAG](#ai-functions-rag)
# MAGIC 12. [Extensions & Future Capabilities](#extensions)
# MAGIC 13. [Fixes Applied (Oct 10, 2026)](#fixes-applied)
# MAGIC 14. [Project Health Summary](#project-health)
# MAGIC 15. [Lessons Learned](#lessons-learned)
# MAGIC 16. [File Inventory](#file-inventory)

# COMMAND ----------

# DBTITLE 1,Executive Summary
# MAGIC %md
# MAGIC ## 1. Executive Summary {#executive-summary}
# MAGIC
# MAGIC CareFlow360 is a **production-ready hospital operations analytics platform** built entirely on Databricks Free Edition. It implements a complete medallion architecture (Bronze → Silver → Gold) for ingesting, cleansing, enriching, and analyzing hospital event data — including admissions, triage assessments, vital signs, and chief complaints.
# MAGIC
# MAGIC ### Key Achievements
# MAGIC
# MAGIC | Capability | Status | Details |
# MAGIC | --- | --- | --- |
# MAGIC | **SDP Pipeline** | ✅ Operational | 15 datasets (4 Bronze, 6 Silver, 5 Gold), serverless + photon |
# MAGIC | **Daily Job** | ✅ Operational | 6-task workflow: generate events → refresh pipeline → create views → policy docs → train ML → validate |
# MAGIC | **ML Model** | ✅ Trained | RandomForest triage classifier, measured patient-held-out metrics (inspect MLflow), UC-registered with Champion alias |
# MAGIC | **AI/BI Dashboards** | ✅ Published | Command Center: 3 pages, 29 widgets, 18 datasets; companion Operations Dashboard: 14 widgets across 6 SQL datasets |
# MAGIC | **Genie Space** | ✅ Production-ready | 8/8 benchmark questions passed |
# MAGIC | **UC Governance** | ✅ Applied | Tags, comments, Delta properties, 3-schema separation |
# MAGIC | **DAB** | ✅ Deployed | Declarative Automation Bundles with multi-resource YAML |
# MAGIC | **AI Functions** | ✅ Tested | ai_classify, ai_extract, ai_forecast, ai_query |
# MAGIC
# MAGIC ### Scale
# MAGIC - **1,000+ current patients** in the dataset (growing ~100/day via scheduled job, random 50–150 per run)
# MAGIC - **7,500+ events** across triage, vitals, ADT, and chief complaints
# MAGIC - **15+ pipeline datasets** materialized through the medallion layers
# MAGIC - **3 presentation schemas** (bronze, silver, gold) with views for consumer access

# COMMAND ----------

# DBTITLE 1,Architecture Overview
# MAGIC %md
# MAGIC ## 2. Architecture Overview {#architecture-overview}
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    CareFlow360 Architecture                      │
# MAGIC ├─────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                  │
# MAGIC │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐ │
# MAGIC │  │  Landing  │──▶│  Bronze  │──▶│  Silver  │──▶│    Gold      │ │
# MAGIC │  │  Volume   │   │  (Auto   │   │ (Clean + │   │ (Analytics + │ │
# MAGIC │  │  (JSON)   │   │  Loader) │   │  Enrich) │   │  ML + RAG)   │ │
# MAGIC │  └──────────┘   └──────────┘   └──────────┘   └──────┬───────┘ │
# MAGIC │                                                       │         │
# MAGIC │                    ┌──────────────────────────────────┘         │
# MAGIC │                    ▼                                            │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
# MAGIC │  │  Views   │  │ Dashboard │  │  Genie   │  │  ML Model    │   │
# MAGIC │  │ (Bronze/ │  │ (AI/BI   │  │  Space   │  │ (RandomForest│   │
# MAGIC │  │ Silver/  │  │  Lakeview│  │ (Q&A)    │  │  + MLflow)   │   │
# MAGIC │  │ Gold)    │  │          │  │          │  │              │   │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘   │
# MAGIC │                                                                  │
# MAGIC │  ┌──────────────────────────────────────────────────────────┐   │
# MAGIC │  │            Daily Incremental Job (2 AM UTC)              │   │
# MAGIC │  │  generate → refresh → views → docs → ML → validate      │   │
# MAGIC │  └──────────────────────────────────────────────────────────┘   │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Data Flow
# MAGIC
# MAGIC 1. **Landing Volume** — JSON files land in `workspace.careflow360.landing` UC volume
# MAGIC 2. **Bronze (Auto Loader)** — Streaming ingestion with schema enforcement and DQ gates
# MAGIC 3. **Silver (Clean + Enrich)** — Deduplication, validation, derived columns, SCD Type 2
# MAGIC 4. **Gold (Analytics + ML)** — Materialized views for KPIs, alerts, patient 360, training features
# MAGIC 5. **Presentation Schemas** — Views in `careflow360_bronze`, `careflow360_silver`, `careflow360_gold` schemas
# MAGIC 6. **Consumers** — Dashboard, Genie space, ML model, AI functions all read from Gold/Presentation
# MAGIC
# MAGIC ### Key Design Principles
# MAGIC - **UC-native**: All data in Unity Catalog with governed schemas, tags, and comments
# MAGIC - **Serverless**: Pipeline runs on serverless compute with photon acceleration
# MAGIC - **Idempotent**: All operations are re-runnable (CREATE IF NOT EXISTS, MERGE, overwrite views)
# MAGIC - **Metadata-driven**: Quality rules, alert thresholds, and clinical ranges in configuration
# MAGIC - **3-Schema Separation**: Pipeline working schema vs. presentation schemas for access control

# COMMAND ----------

# DBTITLE 1,Medallion Architecture Deep Dive
# MAGIC %md
# MAGIC ## 3. Medallion Architecture Deep Dive {#medallion-architecture}
# MAGIC
# MAGIC The CareFlow360 platform uses a **3-schema medallion architecture** that separates pipeline processing from consumer access:
# MAGIC
# MAGIC ### Schema Layout
# MAGIC
# MAGIC | Schema | Purpose | Contains | Access Level |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `workspace.careflow360` | SDP pipeline working schema | 15 pipeline datasets (streaming tables + materialized views) plus physical Gold tables | Data engineering only |
# MAGIC | `workspace.careflow360_bronze` | Bronze presentation | 4 views → `careflow360.bronze_*` | Restricted to DE team |
# MAGIC | `workspace.careflow360_silver` | Silver presentation | 6 views → `careflow360.silver_*` | Analysts + ML engineers |
# MAGIC | `workspace.careflow360_gold` | Gold presentation | 7 views + 3 physical tables | All consumers |
# MAGIC
# MAGIC ### Bronze Layer (4 Streaming Tables)
# MAGIC
# MAGIC | Table | Source | Key Fields | DQ Gates |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `bronze_triage` | JSON → Auto Loader | patient_id, triage_acuity (ESI 1-5), arrival_mode, chief_complaint | expect_or_drop on patient_id, event_time |
# MAGIC | `bronze_vitals` | JSON → Auto Loader | patient_id, systolic_bp, diastolic_bp, heart_rate, spo2, temperature_c | expect_or_drop on patient_id, event_time |
# MAGIC | `bronze_adt` | JSON → Auto Loader | patient_id, event_type (admit/transfer/discharge), department, room | expect_or_drop on patient_id, event_type |
# MAGIC | `bronze_chief_complaints` | JSON → Auto Loader | patient_id, complaint_text, complaint_category | expect_or_drop on patient_id, complaint_text |
# MAGIC
# MAGIC **Key Patterns:**
# MAGIC - Auto Loader with `schemaLocation` for schema evolution tracking
# MAGIC - Explicit `StructType` schema enforcement (not schema inference)
# MAGIC - `expect_or_drop` on critical fields (patient_id, event_time)
# MAGIC - All tables are **streaming tables** (`STREAMING TABLE` in SDP)
# MAGIC
# MAGIC ### Silver Layer (6 Tables + Quarantine)
# MAGIC
# MAGIC | Table | Source | Transformations |
# MAGIC | --- | --- | --- |
# MAGIC | `silver_triage` | bronze_triage | Dedup by (patient_id, encounter_id), validate ESI 1-5, add shock_index, pulse_pressure |
# MAGIC | `silver_triage_quarantine` | bronze_triage | Invalid ESI levels or missing critical fields → quarantine pattern |
# MAGIC | `silver_vitals` | bronze_vitals | Clinical range validation (BP, HR, temp, SpO2, RR), abnormal flag derivation |
# MAGIC | `silver_adt` | bronze_adt | Dedup, validate event_type enum, normalize department names |
# MAGIC | `silver_chief_complaints` | bronze_chief_complaints | Clean text, category normalization |
# MAGIC | `silver_patient_location_history` | silver_adt | SCD Type 2 via `AUTO CDC` — tracks patient location changes over time |
# MAGIC
# MAGIC **Key Patterns:**
# MAGIC - `expect_or_quarantine` for recoverable data quality failures
# MAGIC - Derived clinical columns: `shock_index = heart_rate / systolic_bp`
# MAGIC - SCD Type 2 with `AUTO CDC` for patient location history
# MAGIC - Deduplication using `ROW_NUMBER()` window functions
# MAGIC
# MAGIC ### Gold Layer (5 Pipeline MVs + 2 Notebook-Managed Views + 3 Physical Tables)
# MAGIC
# MAGIC | Table | Type | Description | Rows |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `gold_patient_360` | Materialized View | Comprehensive patient view joining triage + vitals + ADT + complaints | ~1,000 |
# MAGIC | `gold_hospital_command_center` | Materialized View | Hospital-wide KPIs: active patients, ED census, ICU census, alerts, avg acuity | 3 |
# MAGIC | `gold_department_hourly` | Materialized View | Department-level hourly metrics: census, arrivals, avg wait time | 39 |
# MAGIC | `gold_live_alerts` | Materialized View | Real-time clinical alerts: abnormal vitals, high ESI, ED boarding | ~20 |
# MAGIC | `gold_triage_training` | Materialized View | ML training features: numeric + categorical features for triage prediction | ~2,800+ |
# MAGIC | `triage_model_quality` (gold schema) | View | Historical prediction-vs-actual agreement (in-sample) | varies |
# MAGIC | `triage_predictions_current` (gold schema) | View | Current-encounter prediction per current patient | ~1,000 |
# MAGIC | `triage_predictions` (gold schema) | Physical Table | Batch-scored predictions from the Champion RandomForest model (insert-only) | ~1,000 |
# MAGIC | `policy_chunks` (gold schema) | Physical Table | Policy document chunks for RAG / AI Search | 6 |

# COMMAND ----------

# DBTITLE 1,SDP Pipeline Details
# MAGIC %md
# MAGIC ## 4. SDP Pipeline (Bronze → Silver → Gold) {#sdp-pipeline}
# MAGIC
# MAGIC ### Pipeline Configuration
# MAGIC
# MAGIC | Property | Value |
# MAGIC | --- | --- |
# MAGIC | **Pipeline ID** | `dde7c33b-8715-4666-8f6b-ad2af0848966` |
# MAGIC | **Name** | CareFlow360_DAB_Pipeline |
# MAGIC | **Catalog** | `workspace` |
# MAGIC | **Schema** | `careflow360` |
# MAGIC | **Compute** | Serverless + Photon enabled |
# MAGIC | **Channel** | CURRENT |
# MAGIC | **State** | IDLE (healthy) |
# MAGIC | **Last 5 updates** | All COMPLETED ✅ |
# MAGIC
# MAGIC ### Source Files
# MAGIC
# MAGIC The pipeline uses 3 Python source files deployed via DAB:
# MAGIC
# MAGIC ```
# MAGIC src/pipelines/
# MAGIC ├── 01_bronze.py   — 4 Auto Loader streaming tables with schema enforcement
# MAGIC ├── 02_silver.py   — 6 cleansing/enrichment tables with DQ expectations + SCD2
# MAGIC └── 03_gold.py     — 5 materialized views for analytics/ML (the views notebook adds model-quality and current-prediction gold views)
# MAGIC ```
# MAGIC
# MAGIC ### Pipeline Configuration Properties
# MAGIC
# MAGIC ```yaml
# MAGIC pipelines:
# MAGIC   - name: CareFlow360_DAB_Pipeline
# MAGIC     catalog: workspace
# MAGIC     schema: careflow360
# MAGIC     configuration:
# MAGIC       careflow.catalog: workspace
# MAGIC       careflow.schema: careflow360
# MAGIC       careflow.landing_volume: landing
# MAGIC     photon: true
# MAGIC     serverless: true
# MAGIC     channel: CURRENT
# MAGIC     libraries:
# MAGIC       - glob:
# MAGIC           include: transformations/**
# MAGIC ```
# MAGIC
# MAGIC ### How the Pipeline Runs
# MAGIC
# MAGIC 1. **Auto Loader** watches the landing volume for new JSON files
# MAGIC 2. Bronze tables ingest raw events with schema enforcement
# MAGIC 3. Silver tables process bronze data with cleansing, dedup, enrichment
# MAGIC 4. Gold materialized views aggregate silver data for analytics
# MAGIC 5. The pipeline runs incrementally — only new data is processed
# MAGIC 6. A full refresh can be triggered if schema changes are needed
# MAGIC
# MAGIC ### Data Quality Framework
# MAGIC
# MAGIC The pipeline implements a multi-layer DQ strategy:
# MAGIC - **Bronze**: `expect_or_drop` — discard records missing critical identifiers
# MAGIC - **Silver**: `expect_or_quarantine` — divert invalid records to quarantine table
# MAGIC - **Silver**: Clinical range validation (BP 40-300, HR 20-250, Temp 30-45°C, SpO2 50-100%, RR 5-60)
# MAGIC - **Gold**: Aggregation-level validation (no NULL KPIs, positive row counts)

# COMMAND ----------

# DBTITLE 1,Lakeflow Job Details
# MAGIC %md
# MAGIC ## 5. Lakeflow Job — Daily Incremental Pipeline {#lakeflow-job}
# MAGIC
# MAGIC ### Job Configuration
# MAGIC
# MAGIC | Property | Value |
# MAGIC | --- | --- |
# MAGIC | **Job ID** | `484367296292508` |
# MAGIC | **Name** | CareFlow360 Daily Incremental Pipeline |
# MAGIC | **Schedule** | Daily at 2:00 AM UTC (`0 0 2 * * ?`) |
# MAGIC | **Status** | UNPAUSED (active) |
# MAGIC | **Max concurrent runs** | 1 |
# MAGIC | **Queue** | Enabled |
# MAGIC | **Run as** | YOUR_WORKSPACE_USER (owner) |
# MAGIC | **On failure** | Email notification to owner |
# MAGIC
# MAGIC ### Task Chain (Sequential)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐     ┌──────────────────┐     ┌───────────────────────┐     ┌──────────────────┐
# MAGIC │ generate_daily_     │────▶│ refresh_pipeline │────▶│ create_presentation_  │────▶│ train_ml_model   │
# MAGIC │ patients            │     │                   │     │ views                 │     │                  │
# MAGIC │                     │     │                   │     │                       │     │                  │
# MAGIC │ Notebook: 01_gen... │     │ Pipeline: SDP     │     │ Notebook: 03_create... │     │ Notebook: 04_tr..│
# MAGIC │ Timeout: 1800s      │     │ Full refresh: No  │     │ Timeout: 600s          │     │ Timeout: 1200s   │
# MAGIC │ random 50-150/day  │     │ Bronze→Silver→Gold│     │ Bronze/Silver/Gold    │     │ RandomForest,    │
# MAGIC │ append mode         │     │ incremental       │     │ presentation views    │     │ MLflow, UC reg   │
# MAGIC └─────────────────────┘     └──────────────────┘     └───────────────────────┘     └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Task Details
# MAGIC
# MAGIC #### Task 1: generate_sample_events (live key: generate_daily_patients)
# MAGIC - **Notebook**: `01_generate_sample_data`
# MAGIC - **Parameters**: `catalog=workspace, schema=careflow360, volume=landing, num_patients=random, num_batches=5, reset_events=false`
# MAGIC - **Purpose**: Generates a random 50–150 new synthetic patients per run and writes JSON to the landing volume; the actual count is published through the run manifest and job task values
# MAGIC - **Timeout**: 30 minutes
# MAGIC - **Live baseline (Nov 3, 2026)**: 1,000 unique patients; growth now averages ~100 patients/day (random 50–150 per run)
# MAGIC
# MAGIC #### Task 2: refresh_lakehouse (live key: refresh_pipeline)
# MAGIC - **Pipeline**: `CareFlow360_DAB_Pipeline` (incremental refresh, not full)
# MAGIC - **Purpose**: Triggers the SDP pipeline to process new data through Bronze → Silver → Gold
# MAGIC - **Depends on**: generate_sample_events (must succeed first)
# MAGIC
# MAGIC #### Task 3: create_presentation_views
# MAGIC - **Notebook**: `03_create_views` (created Oct 10 — see Fixes section)
# MAGIC - **Parameters**: `catalog=workspace, pipeline_schema=careflow360, bronze/silver/gold_schema=careflow360_*`
# MAGIC - **Purpose**: Creates/updates the 17 views in the 3 presentation schemas pointing to pipeline tables, and maintains the AI-classified chief_complaint_categories Gold table
# MAGIC - **Timeout**: 10 minutes
# MAGIC
# MAGIC #### Task 4: create_policy_docs
# MAGIC - **Notebook**: `05_create_policy_docs`
# MAGIC - **Purpose**: Creates/refreshes the six synthetic policy chunks (idempotent)
# MAGIC
# MAGIC #### Task 5: train_triage_model (live key: train_ml_model)
# MAGIC - **Notebook**: `04_train_triage_model`
# MAGIC - **Parameters**: `catalog=workspace, schema=careflow360` (fixed Oct 10 — see Fixes section)
# MAGIC - **Purpose**: Retrains the RandomForest triage classifier on the growing dataset, logs to MLflow, registers in UC, writes batch predictions
# MAGIC - **Timeout**: 20 minutes
# MAGIC
# MAGIC #### Task 6: validate_project
# MAGIC - **Notebook**: `06_validate`
# MAGIC - **Purpose**: Read-only acceptance gates; in the scheduled run it requires the run's actual new-patient count (from the manifest) to appear across Bronze, Silver, Gold Patient 360 and predictions
# MAGIC
# MAGIC ### Verified Run Status (Nov 3, 2026 — six-task job)
# MAGIC
# MAGIC | Task | Status |
# MAGIC | --- | --- |
# MAGIC | generate_daily_patients (live key) | ✅ SUCCESS — random 50–150 patients per run |
# MAGIC | refresh_pipeline (live key) | ✅ SUCCESS — incremental Bronze → Silver → Gold |
# MAGIC | create_presentation_views | ✅ SUCCESS — 17 presentation views |
# MAGIC | create_policy_docs | ✅ SUCCESS — 6 policy chunks |
# MAGIC | train_ml_model (live key) | ✅ SUCCESS — predictions written |
# MAGIC | validate_project | ✅ SUCCESS — all acceptance gates pass |
# MAGIC
# MAGIC Live baseline (Nov 3, 2026): 1,000 unique patients and 1,000 current predictions.

# COMMAND ----------

# DBTITLE 1,ML/AI Capabilities & Dashboard
# MAGIC %md
# MAGIC ## 6. ML/AI Capabilities {#ml-ai-capabilities}
# MAGIC
# MAGIC ### Triage Acuity Classifier (RandomForest)
# MAGIC
# MAGIC | Property | Value |
# MAGIC | --- | --- |
# MAGIC | **Algorithm** | RandomForestClassifier (96 trees, max_depth=12) |
# MAGIC | **Features** | 12 numeric (age, vitals, shock_index, etc.) + 3 categorical (arrival_mode, day, sex) |
# MAGIC | **Target** | `triage_acuity` (ESI 1-5, 5-class classification) |
# MAGIC | **Accuracy** | Determined from current patient-held-out MLflow run; do not assume a historical score |
# MAGIC | **Training data** | `workspace.careflow360.gold_triage_training` (growing daily; live baseline 1,000 patients as of Nov 3, 2026) |
# MAGIC | **Model registry** | UC-registered as `workspace.careflow360.triage_acuity_model` with `Champion` alias |
# MAGIC | **Predictions** | Written to `workspace.careflow360.gold_triage_predictions` (batch scoring) |
# MAGIC | **MLflow experiment** | `/Workspace/Users/YOUR_WORKSPACE_USER/careflow360/triage_experiment` |
# MAGIC
# MAGIC ### ML Pipeline (scikit-learn)
# MAGIC
# MAGIC ```python
# MAGIC # Preprocessing
# MAGIC ColumnTransformer([
# MAGIC     ('numeric', SimpleImputer(strategy='median'), numeric_features),
# MAGIC     ('categorical', Pipeline([
# MAGIC         SimpleImputer(strategy='most_frequent'),
# MAGIC         OneHotEncoder(handle_unknown='ignore')
# MAGIC     ]), categorical_features)
# MAGIC ])
# MAGIC
# MAGIC # Model
# MAGIC RandomForestClassifier(
# MAGIC     n_estimators=96, max_depth=12,
# MAGIC     min_samples_leaf=2, class_weight='balanced_subsample'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Metrics Tracked in MLflow
# MAGIC - accuracy, f1_macro, precision_macro, recall_macro
# MAGIC - train_rows, test_rows
# MAGIC - Model type, n_estimators, max_depth
# MAGIC - Data classification: synthetic_or_public
# MAGIC - Clinical use: prohibited_demo_only
# MAGIC
# MAGIC ### Batch Scoring
# MAGIC After training, the model is loaded via `mlflow.pyfunc.load_model` and used to score all patients in the training set. Predictions are written to `gold_triage_predictions` for dashboard visualization and downstream analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. AI/BI Dashboard {#ai-bi-dashboard}
# MAGIC
# MAGIC ### Dashboard Configuration
# MAGIC
# MAGIC | Property | Value |
# MAGIC | --- | --- |
# MAGIC | **Name** | CareFlow360 Hospital Command Center |
# MAGIC | **ID** | `01f1beeb451e10439c0b4bf6aa8eeb70` |
# MAGIC | **Type** | Lakeview Dashboard |
# MAGIC | **Datasets** | 6 SQL datasets referencing `careflow360_gold.*` views |
# MAGIC | **Widgets** | 14 verified widgets |
# MAGIC
# MAGIC ### Dashboard Widgets
# MAGIC
# MAGIC | Widget | Type | Dataset | Description |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Active Patients | KPI Counter | command_center | Currently admitted patients (51) |
# MAGIC | ED Census | KPI Counter | command_center | Emergency Department patients (10) |
# MAGIC | ICU Census | KPI Counter | command_center | ICU patients (8) |
# MAGIC | Active Alerts | KPI Counter | command_center | Clinical alerts (9) |
# MAGIC | Avg Acuity | KPI Counter | command_center | Average ESI level (3.29) |
# MAGIC | Department Census | Bar Chart | department_hourly | Patients per department (6 depts) |
# MAGIC | Triage Acuity | Pie Chart | patient_360 | Distribution of ESI levels (5 levels) |
# MAGIC | Patient Flow | Line Chart | department_hourly | Patient flow over time (60 rows) |
# MAGIC | Live Alerts | Table | live_alerts | Active clinical alerts (50 rows) |
# MAGIC | ML Predictions | Bar Chart | triage_predictions | Predicted vs actual acuity (5 levels) |

# COMMAND ----------

# DBTITLE 1,Genie Space & DAB Deployment
# MAGIC %md
# MAGIC ## 8. Genie Space — Hospital Operations Agent {#genie-space}
# MAGIC
# MAGIC ### Genie Space Configuration
# MAGIC
# MAGIC | Property | Value |
# MAGIC | --- | --- |
# MAGIC | **Name** | CareFlow360 Hospital Operations Agent |
# MAGIC | **ID** | `01f103e12eb71094aee3ca8818a5cc87` |
# MAGIC | **Tables** | 6 Gold tables |
# MAGIC | **Benchmark** | 8/8 questions passed ✅ |
# MAGIC | **Status** | PRODUCTION-READY |
# MAGIC
# MAGIC ### Knowledge Configuration
# MAGIC
# MAGIC - **8 text instructions**: Triage scale (ESI 1-5), departments, alert flags, event types, ML model, shock index, timezone (UTC), policies
# MAGIC - **5 SQL example queries**: Reference patterns for common questions
# MAGIC - **10 knowledge snippets**: 3 joins, 4 filters, 3 measures
# MAGIC - **15 column descriptions**: Patient 360 columns explained
# MAGIC - **Entity matching**: 14 columns across 5 tables
# MAGIC - **7 Genie-friendly table descriptions**: Business context for each table
# MAGIC - **10 starter questions**: Common queries users can ask
# MAGIC - **8 benchmark questions**: All passed (accuracy verified)
# MAGIC
# MAGIC ### Example Questions the Genie Can Answer
# MAGIC - "How many patients are currently in the ED?"
# MAGIC - "What's the average triage acuity by department?"
# MAGIC - "Show me all patients with abnormal vital signs"
# MAGIC - "Which departments have the longest wait times?"
# MAGIC - "How many alerts are active right now?"
# MAGIC - "Compare ML predictions vs actual triage levels"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9. DAB (Declarative Automation Bundles) Deployment {#dab-deployment}
# MAGIC
# MAGIC ### DAB Project Structure
# MAGIC
# MAGIC ```
# MAGIC Careflow360/                     -- GitHub repository root (canonical)
# MAGIC   databricks.yml                 -- Root bundle config (schema=careflow360)
# MAGIC   resources/
# MAGIC     01_uc.yml                    -- 4 UC schemas (pipeline + 3 presentation)
# MAGIC     02_pipeline.yml              -- SDP pipeline definition
# MAGIC     03_jobs.yml                  -- Daily incremental job with 6 tasks
# MAGIC   src/
# MAGIC     notebooks/                   -- 9 Databricks notebooks
# MAGIC       00_preflight.py
# MAGIC       01_generate_sample_data.py
# MAGIC       03_create_views.py
# MAGIC       04_train_triage_model.py
# MAGIC       05_create_policy_docs.py
# MAGIC       06_validate.py
# MAGIC       07_ai_functions.py
# MAGIC       08_upgrade_existing_daily_job.py
# MAGIC       09_compare_free_edition_models.py
# MAGIC     pipelines/                   -- 3 SDP source files
# MAGIC       01_bronze.py
# MAGIC       02_silver.py
# MAGIC       03_gold.py
# MAGIC   sql/                           -- SQL (setup, validation, benchmarks)
# MAGIC   extensions/app/                -- Streamlit command-center Databricks App
# MAGIC   docs/                          -- Documentation (11 files)
# MAGIC   tests/                         -- Offline contract tests + static_validate.py
# MAGIC   presentations/                 -- Presentation notebooks
# MAGIC   README.md
# MAGIC   QUICKSTART.md
# MAGIC   CHANGELOG.md
# MAGIC   LICENSE
# MAGIC ```
# MAGIC
# MAGIC ### DAB Resources Deployed
# MAGIC
# MAGIC | Resource | Type | Status |
# MAGIC | --- | --- | --- |
# MAGIC | 4 UC schemas | Unity Catalog schemas | ✅ Created (careflow360, _bronze, _silver, _gold) |
# MAGIC | SDP pipeline | Lakeflow Spark Declarative Pipeline | ✅ Deployed (serverless, photon) |
# MAGIC | Daily job | Lakeflow Job (6 tasks) | ✅ Deployed & operational |
# MAGIC | Landing volume | UC volume | ✅ Created |
# MAGIC | Dashboard | AI/BI Lakeview dashboard | ✅ Published |
# MAGIC | Genie space | Genie data-room | ✅ Configured & verified |
# MAGIC | ML model | UC registered model | ✅ Trained & registered |
# MAGIC | UC tags | Governance tags | ✅ Applied (domain, layer, classification) |

# COMMAND ----------

# DBTITLE 1,UC Governance, AI Functions & Extensions
# MAGIC %md
# MAGIC ## 10. UC Governance & Security {#uc-governance}
# MAGIC
# MAGIC ### Unity Catalog Governance
# MAGIC
# MAGIC All data assets in CareFlow360 are governed through Unity Catalog:
# MAGIC
# MAGIC | Governance Feature | Implementation |
# MAGIC | --- | --- |
# MAGIC | **Catalog** | `workspace` — single catalog for all CareFlow360 assets |
# MAGIC | **Schemas** | 4 schemas: `careflow360` (pipeline), `careflow360_bronze`, `careflow360_silver`, `careflow360_gold` (presentation) |
# MAGIC | **UC Tags** | `domain=healthcare`, `layer=bronze/silver/gold`, `classification=raw/cleansed/analytics` |
# MAGIC | **Table comments** | Every table has a business-context comment in DDL |
# MAGIC | **Column comments** | 15 column descriptions on `patient_360` for Genie and catalog discovery |
# MAGIC | **Delta properties** | `delta.enableDeletionVectors=true`, `delta.autoOptimize.managed=true` |
# MAGIC | **Volume** | `workspace.careflow360.landing` — governed UC volume for file ingestion |
# MAGIC | **Model registry** | UC-registered model `workspace.careflow360.triage_acuity_model` |
# MAGIC | **Run-as identity** | Job runs as owner (YOUR_WORKSPACE_USER) |
# MAGIC
# MAGIC ### 3-Schema Separation Pattern
# MAGIC
# MAGIC The separation between pipeline working schema and presentation schemas enables:
# MAGIC - **Access control**: DE team has write access to `careflow360`; consumers get read access to presentation schemas only
# MAGIC - **Schema isolation**: Pipeline schema changes don't break consumer views
# MAGIC - **Auditing**: Clear lineage from pipeline tables → presentation views → consumers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 11. AI Functions & RAG {#ai-functions-rag}
# MAGIC
# MAGIC ### AI Functions Tested
# MAGIC
# MAGIC | Function | Use Case | Status |
# MAGIC | --- | --- | --- |
# MAGIC | `ai_classify` | Classify patient risk levels from vitals text | ✅ Tested (50 rows) |
# MAGIC | `ai_extract` | Extract structured fields from clinical notes (v2.1) | ✅ Tested (20 rows) |
# MAGIC | `ai_forecast` | Predict future patient volumes | ⚠️ Preview disabled in Free Edition |
# MAGIC | `ai_query` | Natural language to SQL via direct SQL execution | ✅ Working |
# MAGIC
# MAGIC ### RAG / AI Search
# MAGIC
# MAGIC - **Policy documents**: 6 policy chunks stored in `gold_policy_chunks` table
# MAGIC - **Vector Search**: Extension config for AI Search index (ready for deployment)
# MAGIC - **Use case**: Hospital staff can query policy documents via natural language
# MAGIC
# MAGIC ### Policy Document Processing
# MAGIC
# MAGIC The `05_create_policy_docs` notebook:
# MAGIC 1. Creates policy text (ED triage protocols, admission criteria, discharge guidelines)
# MAGIC 2. Splits into chunks (paragraph-level)
# MAGIC 3. Stores in `gold_policy_chunks` with metadata (source, chunk_id, section)
# MAGIC 4. Ready for Vector Search indexing and RAG applications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 12. Extensions & Future Capabilities {#extensions}
# MAGIC
# MAGIC The repository previously carried template stubs for Model Serving, AI Search, Lakebase and Genie bundles. Those are **removed** — they are not usable on Databricks Free Edition and were never deployed. What actually exists:
# MAGIC
# MAGIC | Capability | Purpose | Location / Status |
# MAGIC | --- | --- | --- |
# MAGIC | **Databricks App** | Streamlit healthcare command center | `extensions/app/` — ready to deploy |
# MAGIC | **Genie Space** | Hospital operations agent | Live in workspace (no bundle config) |
# MAGIC | **Model Serving** | Removed — not available on Free Edition | — |
# MAGIC | **AI Search / RAG** | Removed — not available on Free Edition | — |
# MAGIC | **Lakebase Postgres** | Removed — not available on Free Edition | — |

# COMMAND ----------

# DBTITLE 1,Fixes Applied Oct 10
# MAGIC %md
# MAGIC ## 13. Fixes Applied (Oct 10, 2026) {#fixes-applied}
# MAGIC
# MAGIC The Daily Incremental Job had been failing every day since October 4, 2026 (7 consecutive failed runs). Three root causes were identified and fixed:
# MAGIC
# MAGIC ### Fix 1: Missing Notebook `03_create_views`
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC | --- | --- |
# MAGIC | **Task affected** | `create_presentation_views` |
# MAGIC | **Error** | `RESOURCE_NOT_FOUND: Unable to access notebook "03_create_views"` |
# MAGIC | **Root cause** | The file existed only as `03_create_views.py` (plain Python file), not as a Databricks notebook. The job's `notebook_task` requires a proper notebook. |
# MAGIC | **Fix** | Created a Databricks notebook (ID 1064656254159981) at the expected path from the `.py` file content |
# MAGIC | **Impact** | Task 3 (`create_presentation_views`) now succeeds — creates bronze/silver/gold presentation views |
# MAGIC
# MAGIC ### Fix 2: Wrong Schema Parameter for ML Training
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC | --- | --- |
# MAGIC | **Task affected** | `train_ml_model` |
# MAGIC | **Error** | `TABLE_OR_VIEW_NOT_FOUND: workspace.careflow360_gold.gold_triage_training cannot be found` |
# MAGIC | **Root cause** | The job passed `schema=careflow360_gold` (presentation schema), but the actual pipeline table `gold_triage_training` lives in the pipeline working schema `careflow360`. The `03_create_views` notebook creates views in `careflow360_gold` with the `gold_` prefix stripped (e.g., `triage_training`), so `careflow360_gold.gold_triage_training` doesn't exist. |
# MAGIC | **Fix** | Changed the `train_ml_model` task's `schema` parameter from `careflow360_gold` to `careflow360` |
# MAGIC | **Impact** | Task 4 (`train_ml_model`) now finds the training table, trains the model, and writes predictions correctly |
# MAGIC
# MAGIC ### Fix 3: Missing MLflow Experiment Directory
# MAGIC
# MAGIC | Aspect | Details |
# MAGIC | --- | --- |
# MAGIC | **Task affected** | `train_ml_model` |
# MAGIC | **Error** | `RestException: NOT_FOUND: Parent directory does not exist: /Users/.../careflow360` |
# MAGIC | **Root cause** | The notebook's `experiment_path` defaults to `/Workspace/Users/.../careflow360/triage_experiment`. The `careflow360` directory didn't exist in the workspace, so MLflow couldn't create the experiment. |
# MAGIC | **Fix** | Created the missing workspace directory `/Users/YOUR_WORKSPACE_USER/careflow360` |
# MAGIC | **Impact** | MLflow can now create the experiment and log the training run |
# MAGIC
# MAGIC ### Verification
# MAGIC
# MAGIC After all three fixes, the job was repaired and completed successfully:
# MAGIC
# MAGIC | Task | Duration | Status |
# MAGIC | --- | --- | --- |
# MAGIC | generate_daily_patients | 39s | ✅ SUCCESS |
# MAGIC | refresh_pipeline | 383s | ✅ SUCCESS |
# MAGIC | create_presentation_views | 29s | ✅ SUCCESS |
# MAGIC | train_ml_model | ~46s | ✅ SUCCESS |
# MAGIC | **Total** | **~9 min** | **✅ SUCCESS** |

# COMMAND ----------

# DBTITLE 1,Project Health & Lessons Learned
# MAGIC %md
# MAGIC ## 14. Project Health Summary {#project-health}
# MAGIC
# MAGIC ### Component Status (as of Nov 3, 2026)
# MAGIC
# MAGIC | Component | Status | Key Metrics |
# MAGIC | --- | --- | --- |
# MAGIC | **SDP Pipeline** | ✅ Healthy | IDLE, last 5 updates COMPLETED, serverless + photon |
# MAGIC | **Daily Job** | ✅ Operational | All 6 tasks SUCCESS, schedule active (2 AM UTC) |
# MAGIC | **Bronze Tables** | ✅ 4 tables | Auto Loader streaming, schema-enforced, DQ-gated |
# MAGIC | **Silver Tables** | ✅ 6 tables | Cleansed, enriched, SCD2, quarantine pattern |
# MAGIC | **Gold Assets** | ✅ 10 assets | 7 presentation views + 3 physical (predictions, policy chunks, chief-complaint categories) |
# MAGIC | **Presentation Views** | ✅ 17 views | 4 bronze + 6 silver + 7 gold (incl. triage_model_quality, triage_predictions_current) across 3 schemas |
# MAGIC | **ML Model** | ✅ Trained | RandomForest, measured patient-held-out metrics (inspect MLflow), UC Champion alias |
# MAGIC | **MLflow Experiment** | ✅ Created | `/Workspace/Users/.../careflow360/triage_experiment` |
# MAGIC | **AI/BI Dashboards** | ✅ Published | Command Center: 3 pages, 29 widgets, 18 datasets; companion Operations Dashboard: 1 page, 14 widgets, 6 datasets |
# MAGIC | **Genie Space** | ✅ Production-ready | 8/8 benchmarks passed |
# MAGIC | **UC Governance** | ✅ Applied | Tags, comments, Delta properties, 3-schema separation |
# MAGIC | **DAB** | ✅ Deployed | Multi-resource YAML, all resources created |
# MAGIC | **AI Functions** | ✅ Tested | ai_classify, ai_extract, ai_query working |
# MAGIC | **Notebooks** | ✅ 7 notebooks | All present and accessible |
# MAGIC | **Pipeline Source** | ✅ 3 files | 01_bronze, 02_silver, 03_gold deployed |
# MAGIC
# MAGIC ### Growth Trajectory
# MAGIC
# MAGIC | Timeframe | Estimated Patients | Estimated Events |
# MAGIC | --- | --- | --- |
# MAGIC | Day 0 (current, Nov 3) | ~1,000 | ~6,600 |
# MAGIC | Day 7 | ~1,700 | ~11,300 |
# MAGIC | Day 30 | ~4,000 | ~26,600 |
# MAGIC | Day 90 | ~10,000 | ~66,300 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 15. Lessons Learned {#lessons-learned}
# MAGIC
# MAGIC ### Technical Lessons
# MAGIC
# MAGIC 1. **Notebook vs. File**: When a DAB project includes `.py` files in the notebooks directory, they must be converted to Databricks notebooks (not left as plain Python files) if referenced by `notebook_task` in a job.
# MAGIC
# MAGIC 2. **Schema Mapping**: Pipeline working schemas and presentation schemas use different naming conventions. Pipeline tables keep their `gold_` prefix; presentation views strip it. Job parameters must reference the correct schema for each use case.
# MAGIC
# MAGIC 3. **MLflow Directory Dependencies**: MLflow workspace experiments require the parent directory to exist. Always verify directory paths in default widget values, especially when the experiment path is constructed dynamically.
# MAGIC
# MAGIC 4. **3-Schema Separation**: Separating pipeline working schema from presentation schemas provides clean access boundaries but requires careful parameter management — the same table name may differ between schemas (e.g., `gold_triage_training` vs `triage_training`).
# MAGIC
# MAGIC 5. **Repair vs. Re-run**: Using `repair-run` instead of `run-now` saves time and compute by re-running only failed tasks while preserving successful task results.
# MAGIC
# MAGIC ### Architectural Lessons
# MAGIC
# MAGIC 1. **Medallion + Presentation Layer**: Adding a presentation layer (3 schemas with views) on top of the pipeline working schema is a best practice for access control and consumer isolation.
# MAGIC
# MAGIC 2. **Serverless + Photon**: The pipeline runs efficiently on serverless compute with photon acceleration — no cluster management needed, auto-scaling, and fast startup.
# MAGIC
# MAGIC 3. **DAB for Reproducibility**: Using Declarative Automation Bundles makes the entire project reproducible — schemas, pipeline, job, SQL, tests, and the app are all defined as code.
# MAGIC
# MAGIC 4. **Metadata-Driven Quality**: Clinical reference ranges and quality rules stored as configuration (not hardcoded) make the system maintainable and adaptable to different clinical contexts.
# MAGIC
# MAGIC 5. **Incremental Processing**: The SDP pipeline processes only new data incrementally, making daily refreshes fast and cost-effective.

# COMMAND ----------

# DBTITLE 1,File Inventory & Conclusion
# MAGIC %md
# MAGIC ## 16. File Inventory {#file-inventory}
# MAGIC
# MAGIC ### Core Notebooks (9)
# MAGIC
# MAGIC | Notebook | Path | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `00_preflight` | `src/notebooks/00_preflight.py` | UC schema + volume setup, pre-flight checks |
# MAGIC | `01_generate_sample_data` | `src/notebooks/01_generate_sample_data.py` | Synthetic patient data generator (random 50–150/run) |
# MAGIC | `03_create_views` | `src/notebooks/03_create_views.py` | 17 presentation views + AI gold assets |
# MAGIC | `04_train_triage_model` | `src/notebooks/04_train_triage_model.py` | RandomForest triage classifier + MLflow + UC registration |
# MAGIC | `05_create_policy_docs` | `src/notebooks/05_create_policy_docs.py` | Policy document chunking for RAG |
# MAGIC | `06_validate` | `src/notebooks/06_validate.py` | Runtime validation (row counts, schema, clinical ranges) |
# MAGIC | `07_ai_functions` | `src/notebooks/07_ai_functions.py` | AI function demos (ai_classify, ai_extract, ai_forecast, ai_query) |
# MAGIC | `08_upgrade_existing_daily_job` | `src/notebooks/08_upgrade_existing_daily_job.py` | In-place live-job upgrade helper (applied Nov 3) |
# MAGIC | `09_compare_free_edition_models` | `src/notebooks/09_compare_free_edition_models.py` | Optional CPU model comparison benchmark |
# MAGIC
# MAGIC ### Pipeline Source Files (3)
# MAGIC
# MAGIC | File | Path | Layer | Tables |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `01_bronze.py` | `src/pipelines/01_bronze.py` | Bronze | 4 streaming tables (Auto Loader) |
# MAGIC | `02_silver.py` | `src/pipelines/02_silver.py` | Silver | 6 tables (cleanse + enrich + SCD2) |
# MAGIC | `03_gold.py` | `src/pipelines/03_gold.py` | Gold | 5 materialized views |
# MAGIC
# MAGIC ### SQL Notebooks (4)
# MAGIC
# MAGIC | Notebook | Path | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `00_setup` | `sql/00_setup.sql` | Monitoring/quality schema setup, quality rules seeding |
# MAGIC | `dashboard_queries` | `sql/dashboard_queries.sql` | SQL queries for dashboard datasets |
# MAGIC | `genie_benchmarks` | `sql/genie_benchmarks.sql` | Genie space benchmark queries |
# MAGIC | `validation` | `sql/validation.sql` | Data validation SQL queries |
# MAGIC
# MAGIC ### DAB Configuration (4)
# MAGIC
# MAGIC | File | Path | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `databricks.yml` | `databricks.yml` | Root bundle configuration |
# MAGIC | `01_uc.yml` | `resources/01_uc.yml` | 4 UC schemas (pipeline + 3 presentation) |
# MAGIC | `02_pipeline.yml` | `resources/02_pipeline.yml` | SDP pipeline definition |
# MAGIC | `03_jobs.yml` | `resources/03_jobs.yml` | Daily incremental job (6 tasks) |
# MAGIC
# MAGIC ### Documentation (11)
# MAGIC
# MAGIC | File | Purpose |
# MAGIC | --- | --- |
# MAGIC | `ARCHITECTURE.md` | Architecture documentation |
# MAGIC | `FREE_EDITION_SCOPE.md` | Free Edition feature scope |
# MAGIC | `TROUBLESHOOTING.md` | Common issues and solutions |
# MAGIC | `DASHBOARD_GENIE.md` | Dashboard and Genie setup guide |
# MAGIC | `DATABRICKS_WORKSPACE_SYNC.md` | Git ↔ workspace sync runbook |
# MAGIC | `END_TO_END_VERIFICATION.md` | Live acceptance checks |
# MAGIC | `FREE_EDITION_NOTEBOOK_AUDIT.md` | Source-by-source notebook audit |
# MAGIC | `EVERY_NOTEBOOK_EXPLAINED.md` | Notebook-by-notebook guide |
# MAGIC | `KAGGLE_DATA.md` | Historical Kaggle integration note |
# MAGIC | `INTERVIEW_TALK_TRACK.md` | Interview presentation guide |
# MAGIC | `SUMMARY_EACH_STEP.md` | Step-by-step learning summary |
# MAGIC
# MAGIC ### Summary Notebooks (4)
# MAGIC
# MAGIC | Notebook | ID | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `CareFlow360 — Complete Project Presentation` | 3441798886909984 | This notebook — comprehensive project overview |
# MAGIC | `CareFlow360 — Summary Each Step` | 3441798886909987 | Step-by-step learning guide (Steps 0-7 + cross-cutting) |
# MAGIC | `CareFlow360 — Every Notebook Explained` | 3441798886909986 | Detailed explanation of each notebook's purpose and code |
# MAGIC | `CareFlow360 — Presentation Talk Track` | 3441798886909985 | Spoken presentation script with cell references |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Conclusion
# MAGIC
# MAGIC CareFlow360 demonstrates a complete, production-ready hospital analytics platform on Databricks Free Edition, leveraging:
# MAGIC - **Medallion architecture** with 3-schema separation for governance
# MAGIC - **Serverless SDP pipeline** with Auto Loader, DQ gates, and SCD Type 2
# MAGIC - **Daily incremental job** for continuous data growth and ML retraining
# MAGIC - **AI/BI dashboards** — canonical Command Center (3 pages, 29 widgets) plus a companion Operations Dashboard
# MAGIC - **Genie space** with 8/8 benchmark questions passing
# MAGIC - **ML model** with MLflow tracking and UC model registry
# MAGIC - **DAB deployment** for full reproducibility
# MAGIC - **AI functions** for classification, extraction, and querying
# MAGIC
# MAGIC **The platform is fully operational as of November 3, 2026, with a daily schedule that runs all 6 tasks end-to-end (random 50–150 patients per run) without intervention.**

# COMMAND ----------

