# CareFlow360 — Every Notebook Explained (What & Why)

A complete, detailed walkthrough of the CareFlow360 hospital operations analytics platform.

**Core scheduled order:** Generate new synthetic patients → incremental Lakeflow refresh (Bronze/Silver/Gold) → presentation views → (ML training and policy creation in parallel) → validation. The Kaggle normalization notebook, preflight, and optional AI functions notebook are separate/manual learning workflows.

---

## 01_bronze.py — Bronze Layer: Raw Event Ingestion

### What it does
Ingests raw hospital events from JSON files in the landing volume using Auto Loader (cloudFiles). Creates 4 Bronze streaming tables with explicit schema enforcement and data quality gates.

### Tables created (4)
- `bronze_triage` — ED triage assessments with vitals, demographics, acuity
- `bronze_vitals` — Continuous vital sign monitoring events
- `bronze_adt` — Admission/Discharge/Transfer (ADT) location events
- `bronze_chief_complaints` — Patient chief complaint text

### Key design decisions
- **Explicit StructType schemas** — No schema drift; invalid records dropped at ingest
- **Auto Loader (cloudFiles)** — Incremental file discovery, idempotent, handles late arrivals
- **DQ gates** — `expect_or_drop` on event_id, patient_id, event_ts (non-null)
- **Audit columns** — `_ingest_ts` and `_source_file` on every record for traceability

---

## 02_silver.py — Silver Layer: Cleansed & Enriched Events

### What it does
Transforms raw Bronze events into cleansed, deduplicated, enriched streaming tables with derived columns, quarantine, and SCD2 tracking.

### Tables created (6)
- `silver_triage` — Validated triage events with shock_index, pulse_pressure
- `silver_triage_quarantine` — Rejected records with rejection_reason
- `silver_vitals` — Cleansed vitals with operational_alert_flag
- `silver_adt` — Validated ADT events
- `silver_chief_complaints` — Cleaned complaint text
- `silver_patient_location_history` — SCD2 via AUTO CDC

### Key design decisions
- **Watermark + dropDuplicates** — 1-day watermark eliminates duplicates
- **DQ quarantine** — Rejected records retained with rejection_reason
- **Derived columns** — shock_index = HR/systolic_bp, pulse_pressure = systolic-diastolic
- **Operational alert flag** — TRUE for HR<50/>120, SpO2<90, BP<90/>180, temp>38.5/<35, fall=TRUE
- **AUTO CDC SCD2** — Patient-location history tracked using `__START_AT` and `__END_AT`; previous versions are preserved

---

## 03_gold.py — Gold Layer: Analytics-Ready Aggregates

### What it does
Creates business-ready materialized views for dashboards, Genie, ML training, and AI Search.

### Tables created (5 MVs + 2 notebook tables)
- `gold_patient_360` — Latest snapshot per patient: triage + vitals + complaint + location
- `gold_hospital_command_center` — KPI summary: active, ED, ICU, alerts, avg acuity
- `gold_department_hourly` — Hourly ADT events by department
- `gold_live_alerts` — Patients with active vital-sign alerts
- `gold_triage_training` — ML feature table
- `triage_predictions` (physical table in the split Gold presentation schema) — Batch ML predictions
- `policy_chunks` (physical table in the split Gold presentation schema) — RAG policy documents

### Key design decisions
- **Materialized views** — Auto-refreshed by SDP pipeline
- **Window functions** — latest triage per `patient_id`; latest vitals, complaints and location per `(patient_id, encounter_id)` to prevent mixing data from separate visits. Current live alerts are restricted to the admitted patient's current encounter.
- **Patient 360 assembly** — Joins latest triage + vitals + complaint + location

---

## 00 · Preflight Setup
Creates UC schema `workspace.careflow360`, volume `landing`, and landing directories for each event type. Idempotent with `IF NOT EXISTS`.

## 01 · Generate Sample Data
Generates run-scoped synthetic patient identifiers and append-only ADT, triage, vitals and complaint JSON events for Auto Loader. The scheduled source job config requests 100 new patients per execution, not a repeated static cohort. Each run has an independent manifest.

## 02 · Kaggle Normalize
Normalizes external data to JSON schema expected by Bronze. Column mapping, type casting, null handling.

## 03 · Create Views
Creates 15 presentation views (4 bronze, 6 silver, 5 gold) pointing to pipeline tables. Strips layer prefix for clean names. Runs as job task after pipeline refresh.

## 04 · Train Triage ML Model
RandomForest (96 trees, depth 12, up to 2 CPU workers). A deterministic historical-encounter sample is capped at 4,000 rows by default for Free Edition. Evaluation holds out complete patient IDs using GroupShuffleSplit and logs majority-class baselines, accuracy, balanced accuracy and macro-F1. MLflow registers a Unity Catalog model with Champion alias. Insert-only Delta MERGE adds predictions for previously unseen current patient IDs. No fixed accuracy is promised; inspect actual MLflow metrics.

## 05 · Create Policy Documents
6 synthetic policies (ICU escalation, ED surge, bed assignment, discharge, data quality, synthetic data). Chunked for RAG. Delta CDF enabled.

## 06 · Validate
Aggregate checks for required assets, one-row-per-patient grain, acuity ranges and snapshot freshness; in the scheduled job it also requires the current run's new patient IDs to appear in Bronze, Silver, Gold Patient 360 and predictions.

## 07 · AI Functions
The executable notebook uses Free Edition-friendly SQL for KPI aggregates, complaint-keyword grouping and a moving-average baseline; SQL AI functions are optional and are not prerequisites of the scheduled core job.

---

## 09 · Optional model comparison (manual Free Edition notebook)
`09_compare_free_edition_models.py` benchmarks majority-class, Logistic Regression, Random Forest and CPU histogram gradient boosting using a deterministic capped sample (2,000 rows by default). Patients are held out **as groups** in train/validation/test, with model selection on validation macro-F1 and a separate final test. It logs only aggregate metrics to MLflow. It does **not** change the production `Champion` alias, deploy endpoints or add work to the scheduled job. Do not claim one model is superior until its actual workspace metrics and training times have been compared.

## Verification scope

- Source pipeline declares **15 materialized/streaming datasets**. The split-schema layout exposes **15 presentation views** and two additional physical Gold tables.
- As checked on November 1, 2026, the live workspace contained **600 unique current patient IDs**, **2,800 historical triage encounters**, **5,609 ADT events** and **600 current predictions**. These are historical observations, not perpetual row-count constants.
- The published dashboard had **3 pages, 29 widgets and 18 SQL datasets**. SQL queries were tested independently; visual rendering was not independently certified.
- Patient growth and the MLflow model's holdout accuracy must be verified **after** the corrected code has actually been deployed to Databricks. GitHub CI passing is not that deployment.
