# CareFlow360 — Every Notebook Explained (What & Why)

A complete, detailed walkthrough of the CareFlow360 hospital operations analytics platform.

**Run order:** `00_preflight` → `01_generate_sample_data` → `02_kaggle_normalize` → SDP Pipeline (bronze→silver→gold) → `03_create_views` → `04_train_triage_model` → `05_create_policy_docs` → `06_validate` → `07_ai_functions`

---

## 01_bronze.py — Bronze Layer: Raw Event Ingestion

### What it does
Ingests raw hospital events from JSON files in the landing volume using Auto Loader (cloudFiles). Creates 4 Bronze streaming tables with explicit schema enforcement and data quality gates.

### Tables created (4)
- `bronze_triage` (600 rows) — ED triage assessments with vitals, demographics, acuity
- `bronze_vitals` (2,065 rows) — Continuous vital sign monitoring events
- `bronze_adt` (1,385 rows) — Admission/Discharge/Transfer (ADT) location events
- `bronze_chief_complaints` (600 rows) — Patient chief complaint text

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
- `silver_triage` (600 rows) — Validated triage events with shock_index, pulse_pressure
- `silver_triage_quarantine` (0 rows) — Rejected records with rejection_reason
- `silver_vitals` (2,065 rows) — Cleansed vitals with operational_alert_flag
- `silver_adt` (1,385 rows) — Validated ADT events
- `silver_chief_complaints` (600 rows) — Cleaned complaint text
- `silver_patient_location_history` (1,357 rows) — SCD2 via AUTO CDC

### Key design decisions
- **Watermark + dropDuplicates** — 1-day watermark eliminates duplicates
- **DQ quarantine** — Rejected records retained with rejection_reason
- **Derived columns** — shock_index = HR/systolic_bp, pulse_pressure = systolic-diastolic
- **Operational alert flag** — TRUE for HR<50/>120, SpO2<90, BP<90/>180, temp>38.5/<35, fall=TRUE
- **AUTO CDC SCD2** — Patient location tracked with valid_from/valid_to/is_current

---

## 03_gold.py — Gold Layer: Analytics-Ready Aggregates

### What it does
Creates business-ready materialized views for dashboards, Genie, ML training, and AI Search.

### Tables created (5 MVs + 2 notebook tables)
- `gold_patient_360` (600) — Latest snapshot per patient: triage + vitals + complaint + location
- `gold_hospital_command_center` (1) — KPI summary: active, ED, ICU, alerts, avg acuity
- `gold_department_hourly` (90) — Hourly ADT events by department
- `gold_live_alerts` (74) — Patients with active vital-sign alerts
- `gold_triage_training` (600) — ML feature table
- `gold_triage_predictions` (600, notebook) — Batch ML predictions
- `gold_policy_chunks` (6, notebook) — RAG policy documents

### Key design decisions
- **Materialized views** — Auto-refreshed by SDP pipeline
- **Window functions** — `row_number() OVER (PARTITION BY patient_id ORDER BY event_ts DESC)`
- **Patient 360 assembly** — Joins latest triage + vitals + complaint + location

---

## 00 · Preflight Setup
Creates UC schema `workspace.careflow360`, volume `landing`, and landing directories for each event type. Idempotent with `IF NOT EXISTS`.

## 01 · Generate Sample Data
Generates 600 patients with deterministic synthetic data (fixed seed). 600 triage, 2,065 vitals, 1,385 ADT, 600 complaints. JSON format for Auto Loader.

## 02 · Kaggle Normalize
Normalizes external data to JSON schema expected by Bronze. Column mapping, type casting, null handling.

## 03 · Create Views
Creates 15 presentation views (4 bronze, 6 silver, 5 gold) pointing to pipeline tables. Strips layer prefix for clean names. Runs as job task after pipeline refresh.

## 04 · Train Triage ML Model
RandomForest (160 trees, depth 12, 87% accuracy). MLflow + UC registry with Champion alias. 600 batch predictions. Widget-parameterized for portability.

## 05 · Create Policy Documents
6 synthetic policies (ICU escalation, ED surge, bed assignment, discharge, data quality, synthetic data). Chunked for RAG. Delta CDF enabled.

## 06 · Validate
End-to-end validation: object existence, row counts, referential integrity, DQ checks across all layers.

## 07 · AI Functions
ai_classify ✅ (50 complaints), ai_extract v2.1 ✅ (20 extractions), ai_forecast ⚠️ (preview disabled), direct SQL ✅. All references use medallion views.

---

## Key Numbers
| Metric | Value |
|---|---|
| Pipeline tables | 17 |
| Presentation views | 15 |
| Patients | 600 |
| Vital events | 2,065 |
| ADT events | 1,385 |
| Live alerts | 74 |
| ML accuracy | ~87% |
| Genie benchmarks | 8/8 passed |
| Dashboard widgets | 10 |
