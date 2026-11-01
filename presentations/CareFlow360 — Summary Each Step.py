# Databricks notebook source
# DBTITLE 1,Step-by-Step Learning Summary
# MAGIC %md
# MAGIC # 📚 Step-by-Step Learning Summary
# MAGIC ## CareFlow360 Hospital Operations Analytics Platform
# MAGIC A detailed learning guide documenting insights from each step of building the CareFlow360 hospital analytics platform.
# MAGIC
# MAGIC **Table of Contents:** Step 0 (Preflight) → Step 1 (Sample Data) → Step 2 (Bronze) → Step 3 (Silver) → Step 4 (Gold) → Step 5 (ML) → Step 6 (RAG) → Step 7 (AI Functions) → Cross-Cutting Learnings → Production Best Practices

# COMMAND ----------

# DBTITLE 1,Step 0: Preflight Setup
# MAGIC %md
# MAGIC ## 🏗️ Step 0: Preflight Setup
# MAGIC
# MAGIC ### What Was Built
# MAGIC - UC schema `workspace.careflow360` for pipeline tables
# MAGIC - UC volume `workspace.careflow360.landing` for file ingestion
# MAGIC
# MAGIC ### Key Learning: Infrastructure as Code
# MAGIC - **Idempotent** — `CREATE SCHEMA IF NOT EXISTS` makes re-runnable
# MAGIC - **UC volumes over DBFS** — Governed, access-controlled, auditable
# MAGIC - **Single pipeline schema** — 17 tables in one schema, views in separate schemas for RBAC

# COMMAND ----------

# DBTITLE 1,Step 1: Sample Data Generation
# MAGIC %md
# MAGIC ## 📊 Step 1: Sample Data Generation
# MAGIC
# MAGIC ### What Was Built
# MAGIC - 600 synthetic patients with deterministic data (fixed seed)
# MAGIC - 4 event types: triage (600), vitals (2,065), ADT (1,385), chief complaints (600)
# MAGIC
# MAGIC ### Key Learning: Deterministic Synthetic Data
# MAGIC - **Fixed random seed** — Reproducible across runs
# MAGIC - **Realistic distributions** — Triage acuity follows typical ED patterns
# MAGIC - **Temporal consistency** — ADT events follow admit → transfer → discharge sequence
# MAGIC - **Clinical realism** — Vital signs include abnormal values for alert testing

# COMMAND ----------

# DBTITLE 1,Step 2: Bronze Layer Ingestion
# MAGIC %md
# MAGIC ## 🔄 Step 2: Bronze Layer Ingestion
# MAGIC
# MAGIC ### What Was Built
# MAGIC - 4 Bronze streaming tables via Auto Loader
# MAGIC - Explicit StructType schema enforcement
# MAGIC - DQ gates: expect_or_drop on critical fields
# MAGIC
# MAGIC ### Key Learning: The Bronze Layer Contract
# MAGIC - **Bronze = Immutable** — Never modify, update, or delete
# MAGIC - **Schema enforcement** — StructType prevents schema drift at ingest time
# MAGIC - **Auto Loader** — `cloudFiles()` handles incremental file discovery
# MAGIC - **Audit metadata** — `_ingest_ts` and `_source_file` on every record

# COMMAND ----------

# DBTITLE 1,Step 3: Silver Layer Cleansing
# MAGIC %md
# MAGIC ## ✨ Step 3: Silver Layer Cleansing
# MAGIC
# MAGIC ### What Was Built
# MAGIC - 6 Silver tables: triage, quarantine, vitals, adt, complaints, location history
# MAGIC - Derived columns: shock_index, pulse_pressure, operational_alert_flag
# MAGIC - SCD Type 2 via AUTO CDC for patient location
# MAGIC
# MAGIC ### Key Learning: Data Quality as a Product
# MAGIC
# MAGIC #### Quarantine, Don't Discard
# MAGIC Rejected records saved with `rejection_reason` for investigation — not silently dropped.
# MAGIC
# MAGIC #### Derived Clinical Metrics
# MAGIC | Metric | Formula | Clinical Meaning |
# MAGIC |--------|---------|------------------|
# MAGIC | shock_index | HR / systolic_bp | >0.8 = hemodynamic instability |
# MAGIC | pulse_pressure | systolic - diastolic | Wide = vascular stiffness |
# MAGIC | operational_alert_flag | HR/SpO2/BP/temp thresholds | Requires clinical attention |
# MAGIC
# MAGIC #### SCD Type 2 via AUTO CDC
# MAGIC Patient location changes tracked with `valid_from`, `valid_to`, `is_current` — no manual SCD2 code.

# COMMAND ----------

# DBTITLE 1,Step 4: Gold Layer Aggregation
# MAGIC %md
# MAGIC ## 📈 Step 4: Gold Layer Aggregation
# MAGIC
# MAGIC ### What Was Built
# MAGIC - 5 materialized views + 2 physical tables
# MAGIC - Patient 360, command center, department hourly, live alerts, training features
# MAGIC
# MAGIC ### Key Learning: The Gold Layer Pattern
# MAGIC
# MAGIC #### Latest-Per-Patient via Window Functions
# MAGIC `row_number() OVER (PARTITION BY patient_id ORDER BY event_ts DESC)` — gets the most recent record per patient.
# MAGIC
# MAGIC #### Patient 360 Assembly
# MAGIC Joins latest triage + latest vitals + latest complaint + current location = one row per patient.
# MAGIC
# MAGIC #### Command Center KPIs
# MAGIC Single row with `active_patients`, `ed_patients`, `icu_patients`, `operational_alert_patients` for dashboards.

# COMMAND ----------

# DBTITLE 1,Step 5: ML Model Training
# MAGIC %md
# MAGIC ## 🤖 Step 5: ML Model Training
# MAGIC
# MAGIC ### What Was Built
# MAGIC - RandomForest classifier (96 trees, patient-held-out evaluation (verify current MLflow metrics))
# MAGIC - MLflow tracking + UC model registry with Champion alias
# MAGIC - 600 batch-scored predictions
# MAGIC
# MAGIC ### Key Learning: Production ML on Databricks
# MAGIC
# MAGIC #### sklearn Pipeline Pattern
# MAGIC Preprocessing (imputation + one-hot encoding) + model wrapped in a single Pipeline for reproducibility.
# MAGIC
# MAGIC #### MLflow + UC Model Registry
# MAGIC `mlflow.set_experiment()` → `mlflow.sklearn.log_model()` → `mlflow.register_model()` → `set_registered_model_alias("Champion")`
# MAGIC
# MAGIC #### Batch Scoring (Free Edition Compatible)
# MAGIC Load model via `mlflow.pyfunc.load_model("models:/name@Champion")` and predict — no serving endpoint needed.

# COMMAND ----------

# DBTITLE 1,Step 6: RAG Policy Documents
# MAGIC %md
# MAGIC ## 📄 Step 6: RAG Policy Documents
# MAGIC
# MAGIC ### What Was Built
# MAGIC - 6 synthetic hospital policies (ICU escalation, ED surge, bed assignment, discharge, data quality, synthetic data)
# MAGIC - Chunked into ~2-sentence segments for RAG retrieval
# MAGIC - Delta CDF enabled for streaming RAG pipelines
# MAGIC
# MAGIC ### Key Learning: Document Chunking for RAG
# MAGIC - **Chunk size** — 2 sentences balances context and retrieval precision
# MAGIC - **Chunk ID format** — `{doc_id}-C{chunk_number:02d}` for traceability
# MAGIC - **Change Data Feed** — Enables incremental sync to vector search indexes

# COMMAND ----------

# DBTITLE 1,Step 7: AI Functions
# MAGIC %md
# MAGIC ## 🧠 Step 7: AI Functions
# MAGIC
# MAGIC ### What Was Built
# MAGIC - ai_classify: 50 complaints → Cardiac/Respiratory/GI/Neuro/Trauma
# MAGIC - ai_extract v2.1: body_part, symptom, severity from text
# MAGIC - ai_forecast: 12-hour admission volume (preview-gated)
# MAGIC - Direct SQL: Natural-language questions via SQL
# MAGIC
# MAGIC ### Key Learning: SQL AI Functions
# MAGIC - **ai_classify** ✅ — Classifies text into custom labels
# MAGIC - **ai_extract** ✅ — Uses JSON schema format (not named_struct) with v2.1
# MAGIC - **ai_forecast** ⚠️ — Requires preview enablement; uses TABLE() syntax with timestamp horizon
# MAGIC - **ai_query** ❌ — Requires AI endpoint; replaced with direct SQL for Free Edition

# COMMAND ----------

# DBTITLE 1,Cross-Cutting Learnings
# MAGIC %md
# MAGIC ## 🌐 Cross-Cutting Learnings
# MAGIC
# MAGIC ### Medallion Architecture
# MAGIC | Layer | Purpose | Who Accesses |
# MAGIC |-------|---------|--------------|
# MAGIC | Bronze | Raw, immutable, append-only | Data engineering only |
# MAGIC | Silver | Cleansed, deduplicated, enriched | Analysts, ML engineers |
# MAGIC | Gold | Business KPIs, ML features | All consumers, dashboards, Genie |
# MAGIC
# MAGIC ### Unity Catalog Governance
# MAGIC - **Schemas** — Separate by layer for RBAC
# MAGIC - **Tags** — domain=healthcare, layer=bronze/silver/gold, classification=raw/cleansed/analytics
# MAGIC - **Volumes** — Governed file storage for landing data
# MAGIC
# MAGIC ### SDP (Spark Declarative Pipelines)
# MAGIC - **Declarative** — Define what tables to create, not how
# MAGIC - **Streaming-first** — readStream + writeStream built in
# MAGIC - **Auto-refresh** — Materialized views update on pipeline refresh
# MAGIC - **Serverless** — No cluster management required
# MAGIC
# MAGIC ### Genie Space Configuration
# MAGIC - 8 text instructions (business terms, clinical thresholds)
# MAGIC - 5 SQL examples + 10 knowledge snippets
# MAGIC - 15 column descriptions + entity matching on 14 columns
# MAGIC - 8 benchmark questions (all passing)

# COMMAND ----------

# DBTITLE 1,Production Best Practices
# MAGIC %md
# MAGIC ## 🎯 Production Best Practices
# MAGIC
# MAGIC ### Data Engineering
# MAGIC ✅ Use Medallion architecture (Bronze/Silver/Gold)
# MAGIC ✅ Enforce schemas at ingest with StructType
# MAGIC ✅ Quarantine bad records, don't discard
# MAGIC ✅ Use watermarks for streaming deduplication
# MAGIC ✅ Add audit metadata on every Bronze record
# MAGIC ✅ Separate schemas by layer for access control
# MAGIC
# MAGIC ### ML
# MAGIC ✅ Use sklearn Pipeline for preprocessing + model
# MAGIC ✅ Log to MLflow for reproducibility
# MAGIC ✅ Register in UC model registry with aliases
# MAGIC ✅ Batch score for Free Edition compatibility
# MAGIC
# MAGIC ### Governance
# MAGIC ✅ Apply UC tags (domain, layer, classification)
# MAGIC ✅ Use UC volumes, not DBFS
# MAGIC ✅ Comment all schemas, tables, and views
# MAGIC ✅ Enable Delta CDF for change tracking
# MAGIC
# MAGIC ### Genie Space
# MAGIC ✅ Add text instructions for business terms
# MAGIC ✅ Add SQL examples for common patterns
# MAGIC ✅ Add knowledge snippets for joins and filters
# MAGIC ✅ Enable entity matching on categorical columns
# MAGIC ✅ Add column descriptions with business context
# MAGIC ✅ Run benchmarks to verify accuracy

# COMMAND ----------

# DBTITLE 1,Update: October 10, 2026 — Fixes & Status
# MAGIC %md
# MAGIC ## 📊 Update: October 10, 2026 — Fixes Applied & Project Status
# MAGIC
# MAGIC ### Daily Incremental Job — Three Fixes Applied
# MAGIC
# MAGIC The Daily Incremental Job (ID: `484367296292508`) had been failing every day since October 4, 2026 (7 consecutive failed runs). Three root causes were identified and fixed:
# MAGIC
# MAGIC #### Fix 1: Created Missing Notebook `03_create_views`
# MAGIC - **Problem**: The `create_presentation_views` task referenced notebook `03_create_views`, but the file existed only as `03_create_views.py` (a Python file, not a Databricks notebook)
# MAGIC - **Solution**: Created a proper Databricks notebook (ID: 1064656254159981) from the `.py` file content
# MAGIC - **Impact**: Task 3 now succeeds — creates bronze/silver/gold presentation views
# MAGIC
# MAGIC #### Fix 2: Corrected ML Training Schema Parameter
# MAGIC - **Problem**: The `train_ml_model` task used `schema=careflow360_gold` (presentation schema), but the training table `gold_triage_training` lives in the pipeline working schema `careflow360`
# MAGIC - **Solution**: Changed the `schema` parameter from `careflow360_gold` to `careflow360`
# MAGIC - **Impact**: Task 4 now finds the training table and successfully trains the model
# MAGIC
# MAGIC #### Fix 3: Created Missing MLflow Experiment Directory
# MAGIC - **Problem**: MLflow experiment path defaults to `.../careflow360/triage_experiment`, but the `careflow360` directory didn't exist in the workspace
# MAGIC - **Solution**: Created the missing workspace directory `/Users/YOUR_WORKSPACE_USER/careflow360`
# MAGIC - **Impact**: MLflow can now create the experiment and log training runs
# MAGIC
# MAGIC ### Verified Successful Run (Oct 10, 2026)
# MAGIC
# MAGIC | Task | Duration | Status |
# MAGIC | --- | --- | --- |
# MAGIC | generate_daily_patients | 39s | ✅ SUCCESS |
# MAGIC | refresh_pipeline | 383s | ✅ SUCCESS |
# MAGIC | create_presentation_views | 29s | ✅ SUCCESS |
# MAGIC | train_ml_model | ~46s | ✅ SUCCESS |
# MAGIC | **Total** | **~9 min** | **✅ SUCCESS** |
# MAGIC
# MAGIC ### Current Project Status: ✅ FULLY OPERATIONAL
# MAGIC
# MAGIC - **SDP Pipeline**: IDLE, all recent updates COMPLETED, serverless + photon
# MAGIC - **Daily Job**: Active schedule (2 AM UTC), all 4 tasks passing
# MAGIC - **Dashboard**: Published with 10 verified widgets
# MAGIC - **Genie Space**: Production-ready, 8/8 benchmarks passed
# MAGIC - **ML Model**: Trained, UC-registered with Champion alias, predictions written
# MAGIC - **DAB**: Fully deployed with all resources created
# MAGIC
# MAGIC ### New Notebook Added
# MAGIC - **CareFlow360 — Complete Project Presentation** (ID: 255196183767644) — Comprehensive 12-section presentation covering architecture, pipeline, job, ML/AI, dashboard, Genie, DAB, governance, AI functions, fixes, health, lessons, and file inventory.