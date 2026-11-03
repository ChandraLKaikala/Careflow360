# Databricks notebook source
# DBTITLE 1,CareFlow360 — Every Notebook Explained
# MAGIC %md
# MAGIC # 📒 CareFlow360 — Every Notebook Explained
# MAGIC ## Detailed Guide to All Notebooks in the CareFlow360 Project
# MAGIC
# MAGIC **Last Updated**: November 3, 2026
# MAGIC **Status**: ✅ FULLY OPERATIONAL — All fixes applied, daily job passing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This notebook provides a detailed explanation of every notebook in the CareFlow360 Hospital Operations Analytics Platform, including its purpose, key code patterns, and role in the overall architecture.
# MAGIC
# MAGIC ### Notebook Index
# MAGIC
# MAGIC | # | Notebook | Location | Purpose |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 0 | `00_preflight` | `src/notebooks/` | UC schema + volume setup |
# MAGIC | 1 | `01_generate_sample_data` | `src/notebooks/` | Synthetic patient data generator (random 50–150/run) |
# MAGIC | 2 | `03_create_views` | `src/notebooks/` | Presentation layer views (16 views) |
# MAGIC | 3 | `04_train_triage_model` | `src/notebooks/` | ML training + MLflow + UC registration |
# MAGIC | 4 | `05_create_policy_docs` | `src/notebooks/` | Policy document chunking for RAG |
# MAGIC | 5 | `06_validate` | `src/notebooks/` | Runtime data validation |
# MAGIC | 6 | `07_ai_functions` | `src/notebooks/` | AI function demonstrations |
# MAGIC | 7 | `00_setup` | `sql/` | Monitoring schema + quality rules setup |
# MAGIC | 8 | `08_upgrade_existing_daily_job` | `src/notebooks/` | In-place live-job upgrade helper (applied Nov 3) |
# MAGIC | 9 | `09_compare_free_edition_models` | `src/notebooks/` | Optional CPU model comparison |

# COMMAND ----------

# DBTITLE 1,Notebook 0: 00_preflight
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 0: `00_preflight` — Pre-Flight Setup
# MAGIC
# MAGIC **Location**: `src/notebooks/00_preflight.py`
# MAGIC **Role**: Infrastructure setup — runs before everything else
# MAGIC
# MAGIC ### What It Does
# MAGIC 1. Creates the UC schema `workspace.careflow360` if it doesn't exist
# MAGIC 2. Creates the UC volume `workspace.careflow360.landing` for file ingestion
# MAGIC 3. Verifies workspace permissions and catalog access
# MAGIC 4. Prints a summary of all created resources
# MAGIC
# MAGIC ### Key Code Pattern
# MAGIC ```python
# MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
# MAGIC spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
# MAGIC ```
# MAGIC
# MAGIC ### Design Decisions
# MAGIC - **Idempotent**: Uses `IF NOT EXISTS` so it can be re-run safely
# MAGIC - **UC-native**: Uses Unity Catalog schemas and volumes (not DBFS)
# MAGIC - **Widget-driven**: Accepts `catalog` and `schema` as widget parameters for flexibility

# COMMAND ----------

# DBTITLE 1,Notebook 1: 01_generate_sample_data
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 1: `01_generate_sample_data` — Synthetic Patient Data Generator
# MAGIC
# MAGIC **Location**: `src/notebooks/01_generate_sample_data.py`
# MAGIC **Role**: Generates realistic hospital event data for the pipeline
# MAGIC **Used by**: Job task `generate_sample_events` (live key `generate_daily_patients`) — random 50–150 patients per run
# MAGIC
# MAGIC ### What It Does
# MAGIC 1. Generates synthetic patients with realistic clinical attributes (age, sex, arrival mode)
# MAGIC 2. Creates 4 event types per patient:
# MAGIC    - **Triage**: ESI level 1-5, chief complaint, arrival mode
# MAGIC    - **Vitals**: BP, HR, temperature, SpO2, respiratory rate, GCS, pain score
# MAGIC    - **ADT**: Admit/transfer/discharge events with department and room
# MAGIC    - **Chief Complaints**: Free-text complaint with category
# MAGIC 3. Writes JSON files to the landing volume for Auto Loader ingestion
# MAGIC 4. Supports batch mode (one-time) and continuous mode (configurable interval)
# MAGIC
# MAGIC ### Key Parameters (from job)
# MAGIC - `catalog=workspace, schema=careflow360, volume=landing`
# MAGIC - `num_patients=random, num_batches=5, reset_events=false` (scheduled job)
# MAGIC - `seconds_between_batches=0`
# MAGIC
# MAGIC ### Key Code Patterns
# MAGIC - **Run-scoped identifiers** — scheduled runs generate a random 50–150 patients per run
# MAGIC - **Realistic clinical distributions** (ESI weighted toward levels 3-4, vitals within normal ranges with some abnormalities)
# MAGIC - **JSON output** for Auto Loader compatibility
# MAGIC - **Append mode** (`reset_events=false`) for incremental data growth
# MAGIC
# MAGIC ### Data Volume
# MAGIC - Initial seed run: 600 patients → ~4,650 events (triage 600, vitals 2,065, ADT 1,385, complaints 600)
# MAGIC - Live baseline (Nov 3, 2026): 1,000 unique patients, growing ~100/day (random 50–150 per run)

# COMMAND ----------

# DBTITLE 1,Notebook 2: 03_create_views
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 2: `03_create_views` — Presentation Layer Views
# MAGIC
# MAGIC **Location**: `src/notebooks/03_create_views.py`
# MAGIC **Role**: Creates views in bronze/silver/gold presentation schemas pointing to pipeline tables
# MAGIC **Used by**: Job task `create_presentation_views`
# MAGIC **Note**: This notebook was CREATED on Oct 10, 2026 — it previously existed only as a `.py` file, causing the job to fail with `RESOURCE_NOT_FOUND` for 7 consecutive days.
# MAGIC
# MAGIC ### What It Does
# MAGIC 1. Creates 3 presentation schemas if they don't exist:
# MAGIC    - `workspace.careflow360_bronze` — Bronze layer views
# MAGIC    - `workspace.careflow360_silver` — Silver layer views
# MAGIC    - `workspace.careflow360_gold` — Gold layer views
# MAGIC 2. Creates views that point to pipeline tables in `workspace.careflow360`:
# MAGIC    - 4 Bronze views (triage, vitals, adt, chief_complaints)
# MAGIC    - 6 Silver views (triage, triage_quarantine, vitals, adt, chief_complaints, patient_location_history)
# MAGIC    - 5 Gold views (patient_360, hospital_command_center, department_hourly, live_alerts, triage_training)
# MAGIC 3. Each view strips the layer prefix (e.g., `bronze_triage` → `triage`)
# MAGIC
# MAGIC ### Key Code Pattern
# MAGIC ```python
# MAGIC for t in bronze_tables:
# MAGIC     view_name = t.replace("bronze_", "")
# MAGIC     spark.sql(
# MAGIC         f"CREATE OR REPLACE VIEW {CATALOG}.{BRONZE_SCHEMA}.{view_name} "
# MAGIC         f"COMMENT 'Bronze view -> {CATALOG}.{PIPELINE_SCHEMA}.{t}' "
# MAGIC         f"AS SELECT * FROM {CATALOG}.{PIPELINE_SCHEMA}.{t}"
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC - **3-Schema Separation**: Pipeline working schema (`careflow360`) vs. presentation schemas (`careflow360_bronze/silver/gold`) for access control
# MAGIC - **View naming**: Strip layer prefix (e.g., `gold_patient_360` → `patient_360`) for cleaner consumer access
# MAGIC - **Idempotent**: Uses `CREATE OR REPLACE VIEW` so it can be re-run safely
# MAGIC - **Comments on every view**: Documents the source table for lineage

# COMMAND ----------

# DBTITLE 1,Notebook 3: 04_train_triage_model
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 3: `04_train_triage_model` — Resource-Bounded, Patient-Grouped ML
# MAGIC
# MAGIC **Location**: `src/notebooks/04_train_triage_model.py`
# MAGIC **Run type**: ML task of the daily workflow after Lakeflow and presentation views.
# MAGIC
# MAGIC ### Pipeline and Free Edition enhancements
# MAGIC 1. Reads the Gold `triage_training` presentation view (or unified-schema `gold_triage_training`).
# MAGIC 2. Uses a stable XXHASH64 order to cap the pandas training sample to 4,000 encounters by default, rather than crashing as historical encounters accumulate.
# MAGIC 3. Splits **by patient ID** with GroupShuffleSplit, preventing the same patient's different encounters from leaking between train and holdout.
# MAGIC 4. Fits a scikit-learn pipeline with imputation, OneHotEncoder and a 96-tree RandomForest (depth 12, maximum 2 CPU workers).
# MAGIC 5. Logs measured accuracy, balanced accuracy, macro-F1, macro-precision/recall, sample capping, distinct train/test patient counts and a majority-class baseline to MLflow.
# MAGIC 6. Registers a Unity Catalog model under the configured Gold schema and updates the `Champion` alias.
# MAGIC 7. Finds **previously unseen current patient IDs** with a Spark left-anti join, scores only their latest encounter, and inserts only new IDs using Delta MERGE; historical predictions remain untouched.
# MAGIC
# MAGIC **Expected physical prediction target (split layout)**: `workspace.careflow360_gold.triage_predictions`.
# MAGIC **Data**: Fabricated synthetic patient records only. This ML model is **not validated for clinical use**.
# MAGIC
# MAGIC ### Performance interpretation
# MAGIC Do not quote a permanent accuracy percentage. Read the actual held-out MLflow run and compare macro-F1 with the majority-class baseline. A patient-held-out estimate is not directly comparable to the earlier leaked encounter-level split.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Notebook 4: 05_create_policy_docs
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 4: `05_create_policy_docs` — Synthetic Policy Text
# MAGIC
# MAGIC **Location**: `src/notebooks/05_create_policy_docs.py`.
# MAGIC Generates six fabricated hospital-operations policy chunks and enables Delta Change Data Feed for the optional AI Search extension.
# MAGIC
# MAGIC **Physical output**: `workspace.careflow360_gold.policy_chunks` in split-schema DAB deployments; `gold_policy_chunks` in the unified quickstart.
# MAGIC Each chunk has a stable `chunk_id`, document metadata and source text. Do not treat this generated text as authoritative clinical or hospital policy.
# MAGIC AI Search is optional and requires an available workspace embedding endpoint and Free Edition quota.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Notebook 5: 06_validate
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 5: `06_validate` — Read-Only Runtime Acceptance Gates
# MAGIC
# MAGIC **Location**: `src/notebooks/06_validate.py`.
# MAGIC Checks that required Bronze, Silver and Gold assets exist and have expected nonempty outputs; quarantine/alert tables may legitimately be empty.
# MAGIC It checks triage acuities, one Patient 360 row per distinct patient, prediction label ranges and command-center KPI snapshot age.
# MAGIC In the scheduled job, it also receives `expected_run_id` and `expected_new_patients`, then **fails** unless that run's new patient IDs appear in Bronze triage, Silver triage, Gold Patient 360 and predictions.
# MAGIC These are engineering/synthetic data checks, not clinical verification or comprehensive health-data compliance controls.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Notebook 6: 07_ai_functions
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 6: `07_ai_functions` — Free Edition Analytics and Optional AI
# MAGIC
# MAGIC **Location**: `src/notebooks/07_ai_functions.py`.
# MAGIC The executable notebook uses ordinary Spark SQL: KPI rollups, a 12-hour moving-average baseline, and aggregate keyword groups for synthetic chief complaints.
# MAGIC These are deterministic demonstration heuristics, **not a foundation model, clinical classification, or validated forecasting**.
# MAGIC
# MAGIC Databricks SQL `AI_CLASSIFY`, `AI_EXTRACT`, `AI_FORECAST` and `AI_QUERY` are optional ideas only. They are not executed by the core scheduled job and may require unavailable permissions, supported AI endpoints, quotas or previews in Free Edition.
# MAGIC Do not present optional AI function availability or accuracy as verified until you actually run it in the specific Databricks workspace.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Notebook 7: 00_setup & Pipeline Files Summary
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 7: `00_setup` — Monitoring Schema & Quality Rules
# MAGIC
# MAGIC **Location**: `sql/00_setup.sql`
# MAGIC **Role**: Sets up monitoring infrastructure and seeds quality rules
# MAGIC **Language**: SQL notebook
# MAGIC
# MAGIC ### What It Does
# MAGIC 1. Creates monitoring and quality schemas:
# MAGIC    - `workspace.careflow360_monitoring` — Pipeline metrics and health
# MAGIC    - `workspace.careflow360_quality` — Data quality rules and failures
# MAGIC 2. Creates monitoring tables:
# MAGIC    - `pipeline_runs` — Run history and status
# MAGIC    - `data_quality_rules` — Metadata-driven quality rules
# MAGIC    - `alert_thresholds` — Configurable alert thresholds
# MAGIC    - `quality_failure_samples` — Sample of failed records for review
# MAGIC    - `table_metrics_history` — Historical row counts and freshness metrics
# MAGIC 3. Seeds 20 data quality rules across Bronze/Silver/Gold layers
# MAGIC 4. Seeds 10 alert thresholds for clinical and operational metrics
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC - **Metadata-driven**: Quality rules and thresholds stored as data (not hardcoded)
# MAGIC - **Historical tracking**: Metrics history enables trend analysis and SLA monitoring
# MAGIC - **Separation of concerns**: Monitoring schemas separate from pipeline and presentation schemas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Source Files (SDP)
# MAGIC
# MAGIC In addition to the notebooks above, the project includes 3 Python source files for the SDP pipeline:
# MAGIC
# MAGIC | File | Layer | Key Tables | Key Patterns |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `01_bronze.py` | Bronze | 4 streaming tables | Auto Loader, StructType schema, expect_or_drop |
# MAGIC | `02_silver.py` | Silver | 6 tables + quarantine | Dedup, expect_or_quarantine, SCD2 (AUTO CDC), derived columns |
# MAGIC | `03_gold.py` | Gold | 5 materialized views + 2 physical | Aggregations, joins, ML features, RAG chunks |
# MAGIC
# MAGIC These files are deployed via DAB to the pipeline's deployment directory and executed by the SDP pipeline engine.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Summary
# MAGIC
# MAGIC The CareFlow360 project contains **10 notebooks** (9 Python + 1 SQL) and **3 pipeline source files**, covering:
# MAGIC - Infrastructure setup (preflight, setup)
# MAGIC - Data generation (sample data)
# MAGIC - Pipeline processing (bronze, silver, gold source files)
# MAGIC - Presentation layer (create views)
# MAGIC - Machine learning (train triage model)
# MAGIC - RAG / AI Search (policy documents)
# MAGIC - Runtime validation (validate)
# MAGIC - AI functions (classify, extract, forecast, query)
# MAGIC
# MAGIC **All notebooks are accessible and the daily job runs all 6 tasks end-to-end (random 50–150 patients per run) successfully as of November 3, 2026.**