# Databricks notebook source
# DBTITLE 1,CareFlow360 — Every Notebook Explained
# MAGIC %md
# MAGIC # 📒 CareFlow360 — Every Notebook Explained
# MAGIC ## Detailed Guide to All Notebooks in the CareFlow360 Project
# MAGIC
# MAGIC **Last Updated**: October 10, 2026
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
# MAGIC | 0 | `00_preflight` | `core/src/notebooks/` | UC schema + volume setup |
# MAGIC | 1 | `01_generate_sample_data` | `core/src/notebooks/` | Synthetic patient data generator |
# MAGIC | 2 | `03_create_views` | `core/src/notebooks/` | Presentation layer views (FIXED Oct 10) |
# MAGIC | 3 | `04_train_triage_model` | `core/src/notebooks/` | ML training + MLflow + UC registration |
# MAGIC | 4 | `05_create_policy_docs` | `core/src/notebooks/` | Policy document chunking for RAG |
# MAGIC | 5 | `06_validate` | `core/src/notebooks/` | Runtime data validation |
# MAGIC | 6 | `07_ai_functions` | `~/` | AI function demonstrations |
# MAGIC | 7 | `00_setup` | `core/sql/` | Monitoring schema + quality rules setup |

# COMMAND ----------

# DBTITLE 1,Notebook 0: 00_preflight
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 0: `00_preflight` — Pre-Flight Setup
# MAGIC
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/src/notebooks/00_preflight`
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
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/src/notebooks/01_generate_sample_data`
# MAGIC **Role**: Generates realistic hospital event data for the pipeline
# MAGIC **Used by**: Job task `generate_daily_patients` (100 new patients/day)
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
# MAGIC - `num_patients=100, num_batches=2, reset_events=false`
# MAGIC - `seconds_between_batches=0`
# MAGIC
# MAGIC ### Key Code Patterns
# MAGIC - **Fixed random seed** for reproducibility
# MAGIC - **Realistic clinical distributions** (ESI weighted toward levels 3-4, vitals within normal ranges with some abnormalities)
# MAGIC - **JSON output** for Auto Loader compatibility
# MAGIC - **Append mode** (`reset_events=false`) for incremental data growth
# MAGIC
# MAGIC ### Data Volume
# MAGIC - 600 initial patients → ~4,650 events (triage 600, vitals 2,065, ADT 1,385, complaints 600)
# MAGIC - Growing 100 patients/day via the daily job

# COMMAND ----------

# DBTITLE 1,Notebook 2: 03_create_views
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 2: `03_create_views` — Presentation Layer Views
# MAGIC
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/src/notebooks/03_create_views`
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
# MAGIC ## Notebook 3: `04_train_triage_model` — ML Training & Registration
# MAGIC
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/src/notebooks/04_train_triage_model`
# MAGIC **Role**: Trains the RandomForest triage classifier, logs to MLflow, registers in UC, writes batch predictions
# MAGIC **Used by**: Job task `train_ml_model`
# MAGIC **Note**: The `schema` parameter was FIXED on Oct 10, 2026 — changed from `careflow360_gold` to `careflow360` (pipeline working schema where `gold_triage_training` table actually lives).
# MAGIC
# MAGIC ### What It Does
# MAGIC 1. **Data Loading** (Cell 4): Loads training data from `workspace.careflow360.gold_triage_training`, converts to pandas DataFrame, validates minimum 100 rows and 2+ classes
# MAGIC 2. **Feature Engineering** (Cell 4): Defines 12 numeric features (age, vitals, shock_index, etc.) and 3 categorical features (arrival_mode, arrival_day, sex)
# MAGIC 3. **Model Training** (Cell 4-5): Trains a scikit-learn Pipeline with ColumnTransformer (imputation + OneHotEncoder) and RandomForestClassifier (160 trees, max_depth=12, class_weight='balanced_subsample')
# MAGIC 4. **MLflow Logging** (Cell 5): Logs accuracy, f1_macro, precision_macro, recall_macro, train_rows, test_rows; logs model with input_example
# MAGIC 5. **UC Model Registration** (Cell 6): Registers model as `workspace.careflow360.triage_acuity_model` and sets `Champion` alias
# MAGIC 6. **Batch Scoring** (Cell 7): Loads the Champion model, scores all patients, writes predictions to `workspace.careflow360.gold_triage_predictions`
# MAGIC
# MAGIC ### Key Parameters (from job)
# MAGIC - `catalog=workspace, schema=careflow360` (FIXED Oct 10)
# MAGIC - `experiment_path` defaults to `/Workspace/Users/{user}/careflow360/triage_experiment`
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC - **scikit-learn Pipeline**: Preprocessing + model in a single pipeline for reproducibility
# MAGIC - **UC model registry**: Uses `databricks-uc` registry URI for Unity Catalog model management
# MAGIC - **Champion alias**: Latest model version gets the `Champion` alias for production reference
# MAGIC - **Batch scoring**: Writes predictions to a physical Delta table for dashboard consumption
# MAGIC - **Model metadata**: Logs data classification (synthetic) and clinical use (prohibited_demo_only) as params
# MAGIC
# MAGIC ### Performance
# MAGIC - ~87% accuracy on test set (25% holdout)
# MAGIC - Training takes ~30-40 seconds on serverless compute
# MAGIC - Total notebook execution: ~46 seconds (including MLflow logging + UC registration + batch scoring)

# COMMAND ----------

# DBTITLE 1,Notebook 4: 05_create_policy_docs
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 4: `05_create_policy_docs` — Policy Document Chunking for RAG
# MAGIC
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/src/notebooks/05_create_policy_docs`
# MAGIC **Role**: Creates and chunks hospital policy documents for RAG / AI Search
# MAGIC
# MAGIC ### What It Does
# MAGIC 1. Creates 6 synthetic hospital policies:
# MAGIC    - ICU Escalation Protocol
# MAGIC    - ED Surge Capacity Plan
# MAGIC    - Bed Assignment Rules
# MAGIC    - Discharge Criteria
# MAGIC    - Data Quality Standards
# MAGIC    - Synthetic Data Usage Policy
# MAGIC 2. Splits each policy into ~2-sentence chunks for RAG retrieval
# MAGIC 3. Stores chunks in `workspace.careflow360.gold_policy_chunks` with metadata:
# MAGIC    - `chunk_id`: `{doc_id}-C{chunk_number:02d}` format for traceability
# MAGIC    - `source`: Policy document name
# MAGIC    - `section`: Section within the policy
# MAGIC 4. Enables Delta Change Data Feed (CDF) for streaming sync to vector search indexes
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC - **Chunk size**: 2 sentences balances context and retrieval precision
# MAGIC - **Chunk ID format**: Traceable to source document and section
# MAGIC - **CDF enabled**: Supports incremental sync to AI Search vector indexes
# MAGIC - **Physical table** (not view): Policy chunks are generated, not derived from pipeline data
# MAGIC
# MAGIC ### Current Data
# MAGIC - 6 policy documents → 6 chunks in `gold_policy_chunks`

# COMMAND ----------

# DBTITLE 1,Notebook 5: 06_validate
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 5: `06_validate` — Runtime Data Validation
# MAGIC
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/src/notebooks/06_validate`
# MAGIC **Role**: Comprehensive runtime validation of pipeline data quality
# MAGIC
# MAGIC ### What It Does
# MAGIC Runs a series of validation checks across all pipeline tables:
# MAGIC
# MAGIC 1. **Table existence**: Verifies all expected tables exist in the pipeline schema
# MAGIC 2. **Minimum row counts**: Checks each table has at least the expected minimum rows
# MAGIC 3. **Schema validation**: Verifies required columns exist with correct types
# MAGIC 4. **ESI range validation**: Confirms triage acuity values are 1-5
# MAGIC 5. **Future timestamp check**: Ensures no event timestamps are in the future
# MAGIC 6. **ADT event type validation**: Validates event_type is one of admit/transfer/discharge
# MAGIC 7. **Clinical range validation**: Checks vital signs are within plausible clinical ranges
# MAGIC 8. **Null checks**: Identifies critical fields with NULL values
# MAGIC 9. **DQ blocking**: Raises an exception on critical failures (stops execution)
# MAGIC 10. **Logging**: Writes validation results to monitoring tables
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC - **Fail-fast on critical issues**: Raises exception if critical validations fail
# MAGIC - **Comprehensive coverage**: Validates schema, data ranges, business rules, and data quality
# MAGIC - **Logging**: Results logged to monitoring tables for historical tracking
# MAGIC - **Non-blocking warnings**: Non-critical issues are logged but don't stop execution

# COMMAND ----------

# DBTITLE 1,Notebook 6: 07_ai_functions
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 6: `07_ai_functions` — AI Function Demonstrations
# MAGIC
# MAGIC **Location**: `/Users/lokeshchandra.kaikala@gmail.com/07_ai_functions`
# MAGIC **Role**: Demonstrates Databricks SQL AI functions for healthcare use cases
# MAGIC
# MAGIC ### What It Does
# MAGIC Demonstrates four AI functions applicable to hospital operations:
# MAGIC
# MAGIC 1. **`ai_classify`** ✅ — Classifies 50 chief complaints into categories:
# MAGIC    - Cardiac, Respiratory, GI, Neuro, Trauma
# MAGIC    - Uses SQL: `SELECT ai_classify(complaint_text, ARRAY[...]) FROM ...`
# MAGIC
# MAGIC 2. **`ai_extract`** ✅ (v2.1) — Extracts structured fields from clinical text:
# MAGIC    - body_part, symptom, severity
# MAGIC    - Uses JSON schema format with `ai_extract(text, schema_json)`
# MAGIC
# MAGIC 3. **`ai_forecast`** ⚠️ — Predicts 12-hour admission volume:
# MAGIC    - Requires preview enablement (disabled in Free Edition)
# MAGIC    - Uses `TABLE()` syntax with timestamp horizon
# MAGIC
# MAGIC 4. **`ai_query`** ❌ → Direct SQL — Natural language to SQL:
# MAGIC    - AI endpoint required; replaced with direct SQL for Free Edition compatibility
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC - **Free Edition compatible**: Uses only available AI functions; documents limitations
# MAGIC - **Real healthcare use cases**: Classifications and extractions are clinically meaningful
# MAGIC - **Tested with real data**: 50 rows for classify, 20 rows for extract

# COMMAND ----------

# DBTITLE 1,Notebook 7: 00_setup & Pipeline Files Summary
# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 7: `00_setup` — Monitoring Schema & Quality Rules
# MAGIC
# MAGIC **Location**: `CareFlow360_Databricks_Free_Edition/core/sql/00_setup`
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
# MAGIC The CareFlow360 project contains **8 notebooks** (7 Python + 1 SQL) and **3 pipeline source files**, covering:
# MAGIC - Infrastructure setup (preflight, setup)
# MAGIC - Data generation (sample data)
# MAGIC - Pipeline processing (bronze, silver, gold source files)
# MAGIC - Presentation layer (create views)
# MAGIC - Machine learning (train triage model)
# MAGIC - RAG / AI Search (policy documents)
# MAGIC - Runtime validation (validate)
# MAGIC - AI functions (classify, extract, forecast, query)
# MAGIC
# MAGIC **All notebooks are accessible and the daily job runs all 4 tasks end-to-end successfully as of October 10, 2026.**