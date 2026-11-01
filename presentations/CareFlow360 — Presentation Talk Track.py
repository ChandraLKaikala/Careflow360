# Databricks notebook source
# DBTITLE 1,CareFlow360 Presentation Talk Track
# MAGIC %md
# MAGIC # 🎤 CareFlow360 — Presentation Talk Track
# MAGIC ## Exact Script with Notebook & Cell References
# MAGIC
# MAGIC **How to use this notebook**: Each section below is a spoken script. The brackets like `[01_generate_sample_data, Cell 3]` tell you which notebook to open and which cell to point at while reading that paragraph. Navigate to the referenced cell, then read the text aloud.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 1: Introduction & Project Overview
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 1]**
# MAGIC
# MAGIC "Welcome everyone. Today I'll be walking you through CareFlow360, a complete hospital operations analytics platform that I built on Databricks. This is a production-ready system that ingests synthetic hospital event data — admissions, triage assessments, vital signs, and chief complaints — processes it through a medallion architecture, trains a machine learning model for triage prediction, and serves it all through an AI-powered dashboard and a Genie Q&A agent. The entire project is deployed using Declarative Automation Bundles for full reproducibility."
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 2]**
# MAGIC
# MAGIC "Let me start with a quick summary of what's been achieved. The platform has an SDP pipeline with 17 tables across Bronze, Silver, and Gold layers, running on serverless compute with Photon acceleration. There's a daily incremental job that generates new patients, refreshes the pipeline, creates presentation views, and retrains the ML model — all running automatically at 2 AM UTC. The ML model is a RandomForest classifier achieving about 87% accuracy on triage acuity prediction, registered in Unity Catalog with a Champion alias. The AI/BI dashboard has 10 verified widgets, and the Genie space passed all 8 benchmark questions."
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 3]**
# MAGIC
# MAGIC "Here's the high-level architecture. Data flows from left to right: JSON files land in a Unity Catalog volume, Auto Loader ingests them into Bronze streaming tables, the Silver layer cleanses and enriches the data with derived clinical columns, and the Gold layer creates materialized views for analytics and ML. On top of the pipeline, there are three presentation schemas with views for access control, and consumers include the dashboard, the Genie space, and the ML model. The entire flow is orchestrated by the daily incremental job."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 2: Data Generation
# MAGIC
# MAGIC **[01_generate_sample_data, Cell 1]**
# MAGIC
# MAGIC "Let's start at the very beginning — data generation. This notebook creates synthetic hospital events that mimic real emergency department operations. The data is intentionally not clinically validated — it's for demonstrating the pipeline architecture."
# MAGIC
# MAGIC **[01_generate_sample_data, Cell 3]**
# MAGIC
# MAGIC "Here in cell 3, you can see the widget parameters that control the data generation. The job passes catalog=workspace, schema=careflow360, volume=landing, num_patients=100, and critically, reset_events=false — meaning each daily run appends 100 new patients rather than starting fresh. This gives us continuous data growth. The code also validates all inputs for safety — it checks that catalog, schema, and volume names match a safe identifier pattern, and that num_patients is between 50 and 100,000."
# MAGIC
# MAGIC **[01_generate_sample_data, Cell 4]**
# MAGIC
# MAGIC "In cell 4, this is where the clinical realism comes in. The `synthetic_triage` function generates vital signs that correlate with the triage acuity level. For example, an ESI level 1 patient — the most critical — gets a heart rate between 120 and 155, SpO2 between 82 and 91, and systolic BP between 78 and 95. An ESI level 4 patient gets much more normal values. We also define the departments — Emergency, Observation, Medical, Surgical, ICU, and Telemetry — and arrival modes weighted toward walk-ins at 58%."
# MAGIC
# MAGIC **[01_generate_sample_data, Cell 5]**
# MAGIC
# MAGIC "Cell 5 is the main generation loop. For each patient, it creates four events: a triage assessment, a set of vital signs, an ADT event for admission, and a chief complaint. Each event gets a unique event_id, patient_id, encounter_id, and timestamp. The events are written as JSON lines to the landing volume, organized by event type in subdirectories — triage, vitals, adt, and chief_complaints. This JSON structure is what Auto Loader picks up in the next stage."

# COMMAND ----------

# DBTITLE 1,Bronze, Silver & Gold Pipeline Layers
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Part 3: Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **[01_bronze.py, Lines 39-60]**
# MAGIC
# MAGIC "Now let's look at the pipeline itself. This is the Bronze layer source file. Here on lines 39 through 60, you can see the triage schema defined as an explicit StructType. This is important — we're not using schema inference. Every field is explicitly typed: event_id as string, patient_id as string, event_ts as timestamp, systolic_bp as integer, temperature_c as double, and so on. This enforces a contract at ingest time — if a JSON file has a field with the wrong type, it gets handled according to our data quality rules rather than silently corrupting the schema."
# MAGIC
# MAGIC **[01_bronze.py, Lines 99-110]**
# MAGIC
# MAGIC "Lines 99 through 110 define the `autoload` function. This is the heart of the Bronze layer. It uses `spark.readStream.format('cloudFiles')` — that's Auto Loader. Key options: `cloudFiles.format` is set to JSON, and `cloudFiles.schemaEvolutionMode` is set to 'none' — meaning we explicitly prevent schema drift. If a new field appears in the JSON, it won't automatically be added to the table. The function also adds two metadata columns: `_ingest_ts` for the ingestion timestamp, and `_source_file` from the `_metadata.file_path` built-in column, so we always know which file each record came from."
# MAGIC
# MAGIC **[01_bronze.py, Lines 117-138]**
# MAGIC
# MAGIC "Lines 117 through 138 define all four Bronze streaming tables. Each one uses the `@dp.table` decorator from the Spark Declarative Pipelines API and the `@dp.expect_or_drop` decorator for data quality. For example, bronze_triage has `expect_or_drop` on 'triage_has_ids' — it requires that event_id, patient_id, and event_ts are all non-null. If any of these are null, the record is silently dropped. The same pattern applies to vitals, ADT, and chief_complaints. This is our first line of defense — records missing critical identifiers never enter the pipeline."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 4: Silver Layer — Cleansing & Enrichment
# MAGIC
# MAGIC **[02_silver.py, Lines 30-46]**
# MAGIC
# MAGIC "Moving to the Silver layer. Lines 30 through 46 define silver_triage. The first thing you'll notice is the watermark — `withWatermark('event_ts', '1 day')` — this tells Spark how long to wait for late-arriving data before considering a window closed. Then we deduplicate using `dropDuplicates(['event_id'])`. After that, we derive clinical columns: `shock_index` is heart_rate divided by systolic_bp — a real clinical metric used to identify patients at risk of shock. `pulse_pressure` is systolic minus diastolic. We also add `data_quality_status` set to 'accepted'. The `expect_or_drop` decorators validate that triage_acuity is between 1 and 5 and age is between 0 and 120."
# MAGIC
# MAGIC **[02_silver.py, Lines 53-81]**
# MAGIC
# MAGIC "Lines 53 through 81 are particularly interesting — this is the quarantine pattern. Instead of silently dropping invalid records, we create a separate materialized view called `silver_triage_quarantine`. This table captures any triage record where event_id is null, patient_id is null, triage_acuity is outside 1-5, or age is outside 0-120. Crucially, each quarantined record gets a `rejection_reason` column — using a chain of `when` conditions to identify exactly why it was rejected. This is a production best practice — you never want to silently lose data. The quarantine table lets data engineers investigate and fix root causes."
# MAGIC
# MAGIC **[02_silver.py, Lines 88-109]**
# MAGIC
# MAGIC "Lines 88 through 109 define silver_vitals. After deduplication, we add the `operational_alert_flag` — a boolean that's true when SpO2 is below 90, heart rate is above 130 or below 45, systolic BP is below 85, temperature is above 39.5 degrees, or a fall was detected. These are synthetic operational thresholds for the demo, not clinical alerts. But the pattern demonstrates how you'd implement real-time patient monitoring."
# MAGIC
# MAGIC **[02_silver.py, Lines 147-160]**
# MAGIC
# MAGIC "Lines 147 through 160 are one of the most advanced features — AUTO CDC with SCD Type 2. `dp.create_auto_cdc_flow` automatically tracks changes to patient location over time. When a patient moves from the ED to ICU, the SCD Type 2 mechanism creates a new row with a new `__START_AT` timestamp and sets the `__END_AT` on the previous row. This gives us a complete historical timeline of every patient's location throughout their stay. The keys are patient_id and encounter_id, and it's sequenced by event_ts."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 5: Gold Layer — Analytics & ML Features
# MAGIC
# MAGIC **[03_gold.py, Lines 33-38]**
# MAGIC
# MAGIC "In the Gold layer, lines 33 through 38 define a helper function called `latest_per_patient`. This uses a window function — `row_number()` partitioned by patient_id and ordered by event_ts descending — to get only the most recent record for each patient. This is used across multiple Gold tables to build the Patient 360 view."
# MAGIC
# MAGIC **[03_gold.py, Lines 45-91]**
# MAGIC
# MAGIC "Lines 45 through 91 define `gold_patient_360` — the comprehensive patient view. It joins the latest triage, vitals, chief complaints, and SCD2 location history into a single materialized view. The location join specifically looks for records where `__END_AT` is null — meaning the patient's current, active location. The final column `is_currently_admitted` is true when the latest ADT event is not a discharge. This table is what the dashboard and Genie space primarily consume."
# MAGIC
# MAGIC **[03_gold.py, Lines 98-107]**
# MAGIC
# MAGIC "Lines 98 through 107 define `gold_hospital_command_center` — a single-row KPI snapshot. It aggregates patient_360 into five metrics: active_patients count, ED patients, ICU patients, operational alert patients, and average triage acuity for active patients. This is what powers the five KPI counters on the dashboard."
# MAGIC
# MAGIC **[03_gold.py, Lines 146-153]**
# MAGIC
# MAGIC "Finally, lines 146 through 153 define `gold_triage_training` — the ML feature table. It selects 15 features from silver_triage: 12 numeric features including age, all vital signs, gcs_total, pain_score, num_prior_visits, arrival_hour, and shock_index, plus 3 categorical features: arrival_mode, arrival_day, and sex. The target variable is triage_acuity. Records with null triage_acuity are filtered out. This is the table that the ML training notebook reads from."

# COMMAND ----------

# DBTITLE 1,Presentation Views, ML, Dashboard & Genie
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Part 6: Presentation Layer Views
# MAGIC
# MAGIC **[03_create_views, Cell 1]**
# MAGIC
# MAGIC "After the pipeline refreshes, this notebook creates the presentation layer. The key design decision here is the 3-schema separation. The pipeline writes to `workspace.careflow360` — that's the working schema. But consumers shouldn't access that directly. Instead, this notebook creates three separate schemas: `careflow360_bronze`, `careflow360_silver`, and `careflow360_gold` — each with views pointing to the pipeline tables. This gives us access control — data engineers have write access to the pipeline schema, while analysts and dashboard consumers get read access only to the presentation schemas. You can see the pattern: for each pipeline table like `bronze_triage`, we create a view called just `triage` in the bronze presentation schema. The `gold_` prefix is stripped from gold tables, so `gold_patient_360` becomes `patient_360` in the gold schema."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 7: ML Model Training
# MAGIC
# MAGIC **[04_train_triage_model, Cell 1]**
# MAGIC
# MAGIC "Now let's look at the machine learning pipeline. This notebook trains a RandomForest classifier to predict triage acuity — the ESI level from 1 to 5. I want to emphasize: this is a portfolio demonstration model on synthetic data, not a clinically validated model."
# MAGIC
# MAGIC **[04_train_triage_model, Cell 3]**
# MAGIC
# MAGIC "Cell 3 sets up the parameters. The job passes catalog=workspace and schema=careflow360 — that's the pipeline working schema where `gold_triage_training` lives. The experiment path defaults to a workspace directory under the user's home folder. The model name is `triage_acuity_model`, and the full registered name in Unity Catalog becomes `workspace.careflow360.triage_acuity_model`."
# MAGIC
# MAGIC **[04_train_triage_model, Cell 4]**
# MAGIC
# MAGIC "Cell 4 is where the training happens. First, we load the training data from `gold_triage_training` and convert to pandas. We validate that we have at least 100 rows and at least 2 classes. Then we define 12 numeric features and 3 categorical features — the same ones from the Gold training table. We do a train-test split with 25% test size and stratification. The preprocessing pipeline uses `SimpleImputer` with median strategy for numeric features and most-frequent for categorical, followed by `OneHotEncoder` with `handle_unknown='ignore'`. The model itself is a `RandomForestClassifier` with 160 trees, max depth of 12, min_samples_leaf of 2, and `class_weight='balanced_subsample'` to handle class imbalance."
# MAGIC
# MAGIC **[04_train_triage_model, Cell 5]**
# MAGIC
# MAGIC "Cell 5 is the MLflow logging. We set the experiment, set the registry URI to `databricks-uc` for Unity Catalog, then start a run named 'careflow360-random-forest'. Inside the run, we fit the pipeline, predict on the test set, and log five metrics: accuracy, f1_macro, precision_macro, recall_macro, train_rows, and test_rows. We also log parameters including the model type, number of estimators, max depth, and importantly, `data_classification=synthetic_or_public` and `clinical_use=prohibited_demo_only` — these metadata tags ensure anyone using the model knows its limitations."
# MAGIC
# MAGIC **[04_train_triage_model, Cell 6]**
# MAGIC
# MAGIC "Cell 6 registers the model in Unity Catalog. We use `mlflow.register_model` with the run ID, then set the `Champion` alias using `MlflowClient.set_registered_model_alias`. The Champion alias means this is the production version — any downstream consumer loading `models:/workspace.careflow360.triage_acuity_model@Champion` gets this version."
# MAGIC
# MAGIC **[04_train_triage_model, Cell 7]**
# MAGIC
# MAGIC "Cell 7 does batch scoring. We load the Champion model using `mlflow.pyfunc.load_model`, score all patients in the training set, and write predictions to `gold_triage_predictions` as a physical Delta table. This table is what the dashboard's ML Predictions widget reads from. The whole notebook takes about 46 seconds to run on serverless compute."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 8: Dashboard & Genie Space
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 7]**
# MAGIC
# MAGIC "The dashboard is an AI/BI Lakeview dashboard called 'CareFlow360 Hospital Command Center'. It has 10 widgets across 6 SQL datasets, all reading from the `careflow360_gold` presentation schema. The top row has 5 KPI counters: Active Patients showing 51, ED Census at 10, ICU Census at 8, Active Alerts at 9, and Average Acuity at 3.29. Below that, there's a department census bar chart showing patients across 6 departments, a triage acuity pie chart with 5 ESI levels, a patient flow line chart, a live alerts table with 50 rows, and an ML predictions bar chart comparing predicted vs actual acuity levels."
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 8]**
# MAGIC
# MAGIC "The Genie space is called 'CareFlow360 Hospital Operations Agent'. It's configured with 8 text instructions covering business terms like the ESI triage scale, department names, alert flags, and event types. It has 5 SQL example queries, 10 knowledge snippets for joins and filters, 15 column descriptions on the patient_360 table, and entity matching on 14 columns across 5 tables. All 8 benchmark questions passed — meaning the Genie can accurately answer questions like 'How many patients are currently in the ED?' or 'Show me all patients with abnormal vital signs.'"

# COMMAND ----------

# DBTITLE 1,DAB, Fixes, Lessons & Closing
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Part 9: DAB Deployment & Daily Job
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 6]**
# MAGIC
# MAGIC "The entire project is deployed using Declarative Automation Bundles, or DAB. The DAB configuration is split across YAML files: `01_uc.yml` defines the 4 Unity Catalog schemas, `02_pipeline.yml` defines the SDP pipeline, and `03_jobs.yml` defines the daily incremental job. The root `databricks.yml` ties it all together. This means the entire project — schemas, pipeline, job, volumes — can be deployed with a single `bundle deploy` command, making it fully reproducible."
# MAGIC
# MAGIC "The daily job runs at 2 AM UTC and has 4 sequential tasks. Task 1, `generate_daily_patients`, generates 100 new patients and writes JSON to the landing volume — it takes about 39 seconds. Task 2, `refresh_pipeline`, triggers the SDP pipeline to process the new data through Bronze, Silver, and Gold — this takes about 6 minutes. Task 3, `create_presentation_views`, updates the views in the three presentation schemas — 29 seconds. And Task 4, `train_ml_model`, retrains the RandomForest model with the growing dataset and writes fresh predictions — about 46 seconds. Total runtime is about 9 minutes."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 10: Fixes Applied & Lessons Learned
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 10]**
# MAGIC
# MAGIC "I want to be transparent about three issues I found and fixed on October 10th. The daily job had been failing for 7 consecutive days, and there were three root causes."
# MAGIC
# MAGIC "Fix number 1: The `create_presentation_views` task referenced a notebook called `03_create_views`, but that file existed only as a Python file — not as a Databricks notebook. The job's notebook task requires a proper notebook, so it couldn't find it and returned RESOURCE_NOT_FOUND. I created a proper Databricks notebook from the Python file content."
# MAGIC
# MAGIC "Fix number 2: The `train_ml_model` task was using `schema=careflow360_gold` — the presentation schema. But the actual training table `gold_triage_training` lives in the pipeline working schema `careflow360`, not the presentation schema. The presentation view in `careflow360_gold` is named `triage_training` without the `gold_` prefix. So the notebook couldn't find the table. I changed the parameter to `schema=careflow360`."
# MAGIC
# MAGIC "Fix number 3: The MLflow experiment path defaults to a workspace directory called `careflow360` under the user's home folder, but that directory didn't exist. MLflow couldn't create the experiment. I created the missing directory."
# MAGIC
# MAGIC "After all three fixes, the job completed successfully — all 4 tasks passed in about 9 minutes total."
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 11]**
# MAGIC
# MAGIC "The key lessons here: First, when a DAB project includes Python files in the notebooks directory, they must be converted to proper Databricks notebooks if referenced by a job task. Second, pipeline working schemas and presentation schemas use different naming conventions — pipeline tables keep their `gold_` prefix, presentation views strip it. Job parameters must reference the correct schema. Third, MLflow workspace experiments require the parent directory to exist — always verify directory paths in default widget values. And fourth, using `repair-run` instead of `run-now` saved time by only re-running the failed task while preserving the successful ones."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Part 11: Closing & Q&A Prep
# MAGIC
# MAGIC **[CareFlow360 — Complete Project Presentation, Cell 12]**
# MAGIC
# MAGIC "To summarize, CareFlow360 demonstrates a complete, production-ready hospital analytics platform on Databricks Free Edition. It covers the full medallion architecture with 3-schema separation for governance, a serverless SDP pipeline with Auto Loader and SCD Type 2, a daily incremental job for continuous data growth and ML retraining, an AI/BI dashboard with 10 verified widgets, a Genie space with 8 out of 8 benchmarks passing, a UC-registered ML model with MLflow tracking, a DAB deployment for full reproducibility, and AI functions for classification, extraction, and querying. The platform is fully operational as of October 10, 2026, with a daily schedule that runs all 4 tasks end-to-end without intervention."
# MAGIC
# MAGIC "If you want to explore further, I've created three companion notebooks: the 'Complete Project Presentation' notebook has the full 16-section overview, the 'Summary Each Step' notebook has step-by-step learning notes for each component, and the 'Every Notebook Explained' notebook details every notebook's purpose, code patterns, and design decisions. Thank you."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quick Reference: All Referenced Notebooks & Files
# MAGIC
# MAGIC | Reference | Type | ID / Location |
# MAGIC | --- | --- | --- |
# MAGIC | `01_generate_sample_data` | Notebook | ID 1526984651564664, 5 cells |
# MAGIC | `01_bronze.py` | Pipeline source | `core/src/pipelines/01_bronze.py`, 139 lines |
# MAGIC | `02_silver.py` | Pipeline source | `core/src/pipelines/02_silver.py`, 161 lines |
# MAGIC | `03_gold.py` | Pipeline source | `core/src/pipelines/03_gold.py`, 154 lines |
# MAGIC | `03_create_views` | Notebook | ID 1064656254159981, 1 cell |
# MAGIC | `04_train_triage_model` | Notebook | ID 1526984651564665, 7 cells |
# MAGIC | `CareFlow360 — Complete Project Presentation` | Notebook | ID 255196183767644, 12 cells |
# MAGIC | `CareFlow360 — Summary Each Step` | Notebook | ID 1018675845605347, 12 cells |
# MAGIC | `CareFlow360 — Every Notebook Explained` | Notebook | ID 1018675845605346, 9 cells |
# MAGIC | `CareFlow360 Daily Incremental Pipeline` | Job | ID 484367296292508 |
# MAGIC | `CareFlow360_DAB_Pipeline` | SDP Pipeline | ID dde7c33b-8715-4666-8f6b-ad2af0848966 |
# MAGIC | `CareFlow360 Hospital Command Center` | Dashboard | ID 01f1beeb451e10439c0b4bf6aa8eeb70 |
# MAGIC | `CareFlow360 Hospital Operations Agent` | Genie Space | ID 01f103e12eb71094aee3ca8818a5cc87 |

# COMMAND ----------

