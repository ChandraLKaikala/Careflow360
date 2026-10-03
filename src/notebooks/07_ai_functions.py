# Databricks notebook source
# DBTITLE 1,AI Functions for Hospital Analytics
# MAGIC %md
# MAGIC # CareFlow360 AI Functions
# MAGIC Demonstrates Databricks SQL AI functions for hospital analytics:
# MAGIC - `ai_forecast` — Predict future patient volume by department
# MAGIC - `ai_query` — Natural-language questions over the data
# MAGIC - `ai_classify` — Classify chief complaints into categories
# MAGIC
# MAGIC **Note**: This is a synthetic/demo project. AI function outputs are not clinically validated.

# COMMAND ----------

# DBTITLE 1,Patient Volume Forecast
catalog = "workspace"
schema = "careflow360_gold"

# Prepare historical hourly patient volume for forecasting
from pyspark.sql import functions as F

hourly_volume = (spark.table(f"{catalog}.{schema}.department_hourly")
    .filter(F.col("event_type") == "A01_ADMIT")
    .groupBy("event_hour")
    .agg(F.sum("patient_count").alias("y"))
    .withColumnRenamed("event_hour", "ts")
    .orderBy("ts"))

# Save as a temp view for ai_forecast
hourly_volume.createOrReplaceTempView("hourly_admission_volume")
display(hourly_volume.limit(10))

# COMMAND ----------

# DBTITLE 1,Run ai_forecast
# MAGIC %sql
# MAGIC -- AI Forecast: Predict next 12 hours of admission volume
# MAGIC -- Uses Databricks ai_forecast SQL function (Version 1)
# MAGIC WITH forecast_input AS (
# MAGIC   SELECT ts, y FROM hourly_admission_volume
# MAGIC )
# MAGIC SELECT * FROM AI_FORECAST(
# MAGIC   TABLE(forecast_input),
# MAGIC   horizon => (SELECT MAX(ts) + INTERVAL 12 HOURS FROM hourly_admission_volume),
# MAGIC   time_col => 'ts',
# MAGIC   value_col => 'y',
# MAGIC   frequency => '1 hour',
# MAGIC   version => '1'
# MAGIC )
# MAGIC ORDER BY ts;

# COMMAND ----------

# DBTITLE 1,AI Classify Chief Complaints
# MAGIC %sql
# MAGIC -- AI Classify: Categorize chief complaints into clinical categories
# MAGIC -- Uses Databricks ai_classify SQL function
# MAGIC SELECT
# MAGIC   patient_id,
# MAGIC   chief_complaint_clean,
# MAGIC   AI_CLASSIFY(
# MAGIC     chief_complaint_clean,
# MAGIC     ARRAY('Cardiac', 'Respiratory', 'Gastrointestinal', 'Neurological', 'Trauma', 'Other')
# MAGIC   ) AS complaint_category
# MAGIC FROM workspace.careflow360_silver.chief_complaints
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,AI Query Natural Language
# MAGIC %sql
# MAGIC -- Natural-language questions over hospital data
# MAGIC -- ai_query requires a configured AI serving endpoint; here we answer with direct SQL
# MAGIC -- on workspace.careflow360_gold.patient_360
# MAGIC
# MAGIC -- Q1: How many patients are currently admitted in the ICU?
# MAGIC SELECT COUNT(*) AS icu_patient_count
# MAGIC FROM workspace.careflow360_gold.patient_360
# MAGIC WHERE is_currently_admitted AND current_department = 'ICU';
# MAGIC
# MAGIC -- Q2: What is the average triage acuity for admitted patients?
# MAGIC SELECT ROUND(AVG(triage_acuity), 2) AS avg_triage_acuity
# MAGIC FROM workspace.careflow360_gold.patient_360
# MAGIC WHERE is_currently_admitted;

# COMMAND ----------

# DBTITLE 1,AI Extract from Complaints
# MAGIC %sql
# MAGIC -- AI Extract: Pull structured data from chief complaint text
# MAGIC -- Uses Databricks ai_extract SQL function with JSON schema (v2.1)
# MAGIC SELECT
# MAGIC   patient_id,
# MAGIC   chief_complaint_clean,
# MAGIC   AI_EXTRACT(
# MAGIC     chief_complaint_clean,
# MAGIC     '{"body_part": {"type": "string"}, "symptom": {"type": "string"}, "severity_hint": {"type": "string"}}',
# MAGIC     MAP('version', '2.1')
# MAGIC   ) AS extracted_info
# MAGIC FROM workspace.careflow360_silver.chief_complaints
# MAGIC LIMIT 20;

# COMMAND ----------

