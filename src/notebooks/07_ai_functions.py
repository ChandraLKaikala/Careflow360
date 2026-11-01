# Databricks notebook source
# MAGIC %md
# MAGIC # CareFlow360 — Free Edition Analytics and Optional AI
# MAGIC Every *executable* cell below uses standard Spark SQL and requires no model-serving endpoint.
# MAGIC The baseline forecast and keyword groups are deterministic demos, **not** AI or clinical predictions.
# MAGIC Optional AI functions can be tried separately where the workspace grants access.
# MAGIC All examples use synthetic data. Do not use this project for clinical care.

# COMMAND ----------

# DBTITLE 1,Configuration and table contracts
import re
from pyspark.sql import functions as F


def widget(name, default):
    try:
        dbutils.widgets.text(name, str(default), name)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return str(default)


catalog = widget("catalog", spark.sql("SELECT current_catalog() AS catalog").first()[0])
gold_schema = widget("gold_schema", "careflow360_gold")
silver_schema = widget("silver_schema", "careflow360_silver")
for value in (catalog, gold_schema, silver_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")

flow_table = f"{catalog}.{gold_schema}.department_hourly"
patient_table = f"{catalog}.{gold_schema}.patient_360"
complaint_table = f"{catalog}.{silver_schema}.chief_complaints"
for name in (flow_table, patient_table, complaint_table):
    if not spark.catalog.tableExists(name):
        raise RuntimeError(f"Required presentation view not found: {name}")

# COMMAND ----------

# DBTITLE 1,Hourly admission volume (historical observations)
hourly_volume = (
    spark.table(flow_table)
    .filter(F.col("event_type") == F.lit("A01_ADMIT"))
    .groupBy("event_hour")
    .agg(F.sum("event_count").alias("admissions"))
    .select(F.col("event_hour").alias("ts"), "admissions")
    .orderBy("ts")
)
if hourly_volume.limit(1).count() == 0:
    raise RuntimeError("No hourly admission records available. Run the pipeline first.")
hourly_volume.createOrReplaceTempView("careflow_hourly_admissions")
display(hourly_volume.orderBy(F.col("ts").desc()).limit(24))

# COMMAND ----------

# DBTITLE 1,12-hour moving-average *baseline* forecast (no AI endpoint required)
# This produces a clearly labeled heuristic forecast, not a clinical forecast.
forecast = spark.sql("""
WITH last_hour AS (
  SELECT MAX(ts) AS anchor FROM careflow_hourly_admissions
), baseline AS (
  SELECT ROUND(AVG(admissions), 2) AS estimated_admissions_per_hour
  FROM careflow_hourly_admissions
  WHERE ts >= (SELECT anchor - INTERVAL 5 HOURS FROM last_hour)
), future_hours AS (
  SELECT EXPLODE(SEQUENCE(anchor + INTERVAL 1 HOUR,
                          anchor + INTERVAL 12 HOURS,
                          INTERVAL 1 HOUR)) AS forecast_hour
  FROM last_hour
)
SELECT f.forecast_hour, b.estimated_admissions_per_hour,
       '6-hour observed moving-average baseline (synthetic demo)' AS method
FROM future_hours f CROSS JOIN baseline b
ORDER BY f.forecast_hour
""")
display(forecast)

# COMMAND ----------

# DBTITLE 1,Aggregate chief complaint keyword categories
# This rule-based text categorization intentionally avoids patient-level output.
categories = spark.sql(f"""
WITH labeled AS (
  SELECT CASE
    WHEN LOWER(chief_complaint_clean) RLIKE 'chest|palpitation' THEN 'Cardiac-related keyword'
    WHEN LOWER(chief_complaint_clean) RLIKE 'cough|breath' THEN 'Respiratory keyword'
    WHEN LOWER(chief_complaint_clean) RLIKE 'abdominal|vomit|nausea' THEN 'Digestive keyword'
    WHEN LOWER(chief_complaint_clean) RLIKE 'headache|dizz' THEN 'Neurological keyword'
    WHEN LOWER(chief_complaint_clean) RLIKE 'injury|laceration|fall' THEN 'Injury keyword'
    ELSE 'Other keyword' END AS keyword_group
  FROM {complaint_table}
)
SELECT keyword_group, COUNT(*) AS complaint_records
FROM labeled
GROUP BY keyword_group
ORDER BY complaint_records DESC
""")
display(categories)

# COMMAND ----------

# DBTITLE 1,Executive summary from current Patient 360
kpis = spark.sql(f"""
SELECT COUNT(*) AS patient_360_rows,
       SUM(CASE WHEN is_currently_admitted THEN 1 ELSE 0 END) AS active_patients,
       SUM(CASE WHEN is_currently_admitted AND current_department='ICU'
                THEN 1 ELSE 0 END) AS icu_patients,
       ROUND(AVG(CASE WHEN is_currently_admitted THEN triage_acuity END), 2) AS avg_active_triage_acuity
FROM {patient_table}
""")
display(kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional advanced AI features
# MAGIC Databricks SQL `AI_CLASSIFY`, `AI_EXTRACT`, `AI_FORECAST` and `AI_QUERY`
# MAGIC may require workspace-specific feature access or a serving endpoint.
# MAGIC Enable them in a **separate** notebook after confirming availability.
# MAGIC Do not interpret missing permissions as a core pipeline failure.
# MAGIC Keep patient identifiers and row-level synthetic clinical observations out of published outputs.
