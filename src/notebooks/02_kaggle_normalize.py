# Databricks notebook source
# MAGIC %md
# MAGIC # Normalize user-downloaded Kaggle datasets into CareFlow360 events
# MAGIC Kaggle files are deliberately not bundled. Download them yourself under their licenses and upload to the paths documented in `docs/KAGGLE_DATA.md`.

# COMMAND ----------
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
def widget(name, default):
    try:
        dbutils.widgets.text(name, str(default), name)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return str(default)

catalog = widget("catalog", spark.sql("SELECT current_catalog() AS c").first()["c"])
schema = widget("schema", "careflow360_dev")
volume = widget("volume", "landing")
base = f"/Volumes/{catalog}/{schema}/{volume}"

triage_train = f"{base}/kaggle/triagegeist/train.csv"
chief_file = f"{base}/kaggle/triagegeist/chief_complaints.csv"
monitoring_file = f"{base}/kaggle/monitoring/healthcare_monitoring.csv"

# COMMAND ----------
def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        try:
            with open(path, "rb"):
                return True
        except Exception:
            return False


def clean_columns(df):
    out = df
    for c in df.columns:
        clean = re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
        out = out.withColumnRenamed(c, clean)
    return out

# COMMAND ----------
if exists(triage_train):
    triage = clean_columns(spark.read.option("header", True).option("inferSchema", True).csv(triage_train))
    required = {"patient_id", "triage_acuity"}
    missing = required - set(triage.columns)
    if missing:
        raise ValueError(f"Triagegeist train.csv is missing expected columns: {sorted(missing)}")

    # Expected Triagegeist columns are mapped directly; absent optional values become NULL.
    def col_or_null(name, dtype):
        return F.col(name).cast(dtype) if name in triage.columns else F.lit(None).cast(dtype)

    event_ts = F.expr("timestampadd(SECOND, pmod(xxhash64(CAST(patient_id AS STRING)), 14400), current_timestamp() - INTERVAL 4 HOURS)")
    normalized = triage.select(
        F.concat(F.lit("KAG-TRI-"), F.col("patient_id").cast("string")).alias("event_id"),
        F.col("patient_id").cast("string").alias("patient_id"),
        F.concat(F.lit("KAG-E-"), F.col("patient_id").cast("string")).alias("encounter_id"),
        event_ts.alias("event_ts"),
        col_or_null("arrival_mode", "string").alias("arrival_mode"),
        col_or_null("arrival_hour", "int").alias("arrival_hour"),
        col_or_null("arrival_day", "string").alias("arrival_day"),
        col_or_null("age", "int").alias("age"),
        col_or_null("sex", "string").alias("sex"),
        col_or_null("systolic_bp", "int").alias("systolic_bp"),
        col_or_null("diastolic_bp", "int").alias("diastolic_bp"),
        col_or_null("heart_rate", "int").alias("heart_rate"),
        col_or_null("respiratory_rate", "int").alias("respiratory_rate"),
        col_or_null("temperature_c", "double").alias("temperature_c"),
        col_or_null("spo2", "int").alias("spo2"),
        col_or_null("gcs_total", "int").alias("gcs_total"),
        col_or_null("pain_score", "int").alias("pain_score"),
        col_or_null("num_prior_visits", "int").alias("num_prior_visits"),
        F.col("triage_acuity").cast("int").alias("triage_acuity"),
        F.lit("kaggle_triagegeist").alias("source"),
    )
    normalized.repartition(20).write.mode("append").json(f"{base}/events/triage")
    print("Added Triagegeist rows:", normalized.count())
else:
    print("SKIP: Triagegeist train.csv not found at", triage_train)

# COMMAND ----------
if exists(chief_file):
    chief = clean_columns(spark.read.option("header", True).option("inferSchema", True).csv(chief_file))
    complaint_col = "chief_complaint_raw" if "chief_complaint_raw" in chief.columns else ("chief_complaint" if "chief_complaint" in chief.columns else None)
    if "patient_id" not in chief.columns or complaint_col is None:
        raise ValueError("chief_complaints.csv must contain patient_id and a chief complaint text column")

    chief_norm = chief.select(
        F.concat(F.lit("KAG-CC-"), F.col("patient_id").cast("string")).alias("event_id"),
        F.col("patient_id").cast("string").alias("patient_id"),
        F.concat(F.lit("KAG-E-"), F.col("patient_id").cast("string")).alias("encounter_id"),
        F.current_timestamp().alias("event_ts"),
        F.col(complaint_col).cast("string").alias("chief_complaint_raw"),
        F.lit("kaggle_triagegeist").alias("source"),
    )
    chief_norm.repartition(10).write.mode("append").json(f"{base}/events/chief_complaints")
    print("Added chief complaint rows:", chief_norm.count())
else:
    print("SKIP: chief_complaints.csv not found at", chief_file)

# COMMAND ----------
if exists(monitoring_file):
    monitor = clean_columns(spark.read.option("header", True).option("inferSchema", True).csv(monitoring_file))
    aliases = {
        "patient_id": ["patient_id", "patient_number", "patient_no", "patient"],
        "heart_rate": ["heart_rate", "hr"],
        "spo2": ["spo2", "sp_o2", "oxygen_saturation"],
        "systolic_bp": ["systolic_bp", "systolic_blood_pressure", "sbp"],
        "diastolic_bp": ["diastolic_bp", "diastolic_blood_pressure", "dbp"],
        "temperature_c": ["body_temp", "body_temperature", "temperature_c", "temperature"],
        "respiratory_rate": ["respiratory_rate", "resp_rate", "rr"],
        "fall_detected": ["fall_detection", "fall_detected"],
    }

    def pick(field):
        for candidate in aliases[field]:
            if candidate in monitor.columns:
                return candidate
        return None

    patient_col = pick("patient_id")
    if patient_col is None:
        raise ValueError(f"Monitoring CSV has no recognizable patient identifier. Columns: {monitor.columns}")

    def expr(field, dtype):
        c = pick(field)
        return F.col(c).cast(dtype) if c else F.lit(None).cast(dtype)

    mon = monitor.withColumn("_rid", F.monotonically_increasing_id())
    monitor_norm = mon.select(
        F.concat(F.lit("KAG-VIT-"), F.col(patient_col).cast("string"), F.lit("-"), F.col("_rid").cast("string")).alias("event_id"),
        F.col(patient_col).cast("string").alias("patient_id"),
        F.concat(F.lit("KAG-E-"), F.col(patient_col).cast("string")).alias("encounter_id"),
        F.expr("timestampadd(SECOND, pmod(_rid, 7200), current_timestamp() - INTERVAL 2 HOURS)").alias("event_ts"),
        expr("heart_rate", "int").alias("heart_rate"),
        expr("spo2", "int").alias("spo2"),
        expr("systolic_bp", "int").alias("systolic_bp"),
        expr("diastolic_bp", "int").alias("diastolic_bp"),
        expr("temperature_c", "double").alias("temperature_c"),
        expr("respiratory_rate", "int").alias("respiratory_rate"),
        expr("fall_detected", "boolean").alias("fall_detected"),
        F.lit("kaggle_healthcare_monitoring").alias("source"),
    )
    monitor_norm.repartition(25).write.mode("append").json(f"{base}/events/vitals")
    print("Added monitoring rows:", monitor_norm.count())
else:
    print("SKIP: monitoring CSV not found at", monitoring_file)

print("Done. Refresh the Lakeflow pipeline to ingest newly normalized files.")
