# Databricks notebook source
# MAGIC %md
# MAGIC # CareFlow360 v2.2 — Gold data quality & freshness
# MAGIC Read-only monitoring for a synthetic-data portfolio project. It produces aggregates only.
# MAGIC Run on serverless compute. No Genie, external endpoint, or additional job is required.

# COMMAND ----------

import re
from datetime import datetime, timezone
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


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")
    return value


catalog = identifier(widget("catalog", spark.sql("SELECT current_catalog()").first()[0]))
gold_schema = identifier(widget("gold_schema", "careflow360_gold"))
layout = widget("layout", "presentation").strip().lower()
if layout not in ("presentation", "unified"):
    raise ValueError("layout must be presentation or unified")
stale_hours = float(widget("stale_after_hours", "36"))
if stale_hours <= 0:
    raise ValueError("stale_after_hours must be > 0")
strict = widget("fail_on_quality_issues", "false").strip().lower() == "true"


def full_name(base):
    name = base if layout == "presentation" else f"gold_{base}"
    return f"`{catalog}`.`{gold_schema}`.`{name}`"


def read_table(base, required=True):
    path = full_name(base)
    if not spark.catalog.tableExists(path):
        if required:
            raise RuntimeError(f"Required Gold asset missing: {path}")
        print(f"SKIP optional Gold asset: {path}")
        return None
    return spark.table(path)


# COMMAND ----------

patient = read_table("patient_360")
snapshot = read_table("hospital_command_center")
flow = read_table("department_hourly")
alerts = read_table("live_alerts")
predictions = read_table("triage_predictions", required=False)

# Each result below is an aggregate. Do not display patient or encounter identifiers.
quality = patient.agg(
    F.count("*").alias("patient_rows"),
    F.countDistinct("patient_id").alias("distinct_patient_ids"),
    F.sum(F.when(F.col("patient_id").isNull(), 1).otherwise(0)).alias("missing_patient_ids"),
    F.sum(F.when(F.col("triage_acuity").isNull() | (~F.col("triage_acuity").between(1, 5)), 1).otherwise(0)).alias("invalid_triage_rows"),
    F.sum(F.when(F.col("is_currently_admitted") & F.col("current_department").isNull(), 1).otherwise(0)).alias("admitted_missing_department"),
).first().asDict()
print("PATIENT_360 QUALITY (aggregate only)")
for key, value in quality.items():
    print(f"  {key}: {value}")

problems = []
if quality["missing_patient_ids"]:
    problems.append("missing patient identifiers")
if quality["patient_rows"] != quality["distinct_patient_ids"]:
    problems.append("patient_360 is not one row per non-null patient")
if quality["invalid_triage_rows"]:
    problems.append("triage acuity missing or outside 1..5")
if quality["admitted_missing_department"]:
    problems.append("admitted patients without department")

# COMMAND ----------

snapshot_latest = snapshot.agg(F.max("snapshot_ts").alias("latest")).first()["latest"]
print("\nFRESHNESS")
if snapshot_latest is None:
    problems.append("no command-center snapshot")
    print("  Command center: no timestamp")
else:
    now_utc = datetime.now(timezone.utc)
    snapshot_utc = snapshot_latest.replace(tzinfo=timezone.utc) if snapshot_latest.tzinfo is None else snapshot_latest.astimezone(timezone.utc)
    age_hours = (now_utc - snapshot_utc).total_seconds() / 3600.0
    print(f"  KPI snapshot UTC: {snapshot_utc.isoformat()}")
    print(f"  Hours since snapshot: {age_hours:.1f}")
    if age_hours > stale_hours:
        problems.append(f"KPI snapshot older than {stale_hours:.0f} hours")

alert_stats = alerts.agg(F.count("*").alias("alert_rows"), F.max("event_ts").alias("last_alert_event")).first()
print(f"  Synthetic alert rows: {alert_stats['alert_rows']}")
print(f"  Last observed alert event (not a pipeline refresh time): {alert_stats['last_alert_event']}")

flow_rows = flow.groupBy("event_type").agg(F.sum("event_count").alias("events")).orderBy("event_type").collect()
print("\nPATIENT FLOW (event totals, not unique patients)")
for row in flow_rows:
    print(f"  {row['event_type']}: {row['events']}")

if predictions is not None:
    prediction_stats = predictions.agg(
        F.count("*").alias("prediction_rows"),
        F.sum(F.when(F.col("predicted_triage_acuity").isNull() | (~F.col("predicted_triage_acuity").between(1, 5)), 1).otherwise(0)).alias("invalid_prediction_labels"),
    ).first()
    print("\nBATCH MODEL MONITORING")
    print(f"  Prediction rows: {prediction_stats['prediction_rows']}")
    print(f"  Labels missing or outside 1..5: {prediction_stats['invalid_prediction_labels']}")
    if prediction_stats["invalid_prediction_labels"]:
        problems.append("predictions have invalid labels")

print("\nQUALITY SUMMARY")
if problems:
    for issue in problems:
        print(f"  WARN: {issue}")
    if strict:
        raise RuntimeError(f"CareFlow360 quality checks failed: {len(problems)} problem(s)")
else:
    print("  PASS: no issues detected by these checks")

print("NOTE: These checks are educational operational monitors, not clinical safety validation.")
