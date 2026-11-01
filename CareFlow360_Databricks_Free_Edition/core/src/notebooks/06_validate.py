# Databricks notebook source
# MAGIC %md
# MAGIC # CareFlow360 — Deployment validation
# MAGIC Read-only aggregate checks. This is a synthetic portfolio project, not a clinical system.

# COMMAND ----------

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


def safe_identifier(raw):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw):
        raise ValueError(f"Invalid Unity Catalog identifier: {raw!r}")
    return raw


catalog = safe_identifier(widget("catalog", spark.sql("SELECT current_catalog() AS c").first()["c"]))
schema = safe_identifier(widget("schema", "careflow360_dev"))
bronze_schema = safe_identifier(widget("bronze_schema", "careflow360_bronze"))
silver_schema = safe_identifier(widget("silver_schema", "careflow360_silver"))
gold_schema = safe_identifier(widget("gold_schema", "careflow360_gold"))
layout = widget("layout", "auto").lower()
max_snapshot_age_hours = float(widget("max_snapshot_age_hours", "36"))
if layout not in {"auto", "split", "unified"}:
    raise ValueError("layout must be auto, split, or unified")
if max_snapshot_age_hours <= 0:
    raise ValueError("max_snapshot_age_hours must be positive")


def table(schema_name, name):
    return f"{catalog}.{schema_name}.{name}"


# Auto-detect the presentation views that the DAB creates.
if layout == "auto":
    layout = "split" if spark.catalog.tableExists(table(gold_schema, "patient_360")) else "unified"

bronze = ["triage", "vitals", "adt", "chief_complaints"]
silver = ["triage", "triage_quarantine", "vitals", "adt", "chief_complaints", "patient_location_history"]
gold = [
    "patient_360", "hospital_command_center", "department_hourly", "live_alerts",
    "triage_training", "triage_predictions", "policy_chunks",
]

if layout == "split":
    required = (
        [table(bronze_schema, name) for name in bronze]
        + [table(silver_schema, name) for name in silver]
        + [table(gold_schema, name) for name in gold]
    )
    triage = table(silver_schema, "triage")
    patient = table(gold_schema, "patient_360")
    command = table(gold_schema, "hospital_command_center")
    prediction = table(gold_schema, "triage_predictions")
else:
    required = (
        [table(schema, "bronze_" + name) for name in bronze]
        + [table(schema, "silver_" + name) for name in silver]
        + [table(schema, "gold_" + name) for name in gold]
    )
    triage = table(schema, "silver_triage")
    patient = table(schema, "gold_patient_360")
    command = table(schema, "gold_hospital_command_center")
    prediction = table(schema, "gold_triage_predictions")

print(f"Validating CareFlow360 {layout} layout, catalog={catalog}")
errors = []
for name in required:
    if not spark.catalog.tableExists(name):
        errors.append(f"MISSING TABLE: {name}")
        continue
    row_count = spark.table(name).count()
    print(f"{name}: {row_count:,} rows")
    # Quarantine and alerts can legitimately have zero rows.
    if row_count == 0 and not name.endswith(("triage_quarantine", "live_alerts", "gold_live_alerts")):
        errors.append(f"EMPTY REQUIRED TABLE: {name}")

# COMMAND ----------

if spark.catalog.tableExists(triage):
    invalid = spark.table(triage).where(
        F.col("triage_acuity").isNull() | (~F.col("triage_acuity").between(1, 5))
    ).count()
    if invalid:
        errors.append(f"Silver triage invalid/missing acuity: {invalid}")

if spark.catalog.tableExists(patient):
    counts = spark.table(patient).agg(
        F.count("*").alias("rows"),
        F.countDistinct("patient_id").alias("distinct_ids"),
    ).first()
    print(f"Patient 360: {counts['rows']} rows, {counts['distinct_ids']} distinct patients")
    if counts["rows"] != counts["distinct_ids"]:
        errors.append("Patient 360 must contain exactly one row per non-null patient_id")

if spark.catalog.tableExists(prediction):
    invalid = spark.table(prediction).where(
        F.col("predicted_triage_acuity").isNull()
        | (~F.col("predicted_triage_acuity").between(1, 5))
    ).count()
    if invalid:
        errors.append(f"Invalid batch prediction labels: {invalid}")

if spark.catalog.tableExists(command):
    freshness = spark.table(command).agg(
        F.count("*").alias("row_count"),
        F.max("snapshot_ts").alias("latest_ts"),
        ((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("snapshot_ts"))) / 3600.0).alias("age_hours"),
    ).first()
    if freshness["row_count"] == 0 or freshness["latest_ts"] is None:
        errors.append("Missing command-center KPI snapshot")
    else:
        age = float(freshness["age_hours"])
        print(f"Snapshot age: {age:.2f} hours")
        if age > max_snapshot_age_hours:
            errors.append(f"KPI snapshot older than {max_snapshot_age_hours} hours")

# COMMAND ----------

if errors:
    for error in errors:
        print("FAIL:", error)
    raise RuntimeError(f"CareFlow360 failed {len(errors)} validation checks")

print("PASS: required assets, Patient 360 grain, acuity ranges and KPI freshness")
print("These engineering tests do not establish clinical safety or production reliability.")
