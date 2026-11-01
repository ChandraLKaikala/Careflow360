# Databricks notebook source
# MAGIC %md
# MAGIC # CareFlow360 — end-to-end validation
# MAGIC Verify presentation views, generated tables, and key data contracts.
# MAGIC All inputs must be synthetic; this is not a clinical system.

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


def safe_identifier(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid Unity Catalog identifier: {name!r}")
    return name


catalog = safe_identifier(widget("catalog", spark.sql("SELECT current_catalog() AS c").first()["c"]))
bronze_schema = safe_identifier(widget("bronze_schema", "careflow360_bronze"))
silver_schema = safe_identifier(widget("silver_schema", "careflow360_silver"))
gold_schema = safe_identifier(widget("gold_schema", "careflow360_gold"))

# The SDP internal schema is distinct from the three consumer-facing schemas.
# The job passes pipeline_schema for diagnostic purposes, but validates the views.
pipeline_schema = safe_identifier(widget("pipeline_schema", "careflow360"))

presentation = {
    bronze_schema: ["triage", "vitals", "adt", "chief_complaints"],
    silver_schema: [
        "triage", "triage_quarantine", "vitals", "adt",
        "chief_complaints", "patient_location_history",
    ],
    gold_schema: [
        "patient_360", "hospital_command_center", "department_hourly",
        "live_alerts", "triage_training", "triage_predictions", "policy_chunks",
    ],
}

failures = []
counts = {}
for schema_name, objects in presentation.items():
    for name in objects:
        full_name = f"{catalog}.{schema_name}.{name}"
        if not spark.catalog.tableExists(full_name):
            failures.append(f"MISSING OBJECT: {full_name}")
            continue
        row_count = spark.table(full_name).count()
        counts[full_name] = row_count
        print(f"{full_name:75s} {row_count:>10,d} rows")
        # Empty alert/quarantine sets are valid: absence of issues is not a failure.
        if row_count == 0 and name not in {"triage_quarantine", "live_alerts"}:
            failures.append(f"EMPTY OBJECT: {full_name}")

def exists(schema_name, name):
    return f"{catalog}.{schema_name}.{name}" in counts


if exists(gold_schema, "hospital_command_center"):
    n = counts[f"{catalog}.{gold_schema}.hospital_command_center"]
    if n != 1:
        failures.append(f"Command center must contain one snapshot row; found {n}")

if exists(silver_schema, "triage"):
    triage = spark.table(f"{catalog}.{silver_schema}.triage")
    invalid = triage.filter(
        F.col("event_id").isNull()
        | F.col("patient_id").isNull()
        | F.col("event_ts").isNull()
        | (~F.col("triage_acuity").between(1, 5))
        | F.col("triage_acuity").isNull()
        | (~F.col("age").between(0, 120))
        | F.col("age").isNull()
    ).count()
    if invalid:
        failures.append(f"Silver triage contains {invalid} invalid rows")

if exists(gold_schema, "patient_360"):
    p = spark.table(f"{catalog}.{gold_schema}.patient_360")
    duplicate_patients = (
        p.groupBy("patient_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_patients:
        failures.append(f"Patient 360 contains {duplicate_patients} duplicate patient IDs")

if exists(gold_schema, "triage_training") and exists(gold_schema, "triage_predictions"):
    training_n = counts[f"{catalog}.{gold_schema}.triage_training"]
    predictions_n = counts[f"{catalog}.{gold_schema}.triage_predictions"]
    if training_n != predictions_n:
        failures.append(
            f"Prediction row count ({predictions_n}) differs from training source ({training_n})"
        )

if failures:
    print("\nVALIDATION FAILURES")
    for failure in failures:
        print(" -", failure)
    raise RuntimeError(f"CareFlow360 validation failed with {len(failures)} issue(s)")

print(f"\nPASS — verified {len(counts)} presentation assets across Bronze, Silver and Gold.")
