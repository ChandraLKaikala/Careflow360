# Databricks notebook source
# MAGIC %md
# MAGIC # CareFlow360 validation

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
bronze_schema = widget("bronze_schema", "careflow360_bronze")
silver_schema = widget("silver_schema", "careflow360_silver")
gold_schema = widget("gold_schema", "careflow360_gold")
layout = widget("layout", "auto").lower()

# The DAB path exposes layer-specific views in *_bronze, *_silver, *_gold.
# The manual Free Edition quickstart can instead use a single working schema.
if layout == "auto":
    layout = "split" if spark.catalog.tableExists(f"{catalog}.{gold_schema}.patient_360") else "unified"
if layout not in ("split", "unified"):
    raise ValueError("layout must be split, unified or auto")

if layout == "split":
    required = (
        [f"{catalog}.{bronze_schema}.{t}" for t in ["triage", "vitals", "adt", "chief_complaints"]]
        + [f"{catalog}.{silver_schema}.{t}" for t in [
            "triage", "triage_quarantine", "vitals", "adt", "chief_complaints",
            "patient_location_history"
        ]]
        + [f"{catalog}.{gold_schema}.{t}" for t in [
            "patient_360", "hospital_command_center", "department_hourly",
            "live_alerts", "triage_training", "policy_chunks", "triage_predictions"
        ]]
    )
    command_table = f"{catalog}.{gold_schema}.hospital_command_center"
    triage_table = f"{catalog}.{silver_schema}.triage"
    patient_table = f"{catalog}.{gold_schema}.patient_360"
else:
    prefix = f"{catalog}.{schema}"
    required = [f"{prefix}.{t}" for t in [
        "bronze_triage", "bronze_vitals", "bronze_adt", "bronze_chief_complaints",
        "silver_triage", "silver_triage_quarantine", "silver_vitals", "silver_adt",
        "silver_chief_complaints", "silver_patient_location_history",
        "gold_patient_360", "gold_hospital_command_center", "gold_department_hourly",
        "gold_live_alerts", "gold_triage_training", "gold_policy_chunks", "gold_triage_predictions"
    ]]
    command_table = f"{prefix}.gold_hospital_command_center"
    triage_table = f"{prefix}.silver_triage"
    patient_table = f"{prefix}.gold_patient_360"
print("Validating", layout, "layout")

failures = []
for full in required:
    if not spark.catalog.tableExists(full):
        failures.append(f"MISSING TABLE: {full}")
        continue
    count = spark.table(full).count()
    print(f"{full:65s} {count:>10,d} rows")
    if count == 0 and not full.endswith(("triage_quarantine", "live_alerts", "gold_live_alerts")):
        failures.append(f"EMPTY TABLE: {full}")

if spark.catalog.tableExists(command_table):
    cmd = spark.table(command_table).limit(1).collect()
    if not cmd:
        failures.append("Hospital command center has no rows")

if spark.catalog.tableExists(triage_table):
    bad = spark.table(triage_table).where("triage_acuity < 1 OR triage_acuity > 5").count()
    if bad:
        failures.append(f"silver_triage has {bad} invalid acuity values")

if spark.catalog.tableExists(patient_table):
    from pyspark.sql import functions as F
    row = spark.table(patient_table).agg(
        F.count("*").alias("n"),
        F.countDistinct("patient_id").alias("distinct_ids")
    ).first()
    if row["n"] != row["distinct_ids"]:
        failures.append("patient_360 must contain one non-null row per patient")

if failures:
    print("\nVALIDATION FAILURES")
    for failure in failures:
        print(" -", failure)
    raise RuntimeError(f"CareFlow360 validation failed with {len(failures)} issue(s)")

print("\nPASS — all expected CareFlow360 assets are present and non-empty.")
