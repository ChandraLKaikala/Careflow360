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
prefix = f"{catalog}.{schema}"

required = [
    "bronze_triage",
    "bronze_vitals",
    "bronze_adt",
    "bronze_chief_complaints",
    "silver_triage",
    "silver_triage_quarantine",
    "silver_vitals",
    "silver_adt",
    "silver_patient_location_history",
    "gold_patient_360",
    "gold_hospital_command_center",
    "gold_department_hourly",
    "gold_live_alerts",
    "gold_triage_training",
    "gold_policy_chunks",
    "gold_triage_predictions",
]

failures = []
for table in required:
    full = f"{prefix}.{table}"
    if not spark.catalog.tableExists(full):
        failures.append(f"MISSING TABLE: {full}")
        continue
    count = spark.table(full).count()
    print(f"{table:38s} {count:>10,d} rows")
    if count == 0 and table != "silver_triage_quarantine":
        failures.append(f"EMPTY TABLE: {full}")

if spark.catalog.tableExists(f"{prefix}.gold_hospital_command_center"):
    cmd = spark.table(f"{prefix}.gold_hospital_command_center").collect()
    if not cmd:
        failures.append("Hospital command center has no rows")

if spark.catalog.tableExists(f"{prefix}.silver_triage"):
    bad = spark.table(f"{prefix}.silver_triage").where("triage_acuity < 1 OR triage_acuity > 5").count()
    if bad:
        failures.append(f"silver_triage has {bad} invalid acuity values")

if failures:
    print("\nVALIDATION FAILURES")
    for failure in failures:
        print(" -", failure)
    raise RuntimeError(f"CareFlow360 validation failed with {len(failures)} issue(s)")

print("\nPASS — all expected CareFlow360 assets are present and non-empty.")
