# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CareFlow360 — Preflight
# MAGIC Run this first on serverless compute. It discovers your workspace catalog and checks core capabilities.

# COMMAND ----------

from pyspark.sql import functions as F

current_catalog = spark.sql("SELECT current_catalog() AS c").first()["c"]
current_user = spark.sql("SELECT current_user() AS u").first()["u"]

print(f"Current catalog : {current_catalog}")
print(f"Current user    : {current_user}")
print("Split-schema DAB working schema: careflow360")
print(f"Split-schema DAB landing volume: /Volumes/{current_catalog}/careflow360/landing")
print("Bronze/Silver/Gold presentation schemas: careflow360_bronze, careflow360_silver, careflow360_gold")
print("The standalone unified quickstart instead uses careflow360_dev.")
print("Do not create a second Lakeflow pipeline when upgrading the existing workspace.")

# COMMAND ----------

print("Spark version:", spark.version)
try:
    from pyspark import pipelines as dp
    print("Lakeflow pyspark.pipelines API: AVAILABLE")
except Exception as exc:
    print("Lakeflow pyspark.pipelines API: only importable inside a pipeline or unavailable here")
    print(type(exc).__name__, str(exc)[:300])

try:
    import mlflow
    print("MLflow version:", mlflow.__version__)
except Exception as exc:
    print("MLflow import failed:", exc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC For the existing split-schema scheduled job, **pull the latest GitHub main and run 08_upgrade_existing_daily_job.py in dry-run mode** before changing any resources. The separate `core/sql/00_setup.sql` is for the unified quickstart only; do not create its `careflow360_dev` schema as a substitute for the already deployed pipeline.