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
print("Bundle pipeline schema: careflow360 (default)")
print(f"Bundle landing volume: /Volumes/{current_catalog}/careflow360/landing")

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
# MAGIC Configure your Databricks CLI credentials and set the bundle `catalog` variable.
# MAGIC Run `databricks bundle validate -t dev`, review `databricks bundle plan -t dev`,
# MAGIC then deploy and run the `careflow360_job` task chain. No missing SQL setup file is required.