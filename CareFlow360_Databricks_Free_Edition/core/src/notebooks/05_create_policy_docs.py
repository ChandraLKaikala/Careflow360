# Databricks notebook source
# MAGIC %md
# MAGIC # Create synthetic hospital operations policy chunks for AI Search / RAG
# MAGIC These are fabricated training/demo documents, not actual clinical policies.

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
schema = widget("schema", "careflow360")
table_name = f"{catalog}.{schema}.gold_policy_chunks"

# COMMAND ----------

policies = [
    ("POL-001", "ICU Capacity Escalation", "When ICU staffed-bed utilization reaches the local escalation threshold, the hospital operations lead reviews expected discharges, step-down candidates, staffing constraints, and incoming critical-care demand. Escalation actions and ownership must be recorded in the operations log."),
    ("POL-002", "Emergency Department Surge", "During emergency-department surge conditions, operations staff review arrivals, waiting-room load, treatment-space availability, staffing, pending admissions, and discharge barriers. The command center coordinates non-clinical capacity actions and documents decisions."),
    ("POL-003", "Bed Assignment Operations", "Bed assignment should consider department capability, bed readiness, isolation requirements supplied by approved clinical systems, staffing availability, and existing placement priorities. CareFlow360 does not independently make clinical placement decisions."),
    ("POL-004", "Discharge Coordination", "For expected discharges, the operations team tracks transportation, pharmacy completion, follow-up scheduling, documentation status, and other workflow dependencies. Unresolved operational blockers should be assigned to an owner and reviewed during capacity huddles."),
    ("POL-005", "Data Quality Escalation", "Records with missing patient identifiers, invalid timestamps, or materially invalid operational measurements are quarantined from trusted analytics. Data engineering reviews rejected records and documents remediation before replay."),
    ("POL-006", "Synthetic Data Use", "The CareFlow360 demonstration environment contains synthetic or public training data only. It must not be used for real patient care, diagnosis, treatment decisions, or storage of protected health information."),
]

rows = []
for doc_id, title, text in policies:
    sentences = [s.strip() for s in text.split(".") if s.strip()]
    for i in range(0, len(sentences), 2):
        chunk = ". ".join(sentences[i:i+2]) + "."
        rows.append((f"{doc_id}-C{i//2+1:02d}", doc_id, title, chunk, "synthetic_demo_policy"))

policy_df = spark.createDataFrame(rows, ["chunk_id", "document_id", "title", "text", "source"])
(
    policy_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)
spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("Created", table_name, "with", policy_df.count(), "chunks")