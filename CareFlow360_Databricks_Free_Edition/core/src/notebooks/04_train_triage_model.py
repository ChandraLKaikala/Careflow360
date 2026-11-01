# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow triage model
# MAGIC Portfolio-only multiclass model trained on synthetic/public data. This is **not** a clinically validated model.

# COMMAND ----------

import re
import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

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
experiment_path = widget("experiment_path", f"/Workspace/Users/{spark.sql('SELECT current_user() AS u').first()['u']}/careflow360/triage_experiment")
model_name = widget("model_name", "triage_acuity_model")

full_model_name = f"{catalog}.{schema}.{model_name}"
# DAB uses triage_training in the Gold presentation schema; the unified
# quickstart uses gold_triage_training. Detect the existing table explicitly.
presentation_source = f"{catalog}.{schema}.triage_training"
unified_source = f"{catalog}.{schema}.gold_triage_training"
if spark.catalog.tableExists(presentation_source):
    source_table = presentation_source
    prediction_table = f"{catalog}.{schema}.triage_predictions"
    current_patient_table = f"{catalog}.{schema}.patient_360"
elif spark.catalog.tableExists(unified_source):
    source_table = unified_source
    prediction_table = f"{catalog}.{schema}.gold_triage_predictions"
    current_patient_table = f"{catalog}.{schema}.gold_patient_360"
else:
    raise RuntimeError(f"Missing training source: {presentation_source} or {unified_source}")
if not spark.catalog.tableExists(current_patient_table):
    raise RuntimeError(f"Current-patient scoring table is missing: {current_patient_table}")
print("Training source:", source_table)
print("Current-patient scoring source:", current_patient_table)
print("Registered model:", full_model_name)

# COMMAND ----------

df = spark.table(source_table).dropna(subset=["triage_acuity"])
# Avoid unbounded collection into the driver on quota-limited Free Edition.
max_local_rows = int(widget("max_local_rows", "20000"))
if max_local_rows < 100 or max_local_rows > 50000:
    raise ValueError("max_local_rows must be between 100 and 50000")
pdf = df.limit(max_local_rows + 1).toPandas()
if len(pdf) > max_local_rows:
    raise RuntimeError(
        f"Training set exceeds max_local_rows={max_local_rows}; "
        "switch to distributed scoring instead of collecting more rows."
    )
if len(pdf) < 100:
    raise ValueError(f"Need at least 100 training rows; found {len(pdf)}")

numeric = [
    "age", "systolic_bp", "diastolic_bp", "heart_rate", "respiratory_rate",
    "temperature_c", "spo2", "gcs_total", "pain_score", "num_prior_visits",
    "arrival_hour", "shock_index"
]
categorical = ["arrival_mode", "arrival_day", "sex"]
features = numeric + categorical

X = pdf[features]
y = pdf["triage_acuity"].astype(int)
if y.nunique() < 2:
    raise ValueError("Training target contains fewer than 2 classes")

stratify = y if y.value_counts().min() >= 2 else None
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=stratify
)

numeric_pipe = Pipeline([("imputer", SimpleImputer(strategy="median"))])
categorical_pipe = Pipeline([
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("onehot", OneHotEncoder(handle_unknown="ignore")),
])
preprocessor = ColumnTransformer([
    ("numeric", numeric_pipe, numeric),
    ("categorical", categorical_pipe, categorical),
])
model = RandomForestClassifier(
    n_estimators=160,
    max_depth=12,
    min_samples_leaf=2,
    random_state=42,
    class_weight="balanced_subsample",
    n_jobs=-1,
)
pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])

# COMMAND ----------

mlflow.set_experiment(experiment_path)
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="careflow360-random-forest") as run:
    pipeline.fit(X_train, y_train)
    pred = pipeline.predict(X_test)

    metrics = {
        "accuracy": float(accuracy_score(y_test, pred)),
        "f1_macro": float(f1_score(y_test, pred, average="macro", zero_division=0)),
        "precision_macro": float(precision_score(y_test, pred, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(y_test, pred, average="macro", zero_division=0)),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
    }
    mlflow.log_params({
        "model_type": "RandomForestClassifier",
        "n_estimators": 160,
        "max_depth": 12,
        "data_classification": "synthetic_or_public",
        "clinical_use": "prohibited_demo_only",
    })
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(
        sk_model=pipeline,
        artifact_path="model",
        input_example=X_train.head(5),
    )
    run_id = run.info.run_id

print("Run ID:", run_id)
print("Metrics:", metrics)

# COMMAND ----------

registered = mlflow.register_model(f"runs:/{run_id}/model", full_model_name)
client = MlflowClient()
client.set_registered_model_alias(full_model_name, "Champion", registered.version)
print(f"Registered version {registered.version}; alias Champion set")

# COMMAND ----------

# Batch scoring outputs one prediction per *current patient*, not every historical
# training encounter. Keep the full encounter history available for training.
def ensure_current_patient_coverage(scored, patient_count):
    if patient_count < 1:
        raise RuntimeError("Patient 360 has no current patient records to score")
    if len(scored) != patient_count:
        raise RuntimeError(
            f"Expected {patient_count} current patients, but matched {len(scored)} "
            "training rows. Investigate missing/duplicate encounter keys."
        )
    if scored["patient_id"].isna().any() or scored["encounter_id"].isna().any():
        raise RuntimeError("Scoring rows have null patient or encounter IDs")
    if scored["patient_id"].duplicated().any():
        raise RuntimeError("Scoring rows contain duplicate current patient IDs")


current_keys = (
    spark.table(current_patient_table)
    .select("patient_id", "encounter_id")
    .dropDuplicates()
)
current_patient_count = current_keys.count()
if current_patient_count > max_local_rows:
    raise RuntimeError(
        f"Current patient count {current_patient_count} exceeds max_local_rows="
        f"{max_local_rows}; use distributed scoring instead of collecting more rows."
    )
# Join in Spark so the 2,800 historical training encounters are not all published
# as 2,800 simultaneous current patient predictions.
score_src = df.join(current_keys, ["patient_id", "encounter_id"], "inner")
score_pdf = score_src.select("patient_id", "encounter_id", *features).limit(max_local_rows + 1).toPandas()
ensure_current_patient_coverage(score_pdf, current_patient_count)
loaded = mlflow.pyfunc.load_model(f"models:/{full_model_name}@Champion")
score_pdf["predicted_triage_acuity"] = loaded.predict(score_pdf[features]).astype(int)
score_sdf = spark.createDataFrame(score_pdf)

# The deployed triage_predictions Delta table may have appendOnly enabled.
# Never overwrite or delete existing patient predictions. Insert-only MERGE
# is idempotent: matched patient_id rows stay put and new patients are added.
# Spark's Patient 360 materialized view independently applies the latest
# patient state; historical location changes are tracked by AUTO CDC SCD2.
from delta.tables import DeltaTable


def insert_new_patient_predictions(spark, target_name, source):
    if not spark.catalog.tableExists(target_name):
        (source.write.format("delta").mode("errorifexists").saveAsTable(target_name))
        return "created"
    target = spark.table(target_name)
    target_columns = set(target.columns)
    source_columns = set(source.columns)
    if target_columns != source_columns:
        raise RuntimeError(
            f"Prediction target schema does not match source; "
            f"target_only={sorted(target_columns - source_columns)}, "
            f"source_only={sorted(source_columns - target_columns)}"
        )
    dup = (
        target.groupBy("patient_id").count()
        .where(F.col("count") > 1)
        .limit(1).count()
    )
    if dup:
        raise RuntimeError("Existing prediction table has duplicate patient IDs")
    (DeltaTable.forName(spark, target_name).alias("target")
        .merge(source.alias("new"), "target.patient_id = new.patient_id")
        .whenNotMatchedInsertAll()
        .execute())
    return "insert_only_merge"


# Import F only for the duplicate-key verification above.
from pyspark.sql import functions as F
write_mode = insert_new_patient_predictions(spark, prediction_table, score_sdf)
print(f"Prediction table {prediction_table}: {write_mode}; source current patients={len(score_pdf)}")
print("Existing predictions are preserved; only previously unseen patient IDs are inserted.")