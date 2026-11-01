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
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
import math

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
schema = widget("schema", "careflow360_gold")
experiment_path = widget("experiment_path", f"/Workspace/Users/{spark.sql('SELECT current_user() AS u').first()['u']}/careflow360/triage_experiment")
model_name = widget("model_name", "triage_acuity_model")
for label, identifier in (("catalog", catalog), ("schema", schema), ("model_name", model_name)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Unsafe {label} identifier: {identifier!r}")

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

# Project only the bounded feature contract before collecting data into pandas.
# Avoid copying unused pipeline metadata to the quota-limited serverless driver.
numeric = [
    "age", "systolic_bp", "diastolic_bp", "heart_rate", "respiratory_rate",
    "temperature_c", "spo2", "gcs_total", "pain_score", "num_prior_visits",
    "arrival_hour", "shock_index"
]
categorical = ["arrival_mode", "arrival_day", "sex"]
features = numeric + categorical

df = (
    spark.table(source_table)
    .select("patient_id", "encounter_id", "triage_acuity", *features)
    .dropna(subset=["triage_acuity", "patient_id", "encounter_id"])
)
# A deterministic, quota-bounded sample prevents the pandas driver from
# exhausting Free Edition as the number of synthetic encounters grows.
# xxhash64 balances old and new patient encounters; do not use an unseeded
# take()/limit() as a substitute for a stable sampling order.
max_local_rows = int(widget("max_local_rows", "4000"))
if max_local_rows < 100 or max_local_rows > 20000:
    raise ValueError("max_local_rows must be between 100 and 20000 in Free Edition")
candidates = (
    df.orderBy(F.xxhash64("patient_id", "encounter_id"))
    .limit(max_local_rows + 1)
    .toPandas()
)
training_capped = len(candidates) > max_local_rows
pdf = candidates.iloc[:max_local_rows].copy()
if len(pdf) < 100 or pdf["patient_id"].nunique() < 10:
    raise ValueError(
        "Need at least 100 training rows and 10 distinct patients for group-held-out evaluation"
    )
print(f"Training examples: {len(pdf)}; sample capped: {training_capped}; "
      f"unique training patients: {pdf['patient_id'].nunique()}")

X = pdf[features]
y = pdf["triage_acuity"].astype(int)
if y.nunique() < 2:
    raise ValueError("Training target contains fewer than 2 classes")

# Split by *patient*, not encounter: legacy synthetic history has
# multiple encounters for each patient and a row split inflates metrics.
def split_patient_holdout(features_df, labels, patient_ids):
    splitter = GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=42)
    train_idx, test_idx = next(splitter.split(features_df, labels, groups=patient_ids))
    train_groups = set(patient_ids.iloc[train_idx])
    test_groups = set(patient_ids.iloc[test_idx])
    if train_groups & test_groups:
        raise AssertionError("Patient leakage detected across train/test split")
    train_labels = labels.iloc[train_idx]
    test_labels = labels.iloc[test_idx]
    if train_labels.nunique() < 2 or len(test_labels) == 0:
        raise ValueError("Insufficient patient-level diversity after train/test split")
    return (
        features_df.iloc[train_idx], features_df.iloc[test_idx],
        train_labels, test_labels, train_groups, test_groups,
    )


X_train, X_test, y_train, y_test, train_groups, test_groups = (
    split_patient_holdout(X, y, pdf["patient_id"])
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
    n_estimators=96,
    max_depth=12,
    min_samples_leaf=2,
    random_state=42,
    class_weight="balanced_subsample",
    n_jobs=2,  # Limit Free Edition serverless CPU contention.
)
pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])


def beats_majority_baseline(results):
    """Allow promotion only when the patient-held-out demo beats its baseline.

    A failed retrain must leave the existing Champion alias unchanged. This
    guard is NOT a clinical performance or safety validation.
    """
    required = ("f1_macro", "dummy_f1_macro",
                "balanced_accuracy", "dummy_balanced_accuracy")
    if not all(math.isfinite(float(results[name])) for name in required):
        return False
    return (
        results["f1_macro"] > results["dummy_f1_macro"]
        and results["balanced_accuracy"] >= results["dummy_balanced_accuracy"]
    )

# COMMAND ----------

mlflow.set_experiment(experiment_path)
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="careflow360-random-forest") as run:
    pipeline.fit(X_train, y_train)
    pred = pipeline.predict(X_test)

    dummy = DummyClassifier(strategy="most_frequent")
    dummy.fit(X_train, y_train)
    dummy_pred = dummy.predict(X_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_test, pred)),
        "dummy_accuracy": float(accuracy_score(y_test, dummy_pred)),
        "dummy_balanced_accuracy": float(balanced_accuracy_score(y_test, dummy_pred)),
        "dummy_f1_macro": float(f1_score(y_test, dummy_pred, average="macro", zero_division=0)),
        "f1_macro": float(f1_score(y_test, pred, average="macro", zero_division=0)),
        "precision_macro": float(precision_score(y_test, pred, average="macro", zero_division=0)),
        "recall_macro": float(recall_score(y_test, pred, average="macro", zero_division=0)),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "train_unique_patients": int(len(train_groups)),
        "test_unique_patients": int(len(test_groups)),
        "training_sample_capped": int(training_capped),
    }
    mlflow.log_params({
        "model_type": "RandomForestClassifier",
        "n_estimators": 96,
        "max_depth": 12,
        "train_test_strategy": "patient_group_shuffle_split",
        "sampling_strategy": "stable_xxhash64_cap",
        "max_local_rows": max_local_rows,
        "n_jobs": 2,
        "data_classification": "synthetic_or_public",
        "clinical_use": "prohibited_demo_only",
        "release_gate": "macro_f1_gt_dummy_and_balanced_accuracy_gte_dummy",
    })
    mlflow.log_metrics(metrics)
    if not beats_majority_baseline(metrics):
        raise RuntimeError(
            "Retrained model did not beat the majority-class holdout baseline. "
            "Champion was not updated and new predictions were not written."
        )
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
# Only new patient IDs need a prediction: the existing table is append-only.
# Do not bring 600, 20,000, or more already-scored patients back to pandas.
if spark.catalog.tableExists(prediction_table):
    existing_ids = spark.table(prediction_table).select("patient_id").dropDuplicates()
    new_keys = current_keys.join(existing_ids, "patient_id", "left_anti")
else:
    new_keys = current_keys
new_patient_count = new_keys.count()
print(f"Previously unscored current patients: {new_patient_count}")
if new_patient_count > max_local_rows:
    raise RuntimeError(
        f"{new_patient_count} unscored current patients exceed max_local_rows="
        f"{max_local_rows}; score in smaller batches instead of unbounded collection."
    )
if new_patient_count:
    # Join by both patient and encounter, so legacy historical encounters
    # are never mistaken for the patient's current episode.
    score_src = df.join(new_keys, ["patient_id", "encounter_id"], "inner")
    score_pdf = (
        score_src.select("patient_id", "encounter_id", *features)
        .limit(max_local_rows + 1).toPandas()
    )
    ensure_current_patient_coverage(score_pdf, new_patient_count)
    # Score using the fitted, evaluated model already in memory instead of
    # re-downloading the just-registered Champion model from the registry.
    score_pdf["predicted_triage_acuity"] = pipeline.predict(score_pdf[features]).astype(int)
    score_sdf = spark.createDataFrame(score_pdf)
else:
    print("Idempotent scoring: no previously unseen patients; existing predictions preserved.")

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


if new_patient_count:
    write_mode = insert_new_patient_predictions(spark, prediction_table, score_sdf)
    print(f"Prediction table {prediction_table}: {write_mode}; new patients={new_patient_count}")
else:
    print(f"Prediction table {prediction_table}: unchanged (no new patient IDs)")
print("Existing predictions are preserved; only previously unseen patient IDs are inserted.")