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
source_table = f"{catalog}.{schema}.gold_triage_training"
print("Training source:", source_table)
print("Registered model:", full_model_name)

# COMMAND ----------

df = spark.table(source_table).dropna(subset=["triage_acuity"])
pdf = df.toPandas()
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

# Batch scoring demonstration. Keeps the project useful even when a serving endpoint quota is unavailable.
loaded = mlflow.pyfunc.load_model(f"models:/{full_model_name}@Champion")
score_pdf = pdf[["patient_id", "encounter_id"] + features].copy()
score_pdf["predicted_triage_acuity"] = loaded.predict(score_pdf[features]).astype(int)
score_sdf = spark.createDataFrame(score_pdf)
(
    score_sdf.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_triage_predictions")
)
print("Wrote:", f"{catalog}.{schema}.gold_triage_predictions")