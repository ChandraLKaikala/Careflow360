# Databricks notebook source
# MAGIC %md
# MAGIC # Optional CPU model comparison — Databricks Free Edition
# MAGIC Compares majority-class baseline, Logistic Regression, Random Forest,
# MAGIC Extra Trees and histogram gradient boosting on SYNTHETIC encounters.
# MAGIC The chosen model is evaluated once on untouched *patient groups*.
# MAGIC This experiment does NOT deploy/register/promote a model and is NOT
# MAGIC part of the daily scheduled job (to preserve Free Edition quota).
# MAGIC These classifications are for a portfolio demo, not clinical decisions.

# COMMAND ----------

import re
import time
import mlflow
import pandas as pd
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, f1_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def widget(name, default):
    try:
        dbutils.widgets.text(name, str(default), name)
    except Exception:
        pass
    return dbutils.widgets.get(name)


catalog = widget("catalog", "workspace")
schema = widget("schema", "careflow360_gold")
max_local_rows = int(widget("max_local_rows", "2000"))
experiment_path = widget(
    "experiment_path",
    f"/Workspace/Users/{spark.sql('SELECT current_user() AS u').first()['u']}/careflow360/triage_experiment",
)
for value in (catalog, schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Unity Catalog identifier {value!r}")
if not 150 <= max_local_rows <= 4000:
    raise ValueError("Free Edition comparison sample must be between 150 and 4000 rows")

source = f"{catalog}.{schema}.triage_training"
if not spark.catalog.tableExists(source):
    raise RuntimeError(f"Missing Gold training view: {source}")

# COMMAND ----------

numeric = [
    "age", "systolic_bp", "diastolic_bp", "heart_rate",
    "respiratory_rate", "temperature_c", "spo2", "gcs_total",
    "pain_score", "num_prior_visits", "arrival_hour", "shock_index",
]
categorical = ["arrival_mode", "arrival_day", "sex"]
features = numeric + categorical

# Sort by stable patient+encounter hash and collect only a strictly bounded sample.
pdf = (
    spark.table(source)
    .dropna(subset=["patient_id", "encounter_id", "triage_acuity"])
    .orderBy(F.xxhash64("patient_id", "encounter_id"))
    .limit(max_local_rows)
    .select("patient_id", "triage_acuity", *features)
    .toPandas()
)
if len(pdf) < 150 or pdf["patient_id"].nunique() < 30:
    raise ValueError("Need >=150 training rows and >=30 distinct patient IDs")

X = pdf[features]
y = pdf["triage_acuity"].astype(int)
groups = pdf["patient_id"]
if y.nunique() < 2:
    raise ValueError("Need at least two acuity classes")

# Patient-disjoint train (60%), validation (20%), and test (20%).
def three_way_patient_split(X, y, groups):
    first = GroupShuffleSplit(n_splits=1, test_size=0.20, random_state=42)
    dev_idx, test_idx = next(first.split(X, y, groups))
    second = GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=43)
    train_sub, validation_sub = next(
        second.split(X.iloc[dev_idx], y.iloc[dev_idx], groups.iloc[dev_idx])
    )
    train_idx, validation_idx = dev_idx[train_sub], dev_idx[validation_sub]
    train, valid, test = (
        set(groups.iloc[train_idx]),
        set(groups.iloc[validation_idx]),
        set(groups.iloc[test_idx]),
    )
    if train & valid or train & test or valid & test:
        raise AssertionError("Patient identity leaked across splits")
    if y.iloc[train_idx].nunique() < 2:
        raise ValueError("Training group split has fewer than two labels")
    if y.iloc[validation_idx].nunique() < 2 or y.iloc[test_idx].nunique() < 2:
        raise ValueError(
            "Validation/test groups have fewer than two labels; "
            "increase the sample or review synthetic class coverage"
        )
    return train_idx, validation_idx, test_idx


train_idx, val_idx, test_idx = three_way_patient_split(X, y, groups)
X_train, y_train = X.iloc[train_idx], y.iloc[train_idx]
X_val, y_val = X.iloc[val_idx], y.iloc[val_idx]
X_test, y_test = X.iloc[test_idx], y.iloc[test_idx]
print(
    f"Rows: train={len(train_idx)}, validation={len(val_idx)}, test={len(test_idx)}; "
    f"patients={groups.nunique()}"
)

# COMMAND ----------

def preprocessing(scale_numeric=False):
    num_steps = [("imputer", SimpleImputer(strategy="median"))]
    if scale_numeric:
        num_steps.append(("scaler", StandardScaler()))
    return ColumnTransformer([
        ("numeric", Pipeline(num_steps), numeric),
        ("categorical", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]), categorical),
    ])


def candidate_models():
    return {
        "logistic_regression": Pipeline([
            ("preprocessor", preprocessing(scale_numeric=True)),
            ("model", LogisticRegression(max_iter=250)),
        ]),
        "random_forest": Pipeline([
            ("preprocessor", preprocessing()),
            ("model", RandomForestClassifier(
                n_estimators=96, max_depth=12, min_samples_leaf=2,
                random_state=42, class_weight="balanced_subsample", n_jobs=2,
            )),
        ]),
        "extra_trees": Pipeline([
            ("preprocessor", preprocessing()),
            ("model", ExtraTreesClassifier(
                n_estimators=80, max_depth=12, min_samples_leaf=2,
                random_state=42, class_weight="balanced", n_jobs=2,
            )),
        ]),
        "hist_gradient_boosting": Pipeline([
            ("preprocessor", preprocessing()),
            ("model", HistGradientBoostingClassifier(
                max_iter=100, max_leaf_nodes=15, learning_rate=0.08,
                min_samples_leaf=15, l2_regularization=0.1,
                random_state=42,
            )),
        ]),
    }


def measure(model, inputs, labels):
    prediction = model.predict(inputs)
    return {
        "macro_f1": float(f1_score(labels, prediction, average="macro", zero_division=0)),
        "balanced_accuracy": float(balanced_accuracy_score(labels, prediction)),
    }


baseline = DummyClassifier(strategy="most_frequent")
baseline.fit(X_train, y_train)
baseline_score = measure(baseline, X_val, y_val)
results = [{"model": "majority_class_baseline", "validation_macro_f1": baseline_score["macro_f1"],
            "validation_balanced_accuracy": baseline_score["balanced_accuracy"],
            "training_seconds": 0.0}]
fitted = {}
for name, estimator in candidate_models().items():
    started = time.perf_counter()
    estimator.fit(X_train, y_train)
    duration = round(time.perf_counter() - started, 3)
    scores = measure(estimator, X_val, y_val)
    results.append({
        "model": name, "validation_macro_f1": scores["macro_f1"],
        "validation_balanced_accuracy": scores["balanced_accuracy"],
        "training_seconds": duration,
    })
    fitted[name] = estimator

# Choose by validation macro-F1, not test performance. No automatic model promotion.
candidates = [r for r in results if r["model"] != "majority_class_baseline"]
chosen = max(candidates, key=lambda r: (r["validation_macro_f1"], -r["training_seconds"]))
test_score = measure(fitted[chosen["model"]], X_test, y_test)
baseline_test_score = measure(baseline, X_test, y_test)
better_than_baseline = (
    chosen["validation_macro_f1"] > baseline_score["macro_f1"]
    and chosen["validation_balanced_accuracy"] >= baseline_score["balanced_accuracy"]
)
print("Model comparison (synthetic, patient-disjoint validation):")
display(pd.DataFrame(results).sort_values("validation_macro_f1", ascending=False))
print("Chosen on validation:", chosen["model"], "held-out test metrics:", test_score)
print("Baseline validation:", baseline_score, "baseline test:", baseline_test_score)
if not better_than_baseline:
    print("WARNING: The selected candidate did not beat the majority baseline on validation.")
print("Do not promote this comparison model without a separate runtime review.")

# COMMAND ----------

# Log aggregate metrics only; no patient identifiers or record-level predictions.
mlflow.set_experiment(experiment_path)
with mlflow.start_run(run_name="careflow360-free-edition-comparison"):
    mlflow.log_params({
        "task": "synthetic_triage_demo",
        "selection": "validation_macro_f1_patient_disjoint",
        "max_local_rows": max_local_rows,
        "chosen_candidate": chosen["model"],
        "beats_validation_dummy": str(better_than_baseline).lower(),
        "model_registration": "none",
    })
    mlflow.log_metrics({
        "test_macro_f1": test_score["macro_f1"],
        "test_balanced_accuracy": test_score["balanced_accuracy"],
        "test_dummy_macro_f1": baseline_test_score["macro_f1"],
        "test_dummy_balanced_accuracy": baseline_test_score["balanced_accuracy"],
        "validation_dummy_macro_f1": baseline_score["macro_f1"],
        "validation_dummy_balanced_accuracy": baseline_score["balanced_accuracy"],
        **{f"validation_{r['model']}_macro_f1": r["validation_macro_f1"]
           for r in candidates},
    })
