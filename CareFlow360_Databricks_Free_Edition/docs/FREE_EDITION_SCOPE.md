# CareFlow360 — Databricks Free Edition scope

Reviewed against Databricks Free Edition documentation on 2026-11-01. Actual workspace quotas and feature access must be checked before relying on optional services.

## Core path used by this repository

The core project intentionally relies on capabilities that fit a Free Edition learning workspace:

- Serverless notebooks and Python/SQL
- Unity Catalog managed schema/volume/tables
- Delta Lake tables
- Lakeflow Spark Declarative Pipelines
- Auto Loader file ingestion
- Streaming tables and materialized views
- AUTO CDC with SCD Type 2 history
- Lakeflow Jobs / Workflows
- MLflow experiments and Unity Catalog model registration
- AI/BI SQL queries / dashboard-ready Gold tables
- Declarative Automation Bundles (DAB)

## Optional extensions

These are packaged independently because Free Edition quotas, previews, regional availability, or pre-existing resources can prevent deployment even when the feature exists in the product:

- Genie Agent
- AI Search (formerly Vector Search)
- Model Serving
- Databricks Apps
- Lakebase

The core project does not depend on any optional extension.

## Free Edition constraints that shaped the design

At the time this project was assembled, Databricks Free Edition documents important limits such as serverless-only compute, one small SQL warehouse, a five-concurrent-job-task account limit, limited active Lakeflow pipelines, one AI Search endpoint/search unit, limited Model Serving, up to three Apps, and one Lakebase project. Limits can change, so consult the current Free Edition limitations page before deployment.

This repository therefore:

1. Uses serverless compute only.
2. Uses six dependent tasks, with at most two running in parallel; this fits the five-concurrent-task Free Edition limit.
3. Uses one core Lakeflow pipeline.
4. Trains a CPU Random Forest on at most 4,000 historical encounters by default; inserts predictions for new patient IDs only.
5. Provides an optional, **manual** 2,000-row model comparison of Logistic Regression, Random Forest and histogram gradient boosting with a patient-disjoint train/validation/test split; never automatically registers or promotes a candidate.
6. Joins current Patient 360 vitals, complaints and location by both patient ID and encounter ID, avoiding contamination across revisits.
4. Keeps AI Search optional and uses one STANDARD endpoint with a triggered HYBRID index by default; the embedding model endpoint is configurable by workspace/region.
5. Uses scale-to-zero where supported for optional serving resources.
6. Keeps Lakebase/App/Genie/Serving as separate bundles you can deploy only when needed.

## Not a production compliance environment

Free Edition is for learning/non-commercial experimentation. CareFlow360 contains only synthetic/public data and intentionally has no real protected health information (PHI). A production healthcare deployment would require a separate security, compliance, identity, networking, audit, data-retention, validation, and governance design.
