# CareFlow360 — Incremental Synthetic Hospital Operations Lakehouse

> **Existing workspace caution:** This Free Edition package mirrors the root DAB sources.
> GitHub changes are not automatically deployed to workspace notebooks/jobs.
> Before a live deploy, follow [the existing-job sync runbook](../docs/DATABRICKS_WORKSPACE_SYNC.md)
> to avoid duplicate pipelines and preserve the 02:00 UTC job. Use synthetic data only.

An end-to-end Databricks Free Edition portfolio project that simulates a hospital command center using synthetic/public healthcare data.

## What this project demonstrates

- Unity Catalog schemas, managed volumes, tables, views, lineage and governance
- Lakeflow Spark Declarative Pipelines using the current `pyspark.pipelines` API
- Bronze → Silver → Gold medallion architecture
- Auto Loader JSON ingestion, data-quality expectations and a triage quarantine table (Bronze-null-ID drops cannot appear in that table)
- AUTO CDC with SCD Type 2 patient-location history
- Lakeflow Jobs orchestration on serverless compute
- Declarative Automation Bundles (DABs; formerly Databricks Asset Bundles)
- MLflow experiment tracking, Unity Catalog model registration and batch scoring
- AI/BI dashboard-ready Gold tables
- Genie Agent configuration starter
- AI Search (formerly Vector Search) RAG starter
- Model Serving starter
- Databricks App starter
- Lakebase Autoscaling starter
- Synthetic-only core generation; optional public-data ideas are documented, not bundled as an executable Kaggle adapter

## Project scenario

CareFlow360 ingests near-real-time hospital events:

1. ADT events — admission, transfer and discharge
2. ED triage events — demographics, arrival mode, vitals and triage acuity
3. Bedside monitoring events — HR, SpO2, BP, temperature and respiratory rate
4. Chief-complaint text

The core creates **synthetic** micro-batch JSON files using a configurable replay layer (`seconds_between_batches` can be set above zero for a demo). The scheduled job is **daily/triggered**, not a continuously running real-time hospital telemetry feed. Kaggle/public-dataset ETL is not part of the default executable workflow.

## Important safety and compliance note

This repository is for learning and portfolio use. It uses synthetic/public data only. Do **not** load real PHI into Databricks Free Edition. The included predictive model is a technical demonstration and is not a validated clinical decision-support or diagnostic system.

## Fastest path

Read [`QUICKSTART.md`](QUICKSTART.md).

## Repository structure

```text
careflow360_databricks_free_edition/
├── README.md
├── QUICKSTART.md
├── SETUP_COMMANDS.md
├── docs/
├── core/
│   ├── databricks.yml
│   ├── resources/
│   ├── src/pipelines/
│   ├── src/notebooks/
│   └── sql/
├── extensions/
│   ├── genie/
│   ├── ai_search/
│   ├── model_serving/
│   ├── app/
│   └── lakebase/
└── tests/
```

## Recommended build order

1. Import the ZIP into Databricks Workspace.
2. Run `core/src/notebooks/00_preflight.py`.
3. Select **one** setup path: existing split-schema DAB resources or the standalone unified quickstart SQL, as described in `QUICKSTART.md`. Do not accidentally create a second pipeline in a quota-limited workspace.
4. Run `01_generate_sample_data.py` to seed streaming files.
5. Deploy/run the Lakeflow pipeline.
6. Run the training notebook and validation notebook.
7. Build the AI/BI dashboard using `core/sql/dashboard_queries.sql`.
8. Review the [Free Edition notebook audit](../docs/FREE_EDITION_NOTEBOOK_AUDIT.md) and runtime-verification steps before trying the existing scheduled job.
9. Add the optional Genie, AI Search, Model Serving, App and Lakebase extensions.

## ML and validation

The daily model remains a bounded CPU Random Forest, checked against a patient-held-out majority baseline before promoting its new Champion version. The separate optional notebook also evaluates Logistic Regression, Extra Trees, and histogram gradient boosting without automatically promoting anything. See the [audited rationale](../docs/FREE_EDITION_NOTEBOOK_AUDIT.md) and note that offline repository CI does not deploy or execute the Databricks job.

## Naming

Defaults used throughout the project:

- schema: `careflow360` for split-schema DAB pipeline, or `careflow360_dev` for standalone unified quickstart
- volume: `landing`
- Lakeflow pipeline: `careflow360-hospital-pipeline`
- Job: `careflow360-end-to-end`
- ML model: `triage_acuity_model`

Your **catalog name is intentionally not hard-coded** because Databricks workspace catalogs differ by workspace. Run:

```sql
SELECT current_catalog();
```

and use that value wherever `<CATALOG>` appears.
