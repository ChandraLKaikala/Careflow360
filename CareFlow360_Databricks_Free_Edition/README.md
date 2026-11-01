# CareFlow360 — Real-Time Intelligent Hospital Operations Lakehouse

> **Existing workspace caution:** This Free Edition package mirrors the root DAB sources.
> GitHub changes are not automatically deployed to workspace notebooks/jobs.
> Before a live deploy, follow [the existing-job sync runbook](../docs/DATABRICKS_WORKSPACE_SYNC.md)
> to avoid duplicate pipelines and preserve the 02:00 UTC job. Use synthetic data only.

An end-to-end Databricks Free Edition portfolio project that simulates a hospital command center using synthetic/public healthcare data.

## What this project demonstrates

- Unity Catalog schemas, managed volumes, tables, views, lineage and governance
- Lakeflow Spark Declarative Pipelines using the current `pyspark.pipelines` API
- Bronze → Silver → Gold medallion architecture
- Auto Loader streaming ingestion, data-quality expectations and a retained quarantine table
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
- Kaggle adapters for Triagegeist and healthcare-monitoring datasets

## Project scenario

CareFlow360 ingests near-real-time hospital events:

1. ADT events — admission, transfer and discharge
2. ED triage events — demographics, arrival mode, vitals and triage acuity
3. Bedside monitoring events — HR, SpO2, BP, temperature and respiratory rate
4. Chief-complaint text

Static public datasets are transformed into event streams through a configurable micro-batch event-replay layer (`seconds_between_batches` can be set to a nonzero value for a live demo). This is deliberate: Kaggle data is historical, while the project demonstrates the same incremental architecture used for live feeds such as HL7/FHIR/device streams.

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
3. Create the schema and landing volume using `core/sql/00_setup.sql` or DAB.
4. Run `01_generate_sample_data.py` to seed streaming files.
5. Deploy/run the Lakeflow pipeline.
6. Run the training notebook and validation notebook.
7. Build the AI/BI dashboard using `core/sql/dashboard_queries.sql`.
8. Add Kaggle data when ready using `02_kaggle_normalize.py`.
9. Add the optional Genie, AI Search, Model Serving, App and Lakebase extensions.

## Naming

Defaults used throughout the project:

- schema: `careflow360_dev`
- volume: `landing`
- Lakeflow pipeline: `careflow360-hospital-pipeline`
- Job: `careflow360-end-to-end`
- ML model: `triage_acuity_model`

Your **catalog name is intentionally not hard-coded** because Databricks workspace catalogs differ by workspace. Run:

```sql
SELECT current_catalog();
```

and use that value wherever `<CATALOG>` appears.
