# CareFlow360 Quickstart

This path works without downloading any external dataset.

## 1. Bring the repository into Databricks safely

This GitHub repository contains project sources, **not** a prebuilt ZIP archive.
Use a Databricks **Git folder** linked to this GitHub repository (recommended), then
check out a reviewed branch or pull the merged `main` branch.

**Existing CareFlow360 workspace:** Before connecting code to any already-deployed job
or pipeline, read [the workspace sync runbook](docs/DATABRICKS_WORKSPACE_SYNC.md).
Pulling Git files does not automatically change the existing daily job or its tasks.
Do not create a second pipeline by deploying a fresh, unbound bundle.

## 2. Discover your catalog

Open `src/notebooks/00_preflight.py` as a notebook and run it on serverless compute.

Or execute in SQL Editor:

```sql
SELECT current_catalog();
```

Copy the returned catalog name. The examples below call it `<CATALOG>`.

## 3. Create the project schema and volume

Run `sql/00_setup.sql` after replacing `<CATALOG>`.

Minimum commands:

```sql
USE CATALOG `<CATALOG>`;
CREATE SCHEMA IF NOT EXISTS careflow360_dev;
CREATE VOLUME IF NOT EXISTS careflow360_dev.landing;
```

## 4. Generate replayable hospital events

Run:

`src/notebooks/01_generate_sample_data.py`

Set widgets:

- `reset_events`: `false` (safe append; do not delete previous landing files)
- `catalog`: your catalog
- `schema`: `careflow360_dev`
- `volume`: `landing`
- `num_patients`: `1200` for the initial seed (the scheduled daily job instead uses `random`, inserting a random 50-150 patients per run)
- `num_batches`: `20`
- `seconds_between_batches`: `0` for a fast seed, or `5` for a visible real-time replay

It writes multiple JSON files into:

```text
/Volumes/<CATALOG>/careflow360_dev/landing/events/adt/
/Volumes/<CATALOG>/careflow360_dev/landing/events/triage/
/Volumes/<CATALOG>/careflow360_dev/landing/events/vitals/
/Volumes/<CATALOG>/careflow360_dev/landing/events/chief_complaints/
```

Each file represents a micro-batch, allowing Auto Loader to process the data incrementally.

### Real-time demo mode

For a visible streaming demo, switch the pipeline to **Continuous**, start it, then run `01_generate_sample_data.py` in another tab with:

```text
reset_events = false
seconds_between_batches = 5
```

The notebook publishes uniquely named JSON micro-batches every five seconds. Auto Loader ingests them while the Lakeflow pipeline is running. Stop the continuous pipeline after the demo to preserve Free Edition quota.

## 5. Create the Lakeflow pipeline in the UI

If you want the easiest Free Edition path:

1. Go to **Jobs & Pipelines → Pipelines → Create pipeline**.
2. Name: `careflow360-hospital-pipeline`.
3. Select **serverless** compute.
4. Choose **Triggered** mode initially.
5. Add the source folder:
   `core/src/pipelines/`
6. Catalog: `<CATALOG>`.
7. Schema: `careflow360_dev`.
8. Add these pipeline configuration entries:

```text
careflow.catalog          = <CATALOG>
careflow.schema           = careflow360_dev
careflow.landing_volume   = landing
```

9. Start the pipeline.

The graph should build Bronze, Silver and Gold assets plus the SCD2 location-history table.

## 6. Train the triage model

Run:

`src/notebooks/04_train_triage_model.py`

with:

```text
catalog = <CATALOG>
schema  = careflow360_dev
```

This tracks the experiment in MLflow, registers a Unity Catalog model and writes batch predictions to `gold_triage_predictions`.

## 7. Validate everything

Run:

`src/notebooks/06_validate.py`

It checks required tables, row counts and basic Gold-layer invariants.

## 8. Build AI/BI dashboard

Open `sql/dashboard_queries.sql` in SQL Editor, replace `<CATALOG>`, and use the result sets to create the dashboard described in `docs/DASHBOARD_GENIE.md`.

## 9. Deploy with DAB instead of UI

Once the manual path works, use the production-style deployment:

```bash
databricks bundle validate --var="catalog=<CATALOG>"
databricks bundle deploy -t dev --var="catalog=<CATALOG>"
databricks bundle run -t dev careflow360_job --var="catalog=<CATALOG>"
```

`databricks.yml` deliberately requires the catalog variable so that it cannot accidentally deploy into the wrong catalog.

## 10. Add Kaggle datasets

Follow `docs/KAGGLE_DATA.md`. Do not redistribute Kaggle files with this repository; download them yourself under their respective licenses and upload them into the project volume.

## 11. Optional extensions

Deploy the [Streamlit command center app](extensions/app/README.md) after the core is healthy. It contains its own README because some resources need workspace-specific IDs or feature availability.

The former `genie`, `ai_search`, `model_serving`, and `lakebase` template bundles were removed from this repository: those features are not available on Databricks Free Edition and were never part of the deployed project.
