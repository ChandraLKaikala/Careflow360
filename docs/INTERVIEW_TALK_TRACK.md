# Interview Talk Track

## 60-second explanation

“I built CareFlow360, a synthetic real-time hospital operations lakehouse on Databricks. Historical public data is replayed as event micro-batches to simulate ADT, ED triage, chief-complaint and bedside-monitoring feeds. Lakeflow Spark Declarative Pipelines ingest the data with Auto Loader into Bronze, enforce quality and event-time deduplication in Silver, and use AUTO CDC to maintain SCD Type 2 patient-location history. Gold materialized views power a Patient 360, hospital command center, operational alert view and ML feature table. MLflow tracks a multiclass triage experiment and registers a Unity Catalog model. I also packaged optional Genie, AI Search, Model Serving, Databricks App and Lakebase resources using Declarative Automation Bundles.”

## Architecture decisions worth explaining

- Static Kaggle data cannot honestly be called real-time; replay converts records into incremental events.
- Event time and ingestion time are separate.
- Bronze preserves source semantics; Silver owns quality/dedup/state; Gold serves domain use cases.
- AUTO CDC is used for mutable patient-location state/history rather than hand-written merge logic.
- Operational alerts are deliberately labeled synthetic/non-clinical.
- Lakebase is used for application transactions/state, not replacement analytical storage.
- AI Search indexes a governed UC table; Genie only receives curated Gold tables.
- DAB separates code and environment-specific values such as catalog, warehouse ID and model version.

## Production changes

For a real hospital deployment, replace event replay with approved HL7/FHIR/device integrations; use organization-approved PHI controls, networking, identity and audit policies; add source contracts, observability, recovery procedures, CI/CD, tests, model validation, bias/drift review and appropriate clinical governance.
