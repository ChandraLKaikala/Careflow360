# CareFlow360 Architecture

## Logical architecture

```text
Kaggle / synthetic event source
        |
        v
Unity Catalog managed volume
        |
        v
Auto Loader / Lakeflow
        |
  +-----+-------+-------------+
  |             |             |
Bronze ADT   Bronze Triage  Bronze Vitals ...
  |             |             |
  +-------------+-------------+
                |
                v
              SILVER
  dedupe / quality / enrichment / watermarks
                |
                +--> AUTO CDC -> SCD2 patient location history
                |
                v
               GOLD
  Patient 360 / command center / alerts / ML features
       |          |          |          |
       v          v          v          v
    AI/BI      Genie       MLflow    AI Search
                              |          |
                              v          v
                       Model Serving   RAG
                              \          /
                               Databricks App
                                      |
                                   Lakebase
```

## Why one schema with layer prefixes?

The project uses one Unity Catalog schema (`careflow360_dev`) and prefixes tables with `bronze_`, `silver_`, and `gold_`. In a paid multi-team production estate you might isolate environments and layers using catalogs/schemas. For Free Edition, one project schema makes deployment simpler and minimizes resource overhead while retaining the medallion boundaries and lineage.

## Streaming model

Synthetic generation only. `01_generate_sample_data.py` produces many small JSON files under Unity Catalog Volumes. Auto Loader treats them as an incrementally arriving source. This lets you demonstrate event-time processing, watermarks, deduplication, streaming tables and triggered/continuous Lakeflow behavior without requiring a paid Kafka/Event Hubs infrastructure.

In a production hospital, the replay layer would be replaced by approved source integrations such as HL7/FHIR interfaces, device gateways, CDC feeds or cloud event buses.

## CDC / SCD2

`silver_adt` is the event stream. `silver_patient_location_history` uses `dp.create_auto_cdc_flow(... stored_as_scd_type="2")` keyed by patient and encounter, ordered by event timestamp. This creates a full history of location/state changes.

## Security boundary

This repository contains no real PHI. Do not use Free Edition for real protected health information. Production healthcare deployments require organization-specific security, privacy, access-control, networking, audit, contractual and regulatory controls.
