# CareFlow360 — Free Edition source and notebook audit

**Reviewed: November 1, 2026.** Target: Databricks Free Edition; synthetic/public data only. This is **not** a clinical application.

## Coverage and verification scope

Reviewed all nine canonical Python notebooks (about 1,773 source lines and 40 Databricks source sections), the three Lakeflow pipeline files, the six-task DAB job and offline tests. The duplicate `CareFlow360_Databricks_Free_Edition/core/` mirror and the packaged `.ipynb` were removed from the repository on 2026-11-03; the live workspace folder keeps its own working copies. Existing automated checks verify Python/notebook/YAML syntax; unit tests exercise synthetic generation, joins, model fitting and Delta/source contracts. These tests **do not execute the real Databricks job or pipeline**.

## Notebook-by-notebook findings

| Notebook | Sections | Review and outcome |
|---|---:|---|
| 00_preflight.py | 4 | Catalog, runtime, MLflow and pipeline API probes; removed unused import; no infrastructure writes. |
| 01_generate_sample_data.py | 5 | Unique per-run synthetic IDs, append-only JSON files, immutable manifests and replay guards. Capped interactive runs at 2,000 patients and 50 batches; reject non-finite delays. Default scheduled replay is now random 50–150 patients across five batches (`num_patients=random`). Interrupted multi-file runs still require manual investigation. |
| 03_create_views.py | 1 | Validates UC identifiers, creates expected presentation views (17 after the 2026-11-03 addition of `triage_predictions_current`). SELECT * over trusted source views requires review on intentional schema changes. |
| 04_train_triage_model.py | 7 | Patient-disjoint train/test split, bounded 4,000-row feature-only pandas collection and 96-tree Random Forest on at most two workers. Held-out majority-class **release gate** (macro-F1 must exceed dummy; balanced accuracy must not fall below dummy) plus a **Champion-comparison release policy** (added 2026-11-03): a candidate is only promoted when its macro-F1 is at least the current Champion's; otherwise the candidate is registered without the alias. A failed run leaves Champion unchanged. Uses the already-fitted pipeline for scoring; insert-only Delta MERGE preserves prior predictions. Gates are not clinical validation. |
| 05_create_policy_docs.py | 3 | Idempotent synthetic policy chunks and change-sensitive CDF-compatible Delta MERGE; optional AI Search setup is separate. |
| 06_validate.py | 5 | Required-table presence, patient uniqueness, prediction bounds, per-run stream manifest, exact Bronze/Silver/Gold insert counts and KPI freshness. Swapped full-table counts for bounded LIMIT 1 existence probes. Requires one KPI row and rejects future-dated snapshots; finite age setting. |
| 07_ai_functions.py | 7 | SQL aggregates and explicitly non-AI moving-average baseline; removed an unnecessary global sort before a temp view. No paid foundation-model endpoint required. |
| 08_upgrade_existing_daily_job.py | 3 | Guarded, dry-run-by-default upgrade to the existing job/pipeline IDs. Not executed by GitHub. Do not apply until the workspace's code paths, schedules and existing state are checked. |
| 09_compare_free_edition_models.py | 5 | Manual CPU-only experiment; patient-disjoint train, validation and test. Added Extra Trees, checked degenerate held-out labels, logged majority baseline *and* selected model test results. No automatic registration/promotion; not part of daily job. |

## Pipeline and job findings

- **Bronze:** explicit JSON schemas and ingest metadata, with early null-ID drops. An event dropped by Bronze cannot later appear in the Silver quarantine table.
- **Silver:** quality gates, watermark-based event-ID deduplication and AUTO CDC SCD2 location history. **Known remaining scalability issue:** event-ID-only dropDuplicates can keep state longer than intended even with an event-time watermark. dropDuplicatesWithinWatermark is a possible optimization, but changing an existing stateful operator may require checkpoint migration and a deliberate late-data policy. No unsafe state migration was performed in this GitHub-only audit.
- **Gold:** patient-and-encounter-safe location, vitals and complaint joins; only current-admitted encounter alerts; aggregations and ML feature source. Check actual runtime query plans as history grows.
- **Six-task DAB job:** max two task branches execute concurrently; six total tasks does **not** violate the Free Edition limit of five *concurrent* tasks. Keep the one existing triggered pipeline; do not create a duplicate through an unbound deployment.

## Why these ML models?

The input is small, synthetic **tabular** data with numeric and categorical features, not images or free-form text. The model families provide meaningful and affordable comparisons:

1. **Majority-class dummy:** minimum baseline to beat.
2. **Logistic Regression:** cheap and interpretable linear benchmark, with numeric scaling.
3. **Random Forest:** robust nonlinear bagged trees and conservative default for daily jobs.
4. **Extra Trees:** randomized tree ensemble and alternative bias/variance profile.
5. **Histogram Gradient Boosting:** relatively efficient CPU boosted trees for tabular patterns.

A deeper neural network, GPU model or LLM is not inherently better for this dataset, and may exceed Free Edition limits. XGBoost, LightGBM and CatBoost are optional later candidates **only if** dependencies are available, fair-usage quotas permit and patient-disjoint metrics improve. Do not automatically promote the best leaderboard result.

**Scientific limitation:** Synthetic acuity labels are built from synthetic vital-sign patterns. High holdout accuracy can mean the model rediscovered generator rules, not that it is safe or accurate on real patients. No clinical quality claim is made.

## Free Edition limits and acceptance

At review time: serverless-only compute, one small (2X-Small) SQL warehouse, five concurrent job tasks, one active Lakeflow pipeline per type, limited notebook runtime and fair-use quota; optional features have separate limits.

Official documentation:
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- https://docs.databricks.com/aws/en/structured-streaming/watermarks

Offline CI runs:

    python tests/static_validate.py
    python -m unittest discover -s tests -p 'test_*.py' -v

**Remaining live acceptance:** Read the [deployment-safe workspace sync plan](DATABRICKS_WORKSPACE_SYNC.md) and [runtime verification checklist](END_TO_END_VERIFICATION.md). Pull updated GitHub main into the *existing* Databricks Git folder; run the in-place upgrade helper in dry-run; verify checkpoint compatibility and run counts; then run any explicitly authorized job update. GitHub CI alone does not deploy code, prove MLflow model registration or confirm production runtime reliability.
