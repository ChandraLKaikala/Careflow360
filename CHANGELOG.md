# CareFlow360 Changelog

## [2026-11-03] - Random Daily Patient Volume and Full-Stack Consistency Sweep

- **Requirement change:** the scheduled generator now runs with `num_patients: "random"` — notebook 01 resolves it to a random 50-150 patients per run (inside the unchanged 50-2000 guard) and publishes the actual count through the run manifest and job task values. Notebook 06 and notebook 08's post-upgrade verification now validate the run's actual inserted count instead of assuming a fixed +100.
- Live job 484367296292508 ("CareFlow360 Daily Incremental Pipeline") upgraded in place to six tasks; every task description now matches its real purpose (no more copy-pasted "presentation views" descriptions).
- Notebook 03 now also maintains two dashboard-backed gold analytics assets: `careflow360_gold.triage_model_quality` (predicted-vs-actual acuity view, pure SQL so no per-run AI cost) and `careflow360_gold.chief_complaint_categories` (ai_classify table, built once on demand to protect Free Edition AI-function quota). Both source copies (canonical and DAB mirror) are back in sync with the live notebook, including identifier validation.
- Pipeline layer docstrings no longer reference the retired `careflow360_dab` working schema (live schema is `careflow360`).
- DAB `03_jobs.yml`, contract tests (`test_end_to_end_contracts`, `test_incremental_insert_contract`) and QUICKSTART now assert/document the random daily volume; the stale "+100/day" table comment on `careflow360_gold.triage_predictions` was corrected.
- **No new Databricks resources were created by these changes:** the insert-only contract, 02:00 UTC schedule, pipeline wiring and schema layout are unchanged.

## [2026-11-01] - Free Edition Notebook and ML Audit

- Reviewed all nine canonical Databricks notebooks, packaged notebook cell, mirrored sources, pipeline layers and job wiring; recorded findings in [Free Edition Notebook Audit](docs/FREE_EDITION_NOTEBOOK_AUDIT.md).
- Capped one synthetic replay at 2,000 patients/50 batches and rejected non-finite pacing parameters without changing scheduled 100-patient defaults.
- Reduced validation full-table scans to bounded presence checks while preserving exact ten-layer per-run row count validation and adding stricter KPI freshness checks.
- Bounded driver ML samples to 4,000 projected feature rows and required held-out macro-F1 above the dummy baseline before a retrained model is registered/promoted.
- Reused the fitted model for incremental predictions; extended the manual CPU comparison with Extra Trees and held-out dummy metrics.
- Corrected stale Free Edition/Kaggle documentation and removed redundant aggregate sorting.
- **No Databricks workspace deployment, Spark streaming checkpoint migration, or live MLflow run was performed by these GitHub changes.**


## [2026-11-01] - Optional Command Center Quality and Performance

- Added offline-tested app resource validation (three-part UC identifiers, SQL warehouse ID, HTTPS host).
- Reworked the Streamlit dashboard with cached, time-bounded aggregate SQL, manual refresh, clearer freshness warnings and trend charts.
- Isolated section-level query errors, retained diagnostic details in protected logs and added regression tests.
- Documented cache duration, result limits, installation prerequisites and synthetic-only restrictions.
- **Source changes only:** no Databricks App deployment or live end-to-end validation is implied.


## [2026-10-03] - Daily Incremental Pipeline & Project Optimization

### 🎯 Major Changes

#### 1. Daily Incremental Data Generation Job
- **Created**: Automated daily job for continuous data growth
- **Job ID**: 484367296292508
- **Schedule**: Daily at 2:00 AM UTC
- **Purpose**: Generates 100 new patients daily across all layers

**Job Workflow:**
1. Generate 100 new patients → landing zone
2. Refresh pipeline → bronze/silver/gold
3. Update presentation views
4. Retrain ML model with updated dataset

**Growth Projection:**
- Day 0: 600 patients (baseline)
- Week 1: 1,300 patients
- Month 1: 3,600 patients  
- Month 3: 9,600 patients

#### 2. Removed Optional Kaggle Integration
- **Removed**: `src/notebooks/02_kaggle_normalize.py`
- **Reason**: Optional feature not required for core functionality
- **Impact**: Project runs 100% standalone with synthetic data
- **Updated**: `docs/KAGGLE_DATA.md` to document removal

### 📊 Data Growth Across Layers

**16 out of 17 tables grow daily:**

**Bronze Layer (4 tables)** - All grow ✅
- triage: +100/day
- vitals: +~340/day
- adt: +~230/day
- chief_complaints: +100/day

**Silver Layer (6 tables)** - 5 grow, 1 conditional ✅
- triage, vitals, adt, chief_complaints, patient_location_history grow
- triage_quarantine: Only grows if data quality issues

**Gold Layer (7 tables)** - 6 grow, 1 static ✅
- patient_360, department_hourly, live_alerts, triage_training grow
- triage_predictions: Retrains daily with new data
- hospital_command_center: Recalculates (always 1 row)
- policy_chunks: Static reference documents (6 rows)

### 🔧 Technical Details

**Data Generation Parameters:**
```python
num_patients: 100      # New patients per day
num_batches: 2         # Distribution batches
reset_events: false    # Append mode (critical!)
```

**Pipeline Behavior:**
- Incremental refresh (not full refresh)
- Auto Loader detects new landing zone files
- Delta tables merge/append automatically

### 📝 Current State

**Total Tables**: 17 (4 bronze + 6 silver + 7 gold)
**Current Patient Count**: 600 → Growing to 700 after first run
**Pipeline Status**: IDLE (ready for daily runs)
**Job Status**: ACTIVE (scheduled)

### 🎓 Portfolio Impact

This update demonstrates:
- ✅ Automated data pipeline orchestration
- ✅ Incremental ETL patterns
- ✅ ML model retraining with growing datasets
- ✅ Production-ready scheduling
- ✅ Self-contained synthetic data generation

### 🔗 References

- **Job Dashboard**: #job-484367296292508
- **Pipeline**: CareFlow360_DAB_Pipeline (dde7c33b-8715-4666-8f6b-ad2af0848966)
- **Documentation**: See `docs/` folder for architecture details

---

## Previous Updates

See git history for earlier changes.
