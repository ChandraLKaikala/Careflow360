# CareFlow360 Changelog

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
