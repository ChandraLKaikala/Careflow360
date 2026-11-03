# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Generate synthetic real-time hospital events
# MAGIC Creates micro-batch JSON files for ADT, triage, bedside vitals and chief complaints.
# MAGIC The data is synthetic and intentionally **not** clinically validated.

# COMMAND ----------

import hashlib
import json
import math
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

# COMMAND ----------

def widget(name, default, label=None):
    try:
        dbutils.widgets.text(name, str(default), label or name)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return str(default)

catalog = widget("catalog", spark.sql("SELECT current_catalog() AS c").first()["c"])
schema = widget("schema", "careflow360")
volume = widget("volume", "landing")
num_patients_setting = widget("num_patients", "100").strip()
if num_patients_setting.lower() == "random":
    # Daily incremental runs may insert a random patient count instead of a
    # fixed 100. Pick inside the quota-safe 50-150 band (still within the
    # 50-2000 guard below) so one run cannot exhaust Free Edition quota.
    num_patients = random.randint(50, 150)
    print(f"num_patients='random': chose {num_patients} patients for this run")
else:
    num_patients = int(num_patients_setting)
num_batches = int(widget("num_batches", "5"))
seconds_between_batches = float(widget("seconds_between_batches", "0"))
reset_events = widget("reset_events", "false").lower() == "true"
run_id = widget("run_id", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8])

identifier = re.compile(r"^[A-Za-z0-9_\-]+$")
for label, value in [("catalog", catalog), ("schema", schema), ("volume", volume)]:
    if not identifier.match(value):
        raise ValueError(f"Unsafe {label}: {value!r}")

# One interactive replay must not exhaust the serverless notebook's quota.
# The scheduled daily job passes num_patients='random', which resolves to a
# random 50-150 patient count per run before the guards below are applied.
if num_patients < 50 or num_patients > 2000:
    raise ValueError("num_patients must be between 50 and 2000")
if num_batches < 1 or num_batches > 50:
    raise ValueError("num_batches must be between 1 and 50")
if not math.isfinite(seconds_between_batches) or not 0 <= seconds_between_batches <= 300:
    raise ValueError("seconds_between_batches must be between 0 and 300")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", run_id):
    raise ValueError("run_id must contain only letters, numbers, underscore or hyphen (max 40 chars)")

base = f"/Volumes/{catalog}/{schema}/{volume}"
events_root = f"{base}/events"
print("Writing events under:", events_root)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

# Resetting landing files can invalidate Auto Loader state. A daily append job
# must never delete previously ingested event files.
if reset_events:
    raise ValueError("reset_events=true is disabled: never delete an incremental landing volume")

manifest_dir = f"{base}/manifests"
dbutils.fs.mkdirs(manifest_dir)
if any(entry.name == f"{run_id}.json" for entry in dbutils.fs.ls(manifest_dir)):
    raise ValueError(f"Run ID {run_id!r} already exists; use a new run_id")

for stream_name in ["adt", "triage", "vitals", "chief_complaints"]:
    stream_dir = f"{events_root}/{stream_name}"
    dbutils.fs.mkdirs(stream_dir)
    # A prior interrupted run may have event files but no manifest. Do not
    # overwrite paths that Auto Loader might have already checkpointed.
    if any(x.name.startswith(f"batch_{run_id}_") for x in dbutils.fs.ls(stream_dir)):
        raise ValueError(f"Partial run {run_id!r} already has {stream_name} files; investigate before replaying")

random.seed(int(hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16], 16))
now = datetime.now(timezone.utc).replace(microsecond=0)
# Leave enough lookback for all batches and discharge offsets. This makes
# synthetic event timestamps historical, not accidentally future-dated.
largest_batch = math.ceil(num_patients / num_batches)
lookback_minutes = (num_batches - 1) * 10 + 480 + math.ceil(largest_batch * 7 / 60) + 2
base_time = now - timedelta(minutes=lookback_minutes)

FIRST_COMPLAINTS = [
    "chest discomfort and shortness of breath",
    "abdominal pain with nausea",
    "fever and persistent cough",
    "dizziness and weakness",
    "ankle injury after fall",
    "headache with light sensitivity",
    "worsening back pain",
    "vomiting and dehydration",
    "palpitations",
    "minor laceration",
]
DEPARTMENTS = ["Emergency", "Observation", "Medical", "Surgical", "ICU", "Telemetry"]
ARRIVAL_MODES = ["Walk-in", "Ambulance", "Wheelchair", "Police", "Transfer"]
SEXES = ["F", "M", "X"]

# Physical bed capacity per department: (bed-id prefix, bed count).
BED_POOLS = {
    "Emergency": ("ED", 80),
    "Observation": ("OBS", 60),
    "Medical": ("MED", 60),
    "Surgical": ("SUR", 60),
    "ICU": ("ICU", 40),
    "Telemetry": ("TEL", 40),
}


def load_open_beds():
    """Beds still occupied by an open encounter in the live pipeline.

    Read-only best effort: the generator never writes pipeline tables.
    Encounters whose latest ADT event is older than 48 hours are ignored:
    stale open encounters are legacy artifacts of the pre-refactor
    generator (which left encounters open forever), and treating them as
    occupied would starve every bed pool. On a fresh install, an unreadable
    table, or a foreign layout this safely returns an empty set — the
    within-run uniqueness guarantee still holds either way.
    """
    cutoff = iso(now - timedelta(hours=48))
    for table_name in (
        f"{catalog}.{schema}.silver_adt",
        f"{catalog}.careflow360_silver.adt",
    ):
        try:
            if not spark.catalog.tableExists(table_name):
                continue
            rows = spark.sql(
                f"""
                SELECT DISTINCT bed_id AS bed FROM (
                    SELECT patient_id, encounter_id, bed_id, event_type, event_ts,
                           ROW_NUMBER() OVER (
                               PARTITION BY patient_id, encounter_id
                               ORDER BY event_ts DESC
                           ) AS rn
                    FROM {table_name}
                    WHERE bed_id IS NOT NULL AND event_ts >= TIMESTAMP '{cutoff}'
                ) WHERE rn = 1 AND event_type != 'A03_DISCHARGE'
                """
            ).collect()
            return {row["bed"] for row in rows}
        except Exception as exc:
            print(
                f"Bed-occupancy read from {table_name} unavailable ({exc}); "
                "continuing with within-run bed uniqueness only."
            )
    return set()


class BedAllocator:
    """Capacity-aware bed assignment for one generation run.

    Invariants:
      * At most one concurrently-admitted patient per bed WITHIN this run:
        a bed is handed out only for a [start, end) interval that does not
        overlap an interval already granted for that bed. end=None models
        an encounter that stays admitted (nothing is freed this run);
        transfer releases the ED bed and discharge releases the final bed.
      * Beds reported occupied by still-open encounters in the live
        pipeline (load_open_beds) are never reused by this run.
      * When a pool is exhausted the patient gets bed_id=None (realistic
        ED/departmental boarding under capacity pressure). The allocator
        never fabricates a bed shared by two admitted patients.
    """

    def __init__(self, pools, preoccupied=()):
        self._pools = pools
        self._taken = set(preoccupied)
        self._intervals = {}

    _NEVER = datetime.max.replace(tzinfo=timezone.utc)

    @classmethod
    def _overlaps(cls, a_start, a_end, b_start, b_end):
        # end=None means the encounter stays admitted: it never closes, so
        # treat it as open until datetime.max (comparable with tz-aware rows).
        a_end = cls._NEVER if a_end is None else a_end
        b_end = cls._NEVER if b_end is None else b_end
        return a_start < b_end and b_start < a_end

    def allocate(self, department, start, end):
        prefix, size = self._pools[department]
        for number in range(1, size + 1):
            bed = f"{prefix}-{number:03d}"
            if bed in self._taken:
                continue
            busy = self._intervals.get(bed)
            if busy is None:
                busy = self._intervals[bed] = []
            if not any(self._overlaps(start, end, s, e) for s, e in busy):
                busy.append((start, end))
                return bed
        return None


def iso(ts):
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def write_jsonl(path, records):
    payload = "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n"
    # Never overwrite Auto Loader landing files, including after interrupted replays.
    dbutils.fs.put(path, payload, False)


def synthetic_triage(age):
    # Portfolio-only signal generation: these thresholds are not clinical guidance.
    severity = random.random()
    if severity < 0.06:
        acuity = 1
        hr = random.randint(120, 155)
        spo2 = random.randint(82, 91)
        sbp = random.randint(78, 95)
        rr = random.randint(26, 36)
        temp = round(random.uniform(38.0, 40.2), 1)
        gcs = random.randint(9, 13)
    elif severity < 0.20:
        acuity = 2
        hr = random.randint(105, 135)
        spo2 = random.randint(89, 95)
        sbp = random.randint(88, 110)
        rr = random.randint(22, 30)
        temp = round(random.uniform(37.4, 39.6), 1)
        gcs = random.randint(12, 15)
    elif severity < 0.52:
        acuity = 3
        hr = random.randint(85, 115)
        spo2 = random.randint(93, 98)
        sbp = random.randint(100, 145)
        rr = random.randint(16, 25)
        temp = round(random.uniform(36.4, 38.5), 1)
        gcs = random.randint(14, 15)
    elif severity < 0.82:
        acuity = 4
        hr = random.randint(68, 105)
        spo2 = random.randint(95, 100)
        sbp = random.randint(105, 150)
        rr = random.randint(14, 22)
        temp = round(random.uniform(36.2, 37.9), 1)
        gcs = 15
    else:
        acuity = 5
        hr = random.randint(60, 96)
        spo2 = random.randint(96, 100)
        sbp = random.randint(108, 145)
        rr = random.randint(12, 20)
        temp = round(random.uniform(36.3, 37.5), 1)
        gcs = 15
    return acuity, hr, spo2, sbp, rr, temp, gcs

# COMMAND ----------

# An append run must never reuse an earlier patient identifier.
patient_ids = [f"P{run_id}-{i:06d}" for i in range(1, num_patients + 1)]
per_batch = math.ceil(num_patients / num_batches)
summary = {"triage": 0, "vitals": 0, "adt": 0, "chief_complaints": 0}

# Capacity-aware allocator seeded with beds still occupied by open live
# encounters. See BedAllocator for the uniqueness invariants.
bed_allocator = BedAllocator(BED_POOLS, load_open_beds())

for batch_idx in range(num_batches):
    batch_patients = patient_ids[batch_idx * per_batch : (batch_idx + 1) * per_batch]
    if not batch_patients:
        continue

    triage_rows = []
    vital_rows = []
    adt_rows = []
    chief_rows = []

    batch_anchor = base_time + timedelta(minutes=batch_idx * 10)

    for j, patient_id in enumerate(batch_patients):
        encounter_id = f"E{patient_id[1:]}-{batch_idx:03d}"
        arrival = batch_anchor + timedelta(seconds=j * 7 + random.randint(0, 30))
        age = random.randint(1, 94)
        sex = random.choice(SEXES)
        acuity, hr, spo2, sbp, rr, temp, gcs = synthetic_triage(age)
        dbp = max(40, min(110, int(sbp * random.uniform(0.55, 0.72))))
        pain = random.randint(0, 10)
        arrival_mode = random.choices(ARRIVAL_MODES, weights=[58, 26, 7, 2, 7], k=1)[0]
        complaint = random.choice(FIRST_COMPLAINTS)

        triage_rows.append({
            "event_id": f"TRI-{run_id}-{patient_id}-{batch_idx:03d}",
            "patient_id": patient_id,
            "encounter_id": encounter_id,
            "event_ts": iso(arrival + timedelta(minutes=random.randint(1, 8))),
            "arrival_mode": arrival_mode,
            "arrival_hour": arrival.hour,
            "arrival_day": arrival.strftime("%A"),
            "age": age,
            "sex": sex,
            "systolic_bp": sbp,
            "diastolic_bp": dbp,
            "heart_rate": hr,
            "respiratory_rate": rr,
            "temperature_c": temp,
            "spo2": spo2,
            "gcs_total": gcs,
            "pain_score": pain,
            "num_prior_visits": max(0, int(random.expovariate(0.55)) - 1),
            "triage_acuity": acuity,
            "source": "careflow360_synthetic",
        })

        chief_rows.append({
            "event_id": f"CC-{run_id}-{patient_id}-{batch_idx:03d}",
            "patient_id": patient_id,
            "encounter_id": encounter_id,
            "event_ts": iso(arrival + timedelta(minutes=1)),
            "chief_complaint_raw": complaint,
            "source": "careflow360_synthetic",
        })

        initial_department = "Emergency"
        # Encounter closure rules: decide the full admit -> (transfer) ->
        # discharge timeline up front, so the capacity-aware allocator can
        # grant beds only for non-overlapping occupancy intervals — transfer
        # releases the ED bed, discharge releases the final bed. Most
        # encounters close inside the run window; the rest stay admitted,
        # which is the only way a later run sees occupied beds.
        transfer_department = None
        transfer_ts = None
        discharge_ts = None
        will_discharge = random.random() < 0.55
        if will_discharge:
            discharge_ts = arrival + timedelta(minutes=random.randint(220, 470))
        if random.random() < 0.62:
            if acuity <= 2:
                transfer_department = random.choice(["ICU", "Telemetry", "Observation"])
            else:
                transfer_department = random.choice(["Observation", "Medical", "Surgical"])
            transfer_ts = arrival + timedelta(minutes=random.randint(45, 180))
        ed_bed = bed_allocator.allocate(
            "Emergency", arrival,
            transfer_ts if transfer_ts is not None else discharge_ts,
        )
        transfer_bed = (
            bed_allocator.allocate(transfer_department, transfer_ts, discharge_ts)
            if transfer_department is not None
            else None
        )
        adt_rows.append({
            "event_id": f"ADT-A01-{run_id}-{patient_id}-{batch_idx:03d}",
            "patient_id": patient_id,
            "encounter_id": encounter_id,
            "event_ts": iso(arrival),
            "event_type": "A01_ADMIT",
            "department": initial_department,
            # None means ED boarding under capacity pressure, never a shared bed.
            "bed_id": ed_bed,
            "operation": "UPSERT",
            "source": "careflow360_synthetic",
        })

        if transfer_department is not None:
            adt_rows.append({
                "event_id": f"ADT-A02-{run_id}-{patient_id}-{batch_idx:03d}",
                "patient_id": patient_id,
                "encounter_id": encounter_id,
                "event_ts": iso(transfer_ts),
                "event_type": "A02_TRANSFER",
                "department": transfer_department,
                # None means boarding in the target department without a bed.
                "bed_id": transfer_bed,
                "operation": "UPSERT",
                "source": "careflow360_synthetic",
            })

        if will_discharge:
            adt_rows.append({
                "event_id": f"ADT-A03-{run_id}-{patient_id}-{batch_idx:03d}",
                "patient_id": patient_id,
                "encounter_id": encounter_id,
                "event_ts": iso(discharge_ts),
                "event_type": "A03_DISCHARGE",
                "department": transfer_department or initial_department,
                "bed_id": None,
                "operation": "UPSERT",
                "source": "careflow360_synthetic",
            })

        vital_count = random.randint(3, 7)
        for v in range(vital_count):
            vital_ts = arrival + timedelta(minutes=15 * v + random.randint(0, 4))
            vital_rows.append({
                "event_id": f"VIT-{run_id}-{patient_id}-{batch_idx:03d}-{v:02d}",
                "patient_id": patient_id,
                "encounter_id": encounter_id,
                "event_ts": iso(vital_ts),
                "heart_rate": max(35, min(190, hr + random.randint(-12, 12))),
                "spo2": max(75, min(100, spo2 + random.randint(-2, 3))),
                "systolic_bp": max(65, min(220, sbp + random.randint(-12, 15))),
                "diastolic_bp": max(35, min(130, dbp + random.randint(-8, 10))),
                "temperature_c": round(max(34.0, min(41.0, temp + random.uniform(-0.4, 0.5))), 1),
                "respiratory_rate": max(7, min(45, rr + random.randint(-3, 4))),
                "fall_detected": random.random() < 0.005,
                "source": "careflow360_synthetic",
            })

    batch_name = f"batch_{run_id}_{batch_idx:04d}.json"
    write_jsonl(f"{events_root}/triage/{batch_name}", triage_rows)
    write_jsonl(f"{events_root}/vitals/{batch_name}", vital_rows)
    write_jsonl(f"{events_root}/adt/{batch_name}", adt_rows)
    write_jsonl(f"{events_root}/chief_complaints/{batch_name}", chief_rows)

    summary["triage"] += len(triage_rows)
    summary["vitals"] += len(vital_rows)
    summary["adt"] += len(adt_rows)
    summary["chief_complaints"] += len(chief_rows)

    print(f"Published micro-batch {batch_idx + 1}/{num_batches}: {batch_name}")
    if seconds_between_batches > 0 and batch_idx < num_batches - 1:
        time.sleep(seconds_between_batches)

manifest = {
    "generated_at_utc": iso(now),
    "num_patients": num_patients,
    "num_batches": num_batches,
    "run_id": run_id,
    "seconds_between_batches": seconds_between_batches,
    "rows": summary,
    "disclaimer": "Synthetic portfolio data. Not clinically validated.",
}
dbutils.fs.put(f"{manifest_dir}/{run_id}.json", json.dumps(manifest, indent=2), False)
dbutils.fs.put(f"{base}/source_manifest.json", json.dumps(manifest, indent=2), True)

# Publish the exact run identity to the downstream validation task. Without
# this, a job can succeed while repeatedly refreshing the same patient IDs.
# In a standalone interactive notebook Databricks ignores taskValues.set().
dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="num_patients", value=num_patients)

print(json.dumps(manifest, indent=2))
print("Synthetic micro-batches are ready. Start/refresh the Lakeflow pipeline next.")