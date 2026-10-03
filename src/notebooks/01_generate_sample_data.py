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

import json
import math
import random
import re
import time
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
schema = widget("schema", "careflow360_dev")
volume = widget("volume", "landing")
num_patients = int(widget("num_patients", "1200"))
num_batches = int(widget("num_batches", "20"))
seconds_between_batches = float(widget("seconds_between_batches", "0"))
reset_events = widget("reset_events", "true").lower() == "true"
run_id = widget("run_id", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))

identifier = re.compile(r"^[A-Za-z0-9_\-]+$")
for label, value in [("catalog", catalog), ("schema", schema), ("volume", volume)]:
    if not identifier.match(value):
        raise ValueError(f"Unsafe {label}: {value!r}")

if num_patients < 50 or num_patients > 100000:
    raise ValueError("num_patients must be between 50 and 100000")
if num_batches < 1 or num_batches > 500:
    raise ValueError("num_batches must be between 1 and 500")
if seconds_between_batches < 0 or seconds_between_batches > 300:
    raise ValueError("seconds_between_batches must be between 0 and 300")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", run_id):
    raise ValueError("run_id must contain only letters, numbers, underscore or hyphen (max 40 chars)")

base = f"/Volumes/{catalog}/{schema}/{volume}"
events_root = f"{base}/events"
print("Writing events under:", events_root)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

if reset_events:
    try:
        dbutils.fs.rm(events_root, True)
    except Exception:
        pass

for stream_name in ["adt", "triage", "vitals", "chief_complaints"]:
    dbutils.fs.mkdirs(f"{events_root}/{stream_name}")

random.seed(421360)
now = datetime.now(timezone.utc).replace(microsecond=0)
base_time = now - timedelta(hours=8)

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


def iso(ts):
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def write_jsonl(path, records):
    payload = "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n"
    dbutils.fs.put(path, payload, True)


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

patient_ids = [f"P{i:06d}" for i in range(1, num_patients + 1)]
per_batch = math.ceil(num_patients / num_batches)
summary = {"triage": 0, "vitals": 0, "adt": 0, "chief_complaints": 0}

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
        encounter_id = f"E{int(patient_id[1:]):06d}-{run_id}-{batch_idx:03d}"
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
        adt_rows.append({
            "event_id": f"ADT-A01-{run_id}-{patient_id}-{batch_idx:03d}",
            "patient_id": patient_id,
            "encounter_id": encounter_id,
            "event_ts": iso(arrival),
            "event_type": "A01_ADMIT",
            "department": initial_department,
            "bed_id": f"ED-{random.randint(1, 80):03d}",
            "operation": "UPSERT",
            "source": "careflow360_synthetic",
        })

        transfer_department = None
        if random.random() < 0.62:
            if acuity <= 2:
                transfer_department = random.choice(["ICU", "Telemetry", "Observation"])
            else:
                transfer_department = random.choice(["Observation", "Medical", "Surgical"])
            transfer_ts = arrival + timedelta(minutes=random.randint(45, 180))
            adt_rows.append({
                "event_id": f"ADT-A02-{run_id}-{patient_id}-{batch_idx:03d}",
                "patient_id": patient_id,
                "encounter_id": encounter_id,
                "event_ts": iso(transfer_ts),
                "event_type": "A02_TRANSFER",
                "department": transfer_department,
                "bed_id": f"{transfer_department[:3].upper()}-{random.randint(1, 60):03d}",
                "operation": "UPSERT",
                "source": "careflow360_synthetic",
            })

        if random.random() < 0.33:
            discharge_ts = arrival + timedelta(minutes=random.randint(220, 470))
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
dbutils.fs.put(f"{base}/source_manifest.json", json.dumps(manifest, indent=2), True)

print(json.dumps(manifest, indent=2))
print("Synthetic micro-batches are ready. Start/refresh the Lakeflow pipeline next.")