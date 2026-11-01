-- Run inside the careflow_app Lakebase Postgres database.

CREATE TABLE IF NOT EXISTS operations_alerts (
    alert_id        TEXT PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    source_event_ts TIMESTAMPTZ,
    alert_type      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'OPEN',
    assigned_to     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    note             TEXT
);

CREATE INDEX IF NOT EXISTS idx_operations_alerts_status
ON operations_alerts(status, created_at DESC);

CREATE TABLE IF NOT EXISTS discharge_tasks (
    task_id       TEXT PRIMARY KEY,
    patient_id    TEXT NOT NULL,
    task_type     TEXT NOT NULL,
    owner         TEXT,
    status        TEXT NOT NULL DEFAULT 'OPEN',
    due_at        TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS command_center_comments (
    comment_id    TEXT PRIMARY KEY,
    entity_type   TEXT NOT NULL,
    entity_id     TEXT NOT NULL,
    author        TEXT,
    comment_text  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
