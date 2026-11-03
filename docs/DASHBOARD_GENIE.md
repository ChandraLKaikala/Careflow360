# AI/BI Dashboard + Genie Agent

## Dashboard

**CareFlow360 — Hospital Command Center** (canonical, published): 3 pages, 29 widgets, 18 SQL datasets. Pages: *Command Overview* (KPIs, census, acuity, flow), *Operations & Freshness* (hours since last KPI snapshot/pipeline refresh/successful job, last-10 job runs, daily run coverage and missing-date counters, source-timestamp ages), and *Quality & Model Monitoring* (Patient 360 integrity, invalid prediction labels, batch predictions, alert sources).

**CareFlow360 — Hospital Operations Dashboard** (companion, published): 1 page, 14 widgets, 6 datasets — a lightweight summary including daily arrivals, prediction distribution, model agreement and AI complaint categories. Treat the Command Center as the single source of truth for operational review; keep this one for quick sharing.

Use the SQL datasets in `sql/dashboard_queries.sql` to create a dashboard named:

**CareFlow360 — Hospital Command Center**

Recommended layout:

1. KPI cards: active patients, ED patients, ICU patients, operational alerts.
2. Line/stacked chart: ADT events by hour and event type.
3. Bar chart: active patient count by department.
4. Table: latest operational alerts.
5. Donut/bar: triage-acuity distribution.
6. Bar: model-prediction distribution.

Add filters for department and event hour where applicable.

## Genie Agent

The former Genie bundle was removed from this repository (it was never part of the Free Edition deployment); create the Genie space manually from the datasets above if your workspace has Genie.

Recommended questions:

- How many patients are currently in the emergency department?
- Which department currently has the most admitted patients?
- Show the latest synthetic operational alerts.
- Compare admission, transfer and discharge events by hour.
- What is the triage-acuity mix by department?

Use `sql/genie_benchmarks.sql` to independently verify Genie answers.

## Guardrail instruction

The provided Genie configuration tells the agent that CareFlow360 contains synthetic/public portfolio data, and that it should answer operational analytics questions rather than provide diagnoses, treatment advice or clinical recommendations.
