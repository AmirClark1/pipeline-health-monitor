# Pipeline Health Monitor

A Snowflake-native Streamlit application that helps operators answer one question quickly:

> **What should I investigate first?**

Pipeline Health Monitor prioritizes active failures, recently recovered pipelines, repeated failure patterns, stale candidates, runtime slowdowns, and root-cause signals using precomputed Snowflake reporting tables.

## Current Release

**v7.9.3 — Phase 1.5 Production Candidate**

This repository contains a scrubbed portfolio version. Environment-specific database names, schemas, roles, warehouses, account identifiers, pipeline names, and operational details are intentionally excluded.

## Key Capabilities

- Operations HUD with current operational status
- Viewer-specific, warehouse-metered dashboard credit estimate
- Active failure prioritization
- Recent failure and recovery visibility
- Failure-frequency risk classification
- Week-over-week trend indicators
- Potentially stale pipeline review
- Runtime slowdown detection against successful-run baselines
- Root-cause grouping and detailed error review
- Pipeline search and guided investigation steps
- Report freshness warnings
- Built-in usage guide and technical reference
- Responsive dark-mode interface

## Operations HUD

The floating Operations HUD combines two high-value signals:

1. **Pipeline attention required** — summarizes the highest-priority operational issue.
2. **Estimated Pipeline Health Monitor credits today** — estimates the current viewer's share of compute on a shared Streamlit warehouse.

The credit value is deliberately labeled as an **estimate**, not an exact Snowflake billing figure. It is apportioned from warehouse metering using the viewer's recorded Pipeline Health Monitor activity.

## Investigation Workflow

1. **Operations HUD** — review the immediate operational summary.
2. **Active Failures** — investigate pipelines whose latest execution failed.
3. **Recent Failures / Recovered** — confirm that recovery is stable.
4. **High Risk** — review repeated failures or elevated warning signals.
5. **Potentially Stale** — validate pipelines whose last recorded run exceeds the configured review threshold.
6. **Runtime Watchlist** — inspect abnormal execution duration.
7. **Root Causes** — identify recurring technical patterns.
8. **Pipeline Investigation** — search a specific object and review suggested next steps.

## Architecture

```text
Snowflake metadata sources
        ↓
SQL transformation and classification logic
        ↓
Precomputed reporting tables and controlled views
        ↓
Streamlit in Snowflake application
        ↓
Operator triage and investigation workflow
```

### Design principles

- Snowflake-native
- Read-only application layer
- Precomputed reporting data
- Cost-conscious query behavior
- Explainable rule-based classifications
- Operator-focused workflow
- Safe fallback demo data for portfolio use

## Repository Files

| File | Description |
|---|---|
| `streamlit_app.py` | Scrubbed v7.9.3 Streamlit application |
| `requirements.txt` | Python dependencies |
| `README.md` | Project overview, architecture, and setup notes |

## Configuration

The portfolio version uses placeholder values near the top of `streamlit_app.py`:

```python
CONFIG = {
    "REPORT_DATABASE": "YOUR_DATABASE",
    "REPORT_SCHEMA": "YOUR_PIPELINE_MONITOR_SCHEMA",
    "SUMMARY_REPORT_TABLE": "PIPELINE_SUMMARY_REPORT",
    "ROOT_CAUSE_REPORT_TABLE": "PIPELINE_ROOT_CAUSE_REPORT",
    "OPERATIONS_HUD_DEMO_MODE": True,
}
```

For a controlled Snowflake deployment:

1. Replace the placeholder database and schema.
2. Deploy the required report tables, credit report view, and query-audit table.
3. Grant the Streamlit owner role access to those objects.
4. Set `OPERATIONS_HUD_DEMO_MODE` to `False` only after the live reporting objects are available.

## Expected Reporting Objects

### `PIPELINE_SUMMARY_REPORT`

The application expects operational fields such as:

- pipeline identity and object type
- latest status and execution time
- 24-hour and seven-day failure counts
- prior-period failure counts and trends
- risk and investigation classifications
- stale hours
- successful runtime baselines
- runtime status and slowdown ratios
- error details and report-generated timestamp

### `PIPELINE_ROOT_CAUSE_REPORT`

Expected fields include:

- `ERROR_MESSAGE`
- `FAILURE_COUNT`
- `PERCENT_OF_FAILURES`
- optional root-cause category

### Credit estimation objects

The live Operations HUD uses controlled reporting objects similar to:

- `PIPELINE_MONITOR_QUERY_AUDIT`
- `PIPELINE_MONITOR_USER_CREDIT_REPORT`

These map owner-rights Streamlit queries to the actual viewer and estimate the viewer's share of shared warehouse compute.

## Metadata Sources

The backend reporting layer can be built from Snowflake metadata sources including:

- `TASK_HISTORY`
- `SERVERLESS_TASK_HISTORY`
- `DYNAMIC_TABLE_REFRESH_HISTORY`
- `QUERY_HISTORY`
- `WAREHOUSE_METERING_HISTORY`

The Streamlit application does not perform broad live metadata scans during normal use. It reads precomputed reporting objects instead.

## Runtime Classification

Runtime alerts compare the latest successful runtime with the seven-day median:

| Status | Rule |
|---|---|
| Critical Slowdown | At least 3× median and at least 120 seconds slower |
| High Slowdown | At least 2× median and at least 60 seconds slower |
| Elevated | At least 1.5× median and at least 30 seconds slower |
| Normal | Baseline exists and thresholds are not met |
| Insufficient History | Fewer than three successful measurements |

These are investigation signals, not SLA violations.

## Current Limitations

- Staleness uses elapsed time and is not yet schedule-aware.
- Dependency-aware downstream impact analysis is not yet implemented.
- AWS DMS, Glue, Lambda, and CloudWatch integration remain future enhancements.
- The credit value is an apportioned estimate and will not exactly match billing exports.

## Roadmap

### Phase 2 — Broader operational visibility

- AWS DMS monitoring
- AWS Glue job visibility
- AWS Lambda execution signals
- CloudWatch and EventBridge integration
- dependency mapping
- schedule-aware stale thresholds

### Phase 3 — Predictive monitoring

- reliability scoring
- failure probability indicators
- anomaly detection
- failure-pattern detection
- ownership and business-criticality reporting
- SLA and recovery metrics

## Security and Scrubbing

Do not commit:

- real Snowflake account identifiers
- workplace database, schema, role, warehouse, or user names
- secrets, keys, tokens, passwords, or connection files
- raw operational exports
- internal email addresses or alert messages
- screenshots containing sensitive pipeline or environment details

## Skills Demonstrated

Snowflake · Snowpark · Streamlit · Python · SQL · Data engineering · Operational monitoring · Root-cause analysis · Cost attribution · UX design · Production hardening

## Project Status

**Phase 1.5 production candidate.** The current version is a complete Snowflake-native operational monitoring portfolio project and a stable baseline for future capability development.
