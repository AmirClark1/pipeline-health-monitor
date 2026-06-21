# Pipeline Health Monitor

A Snowflake-native Streamlit dashboard for pipeline health monitoring, failure trend analysis, root cause grouping, and risk-based prioritization.

This repository contains a scrubbed version of the app source. Environment-specific database, schema, role, warehouse, and account details are intentionally not committed.

## Purpose

This project helps answer one operational question:

**What should I investigate first?**

The app is designed to reduce manual review across failure emails, task history, dynamic table status, and ad hoc dashboards by surfacing the highest-priority pipeline issues first.

Skills Demonstrated

• Snowflake
• Snowpark
• Streamlit
• SQL
• Data Operations Monitoring
• Root Cause Analysis
• Failure Trend Analysis
• Data Engineering Workflow Monitoring
• Dashboard Development

## Current Version

### Phase 1: Snowflake-Native Report

The current app reads from precomputed Snowflake reporting tables:

- `PIPELINE_SUMMARY_REPORT`
- `PIPELINE_ROOT_CAUSE_REPORT`

The Streamlit layer is intentionally cost-safe:

- No live metadata scanning from the app
- No write-back actions
- No AWS integration yet
- No scheduled task creation from Streamlit
- Read-only reporting against precomputed monitor tables

## Features

- Operations summary metrics
- Active failure queue
- High-risk pipeline queue
- Stale pipeline review queue
- Failure trend comparison
- Root cause category grouping
- Pipeline drilldown
- Suggested next investigation steps
- Sidebar filters for status and object type

## Repository Files

- `streamlit_app.py` - scrubbed Streamlit application source
- `requirements.txt` - Python dependencies for local/reference development

## Configuration

The scrubbed app uses placeholder constants near the top of `streamlit_app.py`:

```python
REPORT_DATABASE = "YOUR_DATABASE"
REPORT_SCHEMA = "YOUR_PIPELINE_MONITOR_SCHEMA"
SUMMARY_REPORT_TABLE = "PIPELINE_SUMMARY_REPORT"
ROOT_CAUSE_REPORT_TABLE = "PIPELINE_ROOT_CAUSE_REPORT"
```

Replace those values in the deployed Snowflake environment with the database and schema that contain the precomputed reporting tables.

## Data Contract

`PIPELINE_SUMMARY_REPORT` is expected to include fields such as:

- `PIPELINE_NAME`
- `OBJECT_TYPE`
- `DATABASE_NAME`
- `SCHEMA_NAME`
- `LAST_STATUS`
- `STATUS_SORT`
- `FAILURES_24H`
- `FAILURES_7D`
- `FAILURES_PREVIOUS_7D`
- `FAILURE_TREND`
- `RISK_LEVEL`
- `STALE_HOURS`
- `ERROR_MESSAGE`

`PIPELINE_ROOT_CAUSE_REPORT` is expected to include fields such as:

- `ERROR_MESSAGE`
- `FAILURE_COUNT`
- `PERCENT_OF_FAILURES`

## Roadmap

### Phase 1

Use Snowflake-native metadata and precomputed reporting tables.

Potential source metadata includes:

- `TASK_HISTORY`
- `SERVERLESS_TASK_HISTORY`
- `DYNAMIC_TABLE_REFRESH_HISTORY`
- `QUERY_HISTORY`

### Phase 2

Add broader operational visibility:

- AWS DMS
- AWS Glue
- AWS Lambda
- CloudWatch
- EventBridge
- S3 / Snowpipe

### Phase 3

Advanced analytics:

- Reliability scoring
- SLA tracking
- Failure pattern detection
- Team ownership reporting

## Security Notes

This repository should stay scrubbed. Do not commit:

- Real Snowflake account identifiers
- Production database or schema names
- Role, warehouse, or user names tied to a workplace environment
- Secrets, tokens, passwords, private keys, or connection files
- Raw operational error exports that may contain sensitive object names
