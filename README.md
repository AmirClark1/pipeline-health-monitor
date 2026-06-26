# Pipeline Health Monitor

A Snowflake-native Streamlit application for monitoring pipeline health, identifying active failures, tracking recent recovered failures, grouping root causes, and prioritizing operational investigation.

This repository contains a scrubbed version of the application source. Environment-specific database names, schema names, roles, warehouses, account identifiers, and operational details are intentionally not committed.

---

## Purpose

Pipeline Health Monitor is designed to answer one operational question:

**What should I investigate first?**

The app reduces manual review across failure emails, Snowflake task history, dynamic table refresh history, and ad hoc troubleshooting by surfacing the highest-priority pipeline issues first.

---

## Skills Demonstrated

* Snowflake
* Snowpark
* Streamlit in Snowflake
* Python
* SQL
* Data operations monitoring
* Root cause analysis
* Failure trend analysis
* Dynamic table monitoring
* Task monitoring
* Dashboard development
* Operational triage workflow design

---

## Current Version

### Phase 1: Snowflake-Native Operational Reporting

The current application reads from precomputed Snowflake reporting tables:

* `PIPELINE_SUMMARY_REPORT`
* `PIPELINE_ROOT_CAUSE_REPORT`

The Streamlit layer is intentionally cost-conscious and read-only:

* No live metadata scanning from Streamlit
* No write-back actions
* No AWS integration in Phase 1
* No scheduled task creation from Streamlit
* Reporting is performed against precomputed monitor tables

---

## Key Features

* Operations summary metrics
* Active failure queue
* Recent failures / recovered pipeline visibility
* High-risk pipeline queue
* Stale pipeline review queue
* Week-over-week failure trend comparison
* Root cause category grouping
* Detailed pipeline drilldown
* Full pipeline name search
* Search by database, schema, object name, or alert-style name
* Suggested next investigation steps
* Sidebar filters for status and object type
* Technical details section for troubleshooting
* Report freshness visibility

---

## Dashboard Workflow

The dashboard is organized around an operational triage workflow:

1. **Active Failures** — identify what is currently broken.
2. **Recent Failures / Recovered** — verify pipelines that failed recently but may have recovered.
3. **High Risk** — review pipelines with repeated warning signs.
4. **Stale Review** — investigate pipelines that have not refreshed recently.
5. **Root Causes** — group recurring technical errors into plain-English categories.
6. **Drilldown** — inspect a specific pipeline and review suggested next steps.

---

## Repository Files

| File               | Description                                         |
| ------------------ | --------------------------------------------------- |
| `streamlit_app.py` | Scrubbed Streamlit application source               |
| `requirements.txt` | Python dependencies for local/reference development |
| `README.md`        | Project overview and setup notes                    |

---

## Configuration

The scrubbed app uses placeholder constants near the top of `streamlit_app.py`:

```python
REPORT_DATABASE = "YOUR_DATABASE"
REPORT_SCHEMA = "YOUR_PIPELINE_MONITOR_SCHEMA"
SUMMARY_REPORT_TABLE = "PIPELINE_SUMMARY_REPORT"
ROOT_CAUSE_REPORT_TABLE = "PIPELINE_ROOT_CAUSE_REPORT"
```

Replace these values in the deployed Snowflake environment with the database and schema that contain the precomputed reporting tables.

---

## Expected Data Contract

### `PIPELINE_SUMMARY_REPORT`

Expected fields include:

* `PIPELINE_NAME`
* `OBJECT_TYPE`
* `DATABASE_NAME`
* `SCHEMA_NAME`
* `LAST_STATUS`
* `STATUS_SORT`
* `FAILURES_24H`
* `FAILURES_7D`
* `FAILURES_PREVIOUS_7D`
* `FAILURE_TREND`
* `RISK_LEVEL`
* `STALE_HOURS`
* `ERROR_MESSAGE`

Optional future fields may include:

* `REPORT_GENERATED_AT`
* `LAST_RUN_AT`
* `ISSUE_CATEGORY`
* `REVIEW_CATEGORY`
* `RISK_REASON`
* `PRIORITY`
* `ERROR_CODE`

### `PIPELINE_ROOT_CAUSE_REPORT`

Expected fields include:

* `ERROR_MESSAGE`
* `FAILURE_COUNT`
* `PERCENT_OF_FAILURES`

---

## Metadata Sources

Phase 1 is designed around Snowflake-native metadata sources such as:

* `TASK_HISTORY`
* `SERVERLESS_TASK_HISTORY`
* `DYNAMIC_TABLE_REFRESH_HISTORY`
* `QUERY_HISTORY`

The Streamlit app does not query these metadata sources directly in normal operation. Instead, the app reads from precomputed reporting tables.

---

## Architecture

```text
Snowflake Metadata
        ↓
Report Views / SQL Logic
        ↓
Precomputed Reporting Tables
        ↓
Streamlit in Snowflake Dashboard
        ↓
Operator Investigation Workflow
```

Design principles:

* Snowflake-native
* Read-only dashboard layer
* Precomputed report tables
* Cost-conscious
* Explainable rule-based logic
* Operationally focused
* Designed for future predictive monitoring

---

## Screenshots

Add screenshots to a `screenshots/` folder and reference them here.

```markdown
![Overview](screenshots/overview.png)
![Investigate First](screenshots/investigate-first.png)
![Root Causes](screenshots/root-causes.png)
![Drilldown](screenshots/drilldown.png)
```

Recommended screenshots:

* Overview / KPI summary
* Investigate First tab
* Trends tab
* Root Causes tab
* Drilldown tab

---

## Roadmap

### Phase 1 — Snowflake-Native Monitoring

Current focus:

* Task monitoring
* Dynamic table monitoring
* Failure prioritization
* Root cause grouping
* Stale pipeline review
* Pipeline drilldown
* Suggested next steps

### Phase 1.5 — Operational Improvements

Potential enhancements:

* `REPORT_GENERATED_AT`
* `REPORT_METADATA` table
* `LAST_RUN_AT`
* SQL-side `ISSUE_CATEGORY`
* SQL-side `REVIEW_CATEGORY`
* SQL-side `RISK_REASON`
* Pipeline ownership mapping
* Business criticality classification
* Pipeline coverage metrics

### Phase 2 — Broader Operational Visibility

Potential integrations:

* AWS DMS
* AWS Glue
* AWS Lambda
* CloudWatch
* EventBridge
* S3 / Snowpipe

### Phase 3 — Predictive Monitoring

Potential future capabilities:

* Reliability scoring
* Failure probability scoring
* Anomaly detection
* Failure pattern detection
* Team ownership reporting
* SLA tracking
* Predictive pipeline health indicators

---

## Security Notes

This repository should remain scrubbed.

Do not commit:

* Real Snowflake account identifiers
* Production database or schema names
* Role, warehouse, or user names tied to a workplace environment
* Secrets, tokens, passwords, private keys, or connection files
* Raw operational exports that may contain sensitive object names
* Internal email alerts or screenshots containing sensitive details

---

## Project Status

**Current status:** Phase 1 release candidate

The current version is focused on operational triage and Snowflake-native monitoring. Future versions may expand into ownership tracking, broader cloud pipeline visibility, and predictive monitoring.
