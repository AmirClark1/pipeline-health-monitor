Pipeline Health Monitor
A Streamlit proof-of-concept dashboard for pipeline health monitoring, failure trend analysis, and risk-based prioritization.

Purpose
This project explores a centralized dashboard that helps answer:

What should I investigate first?

The goal is to reduce the need to manually review multiple failure emails, dashboards, and service consoles.

Features
Priority Queue
Risk Scoring
Failure Trend Detection
Pipeline Drilldown
Root Cause Summary
Stale Data Detection
SLA Breach Detection
Versions
V1 Original POC
Initial Streamlit proof of concept using simulated pipeline data.

V2 Chance Feedback Version
Updated based on technical feedback:

Reduced redundant metrics
Focused more on failures
Removed unnecessary health score
Simplified dashboard layout
Added clearer risk score explanation
Improved root cause summary
Roadmap
Phase 1
Use Snowflake-native metadata:

TASK_HISTORY
SERVERLESS_TASK_HISTORY
DYNAMIC_TABLE_REFRESH_HISTORY
QUERY_HISTORY
Phase 2
Add AWS operational visibility:

AWS DMS
AWS Glue
AWS Lambda
CloudWatch
EventBridge
S3/Snowpipe
Phase 3
Advanced analytics:

Reliability scoring
SLA tracking
Failure pattern detection
Team ownership reporting
Status
Prototype only. Current versions use simulated mock data.
