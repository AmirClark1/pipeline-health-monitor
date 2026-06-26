"""
Pipeline Health Monitor
=======================

Author: Amir Clark
Project: Pipeline Health Monitor - Phase 1
Technology: Snowflake, Streamlit, Snowpark, Python

Description:
A Snowflake-native operational dashboard that monitors Task and Dynamic Table
health using precomputed reporting tables. The application prioritizes active
failures, recent recovered failures, stale pipelines, and recurring root causes
while providing guided investigation steps for operators.

Design Goals:
- Read-only reporting
- Low-cost architecture
- No live metadata scanning from the Streamlit layer
- Precomputed Snowflake report tables
- Operational triage workflow
- Manager/operator-friendly design

Note:
Replace the placeholder database and schema values below with your Snowflake
deployment values.
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session


# ======================================================
# App Configuration
# ======================================================

APP_TITLE = "Pipeline Health Monitor"
APP_PHASE = "Phase 1"
APP_DESCRIPTION = (
    "Snowflake-native report using precomputed Task and Dynamic Table metadata."
)

REPORT_DATABASE = "YOUR_DATABASE"
REPORT_SCHEMA = "YOUR_PIPELINE_MONITOR_SCHEMA"
SUMMARY_REPORT_TABLE = "PIPELINE_SUMMARY_REPORT"
ROOT_CAUSE_REPORT_TABLE = "PIPELINE_ROOT_CAUSE_REPORT"

STALE_THRESHOLD_HOURS = 6
RESULT_LIMIT = 500


# ======================================================
# Streamlit Page Setup
# ======================================================

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.title(APP_TITLE)
st.caption(f"{APP_PHASE} — {APP_DESCRIPTION}")

session = get_active_session()

summary_table = f"{REPORT_DATABASE}.{REPORT_SCHEMA}.{SUMMARY_REPORT_TABLE}"
root_cause_table = f"{REPORT_DATABASE}.{REPORT_SCHEMA}.{ROOT_CAUSE_REPORT_TABLE}"


# ======================================================
# Helper Functions
# ======================================================

def categorize_error(error: str) -> str:
    """
    Convert raw Snowflake error messages into operational issue categories.
    """
    if pd.isna(error) or str(error).strip() == "":
        return "No Error"

    error_text = str(error).lower()

    if "privilege" in error_text or "access control" in error_text:
        return "Permissions"
    if "timeout" in error_text or "warehouse timeout" in error_text:
        return "Performance / Timeout"
    if "mismatch" in error_text or "column" in error_text or "schema" in error_text:
        return "Schema Change"
    if "dynamic table" in error_text or "refresh" in error_text:
        return "Dynamic Table Refresh"
    if "sql compilation" in error_text or "statement_error" in error_text:
        return "SQL / Query Issue"
    if "internal error" in error_text or "incident" in error_text:
        return "Snowflake Internal Error"

    return "Other"


def get_risk_reason(row: pd.Series) -> str:
    """
    Explain why a pipeline is being flagged for review.
    """
    last_status = row.get("LAST_STATUS")
    failures_24h = row.get("FAILURES_24H", 0) or 0
    failures_7d = row.get("FAILURES_7D", 0) or 0
    stale_hours = row.get("STALE_HOURS", 0) or 0
    risk_level = row.get("RISK_LEVEL")
    failure_trend = row.get("FAILURE_TREND")

    if last_status == "FAILED" and failures_7d >= 5:
        return "Currently failed with repeated failures this week"
    if last_status == "FAILED":
        return "Current latest run failed"
    if failures_24h > 0:
        return "Failed recently but latest status may have recovered"
    if failures_7d > 0:
        return "Recent failures detected this week"
    if risk_level in ["HIGH", "CRITICAL"]:
        return "High-risk pipeline"
    if stale_hours > STALE_THRESHOLD_HOURS:
        return "Pipeline may be stale"
    if failure_trend == "▲ Getting Worse":
        return "Failure count increased compared to last week"
    if last_status == "SKIPPED":
        return "Pipeline was skipped"

    return "No major issue detected"


def review_category(row: pd.Series) -> str:
    """
    Assign the pipeline to an operational triage category.
    """
    if row.get("LAST_STATUS") == "FAILED":
        return "Active Failure"
    if (row.get("FAILURES_24H", 0) or 0) > 0:
        return "Recent Failure"
    if row.get("RISK_LEVEL") in ["HIGH", "CRITICAL"]:
        return "High Risk"
    if (row.get("STALE_HOURS", 0) or 0) > STALE_THRESHOLD_HOURS:
        return "Stale Review"

    return "No Action"


def priority_label(row: pd.Series) -> str:
    """
    Convert review category into an operator-friendly priority label.
    """
    if row["REVIEW_CATEGORY"] == "Active Failure":
        return "Critical"
    if row["REVIEW_CATEGORY"] == "Recent Failure":
        return "Warning"
    if row["REVIEW_CATEGORY"] == "High Risk":
        return "Monitor"
    if row["REVIEW_CATEGORY"] == "Stale Review":
        return "Review"

    return "Healthy"


def normalize_text(value: str) -> str:
    """
    Normalize text so drilldown search can match names with or without
    dashes, underscores, or spaces.
    """
    return (
        str(value)
        .lower()
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


def format_stale_hours(stale_hours: float) -> str:
    """
    Convert stale-hour numeric values into readable text.
    """
    if pd.isna(stale_hours):
        return "Unknown"

    if stale_hours < 1:
        return "Less than 1 hour ago"
    if stale_hours == 1:
        return "1 hour ago"

    return f"{int(stale_hours)} hours ago"


def clean_display(input_df: pd.DataFrame, display_cols: list[str]) -> pd.DataFrame:
    """
    Format dataframe columns for user-facing tables.
    """
    output_df = input_df[display_cols].copy()

    output_df["LAST_STATUS"] = output_df["LAST_STATUS"].replace(
        {
            "FAILED": "Failed",
            "SKIPPED": "Skipped",
            "SUCCEEDED": "Healthy",
        }
    )

    output_df = output_df.rename(
        columns={
            "PRIORITY": "Priority",
            "FULL_PIPELINE_NAME": "Full Pipeline Name",
            "PIPELINE_NAME": "Pipeline",
            "OBJECT_TYPE": "Type",
            "LAST_STATUS": "Status",
            "REVIEW_CATEGORY": "Investigation Status",
            "ISSUE_CATEGORY": "Issue Category",
            "FAILURES_24H": "Failures Last 24H",
            "FAILURES_7D": "Failures This Week",
            "FAILURES_PREVIOUS_7D": "Failures Last Week",
            "FAILURE_CHANGE": "Failure Change",
            "FAILURE_TREND": "Trend",
            "RISK_LEVEL": "Risk Score",
            "RISK_REASON": "Operational Impact",
            "STALE_HOURS": "Last Refresh (Hours)",
            "ERROR_MESSAGE": "Last Error",
        }
    )

    return output_df


# ======================================================
# Load Report Data
# ======================================================

summary_query = f"""
SELECT *
FROM {summary_table}
ORDER BY STATUS_SORT DESC, FAILURES_7D DESC, STALE_HOURS DESC
LIMIT {RESULT_LIMIT}
"""

root_query = f"""
SELECT *
FROM {root_cause_table}
ORDER BY FAILURE_COUNT DESC
"""

refresh_col, caption_col = st.columns([1, 5])

with refresh_col:
    if st.button("Refresh"):
        st.rerun()

try:
    df = session.sql(summary_query).to_pandas()
except Exception as exc:
    st.error(f"Failed to load summary report data: {str(exc)}")
    st.caption(
        "Check that the summary report table exists and that the active role has access."
    )
    st.stop()

try:
    root_df = session.sql(root_query).to_pandas()
except Exception as exc:
    st.warning(f"Could not load root cause report data: {str(exc)}")
    root_df = pd.DataFrame()

try:
    dashboard_loaded_at = session.sql(
        "SELECT CURRENT_TIMESTAMP() AS DASHBOARD_LOADED_AT"
    ).to_pandas()["DASHBOARD_LOADED_AT"].iloc[0]
except Exception:
    dashboard_loaded_at = None


# ======================================================
# Validate Report Schema
# ======================================================

required_cols = [
    "PIPELINE_NAME",
    "LAST_STATUS",
    "FAILURES_24H",
    "FAILURES_7D",
    "FAILURES_PREVIOUS_7D",
    "FAILURE_TREND",
    "RISK_LEVEL",
    "STALE_HOURS",
    "ERROR_MESSAGE",
    "STATUS_SORT",
    "OBJECT_TYPE",
    "DATABASE_NAME",
    "SCHEMA_NAME",
]

missing_cols = [column for column in required_cols if column not in df.columns]

if missing_cols:
    st.error(f"Summary report table is missing expected columns: {', '.join(missing_cols)}")
    st.stop()

if df.empty:
    st.warning("No pipeline records found in the summary report table.")
    st.stop()


# ======================================================
# Report Freshness
# ======================================================

if "REPORT_GENERATED_AT" in df.columns:
    report_generated_at = df["REPORT_GENERATED_AT"].max()
    freshness_text = (
        f"Report generated: {str(report_generated_at)[:16]} | "
        f"Dashboard viewed: {str(dashboard_loaded_at)[:16]}"
    )

    report_age_hours = (
        pd.Timestamp.now(tz="UTC") - pd.to_datetime(report_generated_at)
    ).total_seconds() / 3600
else:
    freshness_text = f"Dashboard viewed: {str(dashboard_loaded_at)[:16]}"
    report_age_hours = None

with caption_col:
    st.caption(freshness_text)

if report_age_hours is not None and report_age_hours > 2:
    st.warning(
        f"Report data is {int(report_age_hours)} hours old. "
        "The refresh process may need attention."
    )


# ======================================================
# Business Logic
# ======================================================

if "ISSUE_CATEGORY" not in df.columns:
    df["ISSUE_CATEGORY"] = df["ERROR_MESSAGE"].apply(categorize_error)

if "RISK_REASON" not in df.columns:
    df["RISK_REASON"] = df.apply(get_risk_reason, axis=1)

if "REVIEW_CATEGORY" not in df.columns:
    df["REVIEW_CATEGORY"] = df.apply(review_category, axis=1)

df["FAILURE_CHANGE"] = df["FAILURES_7D"] - df["FAILURES_PREVIOUS_7D"]

if "PRIORITY" not in df.columns:
    df["PRIORITY"] = df.apply(priority_label, axis=1)

df["FULL_PIPELINE_NAME"] = (
    df["DATABASE_NAME"].astype(str)
    + "-"
    + df["SCHEMA_NAME"].astype(str)
    + "-"
    + df["PIPELINE_NAME"].astype(str)
)


# ======================================================
# Sidebar Filters
# ======================================================

st.sidebar.header("Filters")

selected_status = st.sidebar.multiselect(
    "Status",
    options=sorted(df["LAST_STATUS"].dropna().unique()),
    default=sorted(df["LAST_STATUS"].dropna().unique()),
)

selected_object_type = st.sidebar.multiselect(
    "Object Type",
    options=sorted(df["OBJECT_TYPE"].dropna().unique()),
    default=sorted(df["OBJECT_TYPE"].dropna().unique()),
)

df = df[
    df["LAST_STATUS"].isin(selected_status)
    & df["OBJECT_TYPE"].isin(selected_object_type)
]

if df.empty:
    st.warning("No pipelines match the selected filters.")
    st.stop()


# ======================================================
# Operational Groups
# ======================================================

failed_df = df[df["LAST_STATUS"] == "FAILED"]
high_risk_df = df[df["RISK_LEVEL"].isin(["HIGH", "CRITICAL"])]
stale_df = df[
    (df["STALE_HOURS"] > STALE_THRESHOLD_HOURS)
    & (df["LAST_STATUS"] != "FAILED")
]
recent_failure_df = df[(df["FAILURES_24H"] > 0) | (df["FAILURES_7D"] > 0)].copy()
recovered_failure_df = recent_failure_df[recent_failure_df["LAST_STATUS"] != "FAILED"]

failed_count = len(failed_df)
recent_failure_count = len(recent_failure_df)
high_risk_count = len(high_risk_df)
stale_count = len(stale_df)

if len(root_df) > 0:
    root_df["ROOT_CAUSE_CATEGORY"] = root_df["ERROR_MESSAGE"].apply(categorize_error)
    top_category = (
        root_df.groupby("ROOT_CAUSE_CATEGORY")["FAILURE_COUNT"]
        .sum()
        .sort_values(ascending=False)
        .index[0]
    )
else:
    top_category = "No recurring root cause detected"

highest_priority = df.sort_values(
    by=["STATUS_SORT", "FAILURES_7D", "STALE_HOURS"],
    ascending=[False, False, False],
).head(1)

highest_priority_pipeline = highest_priority.iloc[0]["PIPELINE_NAME"]


# ======================================================
# KPI Cards
# ======================================================

col1, col2, col3, col4 = st.columns(4)

col1.metric("Failed", failed_count)
col2.metric("Recent Failures", recent_failure_count)
col3.metric("High Risk", high_risk_count)
col4.metric("Stale Review", stale_count)


# ======================================================
# Status Summary
# ======================================================

if failed_count > 0:
    st.error(
        f"{failed_count} pipeline(s) need immediate attention. "
        f"Top priority: {highest_priority_pipeline}."
    )
elif recent_failure_count > 0:
    st.warning(
        f"{recent_failure_count} pipeline(s) had recent failures. "
        "Some may have recovered, but they should still be reviewed."
    )
elif high_risk_count > 0:
    st.warning(f"{high_risk_count} high-risk pipeline(s) should be reviewed.")
elif stale_count > 0:
    st.warning(f"{stale_count} pipeline(s) exceeded the expected refresh threshold.")
else:
    st.success("No active or recent failures detected.")

with st.expander("How to Read This Report"):
    st.write(
        """
This report is meant to answer one simple question: **What should I investigate first?**

Priority order:
1. Active Failures — fix what is currently broken.
2. Recent Failures / Recovered — verify pipelines that recently failed.
3. High Risk — watch pipelines with repeated warning signs.
4. Stale Review — investigate pipelines that may not be refreshing.

Trend indicators:
- ▲ Getting Worse
- ▼ Improving
- ▬ Stable
"""
    )

st.divider()


# ======================================================
# Dashboard Tabs
# ======================================================

tab1, tab2, tab3, tab4 = st.tabs(
    ["Investigate First", "Trends", "Root Causes", "Drilldown"]
)

display_cols = [
    "PRIORITY",
    "FULL_PIPELINE_NAME",
    "PIPELINE_NAME",
    "OBJECT_TYPE",
    "LAST_STATUS",
    "REVIEW_CATEGORY",
    "ISSUE_CATEGORY",
    "FAILURES_24H",
    "FAILURES_7D",
    "FAILURES_PREVIOUS_7D",
    "FAILURE_CHANGE",
    "FAILURE_TREND",
    "RISK_LEVEL",
    "RISK_REASON",
    "STALE_HOURS",
    "ERROR_MESSAGE",
]


# ======================================================
# Tab 1: Investigate First
# ======================================================

with tab1:
    st.subheader("What Should I Investigate First?")

    high_risk_only_df = high_risk_df[
        (high_risk_df["LAST_STATUS"] != "FAILED")
        & (~high_risk_df["PIPELINE_NAME"].isin(recovered_failure_df["PIPELINE_NAME"]))
    ]

    if (
        failed_count == 0
        and len(recovered_failure_df) == 0
        and len(high_risk_only_df) == 0
        and stale_count == 0
    ):
        st.success("All pipelines are healthy. No investigation needed.")
    else:
        st.info(
            "**Priority order:** Active Failures → Recent Recovered → High Risk → Stale Review."
        )

        st.subheader("Active Failures")

        if failed_df.empty:
            st.success("No active failures.")
        else:
            st.error(f"{failed_count} pipeline(s) currently failing.")
            st.dataframe(
                clean_display(
                    failed_df.sort_values(
                        by=["FAILURES_7D", "STALE_HOURS"],
                        ascending=[False, False],
                    ),
                    display_cols,
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Recent Failures / Recovered")

        if recovered_failure_df.empty:
            st.success(
                "No recent failures that have recovered. "
                "Check Active Failures for current issues."
            )
        else:
            st.warning(
                f"{len(recovered_failure_df)} pipeline(s) recently failed but may have recovered."
            )
            st.dataframe(
                clean_display(
                    recovered_failure_df.sort_values(
                        by=["FAILURES_24H", "FAILURES_7D", "STALE_HOURS"],
                        ascending=[False, False, True],
                    ),
                    display_cols,
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("High Risk")

        if high_risk_only_df.empty:
            st.success("No additional high-risk pipelines outside of active or recent failures.")
        else:
            st.warning(f"{len(high_risk_only_df)} pipeline(s) showing repeated warning signs.")
            st.dataframe(
                clean_display(
                    high_risk_only_df.sort_values(
                        by=["FAILURES_7D", "STALE_HOURS"],
                        ascending=[False, False],
                    ),
                    display_cols,
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Stale Review")

        if stale_df.empty:
            st.success("No stale pipelines requiring review.")
        else:
            st.warning(f"{len(stale_df)} pipeline(s) exceeded the expected refresh threshold.")
            st.dataframe(
                clean_display(
                    stale_df.sort_values(
                        by=["STALE_HOURS", "PIPELINE_NAME"],
                        ascending=[False, True],
                    ),
                    display_cols,
                ),
                use_container_width=True,
                hide_index=True,
            )


# ======================================================
# Tab 2: Trends
# ======================================================

with tab2:
    st.subheader("Pipeline Trends")
    st.caption(
        "This section shows what types of issues are showing up and which pipelines are getting worse."
    )

    st.subheader("Issue Category Distribution")
    st.caption("This chart groups technical errors into plain-English issue types.")

    if len(root_df) > 0:
        category_chart_df = (
            root_df.groupby("ROOT_CAUSE_CATEGORY")["FAILURE_COUNT"]
            .sum()
            .sort_values(ascending=False)
        )
        st.bar_chart(category_chart_df)
    else:
        st.success("No failure issue categories detected.")

    st.subheader("Problem Trend Summary")
    st.caption("This section only shows pipelines that are getting worse or improving.")

    problem_trends = (
        df[df["FAILURE_TREND"] != "▬ Stable"]["FAILURE_TREND"]
        .fillna("Unknown")
        .value_counts()
        .reset_index()
    )

    problem_trends.columns = ["Trend", "Pipeline Count"]

    if problem_trends.empty:
        st.success("No worsening or improving trend detected.")
    else:
        metric_cols = st.columns(min(len(problem_trends), 4))

        for idx, row in problem_trends.iterrows():
            if idx >= 4:
                break

            metric_cols[idx].metric(
                row["Trend"].replace("▲ ", "").replace("▼ ", "").replace("▬ ", ""),
                int(row["Pipeline Count"]),
            )

        st.dataframe(problem_trends, use_container_width=True, hide_index=True)


# ======================================================
# Tab 3: Root Causes
# ======================================================

with tab3:
    st.subheader("Top Root Causes")
    st.caption(
        "These are the most common failure messages. Repeated errors may point to recurring issues."
    )

    if len(root_df) == 0:
        st.success("No failure root causes detected.")
    else:
        category_summary = (
            root_df.groupby("ROOT_CAUSE_CATEGORY")["FAILURE_COUNT"]
            .sum()
            .reset_index()
            .sort_values(by="FAILURE_COUNT", ascending=False)
        )

        category_summary = category_summary.rename(
            columns={
                "ROOT_CAUSE_CATEGORY": "Issue Category",
                "FAILURE_COUNT": "Failure Count",
            }
        )

        st.subheader("Root Cause Categories")
        st.caption("This groups technical errors into plain-English issue types.")
        st.dataframe(category_summary, use_container_width=True, hide_index=True)

        root_display = root_df.rename(
            columns={
                "ERROR_MESSAGE": "Error Message",
                "FAILURE_COUNT": "Failure Count",
                "PERCENT_OF_FAILURES": "% of Failures",
                "ROOT_CAUSE_CATEGORY": "Issue Category",
            }
        )

        st.subheader("Detailed Error Messages")
        st.dataframe(root_display, use_container_width=True, hide_index=True)


# ======================================================
# Tab 4: Drilldown
# ======================================================

with tab4:
    st.subheader("Pipeline Drilldown")
    st.caption("Search by pipeline name, database, schema, or full alert-style name.")

    search_text = st.text_input(
        "Search pipeline name",
        placeholder=(
            "Example: ENVIRONMENT_ROSTER, WORKDAY_REPORTS, "
            "COT_DATA_LAKE-WORKDAY_REPORTS-ENVIRONMENT_ROSTER"
        ),
    )

    drilldown_df = df.copy()

    drilldown_df["SEARCH_NAME"] = (
        drilldown_df["FULL_PIPELINE_NAME"].astype(str)
        + " "
        + drilldown_df["PIPELINE_NAME"].astype(str)
        + " "
        + drilldown_df["DATABASE_NAME"].astype(str)
        + " "
        + drilldown_df["SCHEMA_NAME"].astype(str)
    )

    if search_text:
        normalized_search = normalize_text(search_text)
        drilldown_df = drilldown_df[
            drilldown_df["SEARCH_NAME"]
            .apply(normalize_text)
            .str.contains(normalized_search, case=False, na=False)
        ]

    if drilldown_df.empty:
        st.warning("No pipelines match your search.")
    else:
        drilldown_df = drilldown_df.sort_values(
            by=["STATUS_SORT", "FAILURES_7D", "STALE_HOURS"],
            ascending=[False, False, False],
        )

        st.caption(f"{len(drilldown_df)} matching pipeline(s) found.")

        selected_pipeline = st.selectbox(
            "Select a pipeline",
            options=drilldown_df["FULL_PIPELINE_NAME"].unique(),
            index=0,
            placeholder="Select a pipeline",
        )

        selected_rows = drilldown_df[
            drilldown_df["FULL_PIPELINE_NAME"] == selected_pipeline
        ]

        if selected_rows.empty:
            st.warning("Selected pipeline was not found in the filtered data.")
            st.stop()

        row = selected_rows.iloc[0]

        stale_hours_val = row["STALE_HOURS"] if pd.notna(row["STALE_HOURS"]) else 0
        stale_text = format_stale_hours(stale_hours_val)

        d1, d2, d3, d4, d5, d6 = st.columns(6)

        d1.metric("Status", row["LAST_STATUS"] if pd.notna(row["LAST_STATUS"]) else "Unknown")
        d2.metric("Pipeline Type", row["OBJECT_TYPE"] if pd.notna(row["OBJECT_TYPE"]) else "Unknown")
        d3.metric("Investigation Status", row["REVIEW_CATEGORY"])
        d4.metric("Failures Last 24H", int(row["FAILURES_24H"]))
        d5.metric("Failures This Week", int(row["FAILURES_7D"]))
        d6.metric("Risk Score", row["RISK_LEVEL"])

        st.write(f"**Full Pipeline Name:** {row['FULL_PIPELINE_NAME']}")
        st.write(f"**Pipeline:** {row['PIPELINE_NAME']}")
        st.write(f"**Database:** {row['DATABASE_NAME']}")
        st.write(f"**Schema:** {row['SCHEMA_NAME']}")
        st.write(f"**Type:** {row['OBJECT_TYPE']}")
        st.write(f"**Priority:** {row['PRIORITY']}")
        st.write(f"**Issue Category:** {row['ISSUE_CATEGORY']}")
        st.write(f"**Trend:** {row['FAILURE_TREND']}")
        st.write(f"**Last Refresh:** {stale_text}")
        st.write(f"**Operational Impact:** {row['RISK_REASON']}")

        if row["REVIEW_CATEGORY"] != "No Action":
            st.warning(f"Investigation status: {row['REVIEW_CATEGORY']}.")

        if stale_hours_val > STALE_THRESHOLD_HOURS:
            st.warning(f"This pipeline may be stale — last refresh was {stale_text}.")

        if pd.notna(row["ERROR_MESSAGE"]) and str(row["ERROR_MESSAGE"]).strip() != "":
            st.error(f"**Last Error:** {row['ERROR_MESSAGE']}")

        st.subheader("Week-over-Week Comparison")

        this_week = int(row["FAILURES_7D"])
        last_week = int(row["FAILURES_PREVIOUS_7D"])
        change = this_week - last_week

        w1, w2, w3 = st.columns(3)

        w1.metric("Failures This Week", this_week)
        w2.metric("Failures Last Week", last_week)
        w3.metric("Change", f"{'+' if change > 0 else ''}{change}")

        st.subheader("Suggested Next Steps")

        issue_category = row["ISSUE_CATEGORY"]
        object_type = row["OBJECT_TYPE"] if pd.notna(row["OBJECT_TYPE"]) else "Unknown"

        st.caption(f"Pipeline type: **{object_type}**")

        if row["LAST_STATUS"] == "FAILED":
            if issue_category == "Permissions":
                st.error("Permission-related failure detected.")
                st.caption(
                    "This failure usually indicates the pipeline lost access to one or more required Snowflake objects."
                )
                st.error(
                    """
1. Review the permission or access control error.
2. Confirm the owner role has the required privileges.
3. Check whether EXECUTE TASK or object access is missing.
4. Compare role grants against the last successful run.
5. Escalate to the owning/admin team if access cannot be corrected.
"""
                )

            elif issue_category == "Schema Change":
                st.error("Schema change failure detected.")
                st.caption(
                    "This failure usually indicates a mismatch between expected and actual table structure."
                )
                st.error(
                    """
1. Review the schema or column mismatch error.
2. Compare source columns against the expected table or dynamic table definition.
3. Check recent DDL or upstream source changes.
4. Recreate or refresh the affected object if appropriate.
5. Escalate if the source structure changed unexpectedly.
"""
                )

            elif issue_category == "Performance / Timeout":
                st.error("Performance or timeout failure detected.")
                st.caption(
                    "This failure usually indicates the pipeline exceeded available compute resources or time limits."
                )
                st.error(
                    """
1. Review the query or task runtime history.
2. Check whether the warehouse was overloaded or undersized.
3. Look for long-running or blocking operations.
4. Consider whether the query needs optimization.
5. Escalate if the timeout continues after retry.
"""
                )

            elif issue_category == "Dynamic Table Refresh":
                st.error("Dynamic table refresh failure detected.")
                st.caption(
                    "This failure usually indicates the dynamic table could not complete its scheduled refresh."
                )
                st.error(
                    """
1. Review the dynamic table refresh error message.
2. Check whether upstream source tables have changed.
3. Verify the dynamic table definition is still valid.
4. Check for dependency failures in upstream dynamic tables.
5. Manually refresh or recreate if the issue persists.
"""
                )

            elif issue_category == "SQL / Query Issue":
                st.error("SQL compilation or query failure detected.")
                st.caption(
                    "This failure usually indicates a syntax error or reference to an object that no longer exists."
                )
                st.error(
                    """
1. Review the SQL compilation or statement error.
2. Check for recent changes to referenced objects.
3. Verify column names and data types match expectations.
4. Test the query manually to reproduce the error.
5. Fix and redeploy the task or dynamic table definition.
"""
                )

            elif issue_category == "Snowflake Internal Error":
                st.error("Snowflake internal error detected.")
                st.caption(
                    "This failure originated within Snowflake infrastructure, not from pipeline logic."
                )
                st.error(
                    """
1. Review the Snowflake incident or internal error message.
2. Retry the operation if appropriate.
3. Check whether the issue affects multiple pipelines.
4. Capture the query ID or incident ID.
5. Escalate if the internal error persists.
"""
                )

            else:
                st.error("Pipeline failure detected.")
                st.caption("Review the error message below for additional context on the root cause.")
                st.error(
                    """
1. Review the last error message.
2. Check whether this failure happened more than once this week.
3. Check the upstream source or dependency.
4. Review the related task/query history in Snowflake.
5. Re-run or escalate if the failure is still active.
"""
                )

        elif row["FAILURES_24H"] > 0 or row["FAILURES_7D"] > 0:
            st.warning("Recent failures detected — pipeline may have recovered.")
            st.caption(
                "The latest run succeeded, but failures occurred recently. Verify the recovery is complete."
            )
            st.warning(
                """
1. Review the failed run timestamps in Snowflake task or dynamic table history.
2. Confirm whether the latest successful run fully corrected the issue.
3. Check whether the same error has repeated this week.
4. Validate downstream data if the failure affected reporting.
5. Escalate only if the failure keeps recurring or affects downstream data.
"""
            )

        elif row["FAILURE_TREND"] == "▲ Getting Worse":
            st.warning("Failure trend is increasing.")
            st.caption("This pipeline has more failures this week compared to last week.")
            st.warning(
                """
1. Compare failures this week versus last week.
2. Check if failures are becoming more frequent.
3. Look for repeated error messages.
4. Review recent changes to the pipeline or upstream source.
5. Consider flagging this pipeline for closer monitoring.
"""
            )

        elif stale_hours_val > STALE_THRESHOLD_HOURS:
            st.warning("Pipeline has not refreshed recently.")
            st.caption(
                "This pipeline may be suspended, waiting on upstream data, or experiencing a silent failure."
            )
            st.warning(
                """
1. Confirm the expected pipeline schedule.
2. Check whether the task or dynamic table refresh is suspended.
3. Verify upstream data arrived.
4. Review the last successful refresh.
5. Escalate if the pipeline should have refreshed but has not.
"""
            )

        else:
            st.success("No immediate investigation steps needed based on the current report.")

        with st.expander("Technical Details"):
            st.write("Full pipeline name:", row["FULL_PIPELINE_NAME"])
            st.write("Raw pipeline name:", row["PIPELINE_NAME"])
            st.write("Database:", row["DATABASE_NAME"])
            st.write("Schema:", row["SCHEMA_NAME"])
            st.write("Object type:", row["OBJECT_TYPE"])
            st.write("Last status:", row["LAST_STATUS"])
            st.write("Priority:", row["PRIORITY"])
            st.write("Failures last 24H:", row["FAILURES_24H"])
            st.write("Failures this week:", row["FAILURES_7D"])

            if "ERROR_CODE" in row.index:
                st.write("Error code:", row["ERROR_CODE"])


# ======================================================
# Footer
# ======================================================

st.divider()

st.caption(
    f"""
{APP_TITLE} | {APP_PHASE}

Source tables:
- {summary_table}
- {root_cause_table}

Architecture:
- Snowflake-native
- Read-only reporting
- Precomputed report tables
- No live metadata scanning from the Streamlit layer
- No write-back actions
- Cost-optimized for operational review
"""
)
