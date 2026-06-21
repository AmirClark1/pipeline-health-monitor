import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Pipeline Health Monitor", layout="wide")

st.title("Pipeline Health Monitor")
st.caption(
    "Phase 1: Snowflake-native report using precomputed Task and Dynamic Table metadata."
)

# Scrubbed placeholders. Replace these in your Snowflake deployment with the
# database and schema that contain the precomputed reporting tables.
REPORT_DATABASE = "YOUR_DATABASE"
REPORT_SCHEMA = "YOUR_PIPELINE_MONITOR_SCHEMA"
SUMMARY_REPORT_TABLE = "PIPELINE_SUMMARY_REPORT"
ROOT_CAUSE_REPORT_TABLE = "PIPELINE_ROOT_CAUSE_REPORT"

session = get_active_session()

summary_query = f"""
SELECT *
FROM {REPORT_DATABASE}.{REPORT_SCHEMA}.{SUMMARY_REPORT_TABLE}
ORDER BY STATUS_SORT DESC, FAILURES_7D DESC, STALE_HOURS DESC
LIMIT 500
"""

root_query = f"""
SELECT *
FROM {REPORT_DATABASE}.{REPORT_SCHEMA}.{ROOT_CAUSE_REPORT_TABLE}
ORDER BY FAILURE_COUNT DESC
"""


def categorize_error(error):
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


def get_risk_reason(row):
    last_status = row.get("LAST_STATUS")
    failures_7d = row.get("FAILURES_7D", 0) or 0
    stale_hours = row.get("STALE_HOURS", 0) or 0
    risk_level = row.get("RISK_LEVEL")
    failure_trend = row.get("FAILURE_TREND")

    if last_status == "FAILED" and failures_7d >= 5:
        return "Currently failed with repeated failures this week"
    if last_status == "FAILED":
        return "Current latest run failed"
    if risk_level in ["HIGH", "CRITICAL"]:
        return "High-risk pipeline"
    if stale_hours > 6:
        return "Pipeline may be stale"
    if failure_trend == "▲ Getting Worse":
        return "Failure count increased compared to last week"
    if last_status == "SKIPPED":
        return "Pipeline was skipped"
    return "No major issue detected"


def review_category(row):
    if row.get("LAST_STATUS") == "FAILED":
        return "Active Failure"
    if row.get("RISK_LEVEL") in ["HIGH", "CRITICAL"]:
        return "High Risk"
    if (row.get("STALE_HOURS", 0) or 0) > 6:
        return "Stale / Review Needed"
    return "No Action"


def clean_display(input_df, display_cols):
    output_df = input_df[display_cols].copy()

    output_df["LAST_STATUS"] = output_df["LAST_STATUS"].replace(
        {
            "FAILED": "Failed",
            "SKIPPED": "Skipped",
            "SUCCEEDED": "Healthy",
        }
    )

    return output_df.rename(
        columns={
            "PIPELINE_NAME": "Pipeline",
            "OBJECT_TYPE": "Type",
            "LAST_STATUS": "Status",
            "ISSUE_CATEGORY": "Issue Category",
            "FAILURES_24H": "Failures Today",
            "FAILURES_7D": "Failures This Week",
            "FAILURES_PREVIOUS_7D": "Failures Last Week",
            "FAILURE_TREND": "Trend",
            "RISK_LEVEL": "Risk",
            "RISK_REASON": "Why This Matters",
            "STALE_HOURS": "Hours Since Last Run",
            "ERROR_MESSAGE": "Last Error",
        }
    )


df = session.sql(summary_query).to_pandas()
root_df = session.sql(root_query).to_pandas()

last_report_time = session.sql(
    """
SELECT CURRENT_TIMESTAMP() AS LAST_REPORT_TIME
"""
).to_pandas()["LAST_REPORT_TIME"].iloc[0]

if df.empty:
    st.warning("No pipeline records found in the summary report table.")
    st.stop()

df["ISSUE_CATEGORY"] = df["ERROR_MESSAGE"].apply(categorize_error)
df["RISK_REASON"] = df.apply(get_risk_reason, axis=1)
df["REVIEW_CATEGORY"] = df.apply(review_category, axis=1)

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

failed_df = df[df["LAST_STATUS"] == "FAILED"]
skipped_df = df[df["LAST_STATUS"] == "SKIPPED"]
healthy_df = df[df["LAST_STATUS"] == "SUCCEEDED"]
high_risk_df = df[df["RISK_LEVEL"].isin(["HIGH", "CRITICAL"])]
stale_df = df[(df["STALE_HOURS"] > 6) & (df["LAST_STATUS"] != "FAILED")]

failed_count = len(failed_df)
skipped_count = len(skipped_df)
healthy_count = len(healthy_df)
high_risk_count = len(high_risk_df)
stale_count = len(stale_df)
total_count = len(df)

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

top_pipeline = highest_priority.iloc[0]["PIPELINE_NAME"]
top_pipeline_failures = int(highest_priority.iloc[0]["FAILURES_7D"])
top_pipeline_risk = highest_priority.iloc[0]["RISK_LEVEL"]
top_pipeline_category = highest_priority.iloc[0]["ISSUE_CATEGORY"]

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Failed", failed_count)
col2.metric("Skipped", skipped_count)
col3.metric("Healthy", healthy_count)
col4.metric("High Risk", high_risk_count)
col5.metric("Stale Review", stale_count)
col6.metric("Total", total_count)

with st.expander("How to Read This Report"):
    st.write(
        """
This report is meant to answer one simple question:

**What should I investigate first?**

- **Failed** means the latest run failed.
- **Skipped** means the pipeline did not run. Some skipped pipelines may be normal.
- **Healthy** means the latest run succeeded.
- **High Risk** means the pipeline has had repeated failures recently.
- **Stale Review** means the pipeline has not refreshed recently and may need review.
- **Failure Trend** compares this week to last week.
- **Stale Hours** means how long it has been since the pipeline last ran.

Start at the **Investigate First** tab. Items at the top should be reviewed first.
"""
    )

st.divider()
st.subheader("What Broke Today?")

today_df = df[(df["LAST_STATUS"] == "FAILED") | (df["FAILURES_24H"] > 0)].copy()

if today_df.empty:
    st.success("No new failures detected today.")
else:
    today_display = today_df[
        [
            "PIPELINE_NAME",
            "OBJECT_TYPE",
            "LAST_STATUS",
            "ISSUE_CATEGORY",
            "FAILURES_24H",
            "FAILURES_7D",
            "ERROR_MESSAGE",
        ]
    ].copy()

    today_display = today_display.rename(
        columns={
            "PIPELINE_NAME": "Pipeline",
            "OBJECT_TYPE": "Type",
            "LAST_STATUS": "Status",
            "ISSUE_CATEGORY": "Issue Category",
            "FAILURES_24H": "Failures Today",
            "FAILURES_7D": "Failures This Week",
            "ERROR_MESSAGE": "Last Error",
        }
    )

    st.dataframe(today_display, use_container_width=True, hide_index=True)

st.subheader("Operations Summary")

if failed_count > 0:
    st.error(f"{failed_count} pipeline(s) are currently failed. Start with those first.")
elif high_risk_count > 0:
    st.warning(f"{high_risk_count} high-risk pipeline(s) should be reviewed.")
elif stale_count > 0:
    st.warning(f"{stale_count} stale pipeline(s) may need review.")
else:
    st.success("No active failures detected.")

st.subheader("Executive Summary")
st.info(
    f"""
**Current snapshot:** {failed_count} failed pipeline(s), {skipped_count} skipped pipeline(s), and {stale_count} stale/review-needed pipeline(s).

**Highest priority item:** `{top_pipeline}` - {top_pipeline_failures} failure(s) this week, Risk: {top_pipeline_risk}, Issue: {top_pipeline_category}

**Most common issue type:** {top_category}

Use the **Investigate First** tab for the priority list, then use **Drilldown** for details and suggested next steps.
"""
)

st.caption(f"App refreshed: {str(last_report_time)[:16]}")
st.divider()

tab1, tab2, tab3, tab4 = st.tabs(
    ["Investigate First", "Trends", "Root Causes", "Drilldown"]
)

display_cols = [
    "PIPELINE_NAME",
    "OBJECT_TYPE",
    "LAST_STATUS",
    "ISSUE_CATEGORY",
    "FAILURES_24H",
    "FAILURES_7D",
    "FAILURES_PREVIOUS_7D",
    "FAILURE_TREND",
    "RISK_LEVEL",
    "RISK_REASON",
    "STALE_HOURS",
    "ERROR_MESSAGE",
]

with tab1:
    st.subheader("What Should I Investigate First?")
    st.info(
        "This section separates active failures, high-risk items, and stale pipelines "
        "so healthy items do not clutter the emergency list."
    )

    st.subheader("Active Failures")
    if failed_df.empty:
        st.success("No active failures.")
    else:
        st.dataframe(
            clean_display(
                failed_df.sort_values(
                    by=["FAILURES_7D", "STALE_HOURS"], ascending=[False, False]
                ),
                display_cols,
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("High Risk")
    high_risk_only_df = high_risk_df[high_risk_df["LAST_STATUS"] != "FAILED"]

    if high_risk_only_df.empty:
        st.success("No additional high-risk pipelines outside of active failures.")
    else:
        st.dataframe(
            clean_display(
                high_risk_only_df.sort_values(
                    by=["FAILURES_7D", "STALE_HOURS"], ascending=[False, False]
                ),
                display_cols,
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Stale / Review Needed")
    if stale_df.empty:
        st.success("No stale pipelines requiring review.")
    else:
        st.dataframe(
            clean_display(
                stale_df.sort_values(by="STALE_HOURS", ascending=False), display_cols
            ),
            use_container_width=True,
            hide_index=True,
        )

with tab2:
    st.subheader("Pipeline Trends")
    st.caption(
        "This section shows what types of issues are showing up and whether pipelines are getting better or worse."
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

    st.subheader("Problem Trend Distribution")
    st.caption("This chart focuses on pipelines that are not stable.")

    problem_trends = (
        df[df["FAILURE_TREND"] != "▬ Stable"]["FAILURE_TREND"]
        .fillna("Unknown")
        .value_counts()
    )

    if problem_trends.empty:
        st.success("No worsening or improving trend detected.")
    else:
        st.bar_chart(problem_trends)

    st.info(
        """
**How to read the trend column:**

- **Getting Worse** = more failures this week than last week.
- **Improving** = fewer failures this week than last week.
- **Stable** = about the same as last week.
"""
    )

    trend_sort_order = {"▲ Getting Worse": 1, "▬ Stable": 2, "▼ Improving": 3}

    trend_df = df[
        [
            "PIPELINE_NAME",
            "OBJECT_TYPE",
            "LAST_STATUS",
            "ISSUE_CATEGORY",
            "FAILURES_7D",
            "FAILURES_PREVIOUS_7D",
            "FAILURE_TREND",
            "RISK_LEVEL",
            "RISK_REASON",
        ]
    ].copy()

    trend_df["_SORT"] = trend_df["FAILURE_TREND"].map(trend_sort_order).fillna(4)

    trend_df = trend_df.sort_values(
        by=["_SORT", "FAILURES_7D", "FAILURES_PREVIOUS_7D"],
        ascending=[True, False, False],
    ).drop(columns="_SORT")

    trend_df = trend_df.rename(
        columns={
            "PIPELINE_NAME": "Pipeline",
            "OBJECT_TYPE": "Type",
            "LAST_STATUS": "Status",
            "ISSUE_CATEGORY": "Issue Category",
            "FAILURES_7D": "Failures This Week",
            "FAILURES_PREVIOUS_7D": "Failures Last Week",
            "FAILURE_TREND": "Trend",
            "RISK_LEVEL": "Risk",
            "RISK_REASON": "Why This Matters",
        }
    )

    st.dataframe(trend_df, use_container_width=True, hide_index=True)

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

with tab4:
    st.subheader("Pipeline Drilldown")
    st.caption("Search for one pipeline and review what is happening.")

    search_text = st.text_input(
        "Search pipeline name",
        placeholder="Type part of a pipeline name",
    )

    pipeline_options = df["PIPELINE_NAME"].dropna().sort_values().unique()

    if search_text:
        pipeline_options = [
            pipeline
            for pipeline in pipeline_options
            if search_text.lower() in pipeline.lower()
        ]

    if len(pipeline_options) == 0:
        st.warning("No pipelines match your search.")
    else:
        selected_pipeline = st.selectbox("Select a pipeline", options=pipeline_options)
        row = df[df["PIPELINE_NAME"] == selected_pipeline].iloc[0]

        stale_hours_val = row["STALE_HOURS"] if pd.notna(row["STALE_HOURS"]) else 0

        if stale_hours_val < 1:
            stale_text = "Less than 1 hour ago"
        elif stale_hours_val == 1:
            stale_text = "1 hour ago"
        else:
            stale_text = f"{int(stale_hours_val)} hours ago"

        d1, d2, d3, d4, d5 = st.columns(5)
        d1.metric("Status", row["LAST_STATUS"] if pd.notna(row["LAST_STATUS"]) else "Unknown")
        d2.metric("Review Category", row["REVIEW_CATEGORY"])
        d3.metric("Failures Today", int(row["FAILURES_24H"]))
        d4.metric("Failures This Week", int(row["FAILURES_7D"]))
        d5.metric("Risk", row["RISK_LEVEL"])

        st.write(f"**Database:** {row['DATABASE_NAME']}")
        st.write(f"**Schema:** {row['SCHEMA_NAME']}")
        st.write(f"**Type:** {row['OBJECT_TYPE']}")
        st.write(f"**Issue Category:** {row['ISSUE_CATEGORY']}")
        st.write(f"**Trend:** {row['FAILURE_TREND']}")
        st.write(f"**Last Run:** {stale_text}")
        st.write(f"**Why This Matters:** {row['RISK_REASON']}")

        if row["REVIEW_CATEGORY"] != "No Action":
            st.warning(f"This pipeline is categorized as: {row['REVIEW_CATEGORY']}.")

        if stale_hours_val > 6:
            st.warning(f"This pipeline may be stale - last run was {stale_text}.")

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

        if row["LAST_STATUS"] == "FAILED":
            if issue_category == "Permissions":
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
                st.error(
                    """
1. Review the query or task runtime history.
2. Check whether the warehouse was overloaded or undersized.
3. Look for long-running or blocking operations.
4. Consider whether the query needs optimization.
5. Escalate if the timeout continues after retry.
"""
                )
            elif issue_category == "Snowflake Internal Error":
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
                st.error(
                    """
1. Review the last error message.
2. Check whether this failure happened more than once this week.
3. Check the upstream source or dependency.
4. Review the related task/query history in Snowflake.
5. Re-run or escalate if the failure is still active.
"""
                )

        elif row["FAILURE_TREND"] == "▲ Getting Worse":
            st.warning(
                """
1. Compare failures this week versus last week.
2. Check if failures are becoming more frequent.
3. Look for repeated error messages.
4. Review recent changes to the pipeline or upstream source.
5. Consider flagging this pipeline for closer monitoring.
"""
            )

        elif stale_hours_val > 6:
            st.warning(
                """
1. Confirm the expected pipeline schedule.
2. Check whether the task or dynamic table refresh is suspended.
3. Verify upstream data arrived.
4. Review the last successful run.
5. Escalate if the pipeline should have run but has not refreshed.
"""
            )

        else:
            st.success("No immediate investigation steps needed based on the current report.")

st.divider()
st.caption(
    "Cost-safe design: Streamlit reads from precomputed report tables. "
    "No live metadata scanning, no AWS integration, no write-back actions, and no scheduled task yet."
)
