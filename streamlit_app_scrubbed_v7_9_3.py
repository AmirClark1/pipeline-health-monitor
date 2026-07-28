# ==========================================================
# Pipeline Health Monitor
# Version: 7.9.3 Production Candidate
#
# Changes
# - Engineering feedback implementation
# - UI cleanup
# - Maintainability improvements
# - Production hardening
# - Current-period activity-aware trend classification
# - Explicit No Current Activity reporting
# - Successful-run runtime baselines and slowdown classification
# - Runtime Watchlist and per-pipeline runtime investigation
# - Responsive Root Causes layout for small and large category sets
# - Auto-sized detailed error table
# - Dark translucent credit usage HUD
# - Compact pill collapsed by default with click-to-expand details
# - Threshold-aware pulse animation and update timestamp
# - Removed full-page credit dashboard tab
# - Operations HUD combines credit usage, active failures, and runtime slowdowns
# - Live reporting-view integration with explicit source freshness
# - Safe fallback to demo data when live reporting is disabled
# - Viewer-to-query audit mapping for accurate owner-rights attribution
# - HUD credit history filtered to the current Streamlit viewer
# - Viewer credit estimate apportioned from hourly warehouse metering
# - Explicitly labeled as an estimate because the Streamlit warehouse is shared
# ==========================================================

from __future__ import annotations

import re
from typing import Any

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session


# =============================================================================
# Page and application configuration
# =============================================================================
st.set_page_config(
    page_title="Pipeline Health Monitor",
    layout="wide",
    initial_sidebar_state="expanded",
)

APP_TITLE = "Pipeline Health Monitor"
APP_SUBTITLE = (
    "Phase 1.5 — Snowflake-native failure, runtime, and investigation monitoring "
    "using precomputed Task and Dynamic Table report data."
)

CONFIG = {
    "REPORT_DATABASE": "YOUR_DATABASE",
    "REPORT_SCHEMA": "YOUR_PIPELINE_MONITOR_SCHEMA",
    "SUMMARY_REPORT_TABLE": "PIPELINE_SUMMARY_REPORT",
    "ROOT_CAUSE_REPORT_TABLE": "PIPELINE_ROOT_CAUSE_REPORT",
    "MAX_SUMMARY_ROWS": 500,
    "STALE_THRESHOLD_HOURS": 6,
    "REPORT_STALE_WARNING_HOURS": 2,
    "DISPLAY_TIMEZONE": "UTC",
    "TOP_TREND_ROWS": 25,
    "TOP_RUNTIME_ROWS": 50,
    "TOP_ROOT_CAUSE_ROWS": 25,
    # Set OPERATIONS_HUD_DEMO_MODE to False only after the controlled live
    # reporting view is deployed and the Streamlit owner role has SELECT access.
    "OPERATIONS_HUD_DEMO_MODE": True,
    "CREDIT_REPORT_VIEW": "PIPELINE_MONITOR_USER_CREDIT_REPORT",
    "QUERY_AUDIT_TABLE": "PIPELINE_MONITOR_QUERY_AUDIT",
    "APP_QUERY_TAG": "PIPELINE_HEALTH_MONITOR_V2",
    "CREDIT_DAILY_BUDGET": 8.0,
    "CREDIT_MONTHLY_BUDGET": 200.0,
    "CREDIT_PRICE_PER_CREDIT": 3.00,
    "CREDIT_WARNING_PERCENT": 70.0,
    "CREDIT_CRITICAL_PERCENT": 90.0,
    "CREDIT_HISTORY_DAYS": 30,
    "CREDIT_BASELINE_DAYS": 30,
    "CREDIT_ELEVATED_RATIO": 1.10,
    "CREDIT_WARNING_RATIO": 1.25,
    "CREDIT_CRITICAL_RATIO": 1.50,
}

SUMMARY_TABLE = (
    f"{CONFIG['REPORT_DATABASE']}."
    f"{CONFIG['REPORT_SCHEMA']}."
    f"{CONFIG['SUMMARY_REPORT_TABLE']}"
)

ROOT_CAUSE_TABLE = (
    f"{CONFIG['REPORT_DATABASE']}."
    f"{CONFIG['REPORT_SCHEMA']}."
    f"{CONFIG['ROOT_CAUSE_REPORT_TABLE']}"
)

QUERY_AUDIT_TABLE = (
    f"{CONFIG['REPORT_DATABASE']}."
    f"{CONFIG['REPORT_SCHEMA']}."
    f"{CONFIG['QUERY_AUDIT_TABLE']}"
)

REQUIRED_SUMMARY_COLUMNS = {
    "PIPELINE_NAME",
    "LAST_STATUS",
    "FAILURES_24H",
    "FAILURES_7D",
    "FAILURES_PREVIOUS_7D",
    "TOTAL_RUNS_7D",
    "FAILURE_TREND",
    "RISK_LEVEL",
    "STALE_HOURS",
    "ERROR_MESSAGE",
    "STATUS_SORT",
    "OBJECT_TYPE",
    "DATABASE_NAME",
    "SCHEMA_NAME",
    "LAST_RUN_START_TIME",
    "LAST_RUN_DURATION_SECONDS",
    "SUCCESSFUL_RUNTIME_RUNS_7D",
    "AVG_RUNTIME_SECONDS_7D",
    "MEDIAN_RUNTIME_SECONDS_7D",
    "P95_RUNTIME_SECONDS_7D",
    "RUNTIME_VS_MEDIAN_PERCENT",
    "RUNTIME_STATUS",
    "RUNTIME_STATUS_SORT",
}

STATUS_ORDER = {"FAILED": 3, "SKIPPED": 2, "SUCCEEDED": 1}
PRIORITY_ORDER = {"Critical": 4, "Warning": 3, "Monitor": 2, "Review": 1, "Healthy": 0}
RUNTIME_WATCHLIST_STATUSES = {
    "CRITICAL SLOWDOWN",
    "HIGH SLOWDOWN",
    "ELEVATED",
}
RUNTIME_BASELINE_READY_STATUSES = {
    "CRITICAL SLOWDOWN",
    "HIGH SLOWDOWN",
    "ELEVATED",
    "NORMAL",
}


# =============================================================================
# Session and data access
# =============================================================================
session = get_active_session()


def get_streamlit_viewer_user() -> str:
    """Return the Snowflake username of the person viewing this app."""
    try:
        viewer_user = getattr(st.user, "user_name", None)
    except Exception:
        viewer_user = None

    cleaned = str(viewer_user).strip() if viewer_user is not None else ""
    return cleaned.upper() if cleaned else "UNKNOWN_VIEWER"


def record_viewer_query(query_id: str, component_name: str) -> None:
    """Map an owner-rights Snowflake query back to the actual Streamlit viewer."""
    if not query_id:
        return

    merge_sql = f"""
        MERGE INTO {QUERY_AUDIT_TABLE} AS target
        USING (
            SELECT
                ? AS QUERY_ID,
                ? AS VIEWER_USER,
                ? AS APP_NAME,
                ? AS COMPONENT_NAME,
                CURRENT_TIMESTAMP() AS EXECUTED_AT
        ) AS source
        ON target.QUERY_ID = source.QUERY_ID
        WHEN NOT MATCHED THEN INSERT (
            QUERY_ID,
            VIEWER_USER,
            APP_NAME,
            COMPONENT_NAME,
            EXECUTED_AT,
            RECORDED_AT
        ) VALUES (
            source.QUERY_ID,
            source.VIEWER_USER,
            source.APP_NAME,
            source.COMPONENT_NAME,
            source.EXECUTED_AT,
            CURRENT_TIMESTAMP()
        )
    """
    session.sql(
        merge_sql,
        params=[
            query_id,
            get_streamlit_viewer_user(),
            CONFIG["APP_QUERY_TAG"],
            component_name,
        ],
    ).collect()


def tracked_query_to_pandas(
    query: str,
    component_name: str,
    params: list[Any] | None = None,
) -> pd.DataFrame:
    """Run a tagged SELECT, return pandas data, and audit its query ID."""
    job = session.sql(query, params=params).to_pandas(
        block=False,
        statement_params={"QUERY_TAG": CONFIG["APP_QUERY_TAG"]},
    )
    result = job.result()
    record_viewer_query(job.query_id, component_name)
    return result


@st.cache_data(ttl=300, show_spinner=False)
def load_summary_report() -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {SUMMARY_TABLE}
        ORDER BY STATUS_SORT DESC, FAILURES_7D DESC, STALE_HOURS DESC
        LIMIT {int(CONFIG['MAX_SUMMARY_ROWS'])}
    """
    return tracked_query_to_pandas(query, "SUMMARY_REPORT")


@st.cache_data(ttl=300, show_spinner=False)
def load_root_cause_report() -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {ROOT_CAUSE_TABLE}
        ORDER BY FAILURE_COUNT DESC
        LIMIT {int(CONFIG['TOP_ROOT_CAUSE_ROWS'])}
    """
    return tracked_query_to_pandas(query, "ROOT_CAUSE_REPORT")


@st.cache_data(ttl=60, show_spinner=False)
def get_snowflake_timestamp(viewer_user: str) -> Any:
    # viewer_user intentionally participates in the cache key so each viewer's
    # session creates at least one auditable app query before the HUD loads.
    result = tracked_query_to_pandas(
        "SELECT CURRENT_TIMESTAMP() AS CURRENT_TS",
        "APP_HEARTBEAT",
    )
    return result["CURRENT_TS"].iloc[0]


# =============================================================================
# Utility functions
# =============================================================================
def safe_number(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    return int(safe_number(value, default))


def safe_text(value: Any, default: str = "Not available") -> str:
    if value is None or pd.isna(value):
        return default
    text = str(value).strip()
    return text if text else default


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None or pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().upper() in {"TRUE", "T", "YES", "Y", "1"}


def normalize_search_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", safe_text(value, "").lower())


def categorize_error(error: Any) -> str:
    text = safe_text(error, "").lower()

    if not text:
        return "No Error"
    if any(token in text for token in ("privilege", "access control", "not authorized")):
        return "Permissions"
    if any(token in text for token in ("timeout", "timed out", "warehouse timeout")):
        return "Performance / Timeout"
    if any(token in text for token in ("mismatch", "invalid identifier", "column", "schema")):
        return "Schema Change"
    if any(token in text for token in ("dynamic table", "refresh failed", "refresh error")):
        return "Dynamic Table Refresh"
    if any(token in text for token in ("sql compilation", "statement_error", "syntax error")):
        return "SQL / Query Issue"
    if any(token in text for token in ("internal error", "incident id", "processing aborted")):
        return "Snowflake Internal Error"
    return "Other"


def get_operational_impact(row: pd.Series) -> str:
    status = safe_text(row.get("LAST_STATUS"), "UNKNOWN")
    failures_24h = safe_int(row.get("FAILURES_24H"))
    failures_7d = safe_int(row.get("FAILURES_7D"))
    risk_level = safe_text(row.get("RISK_LEVEL"), "LOW")
    stale_hours = safe_number(row.get("STALE_HOURS"))
    trend = safe_text(row.get("FAILURE_TREND"), "■ Stable")

    if status == "FAILED" and failures_7d >= 5:
        return "Currently failed with repeated failures this week"
    if status == "FAILED":
        return "Latest run failed"
    if failures_24h > 0:
        return "Failed recently; latest run may have recovered"
    if failures_7d > 0:
        return "Failures occurred during the current reporting week"
    if risk_level in {"HIGH", "CRITICAL"}:
        return "Elevated failure-frequency risk"
    if stale_hours > CONFIG["STALE_THRESHOLD_HOURS"]:
        return "Pipeline may be stale"
    if trend == "▲ Getting Worse":
        return "Failure count increased compared with last week"
    if status == "SKIPPED":
        return "Latest run was skipped"
    return "No immediate issue detected"


def get_review_category(row: pd.Series) -> str:
    status = safe_text(row.get("LAST_STATUS"), "UNKNOWN")
    failures_24h = safe_int(row.get("FAILURES_24H"))
    risk_level = safe_text(row.get("RISK_LEVEL"), "LOW")
    stale_hours = safe_number(row.get("STALE_HOURS"))

    if status == "FAILED":
        return "Active Failure"
    if failures_24h > 0:
        return "Recent Failure"
    if risk_level in {"HIGH", "CRITICAL"}:
        return "High Risk"
    if stale_hours > CONFIG["STALE_THRESHOLD_HOURS"]:
        return "Stale Review"
    return "No Action"


def get_priority(review_category: str) -> str:
    return {
        "Active Failure": "Critical",
        "Recent Failure": "Warning",
        "High Risk": "Monitor",
        "Stale Review": "Review",
    }.get(review_category, "Healthy")


def format_elapsed_hours(hours_value: Any) -> str:
    hours = safe_number(hours_value)
    if hours < 1:
        return "Less than 1 hour ago"
    if hours < 2:
        return "1 hour ago"
    if hours < 24:
        return f"{int(hours)} hours ago"

    days = hours / 24
    if days < 2:
        return "1 day ago"
    return f"{int(days)} days ago"


def format_duration_seconds(seconds_value: Any) -> str:
    if seconds_value is None or pd.isna(seconds_value):
        return "Not measured"

    seconds = max(safe_number(seconds_value), 0.0)
    if seconds < 1:
        return f"{seconds:.2f} sec"
    if seconds < 60:
        return f"{seconds:.1f} sec"
    if seconds < 3600:
        rounded_seconds = int(round(seconds))
        minutes, remaining_seconds = divmod(rounded_seconds, 60)
        return f"{minutes}m {remaining_seconds:02d}s"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes:02d}m"


def format_runtime_percent(percent_value: Any) -> str:
    if percent_value is None or pd.isna(percent_value):
        return "Not available"
    return f"{safe_number(percent_value):+.1f}%"


def truncate_text(value: Any, max_length: int = 160) -> str:
    text = safe_text(value, "")
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "…"


def prepare_summary_frame(raw_df: pd.DataFrame) -> pd.DataFrame:
    frame = raw_df.copy()
    frame.columns = [str(column).upper() for column in frame.columns]

    missing = sorted(REQUIRED_SUMMARY_COLUMNS.difference(frame.columns))
    if missing:
        raise ValueError(
            "Summary report is missing required columns: " + ", ".join(missing)
        )

    numeric_columns = [
        "FAILURES_24H",
        "FAILURES_7D",
        "FAILURES_PREVIOUS_7D",
        "TOTAL_RUNS_7D",
        "STALE_HOURS",
        "STATUS_SORT",
        "SUCCESSFUL_RUNTIME_RUNS_7D",
        "RUNTIME_STATUS_SORT",
    ]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)

    runtime_measure_columns = [
        "LAST_RUN_DURATION_SECONDS",
        "AVG_RUNTIME_SECONDS_7D",
        "MEDIAN_RUNTIME_SECONDS_7D",
        "P95_RUNTIME_SECONDS_7D",
        "RUNTIME_VS_MEDIAN_PERCENT",
    ]
    for column in runtime_measure_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    text_columns = [
        "PIPELINE_NAME",
        "LAST_STATUS",
        "FAILURE_TREND",
        "RISK_LEVEL",
        "ERROR_MESSAGE",
        "OBJECT_TYPE",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "RUNTIME_STATUS",
    ]
    for column in text_columns:
        frame[column] = frame[column].fillna("").astype(str).str.strip()

    frame["LAST_STATUS"] = frame["LAST_STATUS"].str.upper()
    frame["RISK_LEVEL"] = frame["RISK_LEVEL"].str.upper()
    frame["RUNTIME_STATUS"] = (
        frame["RUNTIME_STATUS"].replace("", "NOT MEASURED").str.upper()
    )
    frame["RUNTIME_WATCHLIST_FLAG"] = frame["RUNTIME_STATUS"].isin(
        RUNTIME_WATCHLIST_STATUSES
    )
    frame["RUNTIME_BASELINE_READY_FLAG"] = frame["RUNTIME_STATUS"].isin(
        RUNTIME_BASELINE_READY_STATUSES
    )
    frame["FAILURE_CHANGE"] = (
        frame["FAILURES_7D"] - frame["FAILURES_PREVIOUS_7D"]
    )

    if "ISSUE_CATEGORY" not in frame.columns:
        frame["ISSUE_CATEGORY"] = frame["ERROR_MESSAGE"].apply(categorize_error)
    else:
        frame["ISSUE_CATEGORY"] = (
            frame["ISSUE_CATEGORY"]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        empty_category = frame["ISSUE_CATEGORY"].eq("")
        frame.loc[empty_category, "ISSUE_CATEGORY"] = frame.loc[
            empty_category, "ERROR_MESSAGE"
        ].apply(categorize_error)

    if "RISK_REASON" not in frame.columns:
        frame["RISK_REASON"] = frame.apply(get_operational_impact, axis=1)
    else:
        frame["RISK_REASON"] = frame["RISK_REASON"].fillna("").astype(str)
        empty_reason = frame["RISK_REASON"].str.strip().eq("")
        frame.loc[empty_reason, "RISK_REASON"] = frame.loc[
            empty_reason
        ].apply(get_operational_impact, axis=1)

    if "REVIEW_CATEGORY" not in frame.columns:
        frame["REVIEW_CATEGORY"] = frame.apply(get_review_category, axis=1)
    else:
        frame["REVIEW_CATEGORY"] = (
            frame["REVIEW_CATEGORY"].fillna("").astype(str).str.strip()
        )
        empty_review = frame["REVIEW_CATEGORY"].eq("")
        frame.loc[empty_review, "REVIEW_CATEGORY"] = frame.loc[
            empty_review
        ].apply(get_review_category, axis=1)

    if "PRIORITY" not in frame.columns:
        frame["PRIORITY"] = frame["REVIEW_CATEGORY"].apply(get_priority)
    else:
        frame["PRIORITY"] = frame["PRIORITY"].fillna("").astype(str).str.strip()
        empty_priority = frame["PRIORITY"].eq("")
        frame.loc[empty_priority, "PRIORITY"] = frame.loc[
            empty_priority, "REVIEW_CATEGORY"
        ].apply(get_priority)

    frame["FULL_PIPELINE_NAME"] = (
        frame["DATABASE_NAME"]
        + "-"
        + frame["SCHEMA_NAME"]
        + "-"
        + frame["PIPELINE_NAME"]
    )

    frame["SEARCH_TEXT"] = (
        frame["FULL_PIPELINE_NAME"]
        + " "
        + frame["PIPELINE_NAME"]
        + " "
        + frame["DATABASE_NAME"]
        + " "
        + frame["SCHEMA_NAME"]
        + " "
        + frame["OBJECT_TYPE"]
    ).apply(normalize_search_text)

    frame["PRIORITY_SORT"] = frame["PRIORITY"].map(PRIORITY_ORDER).fillna(0)

    return frame


def prepare_root_cause_frame(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df.copy()

    frame = raw_df.copy()
    frame.columns = [str(column).upper() for column in frame.columns]

    if "ERROR_MESSAGE" not in frame.columns or "FAILURE_COUNT" not in frame.columns:
        raise ValueError(
            "Root-cause report must contain ERROR_MESSAGE and FAILURE_COUNT."
        )

    frame["FAILURE_COUNT"] = pd.to_numeric(
        frame["FAILURE_COUNT"], errors="coerce"
    ).fillna(0)

    if "PERCENT_OF_FAILURES" not in frame.columns:
        total = frame["FAILURE_COUNT"].sum()
        frame["PERCENT_OF_FAILURES"] = (
            frame["FAILURE_COUNT"] / total * 100 if total else 0
        )
    else:
        frame["PERCENT_OF_FAILURES"] = pd.to_numeric(
            frame["PERCENT_OF_FAILURES"], errors="coerce"
        ).fillna(0)

    frame["ROOT_CAUSE_CATEGORY"] = frame["ERROR_MESSAGE"].apply(categorize_error)
    return frame


def table_for_display(input_df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    available_columns = [column for column in columns if column in input_df.columns]
    output = input_df[available_columns].copy()

    rename_map = {
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
        "TOTAL_RUNS_7D": "Runs This Week",
        "FAILURE_CHANGE": "Change",
        "FAILURE_TREND": "Trend",
        "RISK_LEVEL": "Failure Frequency Risk",
        "RISK_REASON": "Operational Impact",
        "STALE_HOURS": "Hours Since Refresh",
        "RUNTIME_STATUS": "Runtime Status",
        "LAST_RUN_DURATION_SECONDS": "Latest Runtime",
        "AVG_RUNTIME_SECONDS_7D": "7D Average",
        "MEDIAN_RUNTIME_SECONDS_7D": "7D Median",
        "P95_RUNTIME_SECONDS_7D": "7D P95",
        "RUNTIME_VS_MEDIAN_PERCENT": "Latest vs Median",
        "SUCCESSFUL_RUNTIME_RUNS_7D": "Successful Runs 7D",
        "ERROR_MESSAGE": "Last Error",
    }
    output = output.rename(columns=rename_map)

    if "Status" in output.columns:
        output["Status"] = output["Status"].replace(
            {"FAILED": "Failed", "SKIPPED": "Skipped", "SUCCEEDED": "Healthy"}
        )
    if "Last Error" in output.columns:
        output["Last Error"] = output["Last Error"].apply(
            lambda value: truncate_text(value, 140)
        )

    for duration_column in [
        "Latest Runtime",
        "7D Average",
        "7D Median",
        "7D P95",
    ]:
        if duration_column in output.columns:
            output[duration_column] = output[duration_column].apply(
                format_duration_seconds
            )

    if "Latest vs Median" in output.columns:
        output["Latest vs Median"] = output["Latest vs Median"].apply(
            format_runtime_percent
        )

    if "Successful Runs 7D" in output.columns:
        output["Successful Runs 7D"] = (
            pd.to_numeric(output["Successful Runs 7D"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    return output


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert a DataFrame into UTF-8 CSV bytes for Streamlit downloads."""
    return df.to_csv(index=False).encode("utf-8")


def render_investigation_status(review_category: str) -> None:
    if review_category == "Active Failure":
        st.error("Investigation status: Active Failure")
    elif review_category == "Recent Failure":
        st.warning("Investigation status: Recent Failure / Recovered")
    elif review_category == "High Risk":
        st.warning("Investigation status: High Risk")
    elif review_category == "Stale Review":
        st.warning("Investigation status: Stale Review")
    else:
        st.success("Investigation status: No immediate action")


def render_trend_status(trend: str) -> None:
    if trend == "▲ Getting Worse":
        st.warning("Trend: Getting Worse")
    elif trend == "▼ Improving":
        st.success("Trend: Improving")
    elif trend == "○ No Current Activity":
        st.warning("Trend: No Current Activity")
        st.caption(
            "Failures were recorded in the previous seven-day period, "
            "but this pipeline recorded no runs in the current period."
        )
    else:
        st.info("Trend: Stable")


def render_runtime_status(row: pd.Series) -> None:
    runtime_status = safe_text(row.get("RUNTIME_STATUS"), "NOT MEASURED").upper()
    latest_runtime = format_duration_seconds(
        row.get("LAST_RUN_DURATION_SECONDS")
    )
    median_runtime = format_duration_seconds(
        row.get("MEDIAN_RUNTIME_SECONDS_7D")
    )
    runtime_delta = format_runtime_percent(
        row.get("RUNTIME_VS_MEDIAN_PERCENT")
    )

    message = (
        f"Runtime status: {runtime_status.title()} — latest {latest_runtime}; "
        f"7-day median {median_runtime}; latest vs median {runtime_delta}."
    )

    if runtime_status == "CRITICAL SLOWDOWN":
        st.error(message)
    elif runtime_status in {"HIGH SLOWDOWN", "ELEVATED"}:
        st.warning(message)
    elif runtime_status == "NORMAL":
        st.success(message)
    elif runtime_status == "INSUFFICIENT HISTORY":
        st.info(
            "Runtime status: Insufficient History — at least three successful "
            "runtime measurements are required for slowdown classification."
        )
    else:
        st.info("Runtime status: Not Measured")


def render_runtime_guidance(row: pd.Series) -> None:
    runtime_status = safe_text(row.get("RUNTIME_STATUS"), "NOT MEASURED").upper()
    if runtime_status not in RUNTIME_WATCHLIST_STATUSES:
        return

    st.subheader("Runtime Investigation Steps")
    st.warning(
        "\n".join(
            [
                "1. Compare the latest execution with the seven-day median and P95 runtime.",
                "2. Review queueing, warehouse load, and concurrency during the slow run.",
                "3. Check for larger input volume, changed query plans, or upstream delays.",
                "4. Compare the slow execution with a recent normal execution in Query History.",
                "5. Escalate through the standard engineering support path if the slowdown repeats or affects delivery time.",
            ]
        )
    )
    st.caption(
        "This is a heuristic watchlist, not an SLA breach. It compares the latest "
        "runtime with the pipeline's own recent successful-run baseline."
    )


def render_suggested_steps(row: pd.Series) -> None:
    issue_category = safe_text(row.get("ISSUE_CATEGORY"), "Other")
    latest_status = safe_text(row.get("LAST_STATUS"), "UNKNOWN")
    failures_24h = safe_int(row.get("FAILURES_24H"))
    failures_7d = safe_int(row.get("FAILURES_7D"))
    trend = safe_text(row.get("FAILURE_TREND"), "■ Stable")
    stale_hours = safe_number(row.get("STALE_HOURS"))

    st.subheader("Suggested Next Steps")
    st.caption(f"Pipeline type: {safe_text(row.get('OBJECT_TYPE'))}")

    if latest_status == "FAILED":
        guidance = {
            "Permissions": (
                "Permission or access-control failure detected.",
                "The execution role may be missing required privileges.",
                [
                    "Review the permission or access-control error.",
                    "Confirm the execution role has the required privileges.",
                    "Check whether EXECUTE TASK or object access is missing.",
                    "Compare current grants with the last successful run.",
                    "Escalate to the owning or administrator team if access cannot be corrected.",
                ],
            ),
            "Schema Change": (
                "Schema or object-reference failure detected.",
                "A referenced column, object, or data type may have changed.",
                [
                    "Review the schema, column, or object-reference error.",
                    "Compare source columns with the expected object definition.",
                    "Check recent DDL or upstream source changes.",
                    "Validate the affected SQL manually.",
                    "Update and redeploy the affected task or dynamic table.",
                ],
            ),
            "Performance / Timeout": (
                "Performance or timeout failure detected.",
                "The pipeline may have exceeded available compute resources or execution limits.",
                [
                    "Review query and task runtime history.",
                    "Check whether the warehouse was overloaded or undersized.",
                    "Look for long-running, queued, or blocking operations.",
                    "Determine whether the SQL requires optimization.",
                    "Retry and escalate if the timeout continues.",
                ],
            ),
            "Dynamic Table Refresh": (
                "Dynamic table refresh failure detected.",
                "The dynamic table could not complete its scheduled refresh.",
                [
                    "Review the dynamic table refresh error.",
                    "Check upstream source and dependency health.",
                    "Verify that the dynamic table definition remains valid.",
                    "Review dependency failures in upstream dynamic tables.",
                    "Manually refresh or recreate the object if the problem persists.",
                ],
            ),
            "SQL / Query Issue": (
                "SQL compilation or query failure detected.",
                "The SQL may contain an invalid identifier, syntax problem, or missing reference.",
                [
                    "Review the SQL compilation or statement error.",
                    "Check recent changes to referenced objects.",
                    "Verify column names and data types.",
                    "Test the SQL manually to reproduce the error.",
                    "Fix and redeploy the task or dynamic table definition.",
                ],
            ),
            "Snowflake Internal Error": (
                "Snowflake internal error detected.",
                "The failure may have originated inside Snowflake infrastructure.",
                [
                    "Review the Snowflake incident or internal-error message.",
                    "Retry the operation when appropriate.",
                    "Check whether multiple pipelines are affected.",
                    "Capture the query ID or incident ID.",
                    "Escalate if the internal error persists.",
                ],
            ),
        }

        title, caption, steps = guidance.get(
            issue_category,
            (
                "Pipeline failure detected.",
                "Use the latest error and execution history to isolate the root cause.",
                [
                    "Review the latest error message.",
                    "Check whether the failure repeated this week.",
                    "Review upstream sources and dependencies.",
                    "Inspect related task or query history.",
                    "Re-run or escalate if the failure remains active.",
                ],
            ),
        )

        st.error(title)
        st.caption(caption)
        st.error("\n".join(f"{index}. {step}" for index, step in enumerate(steps, 1)))
        return

    if failures_24h > 0 or failures_7d > 0:
        st.warning("Recent failures detected — the pipeline may have recovered.")
        st.caption(
            "The latest run is not failed, but recent failures still require verification."
        )
        st.warning(
            """
1. Review the failed run timestamps in Snowflake history.
2. Confirm that the latest successful run fully corrected the issue.
3. Check whether the same error has repeated.
4. Verify downstream data completeness.
5. Escalate only if failures recur or affect consumers.
"""
        )
        return

    if trend == "▲ Getting Worse":
        st.warning("Failure frequency is increasing.")
        st.caption("This pipeline has more failures this week than last week.")
        st.warning(
            """
1. Compare this week with the previous reporting period.
2. Review repeated error messages and recent changes.
3. Check upstream dependencies and schedules.
4. Identify whether failures are clustered at a specific time.
5. Flag the pipeline for closer monitoring.
"""
        )
        return

    if stale_hours > CONFIG["STALE_THRESHOLD_HOURS"]:
        st.warning("Pipeline requires stale review.")
        st.caption("The last recorded refresh exceeded the Phase 1 stale threshold.")
        st.warning(
            """
1. Confirm the expected schedule or target lag.
2. Check whether the task or dynamic table is suspended.
3. Verify that upstream data arrived.
4. Review the last successful execution.
5. Escalate if the pipeline should have refreshed but did not.
"""
        )
        return

    st.success("No immediate investigation steps are required.")


# =============================================================================
# Floating Operations HUD foundation
# =============================================================================
def build_demo_credit_history(days: int = 30) -> pd.DataFrame:
    """Create deterministic portfolio demo data without external dependencies."""
    end_day = date.today()
    records: list[dict[str, Any]] = []

    for offset in range(days - 1, -1, -1):
        usage_date = end_day - timedelta(days=offset)
        day_index = days - offset
        weekday_factor = 0.55 if usage_date.weekday() >= 5 else 1.0
        warehouse_credits = round(
            weekday_factor * (2.4 + (day_index % 6) * 0.32 + (day_index % 3) * 0.18),
            2,
        )
        cortex_credits = round(
            weekday_factor * (0.35 + (day_index % 5) * 0.11),
            2,
        )
        streamlit_credits = round(
            weekday_factor * (0.18 + (day_index % 4) * 0.05),
            2,
        )
        total_credits = round(
            warehouse_credits + cortex_credits + streamlit_credits,
            2,
        )
        records.append(
            {
                "USAGE_DATE": pd.Timestamp(usage_date),
                "WAREHOUSE_CREDITS": warehouse_credits,
                "CORTEX_CREDITS": cortex_credits,
                "STREAMLIT_CREDITS": streamlit_credits,
                "TOTAL_CREDITS": total_credits,
                "DATA_AS_OF": pd.Timestamp(datetime.now()),
            }
        )

    return pd.DataFrame(records)


@st.cache_data(ttl=300, show_spinner=False)
def load_credit_usage_history(viewer_user: str) -> pd.DataFrame:
    """Load estimated app credits apportioned to the current Streamlit viewer."""
    if CONFIG["OPERATIONS_HUD_DEMO_MODE"]:
        return build_demo_credit_history(CONFIG["CREDIT_HISTORY_DAYS"])

    credit_table = (
        f"{CONFIG['REPORT_DATABASE']}.{CONFIG['REPORT_SCHEMA']}."
        f"{CONFIG['CREDIT_REPORT_VIEW']}"
    )
    query = f"""
        SELECT
            USAGE_DATE,
            WAREHOUSE_CREDITS,
            CORTEX_CREDITS,
            STREAMLIT_CREDITS,
            TOTAL_CREDITS,
            DATA_AS_OF
        FROM {credit_table}
        WHERE UPPER(VIEWER_USER) = ?
          AND USAGE_DATE >= DATEADD(
              DAY,
              -{int(CONFIG['CREDIT_HISTORY_DAYS']) + 1},
              CURRENT_DATE()
          )
        ORDER BY USAGE_DATE
    """
    return tracked_query_to_pandas(
        query,
        "USER_CREDIT_REPORT",
        params=[viewer_user],
    )


def prepare_credit_history(raw_df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "USAGE_DATE",
        "WAREHOUSE_CREDITS",
        "CORTEX_CREDITS",
        "STREAMLIT_CREDITS",
        "TOTAL_CREDITS",
    }
    frame = raw_df.copy()
    frame.columns = [str(column).upper() for column in frame.columns]
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            "Credit usage data is missing required columns: " + ", ".join(missing)
        )

    frame["USAGE_DATE"] = pd.to_datetime(frame["USAGE_DATE"], errors="coerce")
    if "DATA_AS_OF" not in frame.columns:
        frame["DATA_AS_OF"] = pd.NaT
    frame["DATA_AS_OF"] = pd.to_datetime(frame["DATA_AS_OF"], errors="coerce")
    frame = frame.dropna(subset=["USAGE_DATE"]).sort_values("USAGE_DATE")
    for column in required.difference({"USAGE_DATE"}):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    return frame



def format_relative_update_time(value) -> str:
    """Return a compact human-friendly age for a timestamp."""
    if value is None or pd.isna(value):
        return "Update time unavailable"

    try:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("America/Phoenix")
        now = pd.Timestamp.now(tz=timestamp.tz)
        seconds = max(0, int((now - timestamp).total_seconds()))
    except Exception:
        return "Update time unavailable"

    if seconds < 10:
        return "Updated just now"
    if seconds < 60:
        return f"Updated {seconds} sec ago"

    minutes = seconds // 60
    if minutes < 60:
        return f"Updated {minutes} min ago"

    hours = minutes // 60
    if hours < 24:
        return f"Updated {hours} hr ago"

    days = hours // 24
    return f"Updated {days} day{'s' if days != 1 else ''} ago"


def format_estimated_credits(credits: float) -> str:
    """Show useful precision without overstating the warehouse estimate."""
    value = max(safe_number(credits), 0.0)
    if value == 0:
        return "0.000000 credits"
    if value < 0.000001:
        return "<0.000001 credits"
    if value < 0.0001:
        return f"{value:.6f} credits"
    if value < 0.01:
        return f"{value:.4f} credits"
    return f"{value:,.2f} credits"


def get_credit_usage_level(credits: float) -> tuple[str, str]:
    """Classify tiny estimated values without misleading percentage swings."""
    value = max(safe_number(credits), 0.0)
    if value == 0:
        return "No Usage", "No measurable warehouse-apportioned usage recorded"
    if value < 0.0001:
        return "Minimal", "Minimal estimated warehouse usage"
    if value < 0.001:
        return "Light", "Light estimated warehouse usage"
    return "Normal", "Estimated warehouse usage is within the monitored range"


def get_adaptive_credit_status(
    today_credits: float,
    baseline_credits: float,
) -> tuple[str, str, float]:
    """Classify current usage against prior complete usage days."""
    if baseline_credits <= 0:
        return (
            "Normal",
            "Historical credit baseline is not yet available",
            1.0,
        )

    ratio = today_credits / baseline_credits

    if ratio >= CONFIG["CREDIT_CRITICAL_RATIO"]:
        return (
            "Critical",
            f"Credit usage is {(ratio - 1.0) * 100:.0f}% above the 30-day baseline",
            ratio,
        )
    if ratio >= CONFIG["CREDIT_WARNING_RATIO"]:
        return (
            "Warning",
            f"Credit usage is {(ratio - 1.0) * 100:.0f}% above the 30-day baseline",
            ratio,
        )
    if ratio >= CONFIG["CREDIT_ELEVATED_RATIO"]:
        return (
            "Elevated",
            f"Credit usage is {(ratio - 1.0) * 100:.0f}% above the 30-day baseline",
            ratio,
        )

    if ratio <= 0.90:
        return (
            "Normal",
            f"Credit usage is {(1.0 - ratio) * 100:.0f}% below the 30-day baseline",
            ratio,
        )

    return (
        "Normal",
        "Credit usage is near the 30-day baseline",
        ratio,
    )

def render_credit_status(status: str, message: str) -> None:
    if status == "Critical":
        st.error(f"Credit status: {status} — {message}")
    elif status in {"Warning", "Elevated"}:
        st.warning(f"Credit status: {status} — {message}")
    else:
        st.success(f"Credit status: {status} — {message}")


def render_credit_progress(label: str, used: float, budget: float) -> None:
    percent = (used / budget * 100.0) if budget > 0 else 0.0
    st.markdown(f"**{label}**")
    st.progress(min(max(percent / 100.0, 0.0), 1.0))
    st.caption(f"{used:.2f} of {budget:.2f} credits used ({percent:.1f}%).")


def render_operations_hud(
    credit_history: pd.DataFrame,
    active_failures: int,
    runtime_slowdowns: int,
    stale_pipelines: int,
    top_priority_pipeline: str,
) -> None:
    """Render a compact Operations HUD that follows page scrolling."""
    if credit_history.empty:
        return

    latest = credit_history.iloc[-1]
    # The report view contains only app queries mapped to this Streamlit viewer.
    # STREAMLIT_CREDITS therefore represents this viewer's attributed app usage.
    today_credits = safe_number(latest["STREAMLIT_CREDITS"])
    latest_date = latest["USAGE_DATE"].normalize()
    prior_history = credit_history[
        (credit_history["USAGE_DATE"].dt.normalize() < latest_date)
        & (credit_history["STREAMLIT_CREDITS"] > 0)
    ].tail(int(CONFIG["CREDIT_BASELINE_DAYS"]))

    baseline_credits = (
        safe_number(prior_history["STREAMLIT_CREDITS"].mean())
        if not prior_history.empty
        else 0.0
    )
    credit_status, credit_status_message, baseline_ratio = (
        get_adaptive_credit_status(today_credits, baseline_credits)
    )
    usage_level, usage_level_message = get_credit_usage_level(today_credits)
    tiny_usage = today_credits < 0.001
    if tiny_usage:
        credit_status = "Normal"
        credit_status_message = usage_level_message

    baseline_percent = baseline_ratio * 100.0 if baseline_credits > 0 else 100.0
    trend_percent = (
        ((today_credits - baseline_credits) / baseline_credits) * 100.0
        if baseline_credits > 0
        else 0.0
    )
    trend_arrow = "▲" if trend_percent > 1 else "▼" if trend_percent < -1 else "■"
    trend_label = (
        usage_level.upper()
        if tiny_usage
        else f"{trend_arrow} {trend_percent:+.0f}% vs baseline"
    )

    if active_failures > 0:
        overall_status = "Critical"
        overall_message = f"{active_failures} active pipeline failure(s)"
    elif credit_status == "Critical":
        overall_status = "Critical"
        overall_message = credit_status_message
    elif runtime_slowdowns > 0:
        overall_status = "Warning"
        overall_message = f"{runtime_slowdowns} runtime slowdown(s)"
    elif credit_status == "Warning":
        overall_status = "Warning"
        overall_message = credit_status_message
    elif stale_pipelines > 0:
        overall_status = "Elevated"
        overall_message = f"{stale_pipelines} pipeline(s) need refresh-age review"
    elif credit_status == "Elevated":
        overall_status = "Elevated"
        overall_message = credit_status_message
    else:
        overall_status = "Normal"
        overall_message = "No active operational alerts"

    if active_failures > 0:
        priority_label = "Pipeline attention required"
        priority_detail = (
            f"{active_failures} active failure(s). "
            f"Top priority: {top_priority_pipeline}."
        )
        collapsed_signal = f"{active_failures} failures"
    elif runtime_slowdowns > 0:
        priority_label = "Runtime review recommended"
        priority_detail = f"{runtime_slowdowns} pipeline slowdown(s) exceed baseline."
        collapsed_signal = f"{runtime_slowdowns} slow"
    elif credit_status in {"Critical", "Warning", "Elevated"}:
        priority_label = "Credit usage outside baseline"
        priority_detail = credit_status_message + "."
        collapsed_signal = f"Credits {trend_arrow}{abs(trend_percent):.0f}%"
    elif stale_pipelines > 0:
        priority_label = "Refresh-age review recommended"
        priority_detail = f"{stale_pipelines} pipeline(s) exceed the fixed refresh-age threshold."
        collapsed_signal = f"{stale_pipelines} review"
    else:
        priority_label = "Environment within monitored range"
        priority_detail = credit_status_message + "."
        collapsed_signal = f"Credits {trend_arrow}{abs(trend_percent):.0f}%"

    raw_data_as_of = pd.to_datetime(latest.get("DATA_AS_OF"), errors="coerce")
    credits_pending = pd.isna(raw_data_as_of)

    # WAREHOUSE_METERING_HISTORY may expose the scheduled end of the current
    # hourly bucket (for example, 9:00 AM while it is still 8:44 AM). Do not
    # display a future timestamp as completed data.
    data_as_of = raw_data_as_of
    current_hour_estimate = False
    if pd.notna(raw_data_as_of):
        now_phoenix = pd.Timestamp.now(tz="America/Phoenix")
        timestamp = pd.Timestamp(raw_data_as_of)

        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("America/Phoenix")
        else:
            timestamp = timestamp.tz_convert("America/Phoenix")

        current_hour_estimate = timestamp > now_phoenix
        data_as_of = min(timestamp, now_phoenix)
    prior_attribution_series = pd.to_datetime(
        credit_history.loc[credit_history.index != latest.name, "DATA_AS_OF"],
        errors="coerce",
    ).dropna()
    latest_prior_attribution = (
        prior_attribution_series.max()
        if not prior_attribution_series.empty
        else pd.NaT
    )

    if credits_pending:
        credit_status = "Pending"
        credit_status_message = "Warehouse metering pending"
        baseline_percent = 0.0
        trend_percent = 0.0
        trend_arrow = "■"
        trend_label = "Metering pending"

        if active_failures == 0 and runtime_slowdowns == 0 and stale_pipelines == 0:
            collapsed_signal = "Metering Pending"

    palette = {
        "Normal": ("#22c55e", "rgba(34,197,94,.16)", "#bbf7d0"),
        "Elevated": ("#f59e0b", "rgba(245,158,11,.17)", "#fde68a"),
        "Warning": ("#f97316", "rgba(249,115,22,.18)", "#fed7aa"),
        "Critical": ("#ef4444", "rgba(239,68,68,.20)", "#fecaca"),
        "Pending": ("#94a3b8", "rgba(148,163,184,.16)", "#cbd5e1"),
    }
    accent, status_background, status_text = palette.get(
        overall_status, palette["Normal"]
    )
    credit_accent, credit_background, credit_text = palette.get(
        credit_status, palette["Normal"]
    )
    progress_width = min(max(baseline_percent / 1.5, 0.0), 100.0)
    mode_label = "DEMO" if CONFIG["OPERATIONS_HUD_DEMO_MODE"] else "LIVE"
    metric_value = (
        "Awaiting warehouse metering"
        if credits_pending
        else format_estimated_credits(today_credits)
    )
    metric_value_class = (
        " operations-hud-total-compact"
        if not credits_pending and len(metric_value) >= 16
        else ""
    )

    relative_update_text = format_relative_update_time(data_as_of)
    attribution_text = (
        "Last completed attribution "
        + latest_prior_attribution.strftime("%b %d · %I:%M %p").lstrip("0")
        if pd.notna(latest_prior_attribution)
        else "No completed metering period available"
    )
    credit_context_text = (
        "Snowflake warehouse metering is still processing today’s usage. This estimate can take up to about 3 hours to appear."
        if credits_pending
        else (
            "Estimated from this viewer’s Pipeline Health Monitor activity "
            "on the shared Streamlit warehouse."
        )
    )
    credit_badge_label = (
        "METERING PENDING"
        if credits_pending
        else f"CREDITS · {usage_level.upper() if tiny_usage else credit_status.upper()}"
    )
    credit_badge_class = (
        " operations-hud-credit-badge-pending" if credits_pending else ""
    )
    trend_display = "none" if credits_pending or tiny_usage else "block"
    updated_at = (
        data_as_of.strftime("%I:%M %p").lstrip("0")
        if pd.notna(data_as_of)
        else None
    )
    data_as_of_label = (
        f"Current-hour estimate as of {updated_at}"
        if current_hour_estimate and updated_at
        else f"Data as of {updated_at}"
        if updated_at
        else attribution_text
    )
    pulse_class = (
        " operations-hud-pulse"
        if overall_status in {"Warning", "Critical"}
        else ""
    )

    st.markdown(
        f"""
<style>
.operations-hud-wrapper {{
    position: fixed;
    right: 3.25rem;
    bottom: 2.35rem;
    width: min(310px, calc(100vw - 1.5rem));
    z-index: 999999;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}}
.operations-hud-wrapper details {{
    margin-left: auto;
    width: 100%;
    color: #e5e7eb;
    background: rgba(18, 24, 33, 0.94);
    border: 1px solid rgba(148, 163, 184, 0.26);
    border-top: 3px solid {accent};
    border-radius: 15px;
    box-shadow: 0 16px 42px rgba(0, 0, 0, 0.42);
    overflow: hidden;
    backdrop-filter: blur(14px) saturate(130%);
    -webkit-backdrop-filter: blur(14px) saturate(130%);
    transition: width .2s ease, box-shadow .2s ease, transform .2s ease;
}}
.operations-hud-wrapper details:not([open]) {{
    width: 280px;
}}
.operations-hud-wrapper details:hover {{
    box-shadow: 0 18px 48px rgba(0, 0, 0, 0.52);
    transform: translateY(-1px);
}}
.operations-hud-wrapper summary {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: .65rem;
    min-height: 46px;
    padding: .64rem .76rem;
    cursor: pointer;
    list-style: none;
    user-select: none;
}}
.operations-hud-wrapper summary::-webkit-details-marker {{ display: none; }}
.operations-hud-summary-left {{
    display: flex;
    align-items: center;
    gap: .52rem;
    min-width: 0;
}}
.operations-hud-dot {{
    flex: 0 0 auto;
    width: 10px;
    height: 10px;
    border-radius: 999px;
    background: {accent};
    box-shadow: 0 0 0 4px {status_background};
}}
.operations-hud-pulse {{ animation: operationsHudPulse 1.45s ease-in-out infinite; }}
@keyframes operationsHudPulse {{
    0%, 100% {{ box-shadow: 0 0 0 4px {status_background}; }}
    50% {{ box-shadow: 0 0 0 8px transparent; }}
}}
.operations-hud-pill-label {{
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    color: #f8fafc;
    font-size: .84rem;
    font-weight: 750;
}}
.operations-hud-pill-value {{
    color: #f8fafc;
    font-size: .91rem;
    font-weight: 800;
    white-space: nowrap;
}}
.operations-hud-wrapper details[open] .operations-hud-pill-value {{ display: none; }}
.operations-hud-expanded-label {{ display: none; }}
.operations-hud-wrapper details[open] .operations-hud-collapsed-label {{ display: none; }}
.operations-hud-wrapper details[open] .operations-hud-expanded-label {{ display: inline; }}
.operations-hud-badge {{
    flex: 0 0 auto;
    font-size: .62rem;
    font-weight: 850;
    letter-spacing: .055em;
    padding: .19rem .42rem;
    border-radius: 999px;
    color: {status_text};
    background: {status_background};
}}
.operations-hud-credit-badge {{
    display: inline-flex;
    align-items: center;
    gap: .25rem;
    padding: .16rem .42rem;
    color: {credit_text};
    font-size: .57rem;
    font-weight: 800;
    white-space: nowrap;
    background: {credit_background};
    border: 1px solid {credit_accent}55;
    border-radius: 999px;
}}
.operations-hud-credit-badge-pending::before {{
    content: "";
    width: 6px;
    height: 6px;
    flex: 0 0 auto;
    border-radius: 999px;
    background: {credit_accent};
    animation: operationsHudAttributionPulse 1.6s ease-in-out infinite;
}}
@keyframes operationsHudAttributionPulse {{
    0%, 100% {{ opacity: .48; transform: scale(.88); }}
    50% {{ opacity: 1; transform: scale(1.08); }}
}}
.operations-hud-insight {{
    margin: .1rem 0 .62rem;
    padding: .55rem .62rem;
    background: rgba(148, 163, 184, .075);
    border-left: 3px solid {accent};
    border-radius: 7px;
}}
.operations-hud-insight strong {{
    display: block;
    color: #f8fafc;
    font-size: .72rem;
}}
.operations-hud-insight span {{
    display: block;
    margin-top: .16rem;
    color: #94a3b8;
    font-size: .65rem;
    line-height: 1.35;
}}
.operations-hud-chevron {{
    color: #94a3b8;
    font-size: .72rem;
    transition: transform .2s ease;
}}
.operations-hud-wrapper details[open] .operations-hud-chevron {{ transform: rotate(180deg); }}
.operations-hud-body {{
    padding: .12rem .86rem .82rem;
    border-top: 1px solid rgba(148, 163, 184, .15);
}}
.operations-hud-status-line {{
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: .75rem;
    margin-top: .7rem;
}}
.operations-hud-kicker {{
    color: #94a3b8;
    font-size: .70rem;
    text-transform: uppercase;
    letter-spacing: .07em;
}}
.operations-hud-total {{
    margin-top: .05rem;
    color: #f8fafc;
    font-size: 1.75rem;
    line-height: 1.05;
    font-weight: 850;
}}
.operations-hud-total small {{
    color: #94a3b8;
    font-size: .72rem;
    font-weight: 650;
}}
.operations-hud-total-compact {{
    font-size: 1.48rem;
    letter-spacing: -.02em;
    white-space: nowrap;
}}
.operations-hud-percent {{
    color: {status_text};
    font-size: .88rem;
    font-weight: 800;
}}
.operations-hud-trend {{
    margin-top: .12rem;
    color: {status_text};
    font-size: .68rem;
    font-weight: 750;
}}
.operations-hud-financials {{
    margin-top: .6rem;
    padding-top: .55rem;
    border-top: 1px solid rgba(148, 163, 184, .15);
}}
.operations-hud-financials summary {{
    min-height: auto;
    padding: 0;
    color: #94a3b8;
    font-size: .65rem;
    font-weight: 700;
}}
.operations-hud-financials[open] summary {{
    margin-bottom: .5rem;
}}
.operations-hud-wrapper .operations-hud-financials,
.operations-hud-wrapper .operations-hud-financials:not([open]) {{
    width: 100%;
    margin: .6rem 0 0;
    color: inherit;
    background: transparent;
    border: 0;
    border-radius: 0;
    box-shadow: none;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    transform: none;
}}
.operations-hud-wrapper .operations-hud-financials:hover {{
    box-shadow: none;
    transform: none;
}}
.operations-hud-wrapper .operations-hud-financials > summary {{
    display: block;
    min-height: auto;
    padding: 0;
}}
.operations-hud-progress {{
    height: 7px;
    margin: .56rem 0 .34rem;
    overflow: hidden;
    border-radius: 999px;
    background: rgba(148, 163, 184, .22);
}}
.operations-hud-progress > div {{
    height: 100%;
    width: {progress_width:.1f}%;
    border-radius: 999px;
    background: {accent};
    box-shadow: 0 0 12px {status_background};
}}
.operations-hud-subtext {{
    color: #94a3b8;
    font-size: .68rem;
    line-height: 1.35;
}}
.operations-hud-grid {{
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: .42rem;
    margin: .72rem 0;
}}
.operations-hud-service {{
    min-width: 0;
    padding: .48rem .3rem;
    text-align: center;
    border: 1px solid rgba(148, 163, 184, .18);
    border-radius: 9px;
    background: rgba(255, 255, 255, .035);
}}
.operations-hud-service span {{
    display: block;
    overflow: hidden;
    color: #94a3b8;
    font-size: .62rem;
    white-space: nowrap;
    text-overflow: ellipsis;
}}
.operations-hud-service strong {{
    display: block;
    margin-top: .1rem;
    color: #f8fafc;
    font-size: .84rem;
}}
.operations-hud-footer-grid {{
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: .4rem;
    padding-top: .62rem;
    border-top: 1px solid rgba(148, 163, 184, .15);
}}
.operations-hud-footer-item span {{
    display: block;
    color: #64748b;
    font-size: .58rem;
    text-transform: uppercase;
    letter-spacing: .04em;
}}
.operations-hud-footer-item strong {{
    display: block;
    margin-top: .08rem;
    color: #cbd5e1;
    font-size: .69rem;
    white-space: nowrap;
}}
.operations-hud-updated {{
    display: flex;
    justify-content: space-between;
    gap: .5rem;
    margin-top: .56rem;
    color: #64748b;
    font-size: .61rem;
}}
@media (max-width: 640px) {{
    .operations-hud-wrapper {{ right: 2.75rem; bottom: 1.5rem; }}
    .operations-hud-wrapper details:not([open]) {{ width: 250px; }}
}}

@keyframes operationsHudFadeIn {{
    from {{ opacity: .72; transform: translateY(3px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes operationsHudValueSettle {{
    from {{ opacity: .78; transform: scale(.985); }}
    to {{ opacity: 1; transform: scale(1); }}
}}
.operations-hud-card {{
    animation: operationsHudFadeIn .35s ease-out;
}}
.operations-hud-total {{
    animation: operationsHudValueSettle .42s ease-out;
}}
.operations-hud-progress > div,
.operations-hud-progress-fill {{
    transition: width .65s cubic-bezier(.22, .61, .36, 1);
}}
.operations-hud-tooltip {{
    position: relative;
    cursor: help;
}}
.operations-hud-help {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    margin-left: 4px;
    border: 1px solid #64748b;
    border-radius: 999px;
    color: #94a3b8;
    font-size: .58rem;
    font-weight: 800;
    vertical-align: 1px;
}}
.operations-hud-tooltip::after {{
    content: attr(data-tooltip);
    position: absolute;
    left: 0;
    bottom: calc(100% + 8px);
    width: 250px;
    padding: 8px 10px;
    border: 1px solid #334155;
    border-radius: 8px;
    background: #0f172a;
    color: #cbd5e1;
    font-size: .69rem;
    font-weight: 500;
    line-height: 1.35;
    box-shadow: 0 8px 22px rgba(0, 0, 0, .35);
    opacity: 0;
    visibility: hidden;
    transform: translateY(3px);
    transition: opacity .18s ease, transform .18s ease, visibility .18s ease;
    pointer-events: none;
    z-index: 9999;
}}
.operations-hud-tooltip:hover::after {{
    opacity: 1;
    visibility: visible;
    transform: translateY(0);
}}
@media (prefers-reduced-motion: reduce) {{
    .operations-hud-card,
    .operations-hud-total {{
        animation: none !important;
    }}
    .operations-hud-progress > div,
    .operations-hud-progress-fill {{
        transition: none !important;
    }}
}}

</style>
<div class="operations-hud-wrapper">
  <details>
    <summary title="Click to expand or collapse operational details">
      <span class="operations-hud-summary-left">
        <span class="operations-hud-dot{pulse_class}"></span>
        <span class="operations-hud-pill-label operations-hud-collapsed-label">Ops</span>
        <span class="operations-hud-pill-label operations-hud-expanded-label">Operations HUD</span>
        <span class="operations-hud-pill-value">{collapsed_signal}</span>
      </span>
      <span class="operations-hud-badge">OPS · {overall_status.upper()}</span>
      <span class="operations-hud-chevron">⌃</span>
    </summary>
    <div class="operations-hud-body">
      <div class="operations-hud-insight">
        <strong>{priority_label}</strong>
        <span>{priority_detail}</span>
      </div>
      <div class="operations-hud-status-line">
        <div>
          <div class="operations-hud-kicker operations-hud-tooltip" data-tooltip="Estimated from shared YOUR_STREAMLIT_WAREHOUSE compute and this viewer’s Pipeline Health Monitor execution-time share.">Your estimated Pipeline Health Monitor credits today <span class="operations-hud-help">?</span></div>
          <div class="operations-hud-total{metric_value_class}">{metric_value}</div>
        </div>
        <div style="text-align:right;">
          <span class="operations-hud-credit-badge{credit_badge_class}">{credit_badge_label}</span>
          <div class="operations-hud-trend" style="display:{trend_display}; margin-top:.42rem;">{trend_label}</div>
        </div>
      </div>
      <div class="operations-hud-progress"><div></div></div>
      <div class="operations-hud-subtext">{credit_context_text}</div>
      <div class="operations-hud-updated">
        <span>{mode_label.title()} data · Warehouse-metered estimate</span>
        <span>{attribution_text if credits_pending else f"{data_as_of_label} · {relative_update_text}"}</span>
      </div>
    </div>
  </details>
</div>
""",
        unsafe_allow_html=True,
    )


# =============================================================================
# Load and validate report data
# =============================================================================
st.title(APP_TITLE)
st.caption(APP_SUBTITLE)

header_col1, header_col2 = st.columns([1, 5])
with header_col1:
    refresh_clicked = st.button("Refresh data", use_container_width=True)
    if refresh_clicked:
        st.cache_data.clear()
        st.rerun()

try:
    with st.spinner("Loading pipeline health report…"):
        summary_raw = load_summary_report()
        summary_df = prepare_summary_frame(summary_raw)
except Exception as exc:
    st.error("The pipeline health report could not be loaded.")
    st.caption(
        "Confirm that the report table exists, the Streamlit owner role can read it, "
        "and the expected columns are present."
    )
    with st.expander("Technical error"):
        st.code(str(exc))
    st.stop()

if summary_df.empty:
    st.warning("The summary report returned no pipeline records.")
    st.stop()

try:
    root_raw = load_root_cause_report()
    root_df = prepare_root_cause_frame(root_raw)
except Exception as exc:
    root_df = pd.DataFrame()
    st.warning("Root-cause data could not be loaded. Other dashboard sections remain available.")
    with st.expander("Root-cause load error"):
        st.code(str(exc))

dashboard_loaded_at = pd.to_datetime(
    get_snowflake_timestamp(get_streamlit_viewer_user()),
    errors="coerce",
    utc=True,
)

if pd.notna(dashboard_loaded_at):
    dashboard_display_at = dashboard_loaded_at.tz_convert(
        CONFIG["DISPLAY_TIMEZONE"]
    )
    dashboard_display_text = dashboard_display_at.strftime("%Y-%m-%d %H:%M")
else:
    dashboard_display_text = "Unavailable"

freshness_parts = [
    f"Dashboard loaded: {dashboard_display_text}"
]

if "REPORT_GENERATED_AT" in summary_df.columns:
    report_generated_at = pd.to_datetime(
        summary_df["REPORT_GENERATED_AT"],
        errors="coerce",
        utc=True,
    ).max()

    if pd.notna(report_generated_at):
        report_display_at = report_generated_at.tz_convert(
            CONFIG["DISPLAY_TIMEZONE"]
        )

        freshness_parts.insert(
            0,
            "Report generated: "
            f"{report_display_at.strftime('%Y-%m-%d %H:%M')}",
        )

        report_age_hours = (
            pd.Timestamp.now(tz="UTC") - report_generated_at
        ).total_seconds() / 3600

        if report_age_hours > CONFIG["REPORT_STALE_WARNING_HOURS"]:
            st.warning(
                f"The precomputed report is approximately "
                f"{int(report_age_hours)} hours old. "
                "Its refresh process may need attention."
            )

with header_col2:
    st.caption(" | ".join(freshness_parts))


# =============================================================================
# Context and documentation
# =============================================================================
with st.expander("Help Improve This Dashboard"):
    st.markdown(
        """
This is the Phase 1.5 proof of concept. Please test it as though you were
investigating a real pipeline issue.

**Feedback requested**

1. Is it clear where you should begin?
2. What slowed you down or felt unnecessary?
3. Are the failure and review categories easy to understand?
4. Would you use **Pipeline Investigation** during troubleshooting?
5. Are the suggested next steps useful?
6. Does the Runtime Watchlist identify useful early-warning signals?
7. Does Pipeline Investigation provide enough context to troubleshoot?
8. What would you add, remove, or rename?
"""
    )

with st.expander("Phase 1.5 Scope"):
    st.markdown(
        """
Phase 1.5 focuses on Snowflake-native monitoring through read-only, precomputed
report tables.

**Included**

- Snowflake Tasks and Dynamic Tables
- Active and recent failure review
- Week-over-week failure trends
- Latest-runtime comparison against seven-day successful-run baselines
- Proactive elevated, high, and critical slowdown watchlists
- Root-cause grouping
- Potentially stale pipeline review using a fixed refresh-age heuristic
- Pipeline investigation and suggested next steps

**Planned for future phases**

- AWS DMS, Glue, and Lambda integration
- Dependency-aware failure classification
- Schedule-aware stale thresholds
- Automated alerting and escalation
- Scheduled background refreshes; report refresh remains manual to control cost

The goal is to help engineers answer one question quickly:
**What should I investigate first?**
"""
    )

with st.expander("How to Read This Report"):
    st.info(
        "💡 Quick Start: Begin with the Operations HUD, then review Active "
        "Failures, Recent Failures / Recovered, High Risk, and Potentially Stale."
    )

    st.markdown(
        f"""
This dashboard helps operators identify, prioritize, and investigate Snowflake
pipeline issues before they affect downstream systems.

### ① Operations HUD

**Start here.** The Operations HUD provides the fastest summary of current
operational health and dashboard compute usage.

- **Pipeline Attention Required** surfaces the highest-priority issue currently detected.
- **Your Estimated Pipeline Health Monitor Credits Today** estimates this viewer's
  dashboard compute usage on the shared `YOUR_STREAMLIT_WAREHOUSE` warehouse.
- The credit value is a **warehouse-metered estimate**, not an exact Snowflake billing figure.
- During the current metering hour, the estimate updates as new dashboard activity occurs.

| Credit status | Meaning |
|---|---|
| **No Usage** | No Pipeline Health Monitor activity detected for this viewer. |
| **Minimal** | Very small estimated compute usage. |
| **Light** | Low estimated compute usage. |
| **Normal** | Typical estimated compute usage. |
| **Elevated** | Higher-than-normal estimated compute usage that may deserve review. |

### ② Active Failures

Review this section first after the HUD. These pipelines failed on their most
recent execution and generally require immediate investigation.

### ③ Recent Failures / Recovered

These pipelines experienced one or more recent failures but later completed a
successful execution. Review them to confirm the issue is resolved and not recurring.

### ④ High Risk

These pipelines accumulated repeated failures or elevated warning signals during
the reporting window. High Risk reflects recent behavior and failure frequency,
not necessarily the latest execution result.

### ⑤ Potentially Stale

A pipeline is marked Potentially Stale when the time since its last recorded run
exceeds **{CONFIG['STALE_THRESHOLD_HOURS']} hours**. This is an investigation
signal—not proof that the pipeline was expected to run during that period.

### Investigation Priority

| Priority | Meaning |
|---|---|
| **Critical** | The latest recorded execution failed. |
| **Warning** | Recent failures occurred, but the latest execution may have recovered. |
| **Monitor** | Failure-frequency risk is elevated. |
| **Review** | The pipeline may be stale based on elapsed time since its last recorded run. |
| **Healthy** | No immediate action is required. |
"""
    )

with st.expander("Metric Definitions & Limitations"):
    st.markdown(
        """
### Failure Frequency Risk

`RISK_LEVEL` summarizes failure frequency during the seven-day reporting window.
It is intentionally separate from immediate Investigation Priority.

For example, a pipeline can have **Critical Investigation Priority** because its
latest run failed while showing **Medium Failure Frequency Risk** because it failed
only a few times during the week.

### Trend Indicators

| Indicator | Meaning |
|---|---|
| **▲ Getting Worse** | Failures increased compared with the previous seven-day period. |
| **▼ Improving** | Failures decreased and the pipeline ran during the current period. |
| **○ No Current Activity** | Prior-period failures exist, but no execution was recorded in the current period. This is not counted as improvement. |
| **■ Stable** | No meaningful week-over-week change was detected. |

### Runtime Indicators

| Runtime status | Classification |
|---|---|
| **Critical Slowdown** | Latest runtime is at least 3× the seven-day median and at least 120 seconds slower. |
| **High Slowdown** | Latest runtime is at least 2× the median and at least 60 seconds slower. |
| **Elevated** | Latest runtime is at least 1.5× the median and at least 30 seconds slower. |
| **Normal** | A usable baseline exists and slowdown thresholds were not met. |
| **Insufficient History** | Fewer than three successful runtime measurements are available. |

Runtime classifications are investigation signals, not SLA violations. Report
refresh remains manual, and the app does not schedule recurring compute.

### Known Limitations — Phase 1.5

- Staleness is based on elapsed time since the last recorded run; it is not yet schedule-aware.
- The dashboard does not yet identify every downstream pipeline affected by a single upstream failure.
- Dependency-aware impact analysis is planned for a future phase.
"""
    )


# =============================================================================
# Sidebar filters
# =============================================================================
st.sidebar.header("Filters")

status_options = sorted(summary_df["LAST_STATUS"].dropna().unique().tolist())
object_type_options = sorted(summary_df["OBJECT_TYPE"].dropna().unique().tolist())

selected_statuses = st.sidebar.multiselect(
    "Status",
    options=status_options,
    default=status_options,
)

selected_object_types = st.sidebar.multiselect(
    "Object Type",
    options=object_type_options,
    default=object_type_options,
)

runtime_status_options = sorted(
    summary_df["RUNTIME_STATUS"].dropna().unique().tolist(),
    key=lambda status: (
        -safe_int(
            summary_df.loc[
                summary_df["RUNTIME_STATUS"] == status,
                "RUNTIME_STATUS_SORT",
            ].max()
        ),
        status,
    ),
)
selected_runtime_statuses = st.sidebar.multiselect(
    "Runtime Status",
    options=runtime_status_options,
    default=runtime_status_options,
    help=(
        "Runtime status compares the latest run with the pipeline's successful-run "
        "seven-day baseline."
    ),
)

review_options = [
    "Active Failure",
    "Recent Failure",
    "High Risk",
    "Stale Review",
    "No Action",
]
selected_review_categories = st.sidebar.multiselect(
    "Investigation Status",
    options=review_options,
    default=review_options,
)

filtered_df = summary_df[
    summary_df["LAST_STATUS"].isin(selected_statuses)
    & summary_df["OBJECT_TYPE"].isin(selected_object_types)
    & summary_df["RUNTIME_STATUS"].isin(selected_runtime_statuses)
    & summary_df["REVIEW_CATEGORY"].isin(selected_review_categories)
].copy()

if filtered_df.empty:
    st.warning("No pipelines match the selected filters.")
    st.stop()


# =============================================================================
# Operational groups and KPI metrics
# =============================================================================
failed_df = filtered_df[
    filtered_df["REVIEW_CATEGORY"] == "Active Failure"
].copy()

recovered_failure_df = filtered_df[
    filtered_df["REVIEW_CATEGORY"] == "Recent Failure"
].copy()

high_risk_only_df = filtered_df[
    filtered_df["REVIEW_CATEGORY"] == "High Risk"
].copy()

stale_df = filtered_df[
    filtered_df["REVIEW_CATEGORY"] == "Stale Review"
].copy()

runtime_watchlist_df = filtered_df[
    filtered_df["RUNTIME_WATCHLIST_FLAG"]
].copy()

failed_count = len(failed_df)
recent_failure_count = len(recovered_failure_df)
high_risk_count = len(high_risk_only_df)
stale_count = len(stale_df)
runtime_watchlist_count = len(runtime_watchlist_df)

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.metric(
        "🔴 Active Failures",
        failed_count,
        help="Pipelines whose latest execution failed and require immediate attention.",
    )

with k2:
    st.metric(
        "🟡 Recent Recovered",
        recent_failure_count,
        help=(
            "Pipelines that failed within the last 24 hours but whose "
            "latest execution is no longer failed."
        ),
    )

with k3:
    st.metric(
        "🟠 Additional High Risk",
        high_risk_count,
        help="Healthy pipelines with repeated failures that should be monitored closely.",
    )

with k4:
    st.metric(
        "🔵 Potentially Stale",
        stale_count,
        help="Pipelines that have not refreshed within the configured stale threshold.",
    )

with k5:
    st.metric(
        "🟣 Runtime Slowdowns",
        runtime_watchlist_count,
        help=(
            "Pipelines whose latest runtime is elevated, high, or critical "
            "against their own seven-day successful-run baseline."
        ),
    )

highest_priority = filtered_df.sort_values(
    by=["PRIORITY_SORT", "FAILURES_7D", "STALE_HOURS"],
    ascending=[False, False, False],
).head(1)

if failed_count:
    top_pipeline = safe_text(highest_priority.iloc[0]["PIPELINE_NAME"])
    runtime_note = (
        f" {runtime_watchlist_count} runtime slowdown(s) are also flagged."
        if runtime_watchlist_count
        else ""
    )
    st.error(
        f"{failed_count} pipeline(s) need immediate attention. "
        f"Top priority: {top_pipeline}.{runtime_note}"
    )
elif len(recovered_failure_df):
    st.warning(
        f"{len(recovered_failure_df)} pipeline(s) recently failed and should be verified."
    )
elif runtime_watchlist_count:
    st.warning(
        f"{runtime_watchlist_count} pipeline(s) are running slower than their "
        "recent successful-run baselines."
    )
else:
    st.success("No active pipeline failures are present in the selected view.")

# Load the compact credit monitor independently so pipeline monitoring remains usable
# even when the optional credit source is unavailable.
try:
    operations_credit_history = prepare_credit_history(
        load_credit_usage_history(get_streamlit_viewer_user())
    )
    hud_top_pipeline = (
        safe_text(highest_priority.iloc[0]["PIPELINE_NAME"], "None")
        if not highest_priority.empty
        else "None"
    )
    render_operations_hud(
        operations_credit_history,
        active_failures=failed_count,
        runtime_slowdowns=runtime_watchlist_count,
        stale_pipelines=stale_count,
        top_priority_pipeline=hud_top_pipeline,
    )
except Exception as exc:
    st.sidebar.warning("Operations HUD credit source unavailable")
    with st.sidebar.expander("Operations HUD source error"):
        st.code(str(exc))

st.divider()


# =============================================================================
# Tabs
# =============================================================================
(
    tab_review,
    tab_trends,
    tab_runtime,
    tab_root,
    tab_investigation,
) = st.tabs(
    [
        "Failures & Review",
        "Trends",
        "Runtime Watchlist",
        "Root Causes",
        "Pipeline Investigation",
    ]
)


# -----------------------------------------------------------------------------
# Failures & Review
# -----------------------------------------------------------------------------
with tab_review:
    st.subheader("Failures & Review")
    st.caption(
        "Review active failures first, followed by recently recovered, "
        "high-risk, and stale pipelines."
    )
    compact_columns = [
        "PRIORITY",
        "FULL_PIPELINE_NAME",
        "PIPELINE_NAME",
        "OBJECT_TYPE",
        "LAST_STATUS",
        "REVIEW_CATEGORY",
        "ISSUE_CATEGORY",
        "FAILURES_24H",
        "FAILURES_7D",
        "FAILURE_TREND",
        "STALE_HOURS",
        "ERROR_MESSAGE",
    ]

    st.subheader("Active Failures")
    if failed_df.empty:
        st.success("No active failures.")
    else:
        st.error(f"{failed_count} pipeline(s) currently failing.")
        active_display = table_for_display(
            failed_df.sort_values(
                by=["FAILURES_24H", "FAILURES_7D", "STALE_HOURS"],
                ascending=[False, False, False],
            ),
            compact_columns,
        )
        st.dataframe(
            active_display,
            use_container_width=True,
            hide_index=True,
            height=min(520, 38 + 35 * len(active_display)),
        )

        st.download_button(
            label="Download Active Failures CSV",
            data=dataframe_to_csv_bytes(active_display),
            file_name="pipeline_health_monitor_active_failures.csv",
            mime="text/csv",
            key="download_active_failures_csv",
        )

    st.subheader("Recent Failures / Recovered")
    if recovered_failure_df.empty:
        st.success("No recovered pipelines with recent failures.")
    else:
        st.warning(
            f"{len(recovered_failure_df)} pipeline(s) recently failed but may have recovered."
        )
        recovered_display = table_for_display(
            recovered_failure_df.sort_values(
                by=["FAILURES_24H", "FAILURES_7D", "STALE_HOURS"],
                ascending=[False, False, True],
            ),
            compact_columns,
        )
        st.dataframe(
            recovered_display,
            use_container_width=True,
            hide_index=True,
            height=min(420, 38 + 35 * len(recovered_display)),
        )

    st.subheader("High Risk")
    if high_risk_only_df.empty:
        st.success(
            "No additional high-risk pipelines outside active or recent failures."
        )
    else:
        st.warning(
            f"{len(high_risk_only_df)} pipeline(s) have elevated failure-frequency risk."
        )
        high_risk_display = table_for_display(
            high_risk_only_df.sort_values(
                by=["FAILURES_7D", "STALE_HOURS"],
                ascending=[False, False],
            ),
            compact_columns,
        )
        st.dataframe(
            high_risk_display,
            use_container_width=True,
            hide_index=True,
            height=min(420, 38 + 35 * len(high_risk_display)),
        )

    st.subheader("Potentially Stale")

    if stale_df.empty:
        st.success("No stale pipelines require review.")
    else:
        st.warning(
            f"{len(stale_df)} pipeline(s) exceeded the stale threshold."
        )

        st.caption(
            f"Phase 1 uses a fixed stale threshold of "
            f"{CONFIG['STALE_THRESHOLD_HOURS']} hours. "
            "This is not yet schedule-aware or dependency-aware."
        )

        stale_group_summary = (
            stale_df.groupby(
                ["DATABASE_NAME", "SCHEMA_NAME"],
                as_index=False,
            )
            .agg(
                STALE_PIPELINE_COUNT=("PIPELINE_NAME", "count"),
                MAX_STALE_HOURS=("STALE_HOURS", "max"),
            )
            .sort_values(
                by=["STALE_PIPELINE_COUNT", "MAX_STALE_HOURS"],
                ascending=[False, False],
            )
        )

        stale_group_summary["GROUP_NAME"] = (
            stale_group_summary["DATABASE_NAME"]
            + "."
            + stale_group_summary["SCHEMA_NAME"]
        )

        summary_display = stale_group_summary[
            [
                "GROUP_NAME",
                "STALE_PIPELINE_COUNT",
                "MAX_STALE_HOURS",
            ]
        ].rename(
            columns={
                "GROUP_NAME": "Database.Schema",
                "STALE_PIPELINE_COUNT": "Stale Pipelines",
                "MAX_STALE_HOURS": "Oldest Refresh Age (Hours)",
            }
        )

        st.markdown("#### Stale Pipelines by Database and Schema")

        st.dataframe(
            summary_display,
            use_container_width=True,
            hide_index=True,
            height=min(
                360,
                38 + 35 * len(summary_display),
            ),
        )

        stale_group_options = ["All Stale Pipelines"] + stale_group_summary[
            "GROUP_NAME"
        ].tolist()

        selected_stale_group = st.selectbox(
            "Filter stale pipelines",
            options=stale_group_options,
            index=0,
        )

        stale_detail_df = stale_df.copy()

        if selected_stale_group != "All Stale Pipelines":
            selected_database, selected_schema = selected_stale_group.split(
                ".",
                1,
            )

            stale_detail_df = stale_detail_df[
                (stale_detail_df["DATABASE_NAME"] == selected_database)
                & (stale_detail_df["SCHEMA_NAME"] == selected_schema)
            ]

        stale_detail_display = table_for_display(
            stale_detail_df.sort_values(
                by=["STALE_HOURS", "PIPELINE_NAME"],
                ascending=[False, True],
            ),
            compact_columns,
        )

        st.markdown("#### Stale Pipeline Details")

        st.caption(
            f"Showing {len(stale_detail_df)} stale pipeline(s)."
        )

        st.dataframe(
            stale_detail_display,
            use_container_width=True,
            hide_index=True,
            height=420,
        )

# -----------------------------------------------------------------------------
# Trends
# -----------------------------------------------------------------------------
with tab_trends:
    st.subheader("Pipeline Trends")
    st.caption(
        "This tab shows which pipelines changed most from the previous "
        "seven-day reporting period."
    )

    worsening_df = filtered_df[
        filtered_df["FAILURE_TREND"] == "▲ Getting Worse"
    ].copy()
    improving_df = filtered_df[
        filtered_df["FAILURE_TREND"] == "▼ Improving"
    ].copy()
    no_activity_df = filtered_df[
        filtered_df["FAILURE_TREND"] == "○ No Current Activity"
    ].copy()

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Getting Worse", len(worsening_df))
    t2.metric("Improving", len(improving_df))
    t3.metric("No Current Activity", len(no_activity_df))
    t4.metric(
        "Largest Increase",
        f"+{safe_int(worsening_df['FAILURE_CHANGE'].max())}"
        if not worsening_df.empty
        else "0",
    )

    trend_columns = [
        "FULL_PIPELINE_NAME",
        "PIPELINE_NAME",
        "OBJECT_TYPE",
        "LAST_STATUS",
        "TOTAL_RUNS_7D",
        "FAILURES_7D",
        "FAILURES_PREVIOUS_7D",
        "FAILURE_CHANGE",
        "FAILURE_TREND",
    ]

    st.subheader("Largest Week-over-Week Increases")
    if worsening_df.empty:
        st.success("No pipelines show a worsening week-over-week failure trend.")
    else:
        top_worsening = worsening_df.sort_values(
            by=["FAILURE_CHANGE", "FAILURES_7D", "STATUS_SORT"],
            ascending=[False, False, False],
        ).head(CONFIG["TOP_TREND_ROWS"])
        st.warning(
            f"{len(worsening_df)} pipeline(s) recorded more failures than last week."
        )
        st.dataframe(
            table_for_display(top_worsening, trend_columns),
            use_container_width=True,
            hide_index=True,
            height=min(600, 38 + 35 * len(top_worsening)),
        )

    st.subheader("Largest Week-over-Week Improvements")
    if improving_df.empty:
        st.info("No pipelines currently show an improving failure trend.")
    else:
        top_improving = improving_df.sort_values(
            by=["FAILURE_CHANGE", "FAILURES_7D"],
            ascending=[True, False],
        ).head(CONFIG["TOP_TREND_ROWS"])
        st.success(
            f"{len(improving_df)} pipeline(s) recorded fewer failures than last week."
        )
        st.dataframe(
            table_for_display(top_improving, trend_columns),
            use_container_width=True,
            hide_index=True,
            height=min(600, 38 + 35 * len(top_improving)),
        )

    st.subheader("No Current-Period Activity")
    if no_activity_df.empty:
        st.success(
            "No pipelines with prior-period failures are missing current-period runs."
        )
    else:
        top_no_activity = no_activity_df.sort_values(
            by=["FAILURES_PREVIOUS_7D", "STALE_HOURS", "STATUS_SORT"],
            ascending=[False, False, False],
        ).head(CONFIG["TOP_TREND_ROWS"])
        st.warning(
            f"{len(no_activity_df)} pipeline(s) had failures last week but "
            "recorded no runs this week."
        )
        st.caption(
            "These pipelines are separated from Improving because a reduction "
            "caused by no execution does not prove recovery."
        )
        st.dataframe(
            table_for_display(top_no_activity, trend_columns),
            use_container_width=True,
            hide_index=True,
            height=min(600, 38 + 35 * len(top_no_activity)),
        )


# -----------------------------------------------------------------------------
# Runtime Watchlist
# -----------------------------------------------------------------------------
with tab_runtime:
    st.subheader("Runtime Watchlist")
    st.caption(
        "This tab compares each pipeline's latest runtime with its own "
        "successful-run baseline from the current seven-day reporting period."
    )

    critical_runtime_df = filtered_df[
        filtered_df["RUNTIME_STATUS"] == "CRITICAL SLOWDOWN"
    ].copy()
    high_runtime_df = filtered_df[
        filtered_df["RUNTIME_STATUS"] == "HIGH SLOWDOWN"
    ].copy()
    elevated_runtime_df = filtered_df[
        filtered_df["RUNTIME_STATUS"] == "ELEVATED"
    ].copy()
    baseline_ready_df = filtered_df[
        filtered_df["RUNTIME_BASELINE_READY_FLAG"]
    ].copy()
    insufficient_history_df = filtered_df[
        filtered_df["RUNTIME_STATUS"].isin(
            {"INSUFFICIENT HISTORY", "NOT MEASURED"}
        )
    ].copy()

    runtime_k1, runtime_k2, runtime_k3, runtime_k4, runtime_k5 = st.columns(5)
    runtime_k1.metric(
        "Critical Slowdowns",
        len(critical_runtime_df),
        help="Latest runtime is at least 3× the median and 120 seconds slower.",
    )
    runtime_k2.metric(
        "High Slowdowns",
        len(high_runtime_df),
        help="Latest runtime is at least 2× the median and 60 seconds slower.",
    )
    runtime_k3.metric(
        "Elevated",
        len(elevated_runtime_df),
        help="Latest runtime is at least 1.5× the median and 30 seconds slower.",
    )
    runtime_k4.metric(
        "Baseline Ready",
        len(baseline_ready_df),
        help="Pipelines with at least three successful runtime measurements.",
    )
    runtime_k5.metric(
        "Insufficient History",
        len(insufficient_history_df),
        help="Pipelines that do not yet have three successful runtime measurements.",
    )

    st.info(
        "**Classification guardrails:** Elevated ≥ 1.5× median and +30 sec; "
        "High ≥ 2× and +60 sec; Critical ≥ 3× and +120 sec. "
        "At least three successful runs are required. These are operational "
        "heuristics, not SLA declarations."
    )
    st.caption(
        "The dashboard reads these results from the precomputed report table. "
        "It does not schedule a task or keep a warehouse running in the background."
    )

    runtime_columns = [
        "RUNTIME_STATUS",
        "FULL_PIPELINE_NAME",
        "PIPELINE_NAME",
        "OBJECT_TYPE",
        "LAST_STATUS",
        "LAST_RUN_DURATION_SECONDS",
        "MEDIAN_RUNTIME_SECONDS_7D",
        "P95_RUNTIME_SECONDS_7D",
        "RUNTIME_VS_MEDIAN_PERCENT",
        "SUCCESSFUL_RUNTIME_RUNS_7D",
        "FAILURES_24H",
    ]

    st.subheader("Slowdown Investigation Queue")
    if runtime_watchlist_df.empty:
        st.success(
            "No pipelines match the elevated, high, or critical runtime thresholds."
        )
    else:
        ordered_runtime_watchlist = runtime_watchlist_df.sort_values(
            by=[
                "RUNTIME_STATUS_SORT",
                "RUNTIME_VS_MEDIAN_PERCENT",
                "LAST_RUN_DURATION_SECONDS",
            ],
            ascending=[False, False, False],
            na_position="last",
        ).head(CONFIG["TOP_RUNTIME_ROWS"])

        if not critical_runtime_df.empty:
            st.error(
                f"{len(critical_runtime_df)} critical runtime slowdown(s) require "
                "prompt review."
            )
        else:
            st.warning(
                f"{len(runtime_watchlist_df)} pipeline(s) are slower than their "
                "recent successful-run baselines."
            )

        runtime_watchlist_display = table_for_display(
            ordered_runtime_watchlist,
            runtime_columns,
        )
        st.dataframe(
            runtime_watchlist_display,
            use_container_width=True,
            hide_index=True,
            height=min(620, 38 + 35 * len(runtime_watchlist_display)),
        )

        st.download_button(
            label="Download Runtime Watchlist CSV",
            data=dataframe_to_csv_bytes(runtime_watchlist_display),
            file_name="pipeline_health_monitor_runtime_watchlist.csv",
            mime="text/csv",
            key="download_runtime_watchlist_csv",
        )

    st.subheader("Runtime Coverage by Pipeline Type")
    runtime_coverage = (
        filtered_df.groupby("OBJECT_TYPE", as_index=False)
        .agg(
            PIPELINES=("PIPELINE_NAME", "count"),
            BASELINE_READY=("RUNTIME_BASELINE_READY_FLAG", "sum"),
            RUNTIME_WATCHLIST=("RUNTIME_WATCHLIST_FLAG", "sum"),
        )
        .sort_values("PIPELINES", ascending=False)
    )
    runtime_coverage["BASELINE_COVERAGE_PERCENT"] = (
        100.0
        * runtime_coverage["BASELINE_READY"]
        / runtime_coverage["PIPELINES"].where(
            runtime_coverage["PIPELINES"].ne(0)
        )
    ).fillna(0).round(1)
    runtime_coverage = runtime_coverage.rename(
        columns={
            "OBJECT_TYPE": "Pipeline Type",
            "PIPELINES": "Pipelines",
            "BASELINE_READY": "Baseline Ready",
            "RUNTIME_WATCHLIST": "Slowdown Watchlist",
            "BASELINE_COVERAGE_PERCENT": "Baseline Coverage %",
        }
    )
    st.dataframe(
        runtime_coverage,
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------------------------------------------------------
# Root Causes
# -----------------------------------------------------------------------------
with tab_root:
    st.subheader("Root Causes")
    st.caption(
        "This tab groups recurring technical failures into plain-language "
        "categories and surfaces the most frequent messages."
    )

    if root_df.empty:
        st.success("No recurring failure root causes are available.")
    else:
        category_summary = (
            root_df.groupby("ROOT_CAUSE_CATEGORY", as_index=False)["FAILURE_COUNT"]
            .sum()
            .sort_values("FAILURE_COUNT", ascending=False)
            .rename(
                columns={
                    "ROOT_CAUSE_CATEGORY": "Issue Category",
                    "FAILURE_COUNT": "Occurrences",
                }
            )
        )

        rc1, rc2, rc3 = st.columns(3)
        rc1.metric("Failure Categories", len(category_summary))
        rc2.metric("Total Occurrences", safe_int(category_summary["Occurrences"].sum()))
        rc3.metric(
            "Top Category",
            safe_text(category_summary.iloc[0]["Issue Category"])
            if not category_summary.empty
            else "None",
        )

        st.subheader("Root Cause Categories")
        total_occurrences = safe_number(category_summary["Occurrences"].sum())
        category_summary["% of Occurrences"] = (
            100.0 * category_summary["Occurrences"] / total_occurrences
            if total_occurrences
            else 0.0
        )
        category_summary["% of Occurrences"] = category_summary[
            "% of Occurrences"
        ].round(1)

        if len(category_summary) <= 3:
            st.caption(
                "A compact ranked view is shown because only a few root-cause "
                "categories are present in the current reporting period."
            )
            st.dataframe(
                category_summary,
                use_container_width=True,
                hide_index=True,
                height=38 + 35 * len(category_summary),
            )
        else:
            category_chart, category_table = st.columns([2, 1])

            with category_chart:
                st.bar_chart(
                    category_summary.set_index("Issue Category")["Occurrences"],
                    height=280,
                )

            with category_table:
                st.dataframe(
                    category_summary,
                    use_container_width=True,
                    hide_index=True,
                    height=min(280, 38 + 35 * len(category_summary)),
                )

        st.subheader("Detailed Error Messages")
        root_display = root_df[
            [
                "ERROR_MESSAGE",
                "FAILURE_COUNT",
                "PERCENT_OF_FAILURES",
                "ROOT_CAUSE_CATEGORY",
            ]
        ].copy()
        root_display["ERROR_MESSAGE"] = root_display["ERROR_MESSAGE"].apply(
            lambda value: truncate_text(value, 220)
        )
        root_display["PERCENT_OF_FAILURES"] = root_display[
            "PERCENT_OF_FAILURES"
        ].round(1)
        root_display = root_display.rename(
            columns={
                "ERROR_MESSAGE": "Error Message",
                "FAILURE_COUNT": "Occurrences",
                "PERCENT_OF_FAILURES": "% of Failures",
                "ROOT_CAUSE_CATEGORY": "Issue Category",
            }
        )
        st.dataframe(
            root_display,
            use_container_width=True,
            hide_index=True,
            height=min(460, 38 + 35 * len(root_display)),
        )


# -----------------------------------------------------------------------------
# Pipeline Investigation
# -----------------------------------------------------------------------------
with tab_investigation:
    st.subheader("Pipeline Investigation")
    st.caption(
        "Search by pipeline, database, schema, object type, or full alert-style name."
    )

    search_text = st.text_input(
        "Search pipeline",
        placeholder=(
            "Example: DAILY_LOAD, OPERATIONS, "
            "DEMO_DATABASE-OPERATIONS-DAILY_LOAD"
        ),
    )

    investigation_df = filtered_df.copy()
    if search_text.strip():
        normalized_query = normalize_search_text(search_text)
        investigation_df = investigation_df[
            investigation_df["SEARCH_TEXT"].str.contains(
                normalized_query, regex=False, na=False
            )
        ]

    if investigation_df.empty:
        st.warning("No pipelines match the current search and filters.")
    else:
        investigation_df = investigation_df.sort_values(
            by=["PRIORITY_SORT", "FAILURES_7D", "STALE_HOURS"],
            ascending=[False, False, False],
        )

        selected_pipeline = st.selectbox(
            "Select a pipeline",
            options=investigation_df["FULL_PIPELINE_NAME"].drop_duplicates().tolist(),
            index=0,
        )

        selected_rows = investigation_df[
            investigation_df["FULL_PIPELINE_NAME"] == selected_pipeline
        ]

        if selected_rows.empty:
            st.warning("The selected pipeline is no longer available in the filtered data.")
            st.stop()

        row = selected_rows.iloc[0]
        status = safe_text(row.get("LAST_STATUS"), "UNKNOWN")
        object_type = safe_text(row.get("OBJECT_TYPE"))
        priority = safe_text(row.get("PRIORITY"), "Healthy")
        risk_level = safe_text(row.get("RISK_LEVEL"), "LOW")
        failures_24h = safe_int(row.get("FAILURES_24H"))
        failures_7d = safe_int(row.get("FAILURES_7D"))
        failures_previous_7d = safe_int(row.get("FAILURES_PREVIOUS_7D"))
        failure_change = safe_int(row.get("FAILURE_CHANGE"))
        trend = safe_text(row.get("FAILURE_TREND"), "■ Stable")
        review_category_value = safe_text(row.get("REVIEW_CATEGORY"), "No Action")
        last_error = safe_text(row.get("ERROR_MESSAGE"), "")
        stale_text = format_elapsed_hours(row.get("STALE_HOURS"))
        runtime_status_value = safe_text(
            row.get("RUNTIME_STATUS"),
            "NOT MEASURED",
        ).upper()

        d1, d2, d3, d4, d5 = st.columns(5)
        d1.metric("Status", status)
        d2.metric("Pipeline Type", object_type)
        d3.metric("Investigation Priority", priority)
        d4.metric("Failures Last 24H", failures_24h)
        d5.metric("Failure Frequency Risk", risk_level)

        detail_left, detail_right = st.columns(2)
        with detail_left:
            st.markdown(f"**Pipeline:** {safe_text(row.get('PIPELINE_NAME'))}")
            st.markdown(f"**Full Pipeline Name:** {safe_text(row.get('FULL_PIPELINE_NAME'))}")
            st.markdown(f"**Type:** {object_type}")
            st.markdown(f"**Last Refresh:** {stale_text}")

        with detail_right:
            st.markdown(f"**Database:** {safe_text(row.get('DATABASE_NAME'))}")
            st.markdown(f"**Schema:** {safe_text(row.get('SCHEMA_NAME'))}")
            st.markdown(f"**Issue Category:** {safe_text(row.get('ISSUE_CATEGORY'))}")
            st.markdown(f"**Operational Impact:** {safe_text(row.get('RISK_REASON'))}")

        render_investigation_status(review_category_value)
        render_trend_status(trend)

        st.subheader("Runtime Performance")
        (
            runtime_detail_1,
            runtime_detail_2,
            runtime_detail_3,
            runtime_detail_4,
            runtime_detail_5,
        ) = st.columns(5)
        runtime_detail_1.metric(
            "Latest Runtime",
            format_duration_seconds(row.get("LAST_RUN_DURATION_SECONDS")),
        )
        runtime_detail_2.metric(
            "7D Average",
            format_duration_seconds(row.get("AVG_RUNTIME_SECONDS_7D")),
        )
        runtime_detail_3.metric(
            "7D Median",
            format_duration_seconds(row.get("MEDIAN_RUNTIME_SECONDS_7D")),
        )
        runtime_detail_4.metric(
            "7D P95",
            format_duration_seconds(row.get("P95_RUNTIME_SECONDS_7D")),
        )
        runtime_detail_5.metric(
            "Latest vs Median",
            format_runtime_percent(row.get("RUNTIME_VS_MEDIAN_PERCENT")),
            help=(
                f"Runtime classification: {runtime_status_value.title()}. "
                "At least three successful runs are required for a baseline."
            ),
        )
        render_runtime_status(row)

        if last_error:
            st.error(f"Last Error: {last_error}")
        else:
            st.success("No current error message is recorded.")

        render_suggested_steps(row)
        render_runtime_guidance(row)

        st.subheader("Week-over-Week Comparison")
        w1, w2, w3 = st.columns(3)
        w1.metric("Failures This Week", failures_7d)
        w2.metric("Failures Last Week", failures_previous_7d)
        w3.metric(
            "Change",
            f"{failure_change:+d}",
            delta=failure_change,
            delta_color="inverse",
        )

        with st.expander("Technical Details"):
            technical_rows = [
                ("Raw last status", status),
                ("Status sort value", safe_int(row.get("STATUS_SORT"))),
                ("Runs in current seven days", safe_int(row.get("TOTAL_RUNS_7D"))),
                ("Failures previous seven days", failures_previous_7d),
                ("Failure change", failure_change),
                ("Raw stale hours", round(safe_number(row.get("STALE_HOURS")), 2)),
                ("Runtime status", runtime_status_value),
                (
                    "Successful runtime runs in seven days",
                    safe_int(row.get("SUCCESSFUL_RUNTIME_RUNS_7D")),
                ),
                (
                    "Latest runtime seconds",
                    round(safe_number(row.get("LAST_RUN_DURATION_SECONDS")), 2),
                ),
                (
                    "Average runtime seconds (7D)",
                    round(safe_number(row.get("AVG_RUNTIME_SECONDS_7D")), 2),
                ),
                (
                    "Median runtime seconds (7D)",
                    round(safe_number(row.get("MEDIAN_RUNTIME_SECONDS_7D")), 2),
                ),
                (
                    "P95 runtime seconds (7D)",
                    round(safe_number(row.get("P95_RUNTIME_SECONDS_7D")), 2),
                ),
                (
                    "Runtime vs median percent",
                    round(safe_number(row.get("RUNTIME_VS_MEDIAN_PERCENT")), 1),
                ),
            ]

            optional_fields = [
                ("Last run timestamp", "LAST_RUN_TIME"),
                ("Last run start timestamp", "LAST_RUN_START_TIME"),
                ("Event timestamp", "EVENT_TIME"),
                ("Error code", "ERROR_CODE"),
                ("Warehouse", "WAREHOUSE"),
                ("Report generated at", "REPORT_GENERATED_AT"),
            ]
            for label, field_name in optional_fields:
                if field_name in row.index and pd.notna(row[field_name]):
                    technical_rows.append((label, safe_text(row[field_name])))

            technical_df = pd.DataFrame(
                technical_rows,
                columns=["Detail", "Value"],
            )
            st.dataframe(
                technical_df,
                use_container_width=True,
                hide_index=True,
            )


# =============================================================================
# Footer
# =============================================================================
st.divider()
st.caption(
    f"Source: {SUMMARY_TABLE} | {ROOT_CAUSE_TABLE} | "
    "Read-only reporting against precomputed Snowflake monitor tables."
)
