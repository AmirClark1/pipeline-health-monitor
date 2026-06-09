import os
import random
import hashlib
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Pipeline Health Monitor",
    page_icon="❤️",
    layout="wide"
)

conn = st.connection("snowflake", ttl=os.getenv("SNOWFLAKE_CONNECTION_TTL"))

st.title("Pipeline Health Monitor")

st.caption(
    "Proof-of-concept dashboard for pipeline health monitoring, failure trend analysis, "
    "and risk-based prioritization"
)

st.caption(
    f"Data as of: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
    "(simulated data refreshed every 5 minutes)"
)

with st.expander("How to Read This Dashboard"):
    st.markdown("""
**Risk Score (0-100)** — Higher = more operational risk. Calculated from:

| Factor | Weight | Example |
|--------|--------|---------|
| Failures in last 24h | x5 per failure | 3 failures = +15 |
| Warnings in last 24h | x2 per warning | 2 warnings = +4 |
| Failures in last 7d | x1 per failure | 10 failures = +10 |
| Hours since last run | x1 per hour | 6 hours stale = +6 |
| Pipeline criticality | HIGH=12, MEDIUM=7 | HIGH pipeline = +12 |
| Runtime spike (>1.5x avg) | +8 if detected | Spike = +8 |
| SLA breach (>1.5x expected freq) | +10 if breached | Breach = +10 |

Score is capped at 100.

**Risk Levels:**
- **CRITICAL** (81-100): Immediate investigation needed
- **HIGH RISK** (61-80): Trending badly, investigate soon
- **WATCH** (31-60): Monitor closely
- **HEALTHY** (0-30): Operating normally

**Failure Rate %** — Based on the last 24 runs (roughly 24 hours for hourly pipelines).
Recent rate is compared against historical rate (runs 25-96) to detect degradation.

**"What should I investigate first?"** — Pipelines are ranked by risk score.
The top of the list is where your attention should go.
    """)

PIPELINE_DEFS = [
    ("DMS_CAD_SYNC", "AWS DMS", "HIGH", "Data Engineering", 1),
    ("UKG_EMPLOYEE_LOAD", "Snowflake Dynamic Table", "HIGH", "Data Engineering", 2),
    ("VECTOR_GIS_LOAD", "AWS Glue", "MEDIUM", "GIS / Data Engineering", 24),
    ("FDC_SHIFTS_LOAD", "Snowflake Task", "MEDIUM", "Data Engineering", 4),
    ("LAMBDA_FILE_VALIDATION", "AWS Lambda", "HIGH", "Cloud Operations", 1),
    ("PAYROLL_EXPORT", "Snowflake Task", "HIGH", "Data Engineering", 24),
    ("PERMITS_DAILY_LOAD", "AWS Glue", "MEDIUM", "Application Support", 24),
    ("PUBLIC_SAFETY_AUDIT_LOAD", "Snowflake Dynamic Table", "HIGH", "Data Engineering", 6),
]

MOCK_ERRORS = [
    "Replication task stopped unexpectedly",
    "Connection timeout after 30s",
    "OutOfMemoryError: Java heap space",
    "Source file arrived late",
    "Timeout while validating inbound file",
    "AccessDeniedException: s3://bucket/path",
    "Table not found: RAW.ORDERS_STAGING",
    "Rate exceeded for API call",
    "Schema mismatch: expected 12 columns, got 11",
    "Credential expired for role DMS_REPLICATION_ROLE",
]


def _seed(name):
    return int(hashlib.md5(name.encode()).hexdigest(), 16) % (2**32)


def criticality_weight(value):
    return {"HIGH": 12, "MEDIUM": 7, "LOW": 3}.get(value, 3)


def risk_level(score):
    if score >= 81:
        return "CRITICAL"
    if score >= 61:
        return "HIGH RISK"
    if score >= 31:
        return "WATCH"
    return "HEALTHY"


def hours_ago(ts, now):
    if pd.isna(ts):
        return "No recent failure"
    hours = int((now - ts).total_seconds() // 3600)
    if hours < 1:
        return "Less than 1 hour ago"
    if hours == 1:
        return "1 hour ago"
    return f"{hours} hours ago"


def trend_label(recent_rate, historical_rate):
    diff = recent_rate - historical_rate
    if diff >= 0.08:
        return "\u25b2 Getting Worse"
    if diff <= -0.08:
        return "\u25bc Improving"
    return "\u25ac Stable"


@st.cache_data(ttl=300)
def generate_run_history():
    now = datetime.now().replace(minute=30, second=0, microsecond=0)
    all_runs = []

    for pipeline_name, service, criticality, owner, expected_freq in PIPELINE_DEFS:
        rng = random.Random(_seed(pipeline_name))
        fail_prob = rng.uniform(0.03, 0.18)

        for i in range(168):
            run_time = now - timedelta(hours=i)
            roll = rng.random()

            if roll < fail_prob:
                status = "FAILED"
            elif roll < fail_prob + 0.05:
                status = "WARNING"
            else:
                status = "SUCCESS"

            duration_seconds = max(10, int(rng.gauss(180, 75)))

            all_runs.append({
                "pipeline_name": pipeline_name,
                "service": service,
                "criticality": criticality,
                "owner": owner,
                "expected_frequency_hours": expected_freq,
                "run_time": run_time,
                "status": status,
                "duration_seconds": duration_seconds,
                "error_message": rng.choice(MOCK_ERRORS) if status == "FAILED" else "",
            })

    return pd.DataFrame(all_runs)


@st.cache_data(ttl=300)
def compute_pipeline_summary(history_df):
    now = history_df["run_time"].max()
    last_24h = now - timedelta(hours=24)
    last_7d = now - timedelta(days=7)

    summaries = []

    for name in history_df["pipeline_name"].unique():
        pipe_df = history_df[history_df["pipeline_name"] == name].sort_values("run_time", ascending=False)
        latest = pipe_df.iloc[0]

        failed_df = pipe_df[pipe_df["status"] == "FAILED"].sort_values("run_time", ascending=False)
        last_failure_time = failed_df.iloc[0]["run_time"] if len(failed_df) > 0 else pd.NaT
        last_failure_ago = hours_ago(last_failure_time, now)

        failures_24h = len(pipe_df[(pipe_df["run_time"] >= last_24h) & (pipe_df["status"] == "FAILED")])
        warnings_24h = len(pipe_df[(pipe_df["run_time"] >= last_24h) & (pipe_df["status"] == "WARNING")])
        failures_7d = len(pipe_df[(pipe_df["run_time"] >= last_7d) & (pipe_df["status"] == "FAILED")])

        recent = pipe_df.head(24)
        historical = pipe_df.iloc[24:96]
        recent_fail_rate = (recent["status"] == "FAILED").mean()
        historical_fail_rate = (historical["status"] == "FAILED").mean() if len(historical) > 0 else 0
        trend = trend_label(recent_fail_rate, historical_fail_rate)

        avg_duration = pipe_df["duration_seconds"].mean()
        latest_duration = latest["duration_seconds"]
        runtime_spike = latest_duration > avg_duration * 1.5
        runtime_change_pct = round(((latest_duration - avg_duration) / avg_duration) * 100, 1)

        stale_hours = round((now - latest["run_time"]).total_seconds() / 3600, 1)
        expected_freq = latest["expected_frequency_hours"] if "expected_frequency_hours" in pipe_df.columns else 1
        sla_breach = stale_hours > expected_freq * 1.5

        raw_risk_score = (
            failures_24h * 5
            + warnings_24h * 2
            + failures_7d * 1
            + stale_hours * 1
            + criticality_weight(latest["criticality"])
            + (8 if runtime_spike else 0)
            + (10 if sla_breach else 0)
        )

        risk_score = min(100, round(raw_risk_score, 1))

        summaries.append({
            "pipeline_name": name,
            "service": latest["service"],
            "criticality": latest["criticality"],
            "owner": latest["owner"],
            "status": latest["status"],
            "last_run": latest["run_time"],
            "last_failure_time": last_failure_time,
            "last_failure_ago": last_failure_ago,
            "failures_24h": failures_24h,
            "warnings_24h": warnings_24h,
            "failures_7d": failures_7d,
            "stale_hours": stale_hours,
            "expected_frequency_hours": expected_freq,
            "sla_breach": sla_breach,
            "latest_duration_seconds": latest_duration,
            "avg_duration_seconds": round(avg_duration, 1),
            "runtime_change_pct": runtime_change_pct,
            "runtime_spike": runtime_spike,
            "last_error": latest["error_message"],
            "recent_fail_rate": recent_fail_rate,
            "historical_fail_rate": historical_fail_rate,
            "trend": trend,
            "risk_score": risk_score,
            "risk_level": risk_level(risk_score),
        })

    return pd.DataFrame(summaries).sort_values("risk_score", ascending=False)


@st.cache_data(ttl=300)
def compute_predictions(history_df):
    predictions = []

    for name in history_df["pipeline_name"].unique():
        pipe_df = history_df[history_df["pipeline_name"] == name].sort_values("run_time", ascending=False)

        recent = pipe_df.head(24)
        historical = pipe_df.iloc[24:96]

        recent_fail_rate = (recent["status"] == "FAILED").mean()
        historical_fail_rate = (historical["status"] == "FAILED").mean() if len(historical) > 0 else 0
        trend = recent_fail_rate - historical_fail_rate

        trend_score = min(1.0, max(0.0, recent_fail_rate * 0.65 + max(0, trend) * 1.75))

        if trend_score > 0.55:
            signal = "CRITICAL"
        elif trend_score > 0.35:
            signal = "LIKELY TO FAIL"
        elif trend_score > 0.18:
            signal = "WATCH"
        else:
            signal = "STABLE"

        predictions.append({
            "pipeline_name": name,
            "service": pipe_df.iloc[0]["service"],
            "criticality": pipe_df.iloc[0]["criticality"],
            "recent_fail_rate": recent_fail_rate,
            "historical_fail_rate": historical_fail_rate,
            "trend_value": trend,
            "trend": trend_label(recent_fail_rate, historical_fail_rate),
            "trend_score": trend_score,
            "signal": signal,
        })

    return pd.DataFrame(predictions).sort_values("trend_score", ascending=False)


@st.cache_data(ttl=300)
def compute_changes_since_yesterday(history_df):
    now = history_df["run_time"].max()
    current_start = now - timedelta(hours=24)
    previous_start = now - timedelta(hours=48)
    rows = []

    for name in history_df["pipeline_name"].unique():
        pipe_df = history_df[history_df["pipeline_name"] == name]

        current_failures = len(pipe_df[
            (pipe_df["run_time"] >= current_start) & (pipe_df["status"] == "FAILED")
        ])

        previous_failures = len(pipe_df[
            (pipe_df["run_time"] >= previous_start) &
            (pipe_df["run_time"] < current_start) &
            (pipe_df["status"] == "FAILED")
        ])

        change = current_failures - previous_failures

        if change > 0:
            direction = "\u25b2 Increased"
        elif change < 0:
            direction = "\u25bc Decreased"
        else:
            direction = "\u25ac No Change"

        rows.append({
            "pipeline_name": name,
            "failures_last_24h": current_failures,
            "failures_previous_24h": previous_failures,
            "change": change,
            "direction": direction,
        })

    return pd.DataFrame(rows).sort_values("change", ascending=False)


history_df = generate_run_history()
df = compute_pipeline_summary(history_df)
preds = compute_predictions(history_df)
changes_df = compute_changes_since_yesterday(history_df)

with st.sidebar:
    st.header("Filters")

    selected_service = st.multiselect(
        "Service",
        options=sorted(df["service"].unique()),
        default=sorted(df["service"].unique())
    )

    selected_pipeline = st.selectbox(
        "Pipeline drilldown",
        options=sorted(df["pipeline_name"].unique())
    )

    stale_threshold = st.slider("Stale threshold (hours)", 1, 48, 6)

    if st.button("Refresh mock data", type="primary"):
        generate_run_history.clear()
        compute_pipeline_summary.clear()
        compute_predictions.clear()
        compute_changes_since_yesterday.clear()
        st.rerun()

filtered_df = df[df["service"].isin(selected_service)]
filtered_preds = preds[preds["service"].isin(selected_service)]

failed_pipelines = len(filtered_df[filtered_df["status"] == "FAILED"])
total_failures_24h = filtered_df["failures_24h"].sum()
total_failures_7d = filtered_df["failures_7d"].sum()

st.subheader("Executive Summary")

top_pipeline = filtered_df.sort_values("risk_score", ascending=False).head(1)

if len(top_pipeline) > 0:
    top = top_pipeline.iloc[0]

    if top["risk_score"] >= 81:
        st.error(
            f"Immediate attention needed: **{top['pipeline_name']}** \u2014 "
            f"risk score {top['risk_score']}/100, {top['failures_24h']} failure(s) in 24h."
        )
    elif top["risk_score"] >= 61:
        st.warning(
            f"Watch closely: **{top['pipeline_name']}** \u2014 "
            f"risk score {top['risk_score']}/100."
        )
    elif top["risk_score"] >= 31:
        st.info(
            f"Monitor: **{top['pipeline_name']}** is the highest-risk item but not critical."
        )
    else:
        st.success("All pipelines operating within acceptable thresholds.")

    action_items = filtered_df[filtered_df["risk_score"] >= 61].sort_values("risk_score", ascending=False).head(3)
    if len(action_items) > 0:
        st.markdown("**Recommended Actions**")
        for _, row in action_items.iterrows():
            st.write(
                f"- Review **{row['pipeline_name']}** \u2014 {row['failures_24h']} failure(s) in 24h, "
                f"{row['failures_7d']} in 7d, trend: {row['trend']}, "
                f"last failure: {row['last_failure_ago']}."
            )
    else:
        st.markdown("**Recommended Actions:** None \u2014 all pipelines within acceptable thresholds.")

with st.container(horizontal=True):
    st.metric("Failed Now", failed_pipelines, border=True)
    st.metric("Failures (24h)", total_failures_24h, border=True)
    st.metric("Failures (7d)", total_failures_7d, border=True)

tab1, tab2, tab3, tab4 = st.tabs([
    "What Should I Investigate First?",
    "Pipeline Drilldown",
    "Failure History",
    "Failure Trends",
])

with tab1:
    st.dataframe(
        filtered_df[
            ["pipeline_name", "service", "status", "failures_24h", "failures_7d",
             "last_failure_ago", "trend", "risk_score", "risk_level", "last_error"]
        ].sort_values("risk_score", ascending=False),
        column_config={
            "pipeline_name": st.column_config.TextColumn("Pipeline"),
            "service": st.column_config.TextColumn("Service"),
            "status": st.column_config.TextColumn("Status"),
            "failures_24h": st.column_config.NumberColumn("Fails 24h"),
            "failures_7d": st.column_config.NumberColumn("Fails 7d"),
            "last_failure_ago": st.column_config.TextColumn("Last Failure"),
            "trend": st.column_config.TextColumn("Trend"),
            "risk_score": st.column_config.ProgressColumn("Risk Score", min_value=0, max_value=100),
            "risk_level": st.column_config.TextColumn("Risk Level"),
            "last_error": st.column_config.TextColumn("Last Error", width="large"),
        },
        hide_index=True,
        use_container_width=True,
    )

    st.subheader("Changes Since Yesterday")
    st.dataframe(
        changes_df,
        column_config={
            "pipeline_name": st.column_config.TextColumn("Pipeline"),
            "failures_last_24h": st.column_config.NumberColumn("Failures Last 24h"),
            "failures_previous_24h": st.column_config.NumberColumn("Failures Previous 24h"),
            "change": st.column_config.NumberColumn("Change"),
            "direction": st.column_config.TextColumn("Direction"),
        },
        hide_index=True,
        use_container_width=True,
    )

with tab2:
    selected_row = df[df["pipeline_name"] == selected_pipeline].iloc[0]
    selected_hist = history_df[history_df["pipeline_name"] == selected_pipeline].sort_values("run_time", ascending=False)

    st.markdown(f"### {selected_pipeline}")

    d1, d2, d3 = st.columns(3)
    d1.metric("Risk Score", f"{selected_row['risk_score']}/100", border=True)
    d2.metric("Failures (24h)", selected_row["failures_24h"], border=True)
    d3.metric("Failures (7d)", selected_row["failures_7d"], border=True)

    st.write(f"**Service:** {selected_row['service']} | **Owner:** {selected_row['owner']} | **Criticality:** {selected_row['criticality']}")
    st.write(f"**Status:** {selected_row['status']} | **Trend:** {selected_row['trend']} | **Last Failure:** {selected_row['last_failure_ago']}")
    st.write(f"**Runtime Change:** {selected_row['runtime_change_pct']}% vs avg | **SLA Breach:** {'Yes' if selected_row['sla_breach'] else 'No'}")

    common_errors = selected_hist[selected_hist["status"] == "FAILED"]["error_message"].value_counts()
    if len(common_errors) > 0:
        st.write(f"**Most Common Error:** {common_errors.index[0]}")

    st.subheader("Recent Runs")
    st.dataframe(
        selected_hist[["run_time", "status", "duration_seconds", "error_message"]].head(30),
        column_config={
            "run_time": st.column_config.DatetimeColumn("Run Time", format="MMM DD HH:mm"),
            "status": st.column_config.TextColumn("Status"),
            "duration_seconds": st.column_config.NumberColumn("Runtime (s)"),
            "error_message": st.column_config.TextColumn("Error", width="large"),
        },
        hide_index=True,
        use_container_width=True,
    )

    st.subheader("Runtime Trend")
    runtime_chart = selected_hist.sort_values("run_time").set_index("run_time")["duration_seconds"]
    st.line_chart(runtime_chart)

with tab3:
    history_hours = st.select_slider(
        "Time window",
        options=[24, 48, 72, 120, 168],
        value=72,
        format_func=lambda x: f"Last {x}h" if x < 168 else "Full 7 days",
    )
    history_cutoff = history_df["run_time"].max() - timedelta(hours=history_hours)

    failure_history = history_df[
        (history_df["status"].isin(["FAILED", "WARNING"])) &
        (history_df["service"].isin(selected_service)) &
        (history_df["run_time"] >= history_cutoff)
    ].sort_values("run_time", ascending=False)

    if len(failure_history) == 0:
        st.success(f"No failures or warnings in the last {history_hours} hours!")
    else:
        st.caption(f"Showing {len(failure_history)} event(s) from the last {history_hours} hours")
        st.dataframe(
            failure_history[
                ["pipeline_name", "service", "status", "run_time", "error_message"]
            ].head(100),
            column_config={
                "pipeline_name": st.column_config.TextColumn("Pipeline"),
                "service": st.column_config.TextColumn("Service"),
                "status": st.column_config.TextColumn("Status"),
                "run_time": st.column_config.DatetimeColumn("Run Time", format="MMM DD HH:mm"),
                "error_message": st.column_config.TextColumn("Error", width="large"),
            },
            hide_index=True,
            use_container_width=True,
        )

    st.subheader("Top Root Causes")
    root_cause_df = failure_history[failure_history["status"] == "FAILED"]
    if len(root_cause_df) > 0:
        error_counts = root_cause_df["error_message"].value_counts().head(5)
        total_errors = error_counts.sum()
        root_cause_display = pd.DataFrame({
            "Error": error_counts.index,
            "Count": error_counts.values,
            "% of Failures": [f"{round(c / total_errors * 100, 1)}%" for c in error_counts.values],
        })
        st.dataframe(root_cause_display, hide_index=True, use_container_width=True)
    else:
        st.success("No failed runs in this time window.")

    st.subheader(f"Failure Timeline (last {history_hours}h)")
    timeline_df = history_df[
        (history_df["status"] == "FAILED") &
        (history_df["service"].isin(selected_service)) &
        (history_df["run_time"] >= history_cutoff)
    ].copy()
    timeline_df["date"] = timeline_df["run_time"].dt.date
    daily_failures = timeline_df.groupby(["date", "pipeline_name"]).size().reset_index(name="failures")
    pivot = daily_failures.pivot(index="date", columns="pipeline_name", values="failures").fillna(0)
    st.bar_chart(pivot)

with tab4:
    st.subheader("Failure Trend Detection")
    st.caption(
        "Compares recent failure rate (last 24 runs) against historical rate (runs 25-96) "
        "to detect pipelines that are deteriorating."
    )

    at_risk = filtered_preds[filtered_preds["signal"].isin(["CRITICAL", "LIKELY TO FAIL"])]

    if len(at_risk) > 0:
        st.warning(f"{len(at_risk)} pipeline(s) trending toward failure based on recent pattern changes")
    else:
        st.success("No pipelines currently showing degradation trends.")

    st.dataframe(
        filtered_preds[
            ["pipeline_name", "service", "recent_fail_rate", "historical_fail_rate",
             "trend", "trend_score", "signal"]
        ],
        column_config={
            "pipeline_name": st.column_config.TextColumn("Pipeline"),
            "service": st.column_config.TextColumn("Service"),
            "recent_fail_rate": st.column_config.ProgressColumn("Recent Fail Rate", min_value=0, max_value=1, format="%.0f%%"),
            "historical_fail_rate": st.column_config.ProgressColumn("Historical Fail Rate", min_value=0, max_value=1, format="%.0f%%"),
            "trend": st.column_config.TextColumn("Trend"),
            "trend_score": st.column_config.ProgressColumn("Trend Score", min_value=0, max_value=1, format="%.0f%%"),
            "signal": st.column_config.TextColumn("Signal"),
        },
        hide_index=True,
        use_container_width=True,
    )

st.divider()

st.caption(
    "Prototype only. Uses simulated pipeline run history (168 hours) to demonstrate health monitoring, "
    "failure trend analysis, risk scoring, and operational prioritization."
)