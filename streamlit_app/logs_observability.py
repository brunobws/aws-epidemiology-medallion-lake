####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Logs Observability tab module for pipeline health monitoring.
#   Displays execution logs, success rates, and pipeline metrics
#   for tracking data pipeline reliability and performance.
#
#   Features:
#   - Pipeline health KPIs
#   - Execution trend charts (weekly)
#   - Status distribution visualization
#   - Job performance metrics
#   - Recent execution details table
####################################################################

########### imports ################
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.logger import get_logger
from utils.athena_service import AthenaService
from utils.analytics_service import AnalyticsService
from utils.parser_service import ParserService
from utils.cache_manager import cached_query
from config import (
    ATHENA_LOGS_DATABASE, LOGS_TABLE, CHART_HEIGHT,
    CHART_COLOR_PRIMARY, CHART_COLOR_SUCCESS, CHART_COLOR_ERROR, CHART_COLOR_WARNING,
    DATA_LAYERS
)
from theme import (
    COLOR_ORANGE, COLOR_SUCCESS, COLOR_ERROR, COLOR_WARNING, COLOR_INFO,
    apply_professional_theme, card_css
)
###################################

logger = get_logger(__name__)

# Apply card styling globally
st.markdown(card_css(), unsafe_allow_html=True)


@cached_query(ttl_seconds=300)
def fetch_logs_data(athena_service: AthenaService) -> pd.DataFrame:
    """Fetch execution logs from the logs table (last 90 days)."""
    query = f"""
    SELECT
        start_execution,
        end_execution,
        source,
        table_name,
        job_name,
        status,
        error,
        layer,
        error_description,
        warning_description,
        has_bdq,
        critical_table,
        file_name,
        count,
        info,
        dt_ref
    FROM "{ATHENA_LOGS_DATABASE}"."{LOGS_TABLE}"
    WHERE dt_ref >= DATE_ADD('day', -90, CURRENT_DATE)
    ORDER BY start_execution DESC
    LIMIT 10000
    """
    try:
        df = athena_service.query_logs(query)
        
        # Process timestamps and durations
        df = ParserService.extract_execution_duration(df)
        
        return df
    except Exception as e:
        logger.error(f"Error fetching logs: {str(e)}")
        st.error(f"Failed to fetch logs: {str(e)}")
        return pd.DataFrame()


def render_logs_observability(athena_service: AthenaService):
    """Render the Logs Observability tab."""

    # Load data
    with st.spinner("Loading execution logs..."):
        df = fetch_logs_data(athena_service)

    if df.empty:
        st.warning("No logs data available")
        return

    ####################################################################
    # FILTERS
    ####################################################################
    st.subheader("🔍 Filters")

    col1, col2, col3 = st.columns(3)

    with col1:
        selected_layers = st.multiselect(
            "Layer",
            options=DATA_LAYERS,
            default=DATA_LAYERS,
            key="logs_layers"
        )

    with col2:
        selected_jobs = st.multiselect(
            "Job Name",
            options=sorted(df["job_name"].dropna().unique()),
            default=list(sorted(df["job_name"].dropna().unique()))[:5],
            key="logs_jobs"
        )

    with col3:
        selected_status = st.multiselect(
            "Status",
            options=sorted(df["status"].unique()),
            default=sorted(df["status"].unique()),
            key="logs_status"
        )

    # Apply filters
    filtered_df = df[
        (df["layer"].isin(selected_layers)) &
        (df["job_name"].isin(selected_jobs)) &
        (df["status"].isin(selected_status))
    ]

    st.divider()

    ####################################################################
    # KPIs SECTION
    ####################################################################
    st.subheader("Pipeline Metrics")

    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)

    with kpi_col1:
        total_execs = len(filtered_df)
        st.metric("Total Executions", f"{total_execs:,}")

    with kpi_col2:
        success_rate = AnalyticsService.success_rate(filtered_df, "status", "success")
        st.metric("Success Rate", f"{success_rate:.1f}%")

    with kpi_col3:
        # Count errors - status can be "error" or "FAILED"
        error_count = len(filtered_df[(filtered_df["status"].str.lower() == "error") | 
                                      (filtered_df["status"] == "FAILED")])
        st.metric("Errors", error_count)

    with kpi_col4:
        warning_count = filtered_df["warning_description"].notna().sum()
        st.metric("Warnings", warning_count)

    with kpi_col5:
        avg_duration = filtered_df["duration_seconds"].mean()
        st.metric("Avg Duration", f"{avg_duration:.1f}s" if pd.notna(avg_duration) else "N/A")

    st.divider()

    ####################################################################
    # CHARTS SECTION
    ####################################################################
    col1, col2 = st.columns(2)

    # Chart 1: Executions over time
    with col1:
        st.subheader("Executions Per Day")

        # Simple grouping by date
        chart_data = filtered_df.copy()
        chart_data['exec_date'] = pd.to_datetime(chart_data['start_execution']).dt.date
        daily_exec = chart_data.groupby('exec_date').size().reset_index(name='count')
        daily_exec['exec_date'] = pd.to_datetime(daily_exec['exec_date'])
        daily_exec = daily_exec.sort_values('exec_date')

        if not daily_exec.empty and len(daily_exec) > 0:
            fig = px.line(
                daily_exec,
                x="exec_date",
                y="count",
                title="Daily Execution Count",
                labels={"exec_date": "Date", "count": "Count"},
                color_discrete_sequence=[COLOR_ORANGE],
                height=CHART_HEIGHT,
                markers=True
            )
            # Show only first, middle, and last dates to avoid repetition
            tick_positions = [daily_exec['exec_date'].iloc[0]]
            if len(daily_exec) > 2:
                tick_positions.append(daily_exec['exec_date'].iloc[len(daily_exec)//2])
            tick_positions.append(daily_exec['exec_date'].iloc[-1])
            
            fig.update_xaxes(
                tickformat="%Y-%m-%d",
                tickvals=tick_positions,
                tickangle=0
            )
            fig.update_layout(hovermode="x unified")
            fig = apply_professional_theme(fig)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No data available for the selected filters")

    # Chart 2: Status distribution
    with col2:
        st.subheader("Execution Status Distribution")

        status_counts = ParserService.count_by_status(filtered_df, "status")

        if status_counts:
            # Define color mapping for each status (lowercase as per data)
            status_colors = {
                "success": COLOR_SUCCESS,      # Green
                "error": COLOR_ERROR,          # Red
                "warning": COLOR_WARNING,      # Gold
                "SUCCEEDED": COLOR_SUCCESS,    # Fallback uppercase
                "FAILED": COLOR_ERROR,         # Fallback uppercase
                "running": COLOR_INFO,         # Blue
                "cancelled": "#808080"         # Gray
            }
            
            colors = [status_colors.get(status.lower() if isinstance(status, str) else str(status), COLOR_ERROR) 
                     for status in status_counts.keys()]
            
            fig = go.Figure(data=[go.Pie(
                labels=list(status_counts.keys()),
                values=list(status_counts.values()),
                marker=dict(colors=colors)
            )])
            fig.update_layout(height=CHART_HEIGHT)
            fig = apply_professional_theme(fig)
            st.plotly_chart(fig, width='stretch')

    st.divider()

    col1, col2 = st.columns(2)

    # Chart 3: Execution duration by job
    with col1:
        st.subheader("Avg Duration by Job")

        duration_agg = AnalyticsService.group_by_aggregation(
            filtered_df[filtered_df["duration_seconds"].notna()],
            group_cols=["job_name"],
            agg_col="duration_seconds",
            agg_func="mean",
            sort=True,
            limit=10
        )

        if not duration_agg.empty:
            fig = px.bar(
                duration_agg.sort_values("mean", ascending=True),
                y="job_name",
                x="mean",
                title="Average Execution Duration by Job",
                labels={"job_name": "Job", "mean": "Duration (seconds)"},
                color_discrete_sequence=[COLOR_WARNING],
                height=CHART_HEIGHT,
                orientation="h"
            )
            fig.update_layout(showlegend=False)
            fig = apply_professional_theme(fig)
            st.plotly_chart(fig, width='stretch')

    # Chart 4: Executions by layer
    with col2:
        st.subheader("Executions by Layer")

        layer_agg = AnalyticsService.group_by_aggregation(
            filtered_df,
            group_cols=["layer"],
            agg_col="status",
            agg_func="count",
            sort=True
        )

        if not layer_agg.empty:
            fig = px.bar(
                layer_agg,
                x="layer",
                y="count",
                title="Execution Count by Layer",
                labels={"layer": "Layer", "count": "Count"},
                color_discrete_sequence=[COLOR_ORANGE],
                height=CHART_HEIGHT
            )
            fig.update_layout(showlegend=False)
            fig = apply_professional_theme(fig)
            st.plotly_chart(fig, width='stretch')

    st.divider()

    ####################################################################
    # RECENT EXECUTIONS TABLE SECTION
    ####################################################################
    st.subheader("Recent Executions")

    display_cols = ["start_execution", "job_name", "status", "layer", "error", "duration_seconds"]
    display_df = filtered_df[display_cols].copy()
    display_df.columns = ["Start Time", "Job", "Status", "Layer", "Error", "Duration (s)"]

    st.dataframe(
        display_df.sort_values("Start Time", ascending=False),
        width='stretch',
        height=400,
    )

    ####################################################################
    # EXPORT SECTION
    ####################################################################
    col1, col2 = st.columns(2)

    with col1:
        csv_data = display_df.to_csv(index=False)
        st.download_button(
            label="📥 CSV",
            data=csv_data,
            file_name="execution_logs.csv",
            mime="text/csv",
        )

    with col2:
        json_data = display_df.to_json(orient="records")
        st.download_button(
            label="📥 JSON",
            data=json_data,
            file_name="execution_logs.json",
            mime="application/json",
        )
