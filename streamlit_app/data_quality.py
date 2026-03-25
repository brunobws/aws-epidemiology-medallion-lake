####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Data Quality tab module for monitoring data quality test results.
#   Extracts and displays test failures from quality_logs table,
#   providing insights into data validation metrics.
#
#   Features:
#   - Test result KPIs
#   - Test status visualization (pie chart)
#   - Most failed columns identification
#   - Detailed test results table with filtering
#   - Test data export (CSV, JSON)
####################################################################

########### imports ################
import streamlit as st
import pandas as pd
import json
import ast
import plotly.express as px
import plotly.graph_objects as go
from utils.logger import get_logger
from utils.athena_service import AthenaService
from utils.analytics_service import AnalyticsService
from utils.parser_service import ParserService
from utils.cache_manager import cached_query
from config import (
    ATHENA_LOGS_DATABASE, LOGS_TABLE, CHART_HEIGHT,
    CHART_COLOR_PRIMARY, CHART_COLOR_SUCCESS, CHART_COLOR_ERROR, CHART_COLOR_WARNING
)
from theme import (
    COLOR_ORANGE, COLOR_SUCCESS, COLOR_ERROR, COLOR_WARNING,
    apply_professional_theme, card_css
)
###################################

logger = get_logger(__name__)

# Apply card styling globally
st.markdown(card_css(), unsafe_allow_html=True)


@cached_query(ttl_seconds=300)
def fetch_dq_logs(athena_service: AthenaService) -> pd.DataFrame:
    """Fetch quality logs from the quality_logs table."""
    query = f"""
    SELECT
        start_execution,
        end_execution,
        table_name,
        job_name,
        status,
        has_bdq,
        critical_table,
        error,
        warning_description,
        info,
        dt_ref
    FROM "{ATHENA_LOGS_DATABASE}".quality_logs
    WHERE dt_ref >= DATE_ADD('day', -90, CURRENT_DATE)
    ORDER BY start_execution DESC
    LIMIT 50000
    """
    try:
        df = athena_service.query_logs(query)
        
        # Process durations
        df = ParserService.extract_execution_duration(df)
        
        return df
    except Exception as e:
        logger.error(f"Error fetching DQ logs: {str(e)}")
        st.error(f"Failed to fetch data quality logs: {str(e)}")
        return pd.DataFrame()


def render_data_quality(athena_service: AthenaService):
    """Render the Data Quality tab - focused on test results from quality_logs."""

    # Load data
    with st.spinner("Loading data quality metrics..."):
        df = fetch_dq_logs(athena_service)

    if df.empty:
        st.warning("No data quality data available")
        return

    st.divider()

    ####################################################################
    # KPIs
    ####################################################################
    st.subheader("📊 Data Quality Summary")

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        # Count executions
        total_execs = len(df)
        st.metric("Total Executions", f"{total_execs:,}")

    with kpi_col2:
        # Executions with tests
        has_tests = df[df["info"].notna()].shape[0]
        st.metric("Executions Monitored", f"{has_tests:,}")

    with kpi_col3:
        # Extract test failures from info JSON
        total_failures = 0
        for info_val in df["info"].dropna():
            try:
                if isinstance(info_val, str):
                    try:
                        info_dict = json.loads(info_val)
                    except (json.JSONDecodeError, ValueError):
                        info_dict = ast.literal_eval(info_val)
                else:
                    info_dict = info_val
                
                if isinstance(info_dict, dict):
                    tests = info_dict.get("quality_tests", [])
                    if isinstance(tests, list):
                        for test in tests:
                            if isinstance(test, dict) and test.get("status") == "failure":
                                total_failures += 1
            except Exception as e:
                logger.debug(f"Error counting failures: {str(e)}")
        st.metric("Test Failures", total_failures)

    with kpi_col4:
        # Count successful tests
        total_success = 0
        for info_val in df["info"].dropna():
            try:
                if isinstance(info_val, str):
                    try:
                        info_dict = json.loads(info_val)
                    except (json.JSONDecodeError, ValueError):
                        info_dict = ast.literal_eval(info_val)
                else:
                    info_dict = info_val
                
                if isinstance(info_dict, dict):
                    tests = info_dict.get("quality_tests", [])
                    if isinstance(tests, list):
                        for test in tests:
                            if isinstance(test, dict) and test.get("status") == "success":
                                total_success += 1
            except Exception as e:
                logger.debug(f"Error counting success: {str(e)}")
        st.metric("Test Successes", total_success)

    st.divider()

    ####################################################################
    # DETAILED TEST RESULTS
    ####################################################################
    st.subheader("Test Results Details")

    # Extract all test results from info column
    all_tests = []
    
    for idx, row in df.iterrows():
        try:
            info_val = row.get("info")
            if not info_val:
                continue
            
            # Try JSON parsing first
            if isinstance(info_val, str):
                try:
                    info_dict = json.loads(info_val)
                except (json.JSONDecodeError, ValueError):
                    try:
                        info_dict = ast.literal_eval(info_val)
                    except Exception as e:
                        logger.debug(f"Parse error for row {idx}: {str(e)}")
                        continue
            else:
                info_dict = info_val
            
            # Extract quality tests
            if isinstance(info_dict, dict):
                tests = info_dict.get("quality_tests", [])
                
                if isinstance(tests, list):
                    for test in tests:
                        if isinstance(test, dict):
                            all_tests.append({
                                "start_time": row.get("start_execution"),
                                "table": row.get("table_name"),
                                "job": row.get("job_name"),
                                "column": test.get("column_tested", ""),
                                "test": test.get("test_applied", ""),
                                "status": test.get("status", "")
                            })
        except Exception as e:
            logger.debug(f"Error processing row {idx}: {str(e)}")
            continue
    
    st.info(f"Extracted {len(all_tests)} test results from {len(df)} executions")

    if all_tests:
        tests_df = pd.DataFrame(all_tests)
        
        col1, col2 = st.columns(2)
        
        # Chart 1: Tests by status
        with col1:
            st.subheader("Test Results Summary")
            status_counts = tests_df["status"].value_counts()
            
            status_colors = {
                "success": COLOR_SUCCESS,
                "failure": COLOR_ERROR
            }
            colors = [status_colors.get(s, "#999") for s in status_counts.index]
            
            fig = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="Overall Test Results",
                height=CHART_HEIGHT,
                color_discrete_sequence=colors
            )
            fig = apply_professional_theme(fig)
            st.plotly_chart(fig, width='stretch')
        
        # Chart 2: Failing tests by column
        with col2:
            st.subheader("Most Failed Columns")
            failures = tests_df[tests_df["status"] == "failure"]
            
            if not failures.empty:
                col_failures = failures["column"].value_counts().head(10)
                fig = px.bar(
                    x=col_failures.values,
                    y=col_failures.index,
                    title="Columns with Test Failures",
                    labels={"x": "Failures", "y": "Column"},
                    color_discrete_sequence=[COLOR_ERROR],
                    height=CHART_HEIGHT,
                    orientation="h"
                )
                fig.update_layout(showlegend=False)
                fig = apply_professional_theme(fig)
                st.plotly_chart(fig, width='stretch')
            else:
                st.success("✅ All tests passed!")
        
        st.divider()
        
        # Show all test results table
        st.subheader("All Test Results")
        
        # Filter by status if needed
        filter_status = st.selectbox(
            "Filter by status",
            ["All", "success", "failure"],
            index=0
        )
        
        display_tests = tests_df.copy()
        if filter_status != "All":
            display_tests = display_tests[display_tests["status"] == filter_status]
        
        # Color code the status column
        def color_status(val):
            if val == "failure":
                return "color: red"
            elif val == "success":
                return "color: green"
            return ""
        
        styled_df = display_tests.style.map(color_status, subset=["status"])
        
        st.dataframe(
            display_tests.sort_values("start_time", ascending=False),
            width='stretch',
            height=500
        )
        
        # Export
        col1, col2 = st.columns(2)
        with col1:
            csv = display_tests.to_csv(index=False)
            st.download_button("📥 CSV", csv, "test_results.csv", "text/csv")
        with col2:
            json_data = display_tests.to_json(orient="records")
            st.download_button("📥 JSON", json_data, "test_results.json", "application/json")
    else:
        st.info("No test results found in data")
