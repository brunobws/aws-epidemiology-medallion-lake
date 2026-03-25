"""
Configuration module for the Data Platform Dashboard.

Loads all configuration from environment variables and provides defaults.
This is the single source of truth for all application settings.

Author: Data Team
Version: 2.0.0
"""

import os
from pathlib import Path
from typing import Optional

# ==================== APPLICATION METADATA ====================
APP_NAME = "Data Platform Monitoring & Analytics"
APP_VERSION = "2.0.0"
APP_DESCRIPTION = "Transform raw data into insights - Monitor & analyze your Medallion Data Lake"
APP_ICON = "📊"

# ==================== AWS CONFIGURATION ====================
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "gold")
ATHENA_LOGS_DATABASE = os.getenv("ATHENA_LOGS_DATABASE", "logs")
ATHENA_S3_OUTPUT = os.getenv("ATHENA_S3_OUTPUT", "s3://bws-dl-logs-sae1-prd/athena/query_results/")

# ==================== STREAMLIT UI CONFIGURATION ====================
STREAMLIT_LAYOUT = "wide"
STREAMLIT_THEME = "light"
STREAMLIT_INITIAL_SIDEBAR_STATE = "expanded"

# ==================== DATA TABLES ====================
GOLD_TABLE = "tb_ft_breweries_agg"
LOGS_TABLE = "execution_logs"

# ==================== CACHE CONFIGURATION ====================
ENABLE_CACHE = True
CACHE_TTL = 300  # 5 minutes

# ==================== LOGGING CONFIGURATION ====================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# ==================== QUERY CONFIGURATION ====================
ATHENA_QUERY_TIMEOUT_SECONDS = 300  # 5 minutes
ATHENA_MAX_RETRIES = 3
ATHENA_RESULT_LIMIT = 10000

# ==================== DATA QUALITY CONFIGURATION ====================
# Keywords to identify BDQ-related columns and info
BDQ_KEYWORDS = ["test", "quality", "dq", "bdq", "validation"]
CRITICAL_TABLE_KEYWORDS = ["fact", "dimension", "core"]

# ==================== DISPLAY CONFIGURATION ====================
DEFAULT_ROWS_TO_DISPLAY = 1000
MAX_ROWS_TO_DISPLAY = 10000
CHART_HEIGHT = 450
CHART_COLOR_PRIMARY = "#1f77b4"
CHART_COLOR_SUCCESS = "#2ca02c"
CHART_COLOR_ERROR = "#d62728"
CHART_COLOR_WARNING = "#ff7f0e"

# ==================== FILTER CONFIGURATION ====================
MULTI_SELECT_LIMIT = 50
DATE_FORMAT = "%Y-%m-%d"

# ==================== GOLD TABLE COLUMNS ====================
GOLD_COLUMNS = {
    "nm_country": "Country",
    "nm_state": "State",
    "ds_brewery_type": "Brewery Type",
    "qtd_total_breweries": "Total Breweries",
}

# ==================== LOGS TABLE COLUMNS ====================
LOGS_COLUMNS = {
    "start_execution": "Start Time",
    "end_execution": "End Time",
    "source": "Source",
    "table_name": "Table Name",
    "job_name": "Job Name",
    "status": "Status",
    "error": "Error",
    "layer": "Layer",
    "error_description": "Error Description",
    "warning_description": "Warning Description",
    "has_bdq": "Has BDQ",
    "critical_table": "Critical Table",
    "file_name": "File Name",
    "count": "Record Count",
    "info": "Info (JSON)",
    "dt_ref": "Date Reference",
}

# ==================== LAYER TYPES ====================
DATA_LAYERS = ["bronze", "silver", "gold", "quality"]
STATUS_TYPES = ["SUCCEEDED", "FAILED", "RUNNING", "CANCELLED"]

# ==================== THEME COLORS ====================
COLORS = {
    "success": "#2ca02c",
    "warning": "#ff7f0e",
    "error": "#d62728",
    "info": "#1f77b4",
    "light": "#f0f2f6",
}
