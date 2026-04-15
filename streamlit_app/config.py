####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Centralized configuration module for ArboVigilancia SP Dashboard.
#   Loads all application settings from environment variables with
#   sensible defaults. Single source of truth for all configuration.
#
#   Configuration Categories:
#   - Application metadata
#   - AWS/Athena connectivity
#   - Gold data tables
#   - Cache and performance settings
#   - Display and UI preferences
#   - Epidemiological constants
####################################################################

########### imports ################
import os
###################################


####################################################################
# APPLICATION METADATA
####################################################################
APP_NAME = "EpiMind"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "Painel de vigilância epidemiológica de arboviroses em São Paulo"
APP_ICON = "🦟"


####################################################################
# AWS CONFIGURATION
####################################################################
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "gold")
ATHENA_LOGS_DATABASE = os.getenv("ATHENA_LOGS_DATABASE", "logs")
ATHENA_S3_OUTPUT = os.getenv(
    "ATHENA_S3_OUTPUT",
    "s3://bws-dl-logs-sae1-prd/athena/query_results/",
)


####################################################################
# STREAMLIT UI CONFIGURATION
####################################################################
STREAMLIT_LAYOUT = "wide"
STREAMLIT_INITIAL_SIDEBAR_STATE = "expanded"


####################################################################
# GOLD DATA TABLES
####################################################################
TABLE_ALERTS_WEEKLY = "tb_ft_alerta_semanal"
TABLE_RANKING_ANNUAL = "tb_ft_ranking_anual"
TABLE_DEMOGRAPHIC = "tb_ft_perfil_demografico"


####################################################################
# LOGS / OBSERVABILITY TABLES
####################################################################
LOGS_TABLE = "execution_logs"


####################################################################
# CACHE CONFIGURATION
####################################################################
ENABLE_CACHE = True
CACHE_TTL = 604800  # 7 days


####################################################################
# LOGGING CONFIGURATION
####################################################################
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


####################################################################
# QUERY CONFIGURATION
####################################################################
ATHENA_QUERY_TIMEOUT_SECONDS = 300


####################################################################
# DISPLAY CONFIGURATION
####################################################################
CHART_HEIGHT = 420
DEFAULT_ROWS_TO_DISPLAY = 1000
MAX_ROWS_TO_DISPLAY = 10000
CHART_COLOR_PRIMARY = "#1f77b4"
CHART_COLOR_SUCCESS = "#2ca02c"
CHART_COLOR_ERROR = "#d62728"
CHART_COLOR_WARNING = "#ff7f0e"


####################################################################
# FILTER CONFIGURATION
####################################################################
DATE_FORMAT = "%Y-%m-%d"


####################################################################
# EPIDEMIOLOGICAL CONSTANTS
####################################################################
DISEASES = ["dengue", "chikungunya", "zika"]
DISEASES_PT = {
    "dengue": "Dengue",
    "chikungunya": "Chikungunya",
    "zika": "Zika",
}
DISEASE_COLORS = {
    "dengue": "#D62728",
    "chikungunya": "#FF7F0E",
    "zika": "#1F77B4",
}

ALERT_LEVELS = {1: "Verde", 2: "Amarelo", 3: "Laranja", 4: "Vermelho"}
ALERT_COLORS = {
    1: "#2CA02C",  # verde
    2: "#DAA520",  # amarelo
    3: "#FF7F0E",  # laranja
    4: "#D62728",  # vermelho
}

AGE_BRACKETS = ["0-4", "5-14", "15-29", "30-59", "60+"]
SEX_MAP = {"M": "Masculino", "F": "Feminino", "I": "Ignorado"}


####################################################################
# LAYER TYPES
####################################################################
DATA_LAYERS = ["bronze", "silver", "gold", "quality"]
