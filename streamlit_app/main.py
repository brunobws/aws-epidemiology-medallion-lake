####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Main orchestrator for ArboVigilancia SP Dashboard.
#   Initializes the Streamlit app, manages tabs, and coordinates
#   data fetching across all modules.
#
#   Features:
#   - Epidemiological Analytics (alerts, trends, rankings, demographics)
#   - Observability (pipeline health monitoring)
#   - Data Quality (test results validation)
####################################################################

########### imports ################
import streamlit as st
import sys
from pathlib import Path
###################################

sys.path.insert(0, str(Path(__file__).parent))

from config import APP_NAME, STREAMLIT_LAYOUT
from utils.logger import get_logger
from utils.athena_service import AthenaService
from theme import COLOR_DARK_GRAY, COLOR_LIGHT_GRAY, COLOR_ORANGE, COLOR_BORDER

logger = get_logger(__name__)


####################################################################
# PAGE CONFIGURATION
####################################################################
st.set_page_config(
    page_title=APP_NAME,
    page_icon="🦟",
    layout=STREAMLIT_LAYOUT,
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    * { margin: 0; padding: 0; }
    body { background-color: #FAFAFA; }
    .stApp { background-color: #FAFAFA; }
</style>
""", unsafe_allow_html=True)


####################################################################
# SERVICE INITIALIZATION
####################################################################
@st.cache_resource
def get_athena_service() -> AthenaService:
    """
    Initialize and cache Athena service instance.

    Returns:
        AthenaService: Initialized service or None if health check fails.
    """
    try:
        service = AthenaService()
        if service.health_check():
            logger.info("Athena service initialized successfully")
            return service
        else:
            logger.error("Athena health check failed")
            return None
    except Exception as e:
        logger.error(f"Error initializing Athena service: {str(e)}")
        return None


####################################################################
# UI COMPONENTS
####################################################################
def render_header():
    """Render compact professional header (SaaS style)."""
    st.markdown(f"""
    <div style="padding: 8px 0 10px 0; margin-bottom: 12px; border-bottom: 1px solid {COLOR_BORDER};">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div>
                <h3 style="margin: 0; font-size: 18px; color: {COLOR_DARK_GRAY}; font-weight: 600;">
                    🦟 ArboVigilancia SP
                </h3>
                <p style="margin: 0; font-size: 11px; color: {COLOR_LIGHT_GRAY}; letter-spacing: 0.5px;">
                    Vigilancia Epidemiologica de Arboviroses
                </p>
            </div>
            <div style="text-align: right; white-space: nowrap;">
                <p style="margin: 0 0 6px 0; font-size: 12px; color: {COLOR_DARK_GRAY}; font-weight: 500;">
                    Bruno William da Silva
                </p>
                <div style="font-size: 12px;">
                    <a href="https://github.com/brunobws/aws-epidemiology-medallion-lake" target="_blank"
                       style="color: {COLOR_ORANGE}; text-decoration: none; font-weight: 600;">GitHub</a>
                    &nbsp;|&nbsp;
                    <a href="https://www.linkedin.com/in/brunowsilva/" target="_blank"
                       style="color: {COLOR_ORANGE}; text-decoration: none; font-weight: 600;">LinkedIn</a>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_sidebar():
    """Render sidebar with application controls and information."""
    with st.sidebar:
        st.header("Controles")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Atualizar", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.rerun()

        with col2:
            if st.button("Limpar Cache", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("Cache limpo!")

        st.divider()

        st.subheader("Sobre")
        st.info(
            "Plataforma de vigilancia epidemiologica para arboviroses "
            "(dengue, chikungunya, zika) no estado de Sao Paulo. "
            "Dados da camada Gold do Data Lake."
        )


def render_footer():
    """Render professional footer."""
    st.markdown(f"""
    <div style="border-top: 1px solid {COLOR_BORDER}; padding: 20px 0; margin-top: 40px;
                text-align: center; color: {COLOR_LIGHT_GRAY}; font-size: 12px;">
        <p style="margin-bottom: 5px;">ArboVigilancia SP — Medallion Architecture Data Lake</p>
        <p>Desenvolvido por Bruno William da Silva</p>
    </div>
    """, unsafe_allow_html=True)


####################################################################
# MAIN APPLICATION
####################################################################
def main():
    """Main application entry point and tab orchestrator."""
    render_header()

    athena_service = get_athena_service()

    if athena_service is None:
        st.error(
            "Erro de Conexao: Nao foi possivel conectar ao AWS Athena.\n\n"
            "Verifique:\n"
            "1. Credenciais AWS configuradas\n"
            "2. Regiao correta (sa-east-1)\n"
            "3. Permissoes IAM para Athena"
        )
        st.stop()

    render_sidebar()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Visao Geral",
        "Serie Temporal",
        "Ranking",
        "Perfil Demografico",
        "Observabilidade",
        "Data Quality",
    ])

    with tab1:
        from epidemio_analytics import render_epidemio_analytics
        render_epidemio_analytics(athena_service)

    with tab2:
        from epidemic_timeseries import render_epidemic_timeseries
        render_epidemic_timeseries(athena_service)

    with tab3:
        from epidemic_ranking import render_epidemic_ranking
        render_epidemic_ranking(athena_service)

    with tab4:
        from epidemic_demographic import render_epidemic_demographic
        render_epidemic_demographic(athena_service)

    with tab5:
        from logs_observability import render_logs_observability
        render_logs_observability(athena_service)

    with tab6:
        from data_quality import render_data_quality
        render_data_quality(athena_service)

    render_footer()


if __name__ == "__main__":
    main()
