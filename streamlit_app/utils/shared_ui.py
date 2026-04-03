####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Shared UI components for ArboVigilancia SP Dashboard.
#   Centralizes Athena service init, header, and sidebar rendering
#   reused across all pages.
####################################################################

########### imports ################
import streamlit as st
from utils.athena_service import AthenaService
from utils.cache_manager import clear_all_caches
from utils.logger import get_logger
from theme import COLOR_DARK_GRAY, COLOR_LIGHT_GRAY, COLOR_ORANGE, COLOR_BORDER
###################################

logger = get_logger(__name__)


@st.cache_resource
def get_athena_service() -> AthenaService:
    """Initialize and cache Athena service for the session."""
    try:
        service = AthenaService()
        if service.health_check():
            logger.info("Athena service initialized successfully")
            return service
        logger.error("Athena health check failed")
        return None
    except Exception as e:
        logger.error(f"Error initializing Athena service: {str(e)}")
        return None


def render_header():
    """Compact professional header."""
    st.markdown(f"""
    <div style="padding: 8px 0 10px 0; margin-bottom: 12px; border-bottom: 1px solid {COLOR_BORDER};">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div>
                <h3 style="margin: 0; font-size: 18px; color: {COLOR_DARK_GRAY}; font-weight: 600;">
                    ArboVigilancia SP
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
    """Sidebar with minimal controls."""
    with st.sidebar:
        if st.button("Atualizar Dados", use_container_width=True):
            clear_all_caches()
            st.rerun()

        st.divider()

        st.caption(
            "Vigilancia epidemiologica para arboviroses "
            "(dengue, chikungunya, zika) em Sao Paulo. "
            "Dados da camada Gold do Data Lake."
        )


def render_footer():
    """Professional footer."""
    st.markdown(f"""
    <div style="border-top: 1px solid {COLOR_BORDER}; padding: 20px 0; margin-top: 40px;
                text-align: center; color: {COLOR_LIGHT_GRAY}; font-size: 12px;">
        <p style="margin-bottom: 5px;">ArboVigilancia SP — Medallion Architecture Data Lake</p>
        <p>Desenvolvido por Bruno William da Silva</p>
    </div>
    """, unsafe_allow_html=True)


def require_athena():
    """Return athena_service or stop the page with a connection error message."""
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
    return athena_service
