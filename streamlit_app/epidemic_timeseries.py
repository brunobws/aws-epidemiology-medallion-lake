####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Time series analysis module for ArboVigilancia SP Dashboard.
#   Displays weekly evolution of cases, alerts, and epidemiological
#   indicators over time.
#
#   Features:
#   - Observed vs estimated cases line chart
#   - Alert level distribution by week (stacked area)
#   - Rt evolution with epidemic threshold
#   - Weekly data table with export
####################################################################

########### imports ################
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from config import (
    TABLE_ALERTS_WEEKLY,
    DISEASES,
    DISEASES_PT,
    ALERT_LEVELS,
    CHART_HEIGHT,
    CACHE_TTL,
)
from theme import (
    apply_professional_theme,
    ALERT_VERDE,
    ALERT_AMARELO,
    ALERT_LARANJA,
    ALERT_VERMELHO,
    COLOR_ERROR,
    COLOR_INFO,
    COLOR_ORANGE,
)
###################################

logger = get_logger(__name__)


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_weekly_series(athena_service: AthenaService, disease: str, year: int) -> pd.DataFrame:
    """
    Fetch weekly time series for a disease and year.
    Aggregates across all municipalities.
    """
    query = f"""
    SELECT
        dt_semana_epidemiologica AS week_date,
        CAST(nr_semana_epi AS INT) AS week_num,
        SUM(CAST(vl_casos AS BIGINT)) AS total_cases,
        SUM(CAST(vl_casos_estimados AS DOUBLE)) AS estimated_cases,
        AVG(CAST(vl_rt AS DOUBLE)) AS avg_rt,
        COUNT(CASE WHEN CAST(nr_nivel_alerta AS INT) = 1 THEN 1 END) AS green_count,
        COUNT(CASE WHEN CAST(nr_nivel_alerta AS INT) = 2 THEN 1 END) AS yellow_count,
        COUNT(CASE WHEN CAST(nr_nivel_alerta AS INT) = 3 THEN 1 END) AS orange_count,
        COUNT(CASE WHEN CAST(nr_nivel_alerta AS INT) = 4 THEN 1 END) AS red_count,
        COUNT(DISTINCT cd_geocode) AS municipalities
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
    AND nr_ano_epi = {year}
    GROUP BY dt_semana_epidemiologica, nr_semana_epi
    ORDER BY dt_semana_epidemiologica
    """
    try:
        df = athena_service.query_gold(query)
        numeric_cols = ["week_num", "total_cases", "estimated_cases", "avg_rt",
                        "green_count", "yellow_count", "orange_count", "red_count", "municipalities"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching weekly series: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_available_years(athena_service: AthenaService, disease: str) -> list:
    """Fetch available years for a disease."""
    query = f"""
    SELECT DISTINCT nr_ano_epi
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
    ORDER BY nr_ano_epi DESC
    """
    try:
        df = athena_service.query_gold(query)
        if df.empty:
            return [2026]
        return sorted(df["nr_ano_epi"].astype(int).tolist(), reverse=True)
    except Exception as e:
        logger.error(f"Error fetching years: {str(e)}")
        return [2026]


def render_epidemic_timeseries(athena_service: AthenaService):
    """Render time series analysis tab."""

    # ── Sidebar filters ──────────────────────────────────────
    st.sidebar.markdown("### 📅 Filtros Serie Temporal")
    selected_disease = st.sidebar.selectbox(
        "Doenca",
        DISEASES,
        format_func=lambda x: DISEASES_PT.get(x, x),
        key="ts_disease"
    )

    years = fetch_available_years(athena_service, selected_disease)
    selected_year = st.sidebar.selectbox("Ano epidemiologico", years, key="ts_year")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando serie temporal..."):
        df = fetch_weekly_series(athena_service, selected_disease, selected_year)

    if df.empty:
        st.warning("Nenhum dado disponivel para o periodo selecionado.")
        return

    st.subheader(f"📈 Serie Temporal — {DISEASES_PT[selected_disease]} ({selected_year})")
    st.markdown("---")

    num_weeks = len(df)

    # ── Chart 1: Observed vs Estimated cases ──────────────────
    st.subheader("Casos Observados vs Estimados")

    fig_cases = go.Figure()
    fig_cases.add_trace(go.Scatter(
        x=df["week_date"],
        y=df["total_cases"],
        mode="lines+markers",
        name="Observados",
        line=dict(color=COLOR_ERROR, width=2),
    ))
    fig_cases.add_trace(go.Scatter(
        x=df["week_date"],
        y=df["estimated_cases"],
        mode="lines",
        name="Estimados",
        line=dict(color=COLOR_INFO, width=2, dash="dash"),
    ))
    fig_cases.update_layout(
        height=CHART_HEIGHT,
        xaxis_title="Semana Epidemiologica",
        yaxis_title="Casos",
        hovermode="x unified",
    )
    fig_cases = apply_professional_theme(fig_cases)
    st.plotly_chart(fig_cases, use_container_width=True)

    st.markdown("---")

    # ── Chart 2: Alert level distribution (stacked area) ──────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribuicao de Alertas por Semana")

        if num_weeks <= 1:
            st.info("⚠️ Apenas 1 semana disponivel. Grafico de area requer mais dados.")
        else:
            fig_alerts = go.Figure()
            fig_alerts.add_trace(go.Scatter(
                x=df["week_date"], y=df["green_count"],
                name="Verde", fill="tonexty", stackgroup="one",
                line=dict(color=ALERT_VERDE),
            ))
            fig_alerts.add_trace(go.Scatter(
                x=df["week_date"], y=df["yellow_count"],
                name="Amarelo", fill="tonexty", stackgroup="one",
                line=dict(color=ALERT_AMARELO),
            ))
            fig_alerts.add_trace(go.Scatter(
                x=df["week_date"], y=df["orange_count"],
                name="Laranja", fill="tonexty", stackgroup="one",
                line=dict(color=ALERT_LARANJA),
            ))
            fig_alerts.add_trace(go.Scatter(
                x=df["week_date"], y=df["red_count"],
                name="Vermelho", fill="tonexty", stackgroup="one",
                line=dict(color=ALERT_VERMELHO),
            ))
            fig_alerts.update_layout(
                height=CHART_HEIGHT,
                xaxis_title="Semana",
                yaxis_title="Municipios",
                hovermode="x unified",
            )
            fig_alerts = apply_professional_theme(fig_alerts)
            st.plotly_chart(fig_alerts, use_container_width=True)

    # ── Chart 3: Rt evolution ─────────────────────────────────
    with col2:
        st.subheader("Evolucao do Rt")

        fig_rt = go.Figure()
        fig_rt.add_trace(go.Scatter(
            x=df["week_date"],
            y=df["avg_rt"],
            mode="lines+markers",
            name="Rt medio",
            line=dict(color=COLOR_ORANGE, width=2),
        ))
        # Epidemic threshold line at Rt=1
        fig_rt.add_hline(
            y=1.0,
            line_dash="dash",
            line_color=COLOR_ERROR,
            annotation_text="Limiar epidemico (Rt=1)",
            annotation_position="bottom right",
        )
        fig_rt.update_layout(
            height=CHART_HEIGHT,
            xaxis_title="Semana",
            yaxis_title="Rt",
            hovermode="x unified",
        )
        fig_rt = apply_professional_theme(fig_rt)
        st.plotly_chart(fig_rt, use_container_width=True)

    st.markdown("---")

    # ── Data table ────────────────────────────────────────────
    st.subheader("📋 Dados Semanais")
    display_df = df[["week_date", "week_num", "total_cases", "estimated_cases", "avg_rt", "municipalities"]].copy()
    display_df.columns = ["Data", "Semana", "Casos", "Estimados", "Rt Medio", "Municipios"]

    st.dataframe(
        display_df.style.format({
            "Casos": "{:,.0f}",
            "Estimados": "{:,.0f}",
            "Rt Medio": "{:.3f}",
        }),
        use_container_width=True,
        height=300,
    )

    # ── Export ────────────────────────────────────────────────
    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📥 Exportar Serie Temporal (CSV)",
        data=csv_data,
        file_name=f"serie_temporal_{selected_disease}_{selected_year}.csv",
        mime="text/csv",
    )
