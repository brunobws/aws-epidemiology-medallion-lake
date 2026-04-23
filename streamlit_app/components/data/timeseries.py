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
from services.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from services.data_service import fetch_available_years
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
    title_with_help,
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
def fetch_weekly_series(_athena_service: AthenaService, disease: str, year: int) -> pd.DataFrame:
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
        df = _athena_service.query_gold(query)
        numeric_cols = ["week_num", "total_cases", "estimated_cases", "avg_rt",
                        "green_count", "yellow_count", "orange_count", "red_count", "municipalities"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching weekly series: {str(e)}")
        return pd.DataFrame()


def render_epidemic_timeseries(athena_service: AthenaService, disease: str):
    """Render time series analysis tab."""

    # ── Fetch available years ────────────────────────────────────
    years = fetch_available_years(athena_service, disease, TABLE_ALERTS_WEEKLY, "nr_ano_epi")

    # ── Filters in container at top ──────────────────────────────
    st.markdown("### Filtros")
    col_year = st.columns(1)[0]
    with col_year:
        selected_year = st.selectbox("Ano epidemiológico", years, key="ts_year")

    st.markdown("---")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando série temporal..."):
        df = fetch_weekly_series(athena_service, disease, selected_year)

    if df.empty:
        st.warning("Nenhum dado disponivel para o periodo selecionado.")
        return

    st.markdown(f"""
    <h2 style="text-align: center; font-size: 1.2rem; color: #333; font-weight: 300;">
    {DISEASES_PT[disease]} · {selected_year}
    </h2>
    <p style="text-align: center; font-size: 0.95rem; color: #666; margin-top: -10px;"><b>Como a doença evoluiu semana a semana?</b></p>
    """, unsafe_allow_html=True)
    st.divider()
    st.write("")  # Spacing

    num_weeks = len(df)

    # ── Chart 1: Observed vs Estimated cases ──────────────────
    title_with_help("Casos Observados vs Estimados", "Linha contínua = casos reais. Tracejada = previsão do modelo. Diferenças indicam se a situação é melhor ou pior que esperado")

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
    st.plotly_chart(fig_cases, width="stretch")
    
    st.caption(
        "💡 Quando a linha contínua fica acima da tracejada, significa que estamos tendo MAIS casos do que o modelo esperava — situação pior. "
        "Quando fica abaixo, estamos com MENOS casos — situação melhor que o previsto."
    )

    st.divider()
    st.write("")  # Spacing

    # ── Chart 2: Alert level distribution (stacked area) ──────
    title_with_help("Evolução de Alertas e Rt", "Rt = quantas pessoas CADA INFECTADO contamina em média. Rt < 1 = doença diminuindo (bom). Rt > 1 = doença crescendo (ruim). O gráfico à esquerda mostra quantos municípios estão em cada nível de alerta semana a semana")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Quantos municípios em cada nível?")

        if num_weeks <= 1:
            st.info("⚠️ Apenas 1 semana disponível. Gráfico requer múltiplas semanas.")
        else:
            # Preparar dados em formato long para barras agrupadas
            alerts_data = pd.DataFrame({
                "Semana": df["week_date"].tolist() * 4,
                "Alertas": (df["green_count"].tolist() + 
                           df["yellow_count"].tolist() + 
                           df["orange_count"].tolist() + 
                           df["red_count"].tolist()),
                "Nível": (["Verde"] * len(df) + 
                         ["Amarelo"] * len(df) + 
                         ["Laranja"] * len(df) + 
                         ["Vermelho"] * len(df))
            })
            
            # Mapa de cores
            color_map = {
                "Verde": ALERT_VERDE,
                "Amarelo": ALERT_AMARELO,
                "Laranja": ALERT_LARANJA,
                "Vermelho": ALERT_VERMELHO
            }
            
            fig_alerts = px.bar(
                alerts_data,
                x="Semana",
                y="Alertas",
                color="Nível",
                barmode="group",
                color_discrete_map=color_map,
                category_orders={"Nível": ["Verde", "Amarelo", "Laranja", "Vermelho"]},
                labels={"Alertas": "Municípios", "Nível": "Nível de Alerta"},
                height=CHART_HEIGHT,
            )
            fig_alerts.update_layout(
                hovermode="x unified",
                xaxis_title="Semana",
                yaxis_title="Municípios"
            )
            fig_alerts = apply_professional_theme(fig_alerts)
            st.plotly_chart(fig_alerts, width="stretch")
            
            st.caption(
                "Verde = situação controlada | Amarelo = atenção | Laranja = aviso | Vermelho = crítico. "
                "O gráfico mostra como essa distribuição evoluiu semana a semana em todo o estado."
            )

    # ── Chart 3: Rt evolution ─────────────────────────────────
    with col2:
        st.markdown("#### 📊 Evolução do Rt")

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
        st.plotly_chart(fig_rt, width="stretch")

    st.divider()
    st.write("")  # Spacing

    st.divider()
    st.write("")  # Spacing

    # ── Data table ────────────────────────────────────────────
    st.markdown("#### Dados Semanais Completos")
    
    display_df = df[["week_date", "week_num", "total_cases", "estimated_cases", "avg_rt", "municipalities"]].copy()
    display_df.columns = ["Data", "Semana", "Casos", "Estimados", "Rt Médio", "Municípios"]
    
    # Remover linhas vazias e converter data
    display_df = display_df.dropna(how='all').reset_index(drop=True)
    display_df["Data"] = pd.to_datetime(display_df["Data"]).dt.strftime("%d/%m/%Y")

    st.dataframe(
        display_df.style.format({
            "Casos": "{:,.0f}",
            "Estimados": "{:,.0f}",
            "Rt Médio": "{:.3f}",
        }),
        width="stretch",
        height=300,
        hide_index=True,
    )

    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📥 Exportar Serie Temporal (CSV)",
        data=csv_data,
        file_name=f"serie_temporal_{disease}_{selected_year}.csv",
        mime="text/csv",
    )
