####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Epidemiological analytics module for ArboVigilancia SP Dashboard.
#   Provides disease surveillance insights including current alerts,
#   geographic hotspots, temporal trends, and demographic profiles.
#
#   Features:
#   - Multi-disease alert dashboard (Dengue, Chikungunya, Zika)
#   - Weekly alert status overview
#   - Geographic distribution by mesoregion
#   - Comparative disease analysis
#   - Summary statistics by administrative region
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
    COLOR_SUCCESS,
)
###################################

logger = get_logger(__name__)


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_current_alerts(athena_service: AthenaService, disease: str) -> pd.DataFrame:
    """
    Fetch latest weekly alerts for specified disease.
    Returns the most recent week available per municipality.
    """
    query = f"""
    SELECT
        cd_geocode, nm_municipio, nm_microrregiao, nm_mesorregiao,
        vl_populacao, nr_semana_epi, nr_nivel_alerta, ds_nivel_alerta,
        vl_casos, vl_incidencia, vl_rt, fl_epidemia,
        fl_transmissao, fl_receptividade,
        vl_temp_min, vl_temp_max, nr_ano_epi, ds_doenca, dt_semana_epidemiologica
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
    AND dt_semana_epidemiologica = (
        SELECT MAX(dt_semana_epidemiologica)
        FROM {TABLE_ALERTS_WEEKLY}
        WHERE ds_doenca = '{disease}'
    )
    ORDER BY nr_nivel_alerta DESC, vl_incidencia DESC
    """
    try:
        df = athena_service.query_gold(query)
        numeric_cols = [
            "nr_semana_epi", "nr_nivel_alerta", "vl_casos", "vl_populacao",
            "nr_ano_epi", "vl_incidencia", "vl_rt", "fl_epidemia",
            "fl_transmissao", "fl_receptividade", "vl_temp_min", "vl_temp_max"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.error(f"Error fetching current alerts: {str(e)}")
        st.error(f"Failed to fetch alerts: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_comparative_alerts(athena_service: AthenaService) -> pd.DataFrame:
    """
    Fetch latest alerts for all diseases to compare.
    """
    query = f"""
    SELECT
        ds_doenca, nr_nivel_alerta, COUNT(*) as count_municipalities
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE dt_semana_epidemiologica = (
        SELECT MAX(dt_semana_epidemiologica)
        FROM {TABLE_ALERTS_WEEKLY}
    )
    GROUP BY ds_doenca, nr_nivel_alerta
    ORDER BY ds_doenca, nr_nivel_alerta
    """
    try:
        df = athena_service.query_gold(query)
        for col in ["nr_nivel_alerta", "count_municipalities"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.error(f"Error fetching comparative alerts: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_mesoregion_summary(athena_service: AthenaService, disease: str) -> pd.DataFrame:
    """
    Fetch summary statistics aggregated by mesoregion.
    """
    query = f"""
    SELECT
        nm_mesorregiao,
        SUM(CAST(vl_casos AS BIGINT)) as total_cases,
        AVG(CAST(vl_rt AS DOUBLE)) as avg_rt,
        MAX(CAST(nr_nivel_alerta AS INT)) as max_alert_level,
        COUNT(CASE WHEN CAST(nr_nivel_alerta AS INT) >= 3 THEN 1 END) as municipalities_high_alert,
        COUNT(DISTINCT cd_geocode) as total_municipalities,
        COUNT(CASE WHEN CAST(fl_epidemia AS INT) = 1 THEN 1 END) as municipalities_epidemic
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
    AND dt_semana_epidemiologica = (
        SELECT MAX(dt_semana_epidemiologica)
        FROM {TABLE_ALERTS_WEEKLY}
        WHERE ds_doenca = '{disease}'
    )
    GROUP BY nm_mesorregiao
    ORDER BY total_cases DESC
    """
    try:
        df = athena_service.query_gold(query)
        for col in ["total_cases", "avg_rt", "max_alert_level", "municipalities_high_alert", "total_municipalities", "municipalities_epidemic"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching mesoregion summary: {str(e)}")
        return pd.DataFrame()


def create_gauge_chart(value: float, max_val: float, title: str, color: str) -> go.Figure:
    """Create a simple gauge chart using indicator."""
    fig = go.Figure(data=[go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": title},
        gauge={
            "axis": {"range": [0, max_val]},
            "bar": {"color": color},
            "steps": [
                {"range": [0, max_val * 0.33], "color": "rgba(0,255,0,0.1)"},
                {"range": [max_val * 0.33, max_val * 0.66], "color": "rgba(255,255,0,0.1)"},
                {"range": [max_val * 0.66, max_val], "color": "rgba(255,0,0,0.1)"},
            ],
            "threshold": {
                "line": {"color": "red", "width": 4},
                "thickness": 0.75,
                "value": max_val,
            },
        },
    )])
    fig.update_layout(height=300)
    return apply_professional_theme(fig)


def render_epidemio_analytics(athena_service: AthenaService):
    """Main render function for epidemiological analytics."""

    # ── Sidebar filters ──────────────────────────────────────
    st.sidebar.markdown("### 🎯 Filtros")
    selected_disease = st.sidebar.selectbox(
        "Doença",
        DISEASES,
        format_func=lambda x: DISEASES_PT.get(x, x),
        key="epi_disease"
    )

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando dados de alertas..."):
        current_alerts = fetch_current_alerts(athena_service, selected_disease)
        comparative = fetch_comparative_alerts(athena_service)
        mesoregion_summary = fetch_mesoregion_summary(athena_service, selected_disease)

    if current_alerts.empty:
        st.warning("Nenhum dado de alerta disponível para período selecionado.")
        return

    st.title(f"🦟 Visão Geral — {DISEASES_PT[selected_disease]}")
    st.markdown("---")

    # ── Section 1: KPIs ──────────────────────────────────────
    total_cases = int(current_alerts["vl_casos"].sum())
    total_municipalities = len(current_alerts)
    avg_rt = round(current_alerts["vl_rt"].mean(), 2)
    municipalities_in_epidemic = int(current_alerts[current_alerts["fl_epidemia"] == 1].shape[0])

    # Count alert levels
    alert_distribution = current_alerts["nr_nivel_alerta"].value_counts().to_dict()
    high_alert_count = alert_distribution.get(4, 0) + alert_distribution.get(3, 0)

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Total de Casos",
            f"{total_cases:,}",
            delta=None,
        )

    with col2:
        st.metric(
            "Municípios",
            total_municipalities,
            delta=None,
        )

    with col3:
        st.metric(
            "Rt Médio",
            avg_rt,
            delta="epidemia" if avg_rt > 1.0 else "controlado",
        )

    with col4:
        st.metric(
            "Epidemia Ativa",
            municipalities_in_epidemic,
            delta=None,
        )

    with col5:
        pct_green = (alert_distribution.get(1, 0) / total_municipalities * 100) if total_municipalities > 0 else 0
        status_text = "✅ Controlado" if pct_green > 90 else "⚠️ Atenção"
        st.metric(
            "Situação",
            f"{pct_green:.0f}%",
            delta=status_text,
        )

    st.markdown("---")

    # ── Section 2: Comparative disease analysis ──────────────
    st.subheader("📊 Comparativo entre Doenças")

    if not comparative.empty:
        # Prepare data for stacked bar
        alert_colors_by_level = {1: ALERT_VERDE, 2: ALERT_AMARELO, 3: ALERT_LARANJA, 4: ALERT_VERMELHO}

        fig_comp = go.Figure()
        for alert_level in [1, 2, 3, 4]:
            subset = comparative[comparative["nr_nivel_alerta"] == alert_level]
            if not subset.empty:
                fig_comp.add_trace(go.Bar(
                    x=subset["ds_doenca"],
                    y=subset["count_municipalities"],
                    name=ALERT_LEVELS[alert_level].capitalize(),
                    marker_color=alert_colors_by_level[alert_level],
                ))

        fig_comp.update_layout(
            barmode="stack",
            height=CHART_HEIGHT,
            xaxis_title="Doença",
            yaxis_title="Municípios",
            hovermode="x unified",
        )
        fig_comp = apply_professional_theme(fig_comp)
        st.plotly_chart(fig_comp, use_container_width=True)

    st.markdown("---")

    # ── Section 3: Alert distribuution ──────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🚨 Distribuição de Alertas Atuais")
        alert_counts = current_alerts["nr_nivel_alerta"].value_counts().sort_index()
        alert_labels = [ALERT_LEVELS.get(int(level), str(level)) for level in alert_counts.index]
        alert_colors = [alert_colors_by_level.get(int(level), "#999") for level in alert_counts.index]

        fig_alerts = go.Figure(data=[go.Pie(
            labels=alert_labels,
            values=alert_counts.values,
            marker=dict(colors=alert_colors),
            hole=0.4,
        )])
        fig_alerts.update_layout(height=CHART_HEIGHT)
        fig_alerts = apply_professional_theme(fig_alerts)
        st.plotly_chart(fig_alerts, use_container_width=True)

    # ── Section 4: Mesoregion ranking ────────────────────────
    with col2:
        st.subheader("🗺️ Top Mesorregiões (por casos)")
        if not mesoregion_summary.empty:
            top_meso = mesoregion_summary.nlargest(10, "total_cases")
            fig_meso = px.bar(
                top_meso,
                x="total_cases",
                y="nm_mesorregiao",
                color="avg_rt",
                color_continuous_scale="RdYlGn_r",
                labels={"total_cases": "Total de Casos", "nm_mesorregiao": "", "avg_rt": "Rt Médio"},
                height=CHART_HEIGHT,
                orientation="h",
            )
            fig_meso.update_yaxes(categoryorder="total ascending")
            fig_meso = apply_professional_theme(fig_meso)
            st.plotly_chart(fig_meso, use_container_width=True)

    st.markdown("---")

    # ── Section 5: Detailed mesoregion table ──────────────────
    st.subheader("📋 Resumo por Mesorregião")
    if not mesoregion_summary.empty:
        display_cols = ["nm_mesorregiao", "total_cases", "avg_rt", "max_alert_level", "municipalities_high_alert", "total_municipalities", "municipalities_epidemic"]
        display_df = mesoregion_summary[display_cols].copy()
        display_df.columns = ["Mesorregião", "Casos", "Rt Médio", "Alerta Máx", "Mun. Alerta Alto", "Total Mun.", "Mun. Epidemia"]

        st.dataframe(
            display_df.style.format({
                "Casos": "{:,.0f}",
                "Rt Médio": "{:.2f}",
                "Alerta Máx": "{:.0f}",
            }),
            use_container_width=True,
            height=300,
        )

    st.markdown("---")

    # ── Section 6: Export ────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        csv_data = current_alerts.to_csv(index=False)
        st.download_button(
            label="📥 Exportar Alertas (CSV)",
            data=csv_data,
            file_name=f"alertas_{selected_disease}.csv",
            mime="text/csv",
        )
    with col2:
        csv_data = mesoregion_summary.to_csv(index=False)
        st.download_button(
            label="📥 Exportar Mesorregiões (CSV)",
            data=csv_data,
            file_name=f"mesoregiao_{selected_disease}.csv",
            mime="text/csv",
        )
