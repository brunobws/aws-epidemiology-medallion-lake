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
#   - Weekly alert status overview with sparkline KPI cards
#   - Geographic distribution by mesoregion colored by alert level
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
    kpi_card_with_sparkline,
    ALERT_VERDE,
    ALERT_AMARELO,
    ALERT_LARANJA,
    ALERT_VERMELHO,
    COLOR_SUCCESS,
    COLOR_INFO,
)
###################################

logger = get_logger(__name__)


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_current_alerts(athena_service: AthenaService, disease: str) -> pd.DataFrame:
    """Fetch latest weekly alerts for specified disease."""
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
        st.error(f"Falha ao buscar alertas: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_comparative_alerts(athena_service: AthenaService) -> pd.DataFrame:
    """Fetch latest alerts for all diseases for comparison."""
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
    """Fetch summary statistics aggregated by mesoregion."""
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
        for col in ["total_cases", "avg_rt", "max_alert_level", "municipalities_high_alert",
                    "total_municipalities", "municipalities_epidemic"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching mesoregion summary: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_kpi_trends(athena_service: AthenaService, disease: str) -> pd.DataFrame:
    """Fetch last 8 weeks of aggregate KPIs for sparkline trend lines."""
    query = f"""
    SELECT
        dt_semana_epidemiologica,
        SUM(CAST(vl_casos AS BIGINT)) as total_cases,
        AVG(CAST(vl_rt AS DOUBLE)) as avg_rt,
        COUNT(CASE WHEN CAST(fl_epidemia AS INT) = 1 THEN 1 END) as municipalities_epidemic,
        COUNT(CASE WHEN CAST(nr_nivel_alerta AS INT) = 1 THEN 1 END) * 100.0 / COUNT(*) as pct_green
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
    GROUP BY dt_semana_epidemiologica
    ORDER BY dt_semana_epidemiologica DESC
    LIMIT 8
    """
    try:
        df = athena_service.query_gold(query)
        for col in ["total_cases", "avg_rt", "municipalities_epidemic", "pct_green"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching KPI trends: {str(e)}")
        return pd.DataFrame()


def render_epidemio_analytics(athena_service: AthenaService):
    """Main render function for epidemiological analytics."""

    # ── Sidebar filters ──────────────────────────────────────
    st.sidebar.markdown("### Filtros")
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
        kpi_trends = fetch_kpi_trends(athena_service, selected_disease)

    if current_alerts.empty:
        st.warning("Nenhum dado de alerta disponivel para o periodo selecionado.")
        return

    st.title(f"Visao Geral — {DISEASES_PT[selected_disease]}")
    st.markdown("---")

    # ── Trend series (oldest → newest for sparklines) ────────
    alert_colors_by_level = {1: ALERT_VERDE, 2: ALERT_AMARELO, 3: ALERT_LARANJA, 4: ALERT_VERMELHO}

    if not kpi_trends.empty:
        trends_sorted = kpi_trends.sort_values("dt_semana_epidemiologica")
        cases_trend = trends_sorted["total_cases"].tolist()
        rt_trend = trends_sorted["avg_rt"].tolist()
        epidemic_trend = trends_sorted["municipalities_epidemic"].tolist()
        pct_green_trend = trends_sorted["pct_green"].tolist()
    else:
        cases_trend = rt_trend = epidemic_trend = pct_green_trend = []

    # ── Section 1: KPI cards with sparklines ─────────────────
    total_cases = int(current_alerts["vl_casos"].sum())
    total_municipalities = len(current_alerts)
    avg_rt = round(current_alerts["vl_rt"].mean(), 2)
    municipalities_in_epidemic = int(current_alerts[current_alerts["fl_epidemia"] == 1].shape[0])
    alert_distribution = current_alerts["nr_nivel_alerta"].value_counts().to_dict()
    pct_green = (alert_distribution.get(1, 0) / total_municipalities * 100) if total_municipalities > 0 else 0
    status_text = "Controlado" if pct_green > 90 else "Atencao"

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.markdown(kpi_card_with_sparkline(
            f"{total_cases:,}", "Total de Casos", cases_trend, color=ALERT_VERMELHO
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(kpi_card_with_sparkline(
            str(total_municipalities), "Municipios Monitorados", [], color=COLOR_INFO
        ), unsafe_allow_html=True)

    with col3:
        st.markdown(kpi_card_with_sparkline(
            str(avg_rt), "Rt Medio", rt_trend, color=ALERT_LARANJA
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(kpi_card_with_sparkline(
            str(municipalities_in_epidemic), "Epidemia Ativa", epidemic_trend, color=ALERT_VERMELHO
        ), unsafe_allow_html=True)

    with col5:
        st.markdown(kpi_card_with_sparkline(
            f"{pct_green:.0f}%", f"Verde — {status_text}", pct_green_trend, color=COLOR_SUCCESS
        ), unsafe_allow_html=True)

    st.markdown("---")

    # ── Section 2: Comparative disease analysis ──────────────
    st.subheader("Comparativo entre Doencas")

    if not comparative.empty:
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
            xaxis_title="Doenca",
            yaxis_title="Municipios",
            hovermode="x unified",
        )
        fig_comp = apply_professional_theme(fig_comp)
        st.plotly_chart(fig_comp, use_container_width=True)

    st.markdown("---")

    # ── Section 3: Alert distribution + Mesoregion ───────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribuicao de Alertas Atuais")
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

    # ── Section 4: Mesoregion — colored by alert level ───────
    with col2:
        st.subheader("Situacao por Mesorregiao")
        if not mesoregion_summary.empty:
            top_meso = mesoregion_summary.nlargest(10, "total_cases").copy()
            top_meso["nivel_label"] = top_meso["max_alert_level"].map(
                lambda x: ALERT_LEVELS.get(int(x), "Verde")
            )
            alert_discrete_map = {v: alert_colors_by_level[k] for k, v in ALERT_LEVELS.items()}
            category_order = list(ALERT_LEVELS.values())

            fig_meso = px.bar(
                top_meso,
                x="total_cases",
                y="nm_mesorregiao",
                color="nivel_label",
                color_discrete_map=alert_discrete_map,
                category_orders={"nivel_label": category_order},
                labels={
                    "total_cases": "Total de Casos",
                    "nm_mesorregiao": "",
                    "nivel_label": "Nivel de Alerta",
                },
                height=CHART_HEIGHT,
                orientation="h",
            )
            fig_meso.update_yaxes(categoryorder="total ascending")
            fig_meso = apply_professional_theme(fig_meso)
            st.plotly_chart(fig_meso, use_container_width=True)

    st.markdown("---")

    # ── Section 5: Mesoregion summary table ──────────────────
    st.subheader("Resumo por Mesorregiao")
    if not mesoregion_summary.empty:
        display_cols = [
            "nm_mesorregiao", "total_cases", "avg_rt", "max_alert_level",
            "municipalities_high_alert", "total_municipalities", "municipalities_epidemic"
        ]
        display_df = mesoregion_summary[display_cols].copy()
        display_df.columns = [
            "Mesorregiao", "Casos", "Rt Medio", "Alerta Max",
            "Mun. Alerta Alto", "Total Mun.", "Mun. Epidemia"
        ]
        st.dataframe(
            display_df.style.format({
                "Casos": "{:,.0f}",
                "Rt Medio": "{:.2f}",
                "Alerta Max": "{:.0f}",
            }),
            use_container_width=True,
            height=300,
        )

    st.markdown("---")

    # ── Section 6: Export ────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Exportar Alertas (CSV)",
            data=current_alerts.to_csv(index=False),
            file_name=f"alertas_{selected_disease}.csv",
            mime="text/csv",
        )
    with col2:
        st.download_button(
            label="Exportar Mesorregioes (CSV)",
            data=mesoregion_summary.to_csv(index=False),
            file_name=f"mesoregiao_{selected_disease}.csv",
            mime="text/csv",
        )
