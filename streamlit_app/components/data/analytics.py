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
from services.athena_service import AthenaService
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
    kpi_card_html,
    title_with_help,
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
def fetch_current_alerts(_athena_service: AthenaService, disease: str) -> pd.DataFrame:
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
        df = _athena_service.query_gold(query)
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
def fetch_comparative_alerts(_athena_service: AthenaService) -> pd.DataFrame:
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
        df = _athena_service.query_gold(query)
        for col in ["nr_nivel_alerta", "count_municipalities"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.error(f"Error fetching comparative alerts: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_mesoregion_summary(_athena_service: AthenaService, disease: str) -> pd.DataFrame:
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
        df = _athena_service.query_gold(query)
        for col in ["total_cases", "avg_rt", "max_alert_level", "municipalities_high_alert",
                    "total_municipalities", "municipalities_epidemic"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching mesoregion summary: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_kpi_trends(_athena_service: AthenaService, disease: str) -> pd.DataFrame:
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
        df = _athena_service.query_gold(query)
        for col in ["total_cases", "avg_rt", "municipalities_epidemic", "pct_green"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching KPI trends: {str(e)}")
        return pd.DataFrame()


def render_epidemio_analytics(athena_service: AthenaService, disease: str):
    """Main render function for epidemiological analytics."""

    # â”€â”€ No sidebar filters â€” use global disease â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    # â”€â”€ Load data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    with st.spinner("Carregando dados de alertas..."):
        current_alerts = fetch_current_alerts(athena_service, disease)
        comparative = fetch_comparative_alerts(athena_service)
        mesoregion_summary = fetch_mesoregion_summary(athena_service, disease)
        kpi_trends = fetch_kpi_trends(athena_service, disease)

    if current_alerts.empty:
        st.warning("Nenhum dado de alerta disponivel para o periodo selecionado.")
        return

    st.markdown(f"""
    <h2 style="text-align: center; font-size: 1.2rem; color: #333; font-weight: 300;">
    {DISEASES_PT[disease]}
    </h2>
    <p style="text-align: center; font-size: 0.95rem; color: #666; margin-top: -10px;"><b>Qual Ã© a situaÃ§Ã£o atual da doenÃ§a em SÃ£o Paulo?</b></p>
    """, unsafe_allow_html=True)
    st.divider()
    st.write("")  # Spacing

    # â”€â”€ Section 1: KPI cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    title_with_help("Indicadores Principais", "MÃ©tricas-chave sobre a circulaÃ§Ã£o da doenÃ§a na regiÃ£o")
    
    alert_colors_by_level = {1: ALERT_VERDE, 2: ALERT_AMARELO, 3: ALERT_LARANJA, 4: ALERT_VERMELHO}

    total_cases = int(current_alerts["vl_casos"].sum())
    total_municipalities = len(current_alerts)
    avg_rt = round(current_alerts["vl_rt"].mean(), 2)
    municipalities_in_epidemic = int(current_alerts[current_alerts["fl_epidemia"] == 1].shape[0])
    alert_distribution = current_alerts["nr_nivel_alerta"].value_counts().to_dict()
    pct_green = (alert_distribution.get(1, 0) / total_municipalities * 100) if total_municipalities > 0 else 0
    status_text = "Controlado" if pct_green > 90 else "AtenÃ§Ã£o"

    # â”€â”€ Trend series (oldest â†’ newest for sparklines) â”€â”€â”€â”€â”€â”€â”€â”€
    if not kpi_trends.empty:
        trends_sorted = kpi_trends.sort_values("dt_semana_epidemiologica")
        cases_trend = trends_sorted["total_cases"].tolist()
        rt_trend = trends_sorted["avg_rt"].tolist()
        epidemic_trend = trends_sorted["municipalities_epidemic"].tolist()
        pct_green_trend = trends_sorted["pct_green"].tolist()
    else:
        cases_trend = rt_trend = epidemic_trend = pct_green_trend = []

    col1, col2, col3, col4, col5 = st.columns(5, gap="small")

    with col1:
        st.markdown(kpi_card_with_sparkline(
            f"{total_cases:,}", 
            "Total de Casos", 
            cases_trend, 
            color=ALERT_VERMELHO,
            description="NÃºmero total de casos confirmados da doenÃ§a na semana atual."
        ), unsafe_allow_html=True)

    with col2:
        st.markdown(kpi_card_html(
            str(total_municipalities), 
            "MunicÃ­pios Monitorados",
            description="Quantidade de municÃ­pios que estÃ£o sendo monitorados para esta doenÃ§a."
        ), unsafe_allow_html=True)

    with col3:
        st.markdown(kpi_card_with_sparkline(
            str(avg_rt), 
            "Rt MÃ©dio", 
            rt_trend, 
            color=ALERT_LARANJA,
            description="NÃºmero de reproduÃ§Ã£o (Rt): Rt < 1 indica declÃ­nio, Rt > 1 indica crescimento. Limiar epidÃªmico: Rt = 1."
        ), unsafe_allow_html=True)

    with col4:
        st.markdown(kpi_card_with_sparkline(
            str(municipalities_in_epidemic), 
            "Epidemia Ativa", 
            epidemic_trend, 
            color=ALERT_VERMELHO,
            description="NÃºmero de municÃ­pios com classificaÃ§Ã£o de epidemia ativa segundo critÃ©rios do MinistÃ©rio da SaÃºde."
        ), unsafe_allow_html=True)

    with col5:
        st.markdown(kpi_card_with_sparkline(
            f"{pct_green:.0f}%", 
            f"Verde â€” {status_text}", 
            pct_green_trend, 
            color=COLOR_SUCCESS,
            description="Percentual de municÃ­pios com alerta controlado (nÃ­vel verde)."
        ), unsafe_allow_html=True)

    st.divider()
    st.write("")  # Spacing

    # â”€â”€ Section 2: Comparative disease analysis â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    title_with_help("Comparativo entre DoenÃ§as", "DistribuiÃ§Ã£o de alertas por doenÃ§a e municÃ­pios com circulaÃ§Ã£o ativa")

    if not comparative.empty:
        col_comp_left, col_comp_right = st.columns(2, gap="medium")

        with col_comp_left:
            st.markdown("#### DistribuiÃ§Ã£o por NÃ­vel de Alerta")
            
            # Calcular o total mÃ¡ximo de municÃ­pios para redimensionar eixo Y
            total_mun_max = comparative.groupby("ds_doenca")["count_municipalities"].sum().max()
            y_max = total_mun_max * 1.05  # 5% acima do mÃ¡ximo
            
            fig_comp = go.Figure()
            for alert_level in [1, 2, 3, 4]:
                subset = comparative[comparative["nr_nivel_alerta"] == alert_level]
                if not subset.empty:
                    fig_comp.add_trace(go.Bar(
                        x=subset["ds_doenca"],
                        y=subset["count_municipalities"],
                        name=ALERT_LEVELS[alert_level].capitalize(),
                        marker_color=alert_colors_by_level[alert_level],
                        text=subset["count_municipalities"],
                        textposition="inside",
                        textfont=dict(size=9, color="white"),
                    ))

            fig_comp.update_layout(
                barmode="stack",
                height=CHART_HEIGHT,
                xaxis_title="DoenÃ§a",
                yaxis_title="MunicÃ­pios",
                yaxis=dict(range=[0, y_max]),
                hovermode="x unified",
            )
            fig_comp = apply_professional_theme(fig_comp)
            st.plotly_chart(fig_comp, width="stretch")

        with col_comp_right:
            st.markdown("#### MunicÃ­pios com Alerta Ativo")
            
            # Contar municÃ­pios nÃ£o-verdes por doenÃ§a
            non_green = comparative[comparative["nr_nivel_alerta"] != 1].groupby("ds_doenca")["count_municipalities"].sum()
            
            if non_green.empty or non_green.sum() == 0:
                st.success("âœ“ Todos os municÃ­pios estÃ£o controlados (nÃ­vel verde)!")
            else:
                fig_active = go.Figure(data=[go.Pie(
                    labels=non_green.index,
                    values=non_green.values,
                    hole=0.4,
                    marker=dict(colors=[
                        comparative[(comparative["ds_doenca"] == d) & (comparative["nr_nivel_alerta"] != 1)]["nr_nivel_alerta"].mode()[0]
                        if len(comparative[(comparative["ds_doenca"] == d) & (comparative["nr_nivel_alerta"] != 1)]) > 0
                        else 1
                        for d in non_green.index
                    ]),
                )])
                fig_active.update_layout(height=CHART_HEIGHT)
                fig_active = apply_professional_theme(fig_active)
                st.plotly_chart(fig_active, width="stretch")

    st.divider()
    st.write("")  # Spacing

    # â”€â”€ Section 3: Alert distribution + Mesoregion â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    title_with_help("AnÃ¡lise Regional e de Alertas", "SituaÃ§Ã£o de alertas e distribuiÃ§Ã£o geogrÃ¡fica dos casos por mesorregiÃ£o")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### DistribuiÃ§Ã£o de Alertas Atuais")
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
        st.plotly_chart(fig_alerts, width="stretch")

    # â”€â”€ Section 4: Mesoregion â€” colored by alert level â”€â”€â”€â”€â”€â”€â”€
    with col2:
        st.markdown("#### SituaÃ§Ã£o por MesorregiÃ£o (Top 10 por Casos)")
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
            st.plotly_chart(fig_meso, width="stretch")

    st.markdown("---")

    # â”€â”€ Section 5: Mesoregion summary table â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    st.markdown("#### Resumo por MesorregiÃ£o")
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
            width="stretch",
            height=300,
        )

    st.markdown("---")

    # â”€â”€ Section 6: Export â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="📥 Exportar Alertas Atuais (CSV)",
            data=current_alerts.to_csv(index=False),
            file_name=f"alertas_{disease}.csv",
            mime="text/csv",
        )
    with col2:
        st.download_button(
            label="ðŸ“¥ Exportar MesorregiÃµes (CSV)",
            data=mesoregion_summary.to_csv(index=False),
            file_name=f"mesoregiao_{disease}.csv",
            mime="text/csv",
        )

