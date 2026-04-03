####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Ranking and hotspots module for ArboVigilancia SP Dashboard.
#   Displays annual municipality rankings by disease incidence,
#   identifies hotspots, and provides comparative regional analysis.
#
#   Features:
#   - Top-N municipalities by incidence (horizontal bar)
#   - Treemap by mesoregion/municipality
#   - Regional comparison bar chart
#   - Full ranking table with search
####################################################################

########### imports ################
import streamlit as st
import pandas as pd
import plotly.express as px
from utils.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from config import (
    TABLE_RANKING_ANNUAL,
    DISEASES,
    DISEASES_PT,
    CHART_HEIGHT,
    CACHE_TTL,
)
from theme import (
    apply_professional_theme,
    COLOR_ERROR,
    COLOR_ORANGE,
    COLOR_SUCCESS,
)
###################################

logger = get_logger(__name__)


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_ranking_data(athena_service: AthenaService, disease: str, year: int) -> pd.DataFrame:
    """Fetch annual ranking data for municipalities."""
    query = f"""
    SELECT
        cd_geocode,
        nm_municipio,
        nm_microrregiao,
        nm_mesorregiao,
        vl_populacao,
        vl_total_casos,
        vl_incidencia_acumulada,
        nr_max_alerta,
        nr_semanas_alerta_vermelho,
        nr_semanas_alerta_alto,
        nr_semanas_transmissao_ativa,
        nr_semanas_rt_acima_1,
        vl_rt_medio,
        nr_rank_estado,
        nr_rank_mesorregiao
    FROM {TABLE_RANKING_ANNUAL}
    WHERE ds_doenca = '{disease}'
    AND nr_ano_epi = {year}
    ORDER BY nr_rank_estado
    """
    try:
        df = athena_service.query_gold(query)
        numeric_cols = [
            "vl_populacao", "vl_total_casos", "vl_incidencia_acumulada",
            "nr_max_alerta", "nr_semanas_alerta_vermelho", "nr_semanas_alerta_alto",
            "nr_semanas_transmissao_ativa", "nr_semanas_rt_acima_1", "vl_rt_medio",
            "nr_rank_estado", "nr_rank_mesorregiao"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching ranking data: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_available_years(athena_service: AthenaService, disease: str) -> list:
    """Fetch available years for ranking."""
    query = f"""
    SELECT DISTINCT nr_ano_epi
    FROM {TABLE_RANKING_ANNUAL}
    WHERE ds_doenca = '{disease}'
    ORDER BY nr_ano_epi DESC
    """
    try:
        df = athena_service.query_gold(query)
        if df.empty:
            return [2026]
        return sorted(df["nr_ano_epi"].astype(int).tolist(), reverse=True)
    except Exception as e:
        return [2026]


def render_epidemic_ranking(athena_service: AthenaService):
    """Render ranking and hotspots tab."""

    # ── Sidebar filters ──────────────────────────────────────
    st.sidebar.markdown("### 🏆 Filtros Ranking")
    selected_disease = st.sidebar.selectbox(
        "Doenca",
        DISEASES,
        format_func=lambda x: DISEASES_PT.get(x, x),
        key="rank_disease"
    )

    years = fetch_available_years(athena_service, selected_disease)
    selected_year = st.sidebar.selectbox("Ano epidemiologico", years, key="rank_year")

    top_n = st.sidebar.slider("Top N municipios", 10, 50, 20, key="rank_top_n")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando ranking..."):
        df = fetch_ranking_data(athena_service, selected_disease, selected_year)

    if df.empty:
        st.warning("Nenhum dado de ranking disponivel.")
        return

    st.subheader(f"🏆 Ranking — {DISEASES_PT[selected_disease]} ({selected_year})")
    st.markdown("---")

    # ── Chart 1: Top-N municipalities by incidence ────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Top {top_n} Municipios por Incidencia")
        top_df = df.nlargest(top_n, "vl_incidencia_acumulada")

        fig_top = px.bar(
            top_df.sort_values("vl_incidencia_acumulada", ascending=True),
            x="vl_incidencia_acumulada",
            y="nm_municipio",
            color="vl_rt_medio",
            color_continuous_scale="RdYlGn_r",
            labels={
                "vl_incidencia_acumulada": "Incidencia (por 100k)",
                "nm_municipio": "",
                "vl_rt_medio": "Rt Medio",
            },
            height=max(CHART_HEIGHT, top_n * 22),
            orientation="h",
        )
        fig_top = apply_professional_theme(fig_top)
        st.plotly_chart(fig_top, use_container_width=True)

    # ── Chart 2: Treemap by mesoregion ────────────────────────
    with col2:
        st.subheader("Distribuicao Regional (Treemap)")

        fig_treemap = px.treemap(
            df,
            path=["nm_mesorregiao", "nm_municipio"],
            values="vl_total_casos",
            color="vl_incidencia_acumulada",
            color_continuous_scale="Reds",
            labels={
                "vl_total_casos": "Casos",
                "vl_incidencia_acumulada": "Incidencia",
            },
            height=max(CHART_HEIGHT, top_n * 22),
        )
        fig_treemap = apply_professional_theme(fig_treemap)
        st.plotly_chart(fig_treemap, use_container_width=True)

    st.markdown("---")

    # ── Chart 3: Regional comparison ──────────────────────────
    st.subheader("Comparativo por Mesorregiao")

    meso_agg = df.groupby("nm_mesorregiao").agg({
        "vl_total_casos": "sum",
        "nr_semanas_alerta_alto": "sum",
        "vl_rt_medio": "mean",
        "nm_municipio": "count",
    }).reset_index()
    meso_agg.columns = ["Mesorregiao", "Total Casos", "Sem. Alerta Alto", "Rt Medio", "Municipios"]

    fig_meso = px.bar(
        meso_agg.sort_values("Total Casos", ascending=True),
        y="Mesorregiao",
        x="Total Casos",
        color="Rt Medio",
        color_continuous_scale="RdYlGn_r",
        labels={"Total Casos": "Total de Casos"},
        height=CHART_HEIGHT,
        orientation="h",
    )
    fig_meso = apply_professional_theme(fig_meso)
    st.plotly_chart(fig_meso, use_container_width=True)

    st.markdown("---")

    # ── Full ranking table ────────────────────────────────────
    st.subheader("📋 Ranking Completo")

    search_term = st.text_input("Buscar municipio", "", key="rank_search")
    filtered_df = df.copy()
    if search_term:
        filtered_df = filtered_df[
            filtered_df["nm_municipio"].str.contains(search_term, case=False, na=False)
        ]

    display_cols = [
        "nr_rank_estado", "nm_municipio", "nm_mesorregiao",
        "vl_total_casos", "vl_incidencia_acumulada", "vl_rt_medio",
        "nr_semanas_alerta_vermelho", "nr_semanas_alerta_alto"
    ]
    display_df = filtered_df[display_cols].copy()
    display_df.columns = [
        "Rank SP", "Municipio", "Mesorregiao",
        "Casos", "Incidencia", "Rt Medio",
        "Sem. Vermelho", "Sem. Alto"
    ]

    st.dataframe(
        display_df.style.format({
            "Casos": "{:,.0f}",
            "Incidencia": "{:.2f}",
            "Rt Medio": "{:.3f}",
        }),
        use_container_width=True,
        height=400,
    )

    # ── Export ────────────────────────────────────────────────
    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📥 Exportar Ranking (CSV)",
        data=csv_data,
        file_name=f"ranking_{selected_disease}_{selected_year}.csv",
        mime="text/csv",
    )
