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
    title_with_help,
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


def classificar_porte(populacao: float) -> str:
    """Classifica município por porte baseado em população."""
    if populacao < 20_000:
        return "🟡 Pequeno"
    elif populacao < 100_000:
        return "🟠 Médio"
    else:
        return "🔴 Grande"


def render_epidemic_ranking(athena_service: AthenaService, disease: str):
    """Render ranking and hotspots tab."""

    # ── Fetch available years ────────────────────────────────────
    years = fetch_available_years(athena_service, disease)

    # ── Filters in container at top ──────────────────────────────
    st.markdown("### Filtros")
    col_year, col_top_n = st.columns(2)

    with col_year:
        selected_year = st.selectbox("Ano epidemiológico", years, key="rank_year")

    with col_top_n:
        top_n = st.slider("Top N municípios", 5, 50, 20, key="rank_top_n")

    st.markdown("---")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando ranking..."):
        df = fetch_ranking_data(athena_service, disease, selected_year)

    if df.empty:
        st.warning("Nenhum dado de ranking disponivel.")
        return

    # ── Calculate priority score for better prioritization ──────
    # Priority = Rt (growth factor) * Incidence (volume factor)
    # This identifies municipalities that are both growing AND have high impact
    df["vl_prioridade"] = (
        (df["vl_rt_medio"] * 0.5) +  # 50% weight to growth (Rt)
        (df["vl_incidencia_acumulada"] / df["vl_incidencia_acumulada"].max() * 0.5)  # 50% weight to incidence (normalized)
    )
    
    # ── Add Porte classification (for Part 4)
    df["porte"] = df["vl_populacao"].apply(classificar_porte)
    
    st.subheader(f"Ranking — {DISEASES_PT[disease]} ({selected_year})")
    st.divider()
    st.write("")  # Spacing

    # ── PART 2: Banner de alerta rápido no topo ──────────────────
    # Identificar municípios críticos antes do banner
    critical_threshold_rt = 1.2
    p90_incidence = df["vl_incidencia_acumulada"].quantile(0.90)
    
    critical_mun = df[
        (df["vl_rt_medio"] > critical_threshold_rt) & 
        (df["vl_incidencia_acumulada"] > p90_incidence)
    ].sort_values("vl_prioridade", ascending=False)
    
    n_criticos = len(critical_mun)
    
    # Fórmula compacta
    col_formula, col_help = st.columns([0.95, 0.05])
    with col_formula:
        st.markdown("""
        **Prioridade** = (Rt × 0.5) + (Incidência/100k normalizada × 0.5)  
        **Crítico** = Rt > 1.2 e Incidência acima do 90º percentil
        """)
    
    with col_help:
        with st.popover("ℹ️"):
            st.markdown("""
            **Prioridade** = Métrica que combina **Rt** (crescimento) + **Incidência** (volume)
            
            - Um município com **Rt = 2 e poucas pessoas** é crítico (crescendo rapidamente, mesmo que pequeno)
            - Um município com **Rt = 0.8 e muitas pessoas** é menos crítico (mesmo com volume alto, está controlando)
            - **Resultado**: Você vê municípios pequenos com 5% de infectados (taxa alta) lado de mega-cidades com 0.5% (mas muitos casos)
            
            **90º percentil** = Apenas os 10% piores municípios em incidência
            
            **Rt > 1.2** = Crescimento significativo (não apenas marginal). Um Rt de 1.2 significa que a cada ciclo de transmissão, a doença 20% se propaga 20% mais.
            
            **Porte do município** ajuda a entender o impacto relativo: um pequeno com 200/100k é crítico proporcionalmente.
            """)
    
    # Banner dinamicamente colorido
    if n_criticos > 0:
        st.error(f"**{n_criticos} município(s) em situação crítica** — Rt > 1.2 e incidência acima de {p90_incidence:.0f}/100k (90º percentil estadual)")
    else:
        st.success("Nenhum município em situação crítica. Todos estão sob controle ou com tendência de melhora.")

    st.divider()
    st.write("")  # Spacing

    # ── PART 1 & 3: Reorganized layout com títulos menores ──────
    
    # ── Top N municipalities by priority ────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Top {} municípios".format(top_n))
        top_df = df.nlargest(top_n, "vl_prioridade").sort_values("vl_prioridade", ascending=True)

        fig_top = px.bar(
            top_df,
            x="vl_prioridade",
            y="nm_municipio",
            color="vl_rt_medio",
            color_continuous_scale="RdYlGn_r",
            labels={
                "vl_prioridade": "Prioridade",
                "nm_municipio": "",
                "vl_rt_medio": "Rt Médio",
            },
            height=max(CHART_HEIGHT, top_n * 22),
            orientation="h",
        )
        
        # PART 5: Adicionar linha crítica (threshold = 1.0)
        fig_top.add_vline(
            x=1.0,
            line_dash="dash",
            line_color="#EF553B",
            annotation_text="Limiar crítico",
            annotation_position="top right",
            annotation_font_size=10,
        )
        
        # PART 5: Melhorar hover
        fig_top.update_traces(
            hovertemplate="<b>%{y}</b><br>" +
                         "Prioridade: %{x:.2f}<br>" +
                         "Incidência: %{customdata[0]:.1f}/100k<br>" +
                         "Rt Médio: %{color:.2f}<br>" +
                         "Porte: %{customdata[1]}<extra></extra>",
            customdata=top_df[["vl_incidencia_acumulada", "porte"]].values,
        )
        
        fig_top = apply_professional_theme(fig_top)
        st.plotly_chart(fig_top, use_container_width=True)

    # ── Treemap by mesoregion ────────────────────────────────────
    with col2:
        st.markdown("#### Distribuição por mesorregião")

        df_treemap = df.sort_values("vl_prioridade", ascending=False)
        
        fig_treemap = px.treemap(
            df_treemap,
            path=["nm_mesorregiao", "nm_municipio"],
            values="vl_prioridade",
            color="vl_rt_medio",
            color_continuous_scale="RdYlGn_r",
            labels={
                "vl_prioridade": "Prioridade",
                "vl_rt_medio": "Rt Médio",
            },
            height=max(CHART_HEIGHT, top_n * 22),
        )
        
        fig_treemap.update_traces(
            textinfo="label+value",
            textfont=dict(size=11),
            hovertemplate="<b>%{label}</b><br>Prioridade: %{value:.2f}<extra></extra>",
        )
        
        fig_treemap.update_layout(
            uniformtext=dict(minsize=9, mode='hide'),
        )
        
        fig_treemap = apply_professional_theme(fig_treemap)
        st.plotly_chart(fig_treemap, use_container_width=True)

    st.divider()
    st.write("")  # Spacing

    # ── Regional comparison ──────────────────────────────────────
    st.markdown("#### Volume de casos por mesorregião")

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

    st.divider()
    st.write("")  # Spacing

    # ── PART 4: Tabela de municípios críticos com coluna Porte ───
    if n_criticos > 0:
        st.markdown("#### Municípios que precisam de ação imediata")
        
        critical_display = critical_mun[[
            "nm_municipio",
            "nm_mesorregiao",
            "porte",
            "vl_rt_medio",
            "vl_incidencia_acumulada",
            "vl_prioridade"
        ]].copy()
        
        critical_display.columns = [
            "Município",
            "Mesorregião",
            "Porte",
            "Rt Médio",
            "Incidência/100k",
            "Prioridade"
        ]
        
        st.dataframe(
            critical_display.style.format({
                "Rt Médio": "{:.2f}",
                "Incidência/100k": "{:.1f}",
                "Prioridade": "{:.2f}",
            }),
            use_container_width=True,
            hide_index=True,
        )
        
        st.caption("Municípios pequenos com alta incidência são proporcionalmente mais afetados do que cidades grandes com mais casos absolutos.")
    
    st.divider()
    st.write("")  # Spacing

    # ── Full ranking table ────────────────────────────────────────
    st.markdown("#### 📋 Ranking completo")
    
    search_term = st.text_input("Buscar município", "", key="rank_search")
    filtered_df = df[
        df["nm_municipio"].str.contains(search_term, case=False, na=False)
    ]

    display_cols = [
        "nr_rank_estado", "nm_municipio", "nm_mesorregiao",
        "vl_total_casos", "vl_incidencia_acumulada", "vl_rt_medio",
        "nr_semanas_alerta_vermelho", "nr_semanas_alerta_alto"
    ]
    display_df = filtered_df[display_cols].copy()
    display_df.columns = [
        "Rank SP", "Município", "Mesorregião",
        "Casos", "Incidência", "Rt Médio",
        "Sem. Vermelho", "Sem. Alto"
    ]

    st.dataframe(
        display_df.style.format({
            "Casos": "{:,.0f}",
            "Incidência": "{:.2f}",
            "Rt Médio": "{:.3f}",
        }),
        use_container_width=True,
        height=400,
    )

    st.divider()
    st.write("")  # Spacing

    # ── Export ────────────────────────────────────────────────────
    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📥 Exportar Ranking (CSV)",
        data=csv_data,
        file_name=f"ranking_{disease}_{selected_year}.csv",
        mime="text/csv",
    )
