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
import plotly.graph_objects as go
from utils.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from utils.data_service import fetch_available_years
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
def fetch_ranking_data(_athena_service: AthenaService, disease: str, year: int) -> pd.DataFrame:
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
        df = _athena_service.query_gold(query)
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


def classificar_porte(populacao: float) -> str:
    """Classifica município por porte baseado em população."""
    if populacao < 20_000:
        return "🟡 Pequeno"
    elif populacao < 100_000:
        return "🟠 Médio"
    else:
        return "🔴 Grande"


def classificar_rt(rt: float) -> str:
    """Classifica situação do Rt para exibição simples."""
    if rt > 1.2:
        return "🔴 Crescendo"
    elif rt >= 1.0:
        return "🟡 Atenção"
    else:
        return "🟢 Controlado"


def cor_rt(rt: float) -> str:
    """Retorna cor semântica baseada em Rt."""
    if rt > 1.2:
        return "#E24B4A"   # vermelho — crítico
    elif rt >= 1.0:
        return "#EF9F27"   # âmbar — atenção
    else:
        return "#1D9E75"   # verde — controlado


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
    
    st.markdown(f"""
    <h2 style="text-align: center; font-size: 1.2rem; color: #333; font-weight: 300;">
    {DISEASES_PT[disease]} · {selected_year}
    </h2>
    <p style="text-align: center; font-size: 0.95rem; color: #666; margin-top: -10px;"><b>Quais municípios precisam de atenção agora?</b></p>
    """, unsafe_allow_html=True)
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
    
    # Fórmula compacta + Help button
    col_form, col_help_btn = st.columns([0.88, 0.12])
    
    with col_form:
        st.markdown("""
        <small style="color: #666;">
        <strong>Pontuação de risco</strong> = velocidade de crescimento (Rt) + 
        proporção da população infectada (incidência por habitante)
        <br><br>
        <strong>Em alerta</strong> = doença crescendo rapidamente E alta proporção 
        de infectados para o tamanho da cidade
        </small>
        """, unsafe_allow_html=True)
    
    with col_help_btn:
        st.write("")  # Pequeno espaço
        with st.popover("?", use_container_width=False):
            st.markdown("""
            **Como identificamos municípios em risco?**
            
            Olhamos duas coisas ao mesmo tempo:
            
            🔴 **Velocidade de crescimento (Rt)**
            - Rt > 1.2 = crescimento rápido
            - Rt < 1 = diminuindo
            
            📍 **Proporção de infectados**
            - Uma cidade pequena com 200 infectados/100k é mais crítica que uma metrópole com 50/100k
            
            **Quando acrescentamos à lista?**
            AMBAS as condições: crescendo E alta proporção.
            """)
    
    st.write("")  # Spacing
    
    # Banner dinamicamente colorido
    if n_criticos > 0:
        st.error(f"⚠️ {n_criticos} municípios precisam de atenção agora — doença crescendo rapidamente com alta proporção de infectados por habitante")
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

    # ── Regional comparison: Two separate views ──────────────────
    title_with_help("Análise por mesorregião", """Duas métricas, duas respostas

Volume alto não significa crescimento rápido — e vice-versa. Leia os dois gráficos juntos para identificar prioridades:
- Esquerda: Qual mesorregião tem mais carga de casos?
- Direita: Qual mesorregião está crescendo mais rápido?""")

    st.write("")  # Spacing

    meso_agg = df.groupby("nm_mesorregiao").agg({
        "vl_total_casos": "sum",
        "nr_semanas_alerta_alto": "sum",
        "vl_rt_medio": "mean",
        "nm_municipio": "count",
    }).reset_index()
    meso_agg.columns = ["Mesorregiao", "Total Casos", "Sem. Alerta Alto", "Rt Medio", "Municipios"]

    # ── Gráfico 1: Volume de casos (esquerda) ────────────────────
    df_meso_vol = meso_agg.sort_values("Total Casos", ascending=True)
    
    fig_vol = go.Figure(go.Bar(
        x=df_meso_vol["Total Casos"],
        y=df_meso_vol["Mesorregiao"],
        orientation="h",
        marker_color="#378ADD",
        name="",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Total de casos: %{x:,.0f}<extra></extra>"
        )
    ))

    fig_vol.update_layout(
        title=dict(
            text="Onde há mais casos?",
            font=dict(size=13, color="#444"),
            x=0,
            xanchor="left"
        ),
        xaxis_title="Total de casos notificados",
        yaxis_title="",
        height=420,
        margin=dict(l=0, r=20, t=40, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="closest",
        showlegend=False,
    )
    
    # Linha de referência no valor mediano
    mediana = df_meso_vol["Total Casos"].median()
    fig_vol.add_vline(
        x=mediana,
        line_dash="dot",
        line_color="#aaa",
        annotation_text=f"Mediana: {mediana:.0f}",
        annotation_position="top right",
        annotation_font_size=10,
        annotation_font_color="#888"
    )

    # ── Gráfico 2: Velocidade de crescimento (direita) ──────────
    df_meso_rt = meso_agg.sort_values("Rt Medio", ascending=True)
    
    # Cores por faixa de Rt
    cores = df_meso_rt["Rt Medio"].apply(cor_rt).tolist()
    
    fig_rt = go.Figure(go.Bar(
        x=df_meso_rt["Rt Medio"],
        y=df_meso_rt["Mesorregiao"],
        orientation="h",
        marker_color=cores,
        name="",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Rt Médio: %{x:.2f}<extra></extra>"
        )
    ))

    fig_rt.update_layout(
        title=dict(
            text="Onde está crescendo mais rápido?",
            font=dict(size=13, color="#444"),
            x=0,
            xanchor="left"
        ),
        xaxis_title="Rt Médio (> 1 = crescendo)",
        yaxis_title="",
        height=420,
        margin=dict(l=0, r=20, t=40, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="closest",
        showlegend=False,
    )
    
    # Linha do limiar epidêmico Rt = 1.0
    fig_rt.add_vline(
        x=1.0,
        line_dash="dash",
        line_color="#E24B4A",
        annotation_text="Rt = 1 (limiar)",
        annotation_position="top right",
        annotation_font_size=10,
        annotation_font_color="#E24B4A"
    )

    # Montagem lado a lado
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_vol, use_container_width=True)
    with col2:
        st.plotly_chart(fig_rt, use_container_width=True)

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
        
        # Add Rt classification
        critical_display["Situação"] = critical_display["vl_rt_medio"].apply(classificar_rt)
        
        critical_display = critical_display[[
            "nm_municipio",
            "Situação",
            "nm_mesorregiao",
            "porte",
            "vl_rt_medio",
            "vl_incidencia_acumulada",
            "vl_prioridade"
        ]]
        
        critical_display.columns = [
            "Município",
            "Situação",
            "Mesorregião",
            "Porte",
            "Rt (velocidade)",
            "Infectados/100 mil hab.",
            "Pontuação de risco"
        ]
        
        st.dataframe(
            critical_display.style.format({
                "Rt (velocidade)": "{:.2f}",
                "Infectados/100 mil hab.": "{:.1f}",
                "Pontuação de risco": "{:.2f}",
            }),
            use_container_width=True,
            hide_index=True,
        )
        
        st.caption(
            "💡 Por que aparecem municípios pequenos? Uma cidade com 200 "
            "infectados por 100 mil habitantes está mais impactada do que "
            "uma metrópole com 50 por 100 mil — mesmo tendo menos casos. "
            "Olhamos a proporção, não o número total."
        )
    
    st.divider()
    st.write("")  # Spacing

    # ── Full ranking table ────────────────────────────────────────
    st.markdown("#### Ranking completo")
    
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
        "Ranking SP", "Município", "Mesorregião",
        "Total de casos", "Infectados/100 mil hab.", "Rt (velocidade)",
        "Semanas em alerta", "Semanas críticas"
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
