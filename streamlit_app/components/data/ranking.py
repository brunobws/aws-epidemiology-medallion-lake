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
from services.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from services.data_service import fetch_available_years
from config import (
    TABLE_RANKING_ANNUAL,
    TABLE_ALERTS_WEEKLY,
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


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_current_red_alerts(_athena_service: AthenaService, disease: str) -> pd.DataFrame:
    """Fetch municipalities currently in red alert (nivel 4) in the latest epidemic week."""
    query = f"""
    SELECT
        cd_geocode,
        nm_municipio,
        vl_populacao,
        nr_semana_epi,
        vl_casos,
        vl_incidencia,
        vl_rt,
        dt_semana_epidemiologica
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
      AND nr_nivel_alerta = 4
      AND dt_semana_epidemiologica = (
          SELECT MAX(dt_semana_epidemiologica)
          FROM {TABLE_ALERTS_WEEKLY}
          WHERE ds_doenca = '{disease}'
      )
    ORDER BY vl_incidencia DESC
    """
    try:
        df = _athena_service.query_gold(query)
        numeric_cols = ["vl_populacao", "vl_casos", "vl_incidencia", "vl_rt"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching current red alerts: {str(e)}")
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

    years = fetch_available_years(athena_service, disease, TABLE_RANKING_ANNUAL, "nr_ano_epi")

    st.markdown("### 📊 Monitoramento de Criticidade e Rankings")
    
    # Explicar a diferença de forma didática com um banner moderno
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, rgba(102,126,234,0.05) 0%, rgba(118,75,162,0.05) 100%);
        border: 1px solid rgba(102,126,234,0.2);
        border-left: 4px solid #667eea;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 24px;
    ">
        <h5 style="margin: 0 0 6px 0; color: #1a1a2e; font-weight: 700;">💡 Entendendo os Diferentes Níveis de Análise</h5>
        <p style="margin: 0; color: #4a5568; font-size: 13.5px; line-height: 1.5;">
            • <b>🔴 Alerta Semanal (Momento Atual):</b> Identifica os municípios que estão em nível crítico de transmissão (Alerta Vermelho) <b>nesta exata semana</b>. Essencial para ações emergenciais e distribuição imediata de recursos (ex: insumos, equipes de fumacê).
            <br>
            • <b>📊 Histórico de Risco Acumulado (Anual):</b> Consolida o impacto acumulado ao longo de <b>todo o ano selecionado</b> (Rt médio, semanas sob contágio ativo e casos). Ideal para planejamento de longo prazo, como campanhas de vacinação e contratos anuais, mesmo se a cidade estiver tranquila hoje (ex: Jambeiro).
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Sub-abas para separar o momento atual do histórico
    sub_tab_semana, sub_tab_ano = st.tabs([
        "🔴 Emergência: Alerta Vermelho Semanal",
        "📊 Planejamento: Histórico Acumulado Anual"
    ])

    with sub_tab_semana:
        st.markdown("#### 🔴 Municípios com Alerta Crítico (Semana Atual)")
        with st.spinner("Carregando alertas atuais..."):
            df_atual = fetch_current_red_alerts(athena_service, disease)
            
        if df_atual.empty:
            st.success("🎉 **Excelente notícia!** Nenhum município do Estado de São Paulo está em Alerta Vermelho na semana epidemiológica mais recente.")
        else:
            latest_week = df_atual["nr_semana_epi"].iloc[0]
            latest_date = pd.to_datetime(df_atual["dt_semana_epidemiologica"].iloc[0]).strftime("%d/%m/%Y")
            
            st.warning(
                f"⚠️ **Atenção:** Há **{len(df_atual)}** município(s) em nível de **Alerta Vermelho** (transmissão crítica) "
                f"na semana epidemiológica **{latest_week}** (iniciada em {latest_date})."
            )
            
            # Simple bar chart of weekly incidence for the red alert cities
            st.markdown("##### Ranking de Incidência Semanal (Alerta Vermelho)")
            fig_atual = px.bar(
                df_atual.nlargest(20, "vl_incidencia"),
                x="vl_incidencia",
                y="nm_municipio",
                color="vl_rt",
                color_continuous_scale="Reds",
                labels={
                    "vl_incidencia": "Incidência (casos por 100k hab.)",
                    "nm_municipio": "",
                    "vl_rt": "Rt Atual"
                },
                orientation="h",
                height=min(400, max(200, len(df_atual) * 35))
            )
            fig_atual.update_layout(yaxis={'categoryorder':'total ascending'})
            fig_atual = apply_professional_theme(fig_atual)
            st.plotly_chart(fig_atual, use_container_width=True)
            
            # Display detailed table
            st.markdown("##### Detalhamento dos Municípios em Emergência")
            df_atual_display = df_atual[[
                "nm_municipio", "vl_casos", "vl_incidencia", "vl_rt", "vl_populacao"
            ]].copy()
            df_atual_display.columns = [
                "Município", "Casos na Semana", "Incidência / 100k hab", "Rt (Velocidade)", "População"
            ]
            st.dataframe(
                df_atual_display.style.format({
                    "Casos na Semana": "{:,.0f}",
                    "Incidência / 100k hab": "{:.1f}",
                    "Rt (Velocidade)": "{:.2f}",
                    "População": "{:,.0f}"
                }),
                use_container_width=True,
                hide_index=True
            )

    with sub_tab_ano:
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
            st.warning("Nenhum dado de ranking disponível.")
        else:
            # ── Risk Score (duration-adjusted, population-filtered) ────
            TOTAL_SEMANAS_ANO = int(df["nr_semanas_rt_acima_1"].max()) if df["nr_semanas_rt_acima_1"].max() > 0 else 1
            _semanas_disponiveis = df["nr_semanas_rt_acima_1"].replace(0, pd.NA).dropna()
            TOTAL_SEMANAS_ANO = max(int(_semanas_disponiveis.max()) if not _semanas_disponiveis.empty else 1, 11)
            MIN_POPULACAO     = 5_000  # minimum population to appear in the risk ranking

            df["vl_rt_ajustado"] = (
                df["vl_rt_medio"]
                * (df["nr_semanas_rt_acima_1"] / TOTAL_SEMANAS_ANO).clip(upper=1.0)
            )

            df["vl_prioridade"] = (
                (df["vl_rt_ajustado"] * 0.5)
                + (df["vl_incidencia_acumulada"] / df["vl_incidencia_acumulada"].max() * 0.5)
            )

            # Zero out risk score for very small municipalities (statistical noise)
            df.loc[df["vl_populacao"] < MIN_POPULACAO, "vl_prioridade"] = 0.0
            
            # ── Add Porte classification (for Part 4)
            df["porte"] = df["vl_populacao"].apply(classificar_porte)
            
            st.markdown(f"""
            <h2 style="text-align: center; font-size: 1.2rem; color: #333; font-weight: 300;">
            {DISEASES_PT[disease]} · {selected_year}
            </h2>
            <p style="text-align: center; font-size: 0.95rem; color: #666; margin-top: -10px;"><b>🔥 Hotspots do Ano (Risco Histórico Acumulado)</b></p>
            """, unsafe_allow_html=True)
            st.divider()
            st.write("")  # Spacing

            # ── PART 2: Banner de alerta rápido no topo ──────────────────
            critical_threshold_rt = 0.2
            p90_incidence = df["vl_incidencia_acumulada"].quantile(0.90)

            critical_mun = df[
                (df["vl_rt_ajustado"] > critical_threshold_rt) &
                (df["vl_incidencia_acumulada"] > p90_incidence)
            ].sort_values("vl_prioridade", ascending=False)
            
            n_criticos = len(critical_mun)
            
            col_form, col_help_btn = st.columns([0.88, 0.12])
            
            with col_form:
                st.markdown("""
                <small style="color: #666;">
                <strong>Pontuação de risco</strong> = velocidade de crescimento (Rt) + 
                proporção da population infectada (incidência por habitante)
                <br><br>
                <strong>Em alerta</strong> = doença crescendo rapidamente E alta proporção 
                de infectados para o tamanho da cidade
                </small>
                """, unsafe_allow_html=True)
            
            with col_help_btn:
                st.write("")
                with st.popover("?", width="content"):
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
            
            st.write("")
            
            if n_criticos > 0:
                st.error(f"⚠️ {n_criticos} municípios com alto risco acumulado — transmissão sustentada e alta proporção de infectados por habitante ao longo do ano.")
            else:
                st.success("Nenhum município com alta prioridade acumulada de longo prazo.")

            st.divider()
            st.write("")

            # ── PART 1 & 3: Top N e Treemap ──
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Top {} municípios mais afetados no ano".format(top_n))
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
                
                limiar_critico = (critical_threshold_rt * 0.5) + (p90_incidence / df["vl_incidencia_acumulada"].max() * 0.5)

                fig_top.add_vline(
                    x=limiar_critico,
                    line_dash="dash",
                    line_color="#EF553B",
                    annotation_text="Limiar crítico",
                    annotation_position="top right",
                    annotation_font_size=10,
                )
                
                fig_top.update_traces(
                    hovertemplate="<b>%{y}</b><br>" +
                                 "Prioridade: %{x:.2f}<br>" +
                                 "Incidência: %{customdata[0]:.1f}/100k<br>" +
                                 "Rt Médio: %{color:.2f}<br>" +
                                 "Porte: %{customdata[1]}<extra></extra>",
                    customdata=top_df[["vl_incidencia_acumulada", "porte"]].values,
                )
                
                fig_top = apply_professional_theme(fig_top)
                st.plotly_chart(fig_top, width="stretch")

            with col2:
                st.markdown("#### Distribuição por mesorregião (Peso Anual)")

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
                st.plotly_chart(fig_treemap, width="stretch")

            st.divider()
            st.write("")

            # ── Regional comparison ──
            title_with_help("Análise de Carga Anual por mesorregião", """Volume vs Crescimento

Gráficos integrados mostrando a carga histórica acumulada do ano.""")

            st.write("")

            meso_agg = df.groupby("nm_mesorregiao").agg({
                "vl_total_casos": "sum",
                "nr_semanas_alerta_alto": "sum",
                "vl_rt_medio": "mean",
                "nm_municipio": "count",
            }).reset_index()
            meso_agg.columns = ["Mesorregiao", "Total Casos", "Sem. Alerta Alto", "Rt Medio", "Municipios"]

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
                    text="Onde houve mais casos?",
                    font=dict(size=13, color="#444"),
                    x=0,
                    xanchor="left"
                ),
                xaxis_title="Total de casos acumulados",
                yaxis_title="",
                height=420,
                margin=dict(l=0, r=20, t=40, b=40),
                plot_bgcolor="white",
                paper_bgcolor="white",
                hovermode="closest",
                showlegend=False,
            )
            
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

            df_meso_rt = meso_agg.sort_values("Rt Medio", ascending=True)
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
                    text="Crescimento médio anual do Rt",
                    font=dict(size=13, color="#444"),
                    x=0,
                    xanchor="left"
                ),
                xaxis_title="Rt Médio Anual (> 1 = expansão)",
                yaxis_title="",
                height=420,
                margin=dict(l=0, r=20, t=40, b=40),
                plot_bgcolor="white",
                paper_bgcolor="white",
                hovermode="closest",
                showlegend=False,
            )
            
            fig_rt.add_vline(
                x=1.0,
                line_dash="dash",
                line_color="#E24B4A",
                annotation_text="Rt = 1 (limiar)",
                annotation_position="top right",
                annotation_font_size=10,
                annotation_font_color="#E24B4A"
            )

            col1_m, col2_m = st.columns(2)
            with col1_m:
                st.plotly_chart(fig_vol, width="stretch")
            with col2_m:
                st.plotly_chart(fig_rt, width="stretch")

            st.divider()
            st.write("")

            # ── PART 4: Tabela de municípios com alto risco acumulado ──
            if n_criticos > 0:
                st.markdown("#### Municípios que mais demandam atenção de longo prazo")
                
                critical_display = critical_mun[[
                    "nm_municipio",
                    "nm_mesorregiao",
                    "porte",
                    "vl_rt_medio",
                    "vl_incidencia_acumulada",
                    "vl_prioridade"
                ]].copy()
                
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
                    width="stretch",
                    hide_index=True,
                )
                
                st.caption(
                    "💡 **Como funciona a Pontuação de Risco Anual?** "
                    "Combina dois fatores: (1) **Rt ajustado pela duração** — cidades com transmissão "
                    "acelerada POR VÁRIAS SEMANAS têm peso maior que surtos rápidos de uma semana; "
                    "(2) **incidência relativa** — taxa por 100 mil ao longo do ano, não volume absoluto."
                )
            
            st.divider()
            st.write("")

            # ── Full ranking table ──
            st.markdown("#### Ranking completo do ano")
            
            search_term = st.text_input("Buscar município no ranking anual", "", key="rank_search")
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
                    "Total de casos": "{:,.0f}",
                    "Infectados/100 mil hab.": "{:.2f}",
                    "Rt (velocidade)": "{:.3f}",
                }),
                width="stretch",
                height=400,
            )

            st.divider()
            st.write("")

            # ── Export ──
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="📥 Exportar Ranking Anual (CSV)",
                data=csv_data,
                file_name=f"ranking_{disease}_{selected_year}.csv",
                mime="text/csv",
            )
