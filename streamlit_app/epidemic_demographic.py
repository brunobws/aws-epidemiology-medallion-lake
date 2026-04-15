####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Demographic profile module for ArboVigilancia SP Dashboard.
#   Analyzes SINAN notification data by age, sex, clinical outcome,
#   and geographic distribution.
#
#   Features:
#   - Age pyramid (male/female divergent)
#   - Lethality and cure rate KPIs
#   - Clinical outcome by age bracket
#   - Monthly seasonality heatmap by mesoregion
####################################################################

########### imports ################
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from utils.data_service import fetch_available_years
from config import (
    TABLE_DEMOGRAPHIC,
    DISEASES,
    DISEASES_PT,
    AGE_BRACKETS,
    SEX_MAP,
    CHART_HEIGHT,
    CACHE_TTL,
)
from theme import (
    apply_professional_theme,
    title_with_help,
    COLOR_ERROR,
    COLOR_SUCCESS,
    COLOR_INFO,
)
###################################

logger = get_logger(__name__)

MONTH_LABELS = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
}


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_demographic_data(_athena_service: AthenaService, disease: str, year: int) -> pd.DataFrame:
    """Fetch demographic profile data aggregated by age, sex, and region."""
    query = f"""
    SELECT
        cd_geocode_ibge,
        nm_municipio,
        nm_microrregiao,
        nm_mesorregiao,
        ds_faixa_etaria,
        cs_sexo,
        nr_notificacoes,
        nr_casos_confirmados,
        nr_obitos,
        nr_curas,
        nr_mes_notificacao
    FROM {TABLE_DEMOGRAPHIC}
    WHERE ds_doenca = '{disease}'
    AND nr_ano_notificacao = {year}
    """
    try:
        df = _athena_service.query_gold(query)
        numeric_cols = ["nr_notificacoes", "nr_casos_confirmados", "nr_obitos", "nr_curas", "nr_mes_notificacao"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching demographic data: {str(e)}")
        return pd.DataFrame()


def render_epidemic_demographic(athena_service: AthenaService, disease: str):
    """Render demographic profile tab."""

    # -- Fetch available years ----
    years = fetch_available_years(athena_service, disease, TABLE_DEMOGRAPHIC, "nr_ano_notificacao")

    # ── Filters in container at top ──────────────────────────────
    st.markdown("### Filtros")
    col_year = st.columns(1)[0]
    with col_year:
        selected_year = st.selectbox("Ano de notificação", years, key="demo_year")

    st.markdown("---")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando perfil demográfico..."):
        df = fetch_demographic_data(athena_service, disease, selected_year)

    if df.empty:
        st.warning("Nenhum dado demográfico disponível.")
        return

    st.markdown(f"""
    <h2 style="text-align: center; font-size: 1.2rem; color: #333; font-weight: 300;">
    {DISEASES_PT[disease]} · {selected_year}
    </h2>
    <p style="text-align: center; font-size: 0.95rem; color: #666; margin-top: -10px;"><b>Quem está sendo infectado? Onde? Com que intensidade?</b></p>
    """, unsafe_allow_html=True)
    st.divider()
    st.write("")  # Spacing

    # ── KPIs ──────────────────────────────────────────────────
    total_notif = int(df["nr_notificacoes"].sum())
    total_conf = int(df["nr_casos_confirmados"].sum())
    total_deaths = int(df["nr_obitos"].sum())
    total_cures = int(df["nr_curas"].sum())

    lethality_rate = round(total_deaths / total_conf * 100, 2) if total_conf > 0 else 0
    cure_rate = round(total_cures / total_conf * 100, 2) if total_conf > 0 else 0
    conf_rate = round(total_conf / total_notif * 100, 2) if total_notif > 0 else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Notificações", f"{total_notif:,}")
    with k2:
        st.metric("Confirmados", f"{total_conf:,}")
    with k3:
        st.metric("Letalidade", f"{lethality_rate}%", help="Óbitos / Confirmados")
    with k4:
        st.metric("Cura", f"{cure_rate}%", help="Curados / Confirmados")
    with k5:
        st.metric("Confirmação", f"{conf_rate}%", help="Confirmados / Notificados")

    st.divider()
    st.write("")  # Spacing

    # ── Chart 1: Age pyramid ──────────────────────────────────
    title_with_help("Pirâmide Etária de Notificações", "Distribuição de casos por faixa etária e sexo. Lado esquerdo = masculino, direito = feminino")

    pyr = df.groupby(["ds_faixa_etaria", "cs_sexo"])["nr_notificacoes"].sum().reset_index()
    pyr_m = pyr[pyr["cs_sexo"] == "M"].copy()
    pyr_f = pyr[pyr["cs_sexo"] == "F"].copy()
    pyr_m["nr_notificacoes"] = -pyr_m["nr_notificacoes"]

    fig_pyr = go.Figure()
    fig_pyr.add_trace(go.Bar(
        y=pyr_m["ds_faixa_etaria"],
        x=pyr_m["nr_notificacoes"],
        orientation="h",
        name="Masculino",
        marker_color=COLOR_INFO,
    ))
    fig_pyr.add_trace(go.Bar(
        y=pyr_f["ds_faixa_etaria"],
        x=pyr_f["nr_notificacoes"],
        orientation="h",
        name="Feminino",
        marker_color=COLOR_ERROR,
    ))
    fig_pyr.update_layout(
        barmode="overlay",
        height=500,
        xaxis_title="Notificações",
        yaxis_title="Faixa Etária",
        yaxis=dict(categoryorder="array", categoryarray=AGE_BRACKETS),
    )

    # Make x-axis labels absolute values
    max_val = max(
        abs(pyr_m["nr_notificacoes"].min()) if not pyr_m.empty else 0,
        pyr_f["nr_notificacoes"].max() if not pyr_f.empty else 0
    )
    if max_val > 0:
        step = max(1, int(max_val / 4))
        ticks = list(range(-step * 4, step * 5, step))
        fig_pyr.update_xaxes(tickvals=ticks, ticktext=[str(abs(t)) for t in ticks])

    fig_pyr = apply_professional_theme(fig_pyr)
    st.plotly_chart(fig_pyr, use_container_width=True)
    
    st.caption(
        "💡 A distribuição entre sexos varia por doença. Alguns arbovírus afetam mais mulheres, outros afetam homens e mulheres igualmente. "
        "Faixas etárias diferentes tém riscos diferentes — alguns vírus são mais graves em idosos, outros em crianças."
    )

    st.divider()
    st.write("")  # Spacing

    # ── Chart 2 & 3: Outcomes and sex comparison ──────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Desfecho: Cura, Óbito ou Outro")
        outcome = df.groupby("ds_faixa_etaria").agg(
            curas=("nr_curas", "sum"),
            obitos=("nr_obitos", "sum"),
            outros=("nr_notificacoes", "sum"),
        ).reset_index()
        outcome["outros"] = (outcome["outros"] - outcome["curas"] - outcome["obitos"]).clip(lower=0)

        fig_outcome = go.Figure()
        fig_outcome.add_trace(go.Bar(
            x=outcome["ds_faixa_etaria"], y=outcome["curas"],
            name="Cura", marker_color=COLOR_SUCCESS,
        ))
        fig_outcome.add_trace(go.Bar(
            x=outcome["ds_faixa_etaria"], y=outcome["obitos"],
            name="Obito", marker_color=COLOR_ERROR,
        ))
        fig_outcome.add_trace(go.Bar(
            x=outcome["ds_faixa_etaria"], y=outcome["outros"],
            name="Outros", marker_color="#999",
        ))
        fig_outcome.update_layout(
            barmode="stack",
            height=CHART_HEIGHT,
            xaxis=dict(categoryorder="array", categoryarray=AGE_BRACKETS),
            xaxis_title="Faixa Etária",
            yaxis_title="Notificações",
        )
        fig_outcome = apply_professional_theme(fig_outcome)
        st.plotly_chart(fig_outcome, use_container_width=True)
        
        st.caption(
            "💡 Nem todo confirmado resulta em cura registrada — alguns podem estar em seguimento, "
            "abandonar acompanhamento, ou migrar. Por isso não somam 100%."
        )

    with col2:
        st.markdown("#### Casos por Sexo e Idade")
        sex_age = df[df["cs_sexo"].isin(["M", "F"])].groupby(
            ["ds_faixa_etaria", "cs_sexo"]
        )["nr_notificacoes"].sum().reset_index()
        sex_age["cs_sexo"] = sex_age["cs_sexo"].map(SEX_MAP)

        fig_sex = px.bar(
            sex_age,
            x="ds_faixa_etaria",
            y="nr_notificacoes",
            color="cs_sexo",
            barmode="group",
            color_discrete_map={"Masculino": COLOR_INFO, "Feminino": COLOR_ERROR},
            labels={"ds_faixa_etaria": "Faixa Etaria", "nr_notificacoes": "Notificacoes", "cs_sexo": "Sexo"},
            height=CHART_HEIGHT,
            category_orders={"ds_faixa_etaria": AGE_BRACKETS},
        )
        fig_sex = apply_professional_theme(fig_sex)
        st.plotly_chart(fig_sex, use_container_width=True)

    st.divider()
    st.write("")  # Spacing

    # ── Chart 4: Seasonality heatmap ──────────────────────────
    title_with_help("Sazonalidade: Notificações por Mês e Mesorregião", "Mapa de calor mostrando padrão sazonal de casos ao longo dos meses e regiões")
    
    heat = df.groupby(["nm_mesorregiao", "nr_mes_notificacao"])["nr_notificacoes"].sum().reset_index()
    heat_pivot = heat.pivot_table(
        index="nm_mesorregiao",
        columns="nr_mes_notificacao",
        values="nr_notificacoes",
        fill_value=0,
    )
    heat_pivot.columns = [MONTH_LABELS.get(int(c), str(c)) for c in heat_pivot.columns]

    fig_heat = px.imshow(
        heat_pivot,
        color_continuous_scale="OrRd",
        labels=dict(x="Mes", y="Mesorregiao", color="Notificacoes"),
        height=max(CHART_HEIGHT, len(heat_pivot) * 28),
        aspect="auto",
    )
    fig_heat = apply_professional_theme(fig_heat)
    st.plotly_chart(fig_heat, use_container_width=True)
    
    st.caption(
        "💡 Cores mais quentes (vermelho) = mais casos naquele més e região. "
        "Algumas doenças são sazonais (maior transmissão em certas épocas)."
    )

    st.markdown("---")

    # ── Export ────────────────────────────────────────────────
    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📺 Exportar Perfil Demográfico (CSV)",
        data=csv_data,
        file_name=f"demografico_{disease}_{selected_year}.csv",
        mime="text/csv",
    )
