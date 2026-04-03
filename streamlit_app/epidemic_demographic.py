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
def fetch_demographic_data(athena_service: AthenaService, disease: str, year: int) -> pd.DataFrame:
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
        df = athena_service.query_gold(query)
        numeric_cols = ["nr_notificacoes", "nr_casos_confirmados", "nr_obitos", "nr_curas", "nr_mes_notificacao"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error fetching demographic data: {str(e)}")
        return pd.DataFrame()


@cached_query(ttl_seconds=CACHE_TTL)
def fetch_available_years(athena_service: AthenaService, disease: str) -> list:
    """Fetch available years for demographic data."""
    query = f"""
    SELECT DISTINCT nr_ano_notificacao
    FROM {TABLE_DEMOGRAPHIC}
    WHERE ds_doenca = '{disease}'
    ORDER BY nr_ano_notificacao DESC
    """
    try:
        df = athena_service.query_gold(query)
        if df.empty:
            return [2026]
        return sorted(df["nr_ano_notificacao"].astype(int).tolist(), reverse=True)
    except Exception as e:
        return [2026]


def render_epidemic_demographic(athena_service: AthenaService):
    """Render demographic profile tab."""

    # ── Sidebar filters ──────────────────────────────────────
    st.sidebar.markdown("### 👥 Filtros Demografico")
    selected_disease = st.sidebar.selectbox(
        "Doenca",
        DISEASES,
        format_func=lambda x: DISEASES_PT.get(x, x),
        key="demo_disease"
    )

    years = fetch_available_years(athena_service, selected_disease)
    selected_year = st.sidebar.selectbox("Ano notificacao", years, key="demo_year")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Carregando perfil demografico..."):
        df = fetch_demographic_data(athena_service, selected_disease, selected_year)

    if df.empty:
        st.warning("Nenhum dado demografico disponivel.")
        return

    st.subheader(f"👥 Perfil Demografico — {DISEASES_PT[selected_disease]} ({selected_year})")
    st.markdown("---")

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
        st.metric("Notificacoes", f"{total_notif:,}")
    with k2:
        st.metric("Confirmados", f"{total_conf:,}")
    with k3:
        st.metric("Taxa Letalidade", f"{lethality_rate}%")
    with k4:
        st.metric("Taxa Cura", f"{cure_rate}%")
    with k5:
        st.metric("Confirmacao", f"{conf_rate}%")

    st.markdown("---")

    # ── Chart 1: Age pyramid ──────────────────────────────────
    st.subheader("Piramide Etaria de Notificacoes")

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
        height=CHART_HEIGHT,
        xaxis_title="Notificacoes",
        yaxis_title="Faixa Etaria",
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

    st.markdown("---")

    # ── Chart 2 & 3: Outcomes and sex comparison ──────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Desfecho Clinico por Faixa Etaria")
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
            xaxis_title="Faixa Etaria",
            yaxis_title="Notificacoes",
        )
        fig_outcome = apply_professional_theme(fig_outcome)
        st.plotly_chart(fig_outcome, use_container_width=True)

    with col2:
        st.subheader("Notificacoes por Sexo e Faixa Etaria")
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

    st.markdown("---")

    # ── Chart 4: Seasonality heatmap ──────────────────────────
    st.subheader("Sazonalidade: Notificacoes por Mes e Mesorregiao")

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

    st.markdown("---")

    # ── Export ────────────────────────────────────────────────
    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📥 Exportar Perfil Demografico (CSV)",
        data=csv_data,
        file_name=f"demografico_{selected_disease}_{selected_year}.csv",
        mime="text/csv",
    )
