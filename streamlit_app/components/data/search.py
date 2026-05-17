####################################################################
# Author: Bruno William da Silva
#
# Description:
#   Search component by CEP or City Name.
#   Integrates with ViaCEP API and queries the Gold Layer.
####################################################################

import streamlit as st
import pandas as pd
import requests
from services.athena_service import AthenaService
from utils.cache_manager import cached_query
from utils.logger import get_logger
from config import TABLE_ALERTS_WEEKLY, CACHE_TTL
from theme import (
    kpi_card_html, 
    title_with_help, 
    ALERT_VERDE, 
    ALERT_AMARELO, 
    ALERT_LARANJA, 
    ALERT_VERMELHO,
    COLOR_INFO
)

logger = get_logger(__name__)

@cached_query(ttl_seconds=CACHE_TTL)
def fetch_all_cities(_athena_service: AthenaService, disease: str) -> pd.DataFrame:
    """Fetch distinct cities and their geocodes to populate the dropdown."""
    query = f"""
    SELECT DISTINCT cd_geocode, nm_municipio
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
    ORDER BY nm_municipio
    """
    try:
        return _athena_service.query_gold(query)
    except Exception as e:
        logger.error(f"Error fetching cities: {str(e)}")
        return pd.DataFrame()

def fetch_city_alert_status(athena_service: AthenaService, disease: str, geocode: str) -> pd.DataFrame:
    """Fetch the latest alert for a specific city."""
    query = f"""
    SELECT
        cd_geocode, nm_municipio, vl_populacao, nr_semana_epi, 
        nr_nivel_alerta, ds_nivel_alerta, vl_casos, vl_incidencia, vl_rt, dt_semana_epidemiologica
    FROM {TABLE_ALERTS_WEEKLY}
    WHERE ds_doenca = '{disease}'
      AND CAST(cd_geocode AS VARCHAR) LIKE '{geocode}%'
      AND dt_semana_epidemiologica = (
          SELECT MAX(dt_semana_epidemiologica)
          FROM {TABLE_ALERTS_WEEKLY}
          WHERE ds_doenca = '{disease}'
      )
    """
    try:
        df = athena_service.query_gold(query)
        numeric_cols = ["vl_populacao", "nr_semana_epi", "nr_nivel_alerta", "vl_casos", "vl_incidencia", "vl_rt"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.error(f"Error fetching alert for {geocode}: {str(e)}")
        return pd.DataFrame()

def get_city_from_cep(cep: str):
    """Call ViaCEP to get city name and IBGE code."""
    cep_clean = "".join(filter(str.isdigit, cep))
    if len(cep_clean) != 8:
        return None, "CEP deve conter 8 dígitos."
    
    try:
        response = requests.get(f"https://viacep.com.br/ws/{cep_clean}/json/", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if "erro" in data:
                return None, "CEP não encontrado."
            return data, None
        return None, f"Erro na API do ViaCEP: {response.status_code}"
    except Exception as e:
        return None, f"Erro de conexão com ViaCEP: {str(e)}"

def render_epidemic_search(athena_service: AthenaService, selected_disease: str):
    title_with_help("Busca Direcionada", "Pesquise por CEP ou selecione o município para ver a situação local.")
    
    cities_df = fetch_all_cities(athena_service, selected_disease)
    
    # State variables
    if "search_geocode" not in st.session_state:
        st.session_state.search_geocode = None
    if "search_city_name" not in st.session_state:
        st.session_state.search_city_name = None

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Buscar por CEP")
        cep_input = st.text_input("Digite o CEP (Apenas números ou com traço):", max_chars=9, placeholder="Ex: 13010-140")
        if st.button("Buscar CEP", use_container_width=True):
            if cep_input:
                with st.spinner("Buscando no ViaCEP..."):
                    cep_data, err = get_city_from_cep(cep_input)
                    if err:
                        st.error(err)
                    else:
                        city_name = cep_data.get("localidade")
                        geocode = cep_data.get("ibge")
                        
                        if cep_data.get("uf") != "SP":
                            st.warning("O painel EpiMind cobre apenas o estado de São Paulo no momento.")
                        elif geocode:
                            st.session_state.search_geocode = str(geocode)[:6] # Utilizar base de 6 digitos para compatibilidade
                            st.session_state.search_city_name = city_name
                            st.success(f"📍 Região encontrada: **{city_name} - {cep_data.get('uf')}**")
            else:
                st.warning("Por favor, digite um CEP.")

    with col2:
        st.markdown("#### Buscar por Nome da Cidade")
        if not cities_df.empty:
            city_dict = dict(zip(cities_df['nm_municipio'], cities_df['cd_geocode']))
            city_list = [""] + sorted(list(city_dict.keys()))
            
            selected_city = st.selectbox(
                "Selecione ou digite a cidade:", 
                options=city_list,
                format_func=lambda x: "--- Selecione uma cidade ---" if x == "" else x
            )
            
            if st.button("Consultar Cidade", use_container_width=True):
                if selected_city != "":
                    # Extrai os 6 primeiros digitos por segurança
                    st.session_state.search_geocode = str(city_dict[selected_city])[:6]
                    st.session_state.search_city_name = selected_city
                else:
                    st.warning("Selecione uma cidade na lista.")
        else:
            st.info("Carregando lista de cidades...")

    st.divider()

    # Results section
    if st.session_state.search_geocode:
        with st.spinner(f"Carregando dados epidemiológicos para {st.session_state.search_city_name}..."):
            alert_df = fetch_city_alert_status(athena_service, selected_disease, st.session_state.search_geocode)

        if alert_df.empty:
            st.warning(f"Sem dados recentes disponíveis para {st.session_state.search_city_name}.")
        else:
            row = alert_df.iloc[0]
            nivel = int(row.get("nr_nivel_alerta", 1))
            
            alert_colors = {1: ALERT_VERDE, 2: ALERT_AMARELO, 3: ALERT_LARANJA, 4: ALERT_VERMELHO}
            alert_texts = {1: "Verde (Controlado)", 2: "Amarelo (Atenção)", 3: "Laranja (Alto)", 4: "Vermelho (Crítico)"}
            
            color = alert_colors.get(nivel, COLOR_INFO)
            status = alert_texts.get(nivel, "Desconhecido")
            
            st.markdown(f"### Resultados para: **{row['nm_municipio']}**")
            
            data_semana = row.get("dt_semana_epidemiologica", "")
            if pd.notnull(data_semana) and str(data_semana).strip() != "":
                try:
                    dt_obj = pd.to_datetime(data_semana)
                    data_formatada = dt_obj.strftime('%d/%m/%Y')
                    st.caption(f"📅 Dados referentes à Semana Epidemiológica iniciada em: **{data_formatada}**")
                except:
                    pass
            
            if nivel == 4:
                st.error(f"🚨 **ALERTA MÁXIMO:** A cidade de {row['nm_municipio']} está classificada com Risco Crítico para {selected_disease.capitalize()}.")
            elif nivel == 3:
                st.warning(f"⚠️ **ATENÇÃO:** A cidade de {row['nm_municipio']} está com Risco Alto para {selected_disease.capitalize()}.")
            elif nivel == 2:
                st.info(f"🟡 **AVISO:** A cidade de {row['nm_municipio']} apresenta crescimento nos casos de {selected_disease.capitalize()}.")
            else:
                st.success(f"✅ **TRANQUILO:** A transmissão de {selected_disease.capitalize()} está controlada em {row['nm_municipio']}.")

            c1, c2, c3, c4, c5 = st.columns(5)
            
            with c1:
                st.markdown(kpi_card_html(
                    status,
                    "Nível de Alerta",
                    color=color,
                    description="O nível de alerta é calculado com base na velocidade de crescimento (Rt) e na incidência."
                ), unsafe_allow_html=True)
                
            with c2:
                st.markdown(kpi_card_html(
                    f"{int(row.get('vl_casos', 0)):,}".replace(',', '.'),
                    "Casos na Semana",
                    color=color,
                    description="Número absoluto de casos notificados para a semana epidemiológica selecionada."
                ), unsafe_allow_html=True)
                
            with c3:
                inc_val = row.get("vl_incidencia", 0)
                inc_str = f"{inc_val:.1f}" if pd.notnull(inc_val) else "0.0"
                st.markdown(kpi_card_html(
                    inc_str,
                    "Incidência / 100k hab",
                    color=color,
                    description="Taxa de incidência por 100 mil habitantes. Mede a força da epidemia proporcionalmente."
                ), unsafe_allow_html=True)
                
            with c4:
                rt_val = row.get("vl_rt", 0)
                rt_str = f"{rt_val:.2f}" if pd.notnull(rt_val) else "N/A"
                rt_color = ALERT_VERMELHO if pd.notnull(rt_val) and rt_val > 1.0 else ALERT_VERDE
                st.markdown(kpi_card_html(
                    rt_str,
                    "Rt (Taxa Transmissão)",
                    color=rt_color,
                    description="Se Rt > 1, a doença está em expansão acelerada. Se Rt < 1, os casos estão caindo."
                ), unsafe_allow_html=True)
                
            with c5:
                pop_val = row.get("vl_populacao", 0)
                st.markdown(kpi_card_html(
                    f"{int(pop_val):,}".replace(',', '.'),
                    "População",
                    color=COLOR_INFO,
                    description="População estimada do município baseada nos dados do IBGE."
                ), unsafe_allow_html=True)

            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, rgba(102,126,234,0.1) 0%, rgba(118,75,162,0.1) 100%);
                border: 1px solid rgba(102,126,234,0.3);
                border-left: 4px solid #667eea;
                border-radius: 8px;
                padding: 20px;
                margin-top: 32px;
                margin-bottom: 16px;
            ">
                <h4 style="margin: 0 0 8px 0; color: #1a1a2e; font-size: 18px; font-weight: 700;">
                    Análise Avançada com <span style="color: #667eea;">Inteligência Artificial</span>
                </h4>
                <p style="margin: 0; color: #4a5568; font-size: 14px;">
                    Descubra o que a IA do EpiMind analisa sobre <b>{row['nm_municipio']}</b>. 
                    Gere um relatório contextualizado cruzando métricas epidemiológicas, demográficas e fatores de risco instantaneamente.
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            _, col_ai_btn, _ = st.columns([0.25, 0.5, 0.25])
            with col_ai_btn:
                if st.button("Iniciar Análise por IA", use_container_width=True, type="primary"):
                    prompt = (
                        f"Como está a situação epidemiológica da cidade de {row['nm_municipio']} "
                        f"em relação à {selected_disease.capitalize()}? "
                        "Faça uma análise detalhada considerando o nível de alerta, a taxa de incidência, "
                        "o Rt (tendência de crescimento) e cruze com o perfil demográfico da cidade. "
                        "Destaque pontos críticos ou pontos positivos da vigilância atual."
                    )
                    st.session_state["auto_prompt"] = prompt
                    st.switch_page("pages/2_ai_analyst.py")
