####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   IA Analista page — Natural language Q&A over epidemiological data.
#   Shell/mockup: visual only, not yet connected to AWS Bedrock.
####################################################################

import sys
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(
    page_title="Analista IA — EpiMind",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

from theme import COLOR_DARK_GRAY, COLOR_LIGHT_GRAY, COLOR_BORDER, COLOR_ORANGE
from utils.shared_ui import render_header, render_footer

render_header()

# ── Hero Banner ────────────────────────────────────────────────────
st.markdown("""
<style>
@keyframes pulse-ring {
    0%   { box-shadow: 0 0 0 0 rgba(102,126,234,0.4); }
    70%  { box-shadow: 0 0 0 12px rgba(102,126,234,0); }
    100% { box-shadow: 0 0 0 0 rgba(102,126,234,0); }
}
.ia-hero {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    border-radius: 16px;
    padding: 40px 48px;
    margin-bottom: 32px;
    position: relative;
    overflow: hidden;
}
.ia-hero::before {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 220px; height: 220px;
    background: radial-gradient(circle, rgba(102,126,234,0.25) 0%, transparent 70%);
    border-radius: 50%;
}
.ia-hero::after {
    content: '';
    position: absolute;
    bottom: -40px; left: 50%;
    width: 160px; height: 160px;
    background: radial-gradient(circle, rgba(118,75,162,0.18) 0%, transparent 70%);
    border-radius: 50%;
}
.ia-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(255,153,0,0.15);
    border: 1px solid rgba(255,153,0,0.4);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 12px;
    font-weight: 600;
    color: #FF9900;
    margin-bottom: 16px;
    letter-spacing: 0.5px;
}
.ia-badge-dot {
    width: 7px; height: 7px;
    background: #FF9900;
    border-radius: 50%;
    animation: pulse-ring 1.8s ease-out infinite;
    display: inline-block;
}
.ia-title {
    font-size: 32px;
    font-weight: 700;
    color: #ffffff;
    margin: 0 0 10px 0;
    line-height: 1.2;
}
.ia-subtitle {
    font-size: 15px;
    color: rgba(255,255,255,0.65);
    margin: 0;
    max-width: 560px;
    line-height: 1.6;
}
.ia-tech-chips {
    display: flex;
    gap: 8px;
    margin-top: 24px;
    flex-wrap: wrap;
}
.ia-chip {
    background: rgba(255,255,255,0.08);
    border: 1px solid rgba(255,255,255,0.15);
    border-radius: 6px;
    padding: 4px 12px;
    font-size: 12px;
    color: rgba(255,255,255,0.75);
    font-weight: 500;
}
</style>

<div class="ia-hero">
    <div class="ia-badge">
        <span class="ia-badge-dot"></span>
        Em desenvolvimento
    </div>
    <h1 class="ia-title">Analista IA</h1>
    <p class="ia-subtitle">
        Faca perguntas em linguagem natural sobre os dados epidemiologicos de SP.
        A IA consulta o Data Lake em tempo real e responde com insights claros e contextualizados.
    </p>
    <div class="ia-tech-chips">
        <span class="ia-chip">AWS Bedrock</span>
        <span class="ia-chip">LLM Avançado</span>
        <span class="ia-chip">Amazon Athena</span>
        <span class="ia-chip">Text-to-SQL</span>
        <span class="ia-chip">Gold Layer</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Layout: chat (left) | cards (right) ───────────────────────────
col_chat, col_info = st.columns([3, 2], gap="large")

with col_chat:
    # ── Sample questions ─────────────────────────────────────────
    st.markdown("""
    <style>
    .q-chip {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: #f5f5f7;
        border: 1px solid #e0e0e0;
        border-radius: 20px;
        padding: 6px 14px;
        font-size: 13px;
        color: #37475A;
        cursor: default;
        margin: 4px 4px 4px 0;
        white-space: nowrap;
        transition: background 0.15s;
    }
    .q-section {
        font-size: 12px;
        font-weight: 600;
        color: #999;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 10px;
    }
    </style>
    <p class="q-section">Perguntas de exemplo</p>
    <div style="display:flex; flex-wrap:wrap; gap:0; margin-bottom:20px;">
        <span class="q-chip">Qual cidade teve mais casos de dengue em 2024?</span>
        <span class="q-chip">Quais municípios estão em nível vermelho agora?</span>
        <span class="q-chip">Qual a incidência média na Grande SP esta semana?</span>
        <span class="q-chip">Mostre o Rt das mesorregiões com epidemia ativa</span>
        <span class="q-chip">Perfil demográfico das notificações de chikungunya</span>
        <span class="q-chip">Ranking de incidencia em 2023 vs 2024</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Mockup chat messages ──────────────────────────────────────
    with st.chat_message("user", avatar="🧑‍💼"):
        st.markdown("Quais as 5 cidades com maior incidência de dengue na última semana epidemiológica?")

    with st.chat_message("assistant", avatar="🤖"):
        st.markdown("""
Com base nos dados da semana epidemiológica mais recente disponível no Data Lake:

| # | Município | Incidência (por 100k) | Nível de Alerta |
|---|---|---|---|
| 1 | São José do Rio Preto | 1.847 | Vermelho |
| 2 | Bauru | 1.203 | Vermelho |
| 3 | Ribeirão Preto | 987 | Laranja |
| 4 | Presidente Prudente | 754 | Laranja |
| 5 | Araçatuba | 641 | Amarelo |

**Obs:** Todas essas cidades estão no interior paulista, padrão típico do perfil sazonal de dengue em SP. Mesorregiões de **Bauru** e **Araraquara** concentram os maiores focos ativos.
        """)

    with st.chat_message("user", avatar="🧑‍💼"):
        st.markdown("E como está o Rt médio do estado comparado à semana anterior?")

    with st.chat_message("assistant", avatar="🤖"):
        st.markdown("""
O **Rt médio estadual** na última semana foi de **1.34**, indicando transmissão crescente.

- Semana anterior: Rt = 1.18 (+13.6%)
- Municípios com Rt > 1: **312 de 645** (48.4%)
- Municípios em epidemia ativa: **87** *(fl_epidemia = 1)*

O aumento é consistente com o histórico sazonal do período — a Série Temporal mostra que o pico tipicamente ocorre nas próximas 3 a 5 semanas.
        """)

    # ── Disabled chat input ───────────────────────────────────────
    st.markdown("""
    <style>
    .chat-disabled-wrap {
        position: relative;
        margin-top: 8px;
    }
    .chat-disabled-overlay {
        position: absolute;
        inset: 0;
        background: rgba(255,255,255,0.6);
        border-radius: 8px;
        z-index: 10;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .chat-disabled-label {
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        font-size: 12px;
        font-weight: 600;
        padding: 5px 14px;
        border-radius: 20px;
        letter-spacing: 0.3px;
    }
    </style>
    <div class="chat-disabled-wrap">
        <div class="chat-disabled-overlay">
            <span class="chat-disabled-label">Disponível em breve</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.chat_input("Pergunte sobre os dados epidemiológicos...", disabled=True)


with col_info:
    # ── How it works ──────────────────────────────────────────────
    st.markdown("""
    <style>
    .arch-card {
        background: #FAFAFA;
        border: 1px solid #E8E8E8;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 16px;
    }
    .arch-step {
        display: flex;
        align-items: flex-start;
        gap: 12px;
        margin-bottom: 14px;
    }
    .arch-step:last-child { margin-bottom: 0; }
    .arch-num {
        min-width: 26px;
        height: 26px;
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        font-size: 12px;
        font-weight: 700;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .arch-text strong {
        display: block;
        font-size: 13px;
        color: #232F3E;
        font-weight: 600;
        margin-bottom: 2px;
    }
    .arch-text span {
        font-size: 12px;
        color: #666;
        line-height: 1.5;
    }
    .cap-card {
        background: linear-gradient(135deg, rgba(102,126,234,0.07), rgba(118,75,162,0.07));
        border: 1px solid rgba(102,126,234,0.2);
        border-radius: 12px;
        padding: 18px 20px;
        margin-bottom: 16px;
    }
    .cap-item {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 6px 0;
        font-size: 13px;
        color: #37475A;
        border-bottom: 1px solid rgba(102,126,234,0.1);
    }
    .cap-item:last-child { border-bottom: none; }
    .cap-icon {
        font-size: 16px;
        width: 24px;
        text-align: center;
    }
    .section-label {
        font-size: 11px;
        font-weight: 700;
        color: #999;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin: 0 0 12px 0;
    }
    </style>

    <div class="arch-card">
        <p class="section-label">Como vai funcionar</p>
        <div class="arch-step">
            <div class="arch-num">1</div>
            <div class="arch-text">
                <strong>Sua pergunta</strong>
                <span>Voce digita em linguagem natural, sem precisar saber SQL.</span>
            </div>
        </div>
        <div class="arch-step">
            <div class="arch-num">2</div>
            <div class="arch-text">
                <strong>Modelo de IA gera o SQL</strong>
                <span>O agente de IA via AWS Bedrock interpreta a pergunta em linguagem natural e monta uma query otimizada para o Athena.</span>
            </div>
        </div>
        <div class="arch-step">
            <div class="arch-num">3</div>
            <div class="arch-text">
                <strong>Consulta no Data Lake</strong>
                <span>O SQL roda direto nas tabelas Gold do Athena — dados reais, sem intermediarios.</span>
            </div>
        </div>
        <div class="arch-step">
            <div class="arch-num">4</div>
            <div class="arch-text">
                <strong>Resposta contextualizada</strong>
                <span>O modelo de IA interpreta os resultados e responde com linguagem epidemiologica adequada e insights acionáveis.</span>
            </div>
        </div>
    </div>

    <div class="cap-card">
        <p class="section-label">Capacidades previstas</p>
        <div class="cap-item">
            <span class="cap-icon">📊</span>
            Comparativos temporais e sazonais
        </div>
        <div class="cap-item">
            <span class="cap-icon">🗺️</span>
            Rankings e hotspots geograficos
        </div>
        <div class="cap-item">
            <span class="cap-icon">📈</span>
            Tendencias de Rt e nivel de alerta
        </div>
        <div class="cap-item">
            <span class="cap-icon">👥</span>
            Perfis demográficos e desfechos
        </div>
        <div class="cap-item">
            <span class="cap-icon">🔍</span>
            Consultas por município, mesorregião ou SE
        </div>
        <div class="cap-item">
            <span class="cap-icon">⚠️</span>
            Alertas e situacoes de epidemia ativa
        </div>
    </div>
    """, unsafe_allow_html=True)

render_footer()
