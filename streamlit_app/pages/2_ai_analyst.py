####################################################################
# Author: Bruno William da Silva
# Date: 17/04/2026
#
# Description:
#   IA Analista page — EpiMind conversational assistant.
#   Connects natural language questions to the Gold layer via a
#   two-step pipeline:
#     1. AWS Bedrock (Claude) generates a safe SELECT SQL.
#     2. Query runs on Athena (reuses AthenaService).
#     3. Bedrock receives the results and returns a full
#        epidemiological analysis in Brazilian Portuguese.
#
#   Key Features:
#   - st.chat_input + persistent message history (session_state)
#   - Out-of-scope guard (polite refusal for non-arbovirose topics)
#   - SQL safety validation (SELECT-only)
#   - Risk Score insights embedded in analysis prompt
####################################################################

########### imports ################
import sys
import traceback
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from components.shared.favicon import set_page_favicon
set_page_favicon("🤖")

from theme import COLOR_DARK_GRAY, COLOR_LIGHT_GRAY, COLOR_BORDER, COLOR_ORANGE
from components.shared.ui import render_header, render_footer, get_athena_service
from services.bedrock_service import BedrockService
from utils.logger import get_logger
###################################

logger = get_logger(__name__)


####################################################################
# CONSTANTS
####################################################################
AVATAR_USER      = "🧑‍💼"
AVATAR_ASSISTANT = "🤖"
MAX_RESULT_ROWS  = 50   # rows sent to LLM to avoid huge prompts
HISTORY_KEY      = "epimind_chat_history"

SAMPLE_QUESTIONS = [
    "Quais as 5 cidades com maior incidência de dengue em 2026?",
    "Quais municípios estão em alerta vermelho agora?",
    "Qual o Rt médio do estado na última semana disponível?",
    "Mostre o perfil demográfico das notificações de chikungunya",
    "Quais mesorregiões têm epidemia ativa de dengue?",
    "Ranking dos municípios com mais semanas de Rt > 1 em 2026",
]


####################################################################
# CACHED SERVICE INSTANCES
####################################################################
@st.cache_resource
def get_bedrock_service() -> BedrockService:
    """Initialize and cache the Bedrock client for the session."""
    try:
        svc = BedrockService()
        logger.info("BedrockService initialized successfully")
        return svc
    except Exception as exc:
        logger.error(f"Failed to initialize BedrockService: {exc}")
        return None


####################################################################
# HELPERS
####################################################################
def _df_to_markdown(df: pd.DataFrame, max_rows: int = MAX_RESULT_ROWS) -> tuple[str, int]:
    """
    Convert a DataFrame to a compact Markdown table for the LLM prompt.

    Args:
        df:       Query result DataFrame.
        max_rows: Max rows to include (avoids huge prompts).

    Returns:
        Tuple of (markdown_string, total_row_count).
    """
    total = len(df)
    preview = df.head(max_rows)
    md = preview.to_markdown(index=False) if not preview.empty else "_Nenhum dado retornado._"
    if total > max_rows:
        md += f"\n\n_(exibindo {max_rows} de {total} linhas)_"
    return md, total


def _init_history() -> None:
    """Ensure chat history exists in session state."""
    if HISTORY_KEY not in st.session_state:
        st.session_state[HISTORY_KEY] = []
    if "query_count" not in st.session_state:
        st.session_state["query_count"] = 0


def _add_message(role: str, content: str) -> None:
    """Append a message to the chat history."""
    st.session_state[HISTORY_KEY].append({"role": role, "content": content})


def _render_history() -> None:
    """Render all previous messages from session state."""
    for msg in st.session_state[HISTORY_KEY]:
        avatar = AVATAR_USER if msg["role"] == "user" else AVATAR_ASSISTANT
        with st.chat_message(msg["role"], avatar=avatar):
            st.markdown(msg["content"])


####################################################################
# MAIN PIPELINE
####################################################################
def process_question(question: str, bedrock: BedrockService, athena) -> None:
    """
    Full Text-to-SQL pipeline for one user question.

    Steps:
        0. Display user message.
        1. Generate SQL with Bedrock.
        2. Execute SQL on Athena.
        3. Generate analysis with Bedrock.
        4. Display assistant response.

    Args:
        question: User's natural language question.
        bedrock:  BedrockService instance.
        athena:   AthenaService instance.
    """
    # ── Step 0: Show user message ──────────────────────────────────
    _add_message("user", question)
    with st.chat_message("user", avatar=AVATAR_USER):
        st.markdown(question)

    # ── Step 1: Generate SQL ───────────────────────────────────────
    with st.chat_message("assistant", avatar=AVATAR_ASSISTANT):
        with st.status("🧠 Analisando sua pergunta...", expanded=False) as status:

            try:
                status.update(label="🧠 Gerando consulta SQL...")
                sql = bedrock.generate_sql(question)

            except (RuntimeError, ValueError) as exc:
                error_msg = f"⚠️ Erro ao gerar SQL: {exc}"
                st.error(error_msg)
                _add_message("assistant", error_msg)
                logger.error(f"SQL generation error: {exc}")
                return

            # Out-of-scope response
            if sql is None:
                out_of_scope_msg = (
                    "Olá! Sou o **EpiMind**, especializado em vigilância "
                    "epidemiológica de **dengue, chikungunya e zika em São Paulo**.\n\n"
                    "Sua pergunta parece estar fora do meu escopo. Posso ajudar com:\n"
                    "- Incidência, Rt e níveis de alerta por município\n"
                    "- Rankings e hotspots geográficos em SP\n"
                    "- Perfis demográficos das notificações\n"
                    "- Tendências semanais e comparativos anuais\n\n"
                    "Reformule sua pergunta e terá prazer em responder! 🦟"
                )
                st.markdown(out_of_scope_msg)
                _add_message("assistant", out_of_scope_msg)
                status.update(label="✅ Fora do escopo — respondido", state="complete")
                return

            if sql == "GENERAL_KNOWLEDGE":
                status.update(label="🧠 Gerando resposta (sem consulta a dados)...")
                df = pd.DataFrame()
                md_result = "_Pergunta de conhecimento geral._"
                row_count = 0
            else:
                max_retries = 2
                for attempt in range(max_retries + 1):
                    # Show generated SQL inside the status container (informational)
                    if attempt == 0:
                        st.markdown("**🔍 SQL gerado**")
                    else:
                        st.markdown(f"**🔄 SQL corrigido (Tentativa {attempt})**")
                    st.code(sql, language="sql")

                    # ── Step 2: Execute on Athena ──────────────────────────
                    status.update(label=f"⚡ Executando consulta no Athena (Tentativa {attempt+1})...")
                    try:
                        df = athena.query_gold(sql)
                        break  # Sucesso, sai do loop de retry
                    except Exception as exc:
                        tb = traceback.format_exc()
                        logger.warning(f"Athena query failed on attempt {attempt}: {exc}")
                        
                        if attempt < max_retries:
                            status.update(label=f"🔄 Erro no Athena. Corrigindo SQL (Tentativa {attempt+1})...")
                            try:
                                sql = bedrock.fix_sql(question, sql, str(exc))
                            except Exception as fix_exc:
                                logger.error(f"Failed to fix SQL: {fix_exc}")
                                error_msg = f"⚠️ **Erro ao tentar corrigir a consulta:**\n\n```\n{fix_exc}\n```"
                                st.error(error_msg)
                                _add_message("assistant", error_msg)
                                status.update(label="❌ Erro na correção do SQL", state="error")
                                return
                        else:
                            logger.error(f"Athena query failed after {max_retries} retries:\n{tb}")
                            error_msg = (
                                f"⚠️ **Erro ao executar a consulta no Athena após {max_retries} tentativas:**\n\n"
                                f"```\n{exc}\n```\n\n"
                                "Tente reformular sua pergunta ou verifique os logs."
                            )
                            st.error(error_msg)
                            _add_message("assistant", error_msg)
                            status.update(label="❌ Erro no Athena", state="error")
                            return

                # ── Step 3: Generate analysis ──────────────────────────
                status.update(label="📊 Gerando análise epidemiológica...")
                md_result, row_count = _df_to_markdown(df)

            try:
                analysis = bedrock.generate_analysis(
                    question=question,
                    sql=sql,
                    query_result=md_result,
                    row_count=row_count,
                )
            except RuntimeError as exc:
                error_msg = f"⚠️ Erro ao gerar análise: {exc}"
                st.error(error_msg)
                _add_message("assistant", error_msg)
                status.update(label="❌ Erro na análise", state="error")
                return

            status.update(label="✅ Análise concluída", state="complete")

        # ── Step 4: Display analysis ───────────────────────────────
        st.markdown(analysis)

        # Optionally show raw DataFrame
        if not df.empty:
            with st.expander(f"📋 Dados brutos ({row_count} linhas)", expanded=False):
                st.dataframe(df, width="stretch")

        _add_message("assistant", analysis)
        st.session_state["query_count"] += 1
        logger.info(f"Pipeline complete: question='{question[:80]}', rows={row_count}")


####################################################################
# PAGE LAYOUT
####################################################################
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
        Powered by AWS Bedrock
    </div>
    <h1 class="ia-title">EpiMind — Analista IA</h1>
    <p class="ia-subtitle">
        Faça perguntas em linguagem natural sobre os dados epidemiológicos de SP.
        A IA consulta o Data Lake em tempo real e responde com insights claros e contextualizados.
    </p>
    <div class="ia-tech-chips">
        <span class="ia-chip">AWS Bedrock</span>
        <span class="ia-chip">Claude Haiku</span>
        <span class="ia-chip">Amazon Athena</span>
        <span class="ia-chip">Text-to-SQL</span>
        <span class="ia-chip">Gold Layer</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Layout: chat (left) | info (right) ────────────────────────────
col_chat, col_info = st.columns([3, 2], gap="large")

with col_chat:
    # ── Sample question chips (display-only labels) ───────────────
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
    """ + "".join(f'<span class="q-chip">{q}</span>' for q in SAMPLE_QUESTIONS) + """
    </div>
    """, unsafe_allow_html=True)

    # ── Initialize chat state & render history ────────────────────
    _init_history()
    _render_history()

    # ── Services (lazy-loaded, cached) ────────────────────────────
    bedrock_service = get_bedrock_service()
    athena_service  = get_athena_service()

    # ── Chat input ────────────────────────────────────────────────
    limit_reached = st.session_state.get("query_count", 0) >= 5
    
    if limit_reached:
        st.error(
            "🔒 **Limite de Uso Atingido**\n\n"
            "Para proteger a infraestrutura e evitar custos abusivos, o uso público da IA "
            "está limitado a **5 perguntas por sessão**.\n\n"
            "💡 _Dica: Se precisar de mais testes, você pode simplesmente recarregar a página (F5)._"
        )

    user_input = st.chat_input(
        "Pergunte sobre os dados epidemiológicos de SP...",
        disabled=(bedrock_service is None or athena_service is None or limit_reached),
        key="epimind_chat_input",
    )

    # ── Service availability warnings ─────────────────────────────
    if bedrock_service is None:
        st.error(
            "⚠️ **AWS Bedrock indisponível.** Verifique as credenciais AWS "
            "e as permissões IAM para `bedrock:InvokeModel` na região `sa-east-1`."
        )

    if athena_service is None:
        st.error(
            "⚠️ **AWS Athena indisponível.** Verifique as credenciais AWS "
            "e as permissões IAM para Athena na região `sa-east-1`."
        )

    # ── Process new message ───────────────────────────────────────
    if user_input and bedrock_service and athena_service:
        process_question(
            question=user_input.strip(),
            bedrock=bedrock_service,
            athena=athena_service,
        )

    # ── Clear history button ──────────────────────────────────────
    if st.session_state.get(HISTORY_KEY):
        if st.button("🗑️ Limpar conversa", key="clear_chat", width="content"):
            st.session_state[HISTORY_KEY] = []
            st.rerun()


with col_info:
    # ── How it works card ─────────────────────────────────────────
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
    .risk-card {
        background: rgba(255,153,0,0.06);
        border: 1px solid rgba(255,153,0,0.3);
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 16px;
        font-size: 12px;
        color: #37475A;
        line-height: 1.6;
    }
    .risk-card strong { color: #232F3E; }
    </style>

    <div class="arch-card">
        <p class="section-label">Como funciona</p>
        <div class="arch-step">
            <div class="arch-num">1</div>
            <div class="arch-text">
                <strong>Sua pergunta</strong>
                <span>Você digita em linguagem natural, sem precisar saber SQL.</span>
            </div>
        </div>
        <div class="arch-step">
            <div class="arch-num">2</div>
            <div class="arch-text">
                <strong>IA gera o SQL</strong>
                <span>Claude (Bedrock) interpreta a pergunta e monta uma query segura para o Athena.</span>
            </div>
        </div>
        <div class="arch-step">
            <div class="arch-num">3</div>
            <div class="arch-text">
                <strong>Consulta no Data Lake</strong>
                <span>O SQL roda direto nas tabelas Gold — dados reais, sem intermediários.</span>
            </div>
        </div>
        <div class="arch-step">
            <div class="arch-num">4</div>
            <div class="arch-text">
                <strong>Análise contextualizada</strong>
                <span>A IA interpreta os resultados e responde com linguagem epidemiológica e insights acionáveis.</span>
            </div>
        </div>
    </div>

    <div class="cap-card">
        <p class="section-label">Capacidades</p>
        <div class="cap-item"><span class="cap-icon">📊</span>Comparativos temporais e sazonais</div>
        <div class="cap-item"><span class="cap-icon">🗺️</span>Rankings e hotspots geográficos</div>
        <div class="cap-item"><span class="cap-icon">📈</span>Tendências de Rt e nível de alerta</div>
        <div class="cap-item"><span class="cap-icon">👥</span>Perfis demográficos e desfechos</div>
        <div class="cap-item"><span class="cap-icon">🔍</span>Consultas por município, mesorregião ou SE</div>
        <div class="cap-item"><span class="cap-icon">⚠️</span>Alertas e situações de epidemia ativa</div>
    </div>

    <div class="risk-card">
        <strong>⚡ Pontuação de Risco</strong><br/>
        A IA avalia automaticamente a combinação de
        <strong>velocidade de crescimento (Rt)</strong> +
        <strong>incidência relativa</strong> para identificar
        municípios de alto risco — especialmente cidades pequenas
        com taxa/100k elevada.
    </div>
    """, unsafe_allow_html=True)


render_footer()
