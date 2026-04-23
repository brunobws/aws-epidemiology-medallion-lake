####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Shared UI components for ArboVigilancia SP Dashboard.
#   Centralizes Athena service init, header, and sidebar rendering
#   reused across all pages.
####################################################################

########### imports ################
import streamlit as st
import streamlit.components.v1 as components
from services.athena_service import AthenaService
from utils.cache_manager import clear_all_caches
from utils.logger import get_logger
from theme import COLOR_DARK_GRAY, COLOR_LIGHT_GRAY, COLOR_ORANGE, COLOR_BORDER
###################################

logger = get_logger(__name__)


@st.cache_resource
def get_athena_service() -> AthenaService:
    """Initialize and cache Athena service for the session."""
    try:
        service = AthenaService()
        if service.health_check():
            logger.info("Athena service initialized successfully")
            return service
        logger.error("Athena health check failed")
        return None
    except Exception as e:
        logger.error(f"Error initializing Athena service: {str(e)}")
        return None


def render_header():
    """Compact professional header."""
    st.markdown(f"""
    <div style="padding: 8px 0 10px 0; margin-bottom: 12px; border-bottom: 1px solid {COLOR_BORDER};">
        <div style="display: flex; justify-content: flex-start; align-items: flex-start;">
            <div>
                <h3 style="margin: 0; font-size: 18px; color: {COLOR_DARK_GRAY}; font-weight: 600;">
                    EpiMind
                </h3>
                <p style="margin: 0; font-size: 11px; color: {COLOR_LIGHT_GRAY}; letter-spacing: 0.5px;">
                    Vigilância Epidemiológica de Arboviroses
                </p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_sidebar():
    """Sidebar with global disease filter and controls."""
    from config import DISEASES, DISEASES_PT
    from datetime import datetime, timedelta

    # Gambiarra segura para fuso BRT (UTC-3) sem precisar do pytz
    brt_time = datetime.utcnow() - timedelta(hours=3)

    with st.sidebar:
        st.markdown("### 🦟 Filtro Global")

        # Global disease selector
        disease = st.selectbox(
            "Doença",
            DISEASES,
            format_func=lambda x: DISEASES_PT.get(x, x),
            key="global_disease"
        )

        st.divider()

        # Update button
        if st.button("Atualizar Dados", width="stretch"):
            clear_all_caches()
            st.rerun()

        st.divider()

        # Last update timestamp
        st.caption(f"Última atualização: {brt_time.strftime('%d/%m/%Y %H:%M')}")

        st.caption(
            "Vigilância epidemiológica para arboviroses "
            "(dengue, chikungunya, zika) em São Paulo. "
            "Dados da camada Gold do Data Lake."
        )

    return disease


def render_footer():
    """Professional footer with developer info and links."""
    st.markdown(f"""
    <div style="border-top: 1px solid {COLOR_BORDER}; padding: 20px 0; margin-top: 40px;
                text-align: center; color: {COLOR_LIGHT_GRAY}; font-size: 12px;">
        <p style="margin-bottom: 5px;">EpiMind — Medallion Architecture Data Lake</p>
        <p style="margin-bottom: 8px;">Desenvolvido por CyberSquad</p>
        <div style="font-size: 11px;">
            <a href="https://github.com/brunobws/aws-epidemiology-medallion-lake" target="_blank"
               style="color: {COLOR_ORANGE}; text-decoration: none; font-weight: 600;">GitHub</a>
            &nbsp;|&nbsp;
            <a href="https://www.linkedin.com/in/brunowsilva/" target="_blank"
               style="color: {COLOR_ORANGE}; text-decoration: none; font-weight: 600;">LinkedIn</a>
        </div>
    </div>
    """, unsafe_allow_html=True)


def require_athena():
    """Return athena_service or stop the page with a connection error message."""
    athena_service = get_athena_service()
    if athena_service is None:
        st.error(
            "Erro de Conexao: Nao foi possivel conectar ao AWS Athena.\n\n"
            "Verifique:\n"
            "1. Credenciais AWS configuradas\n"
            "2. Regiao correta (sa-east-1)\n"
            "3. Permissoes IAM para Athena"
        )
        st.stop()
    return athena_service


def render_floating_ia_button():
    """Inject a fixed-position floating button into the real Streamlit page.

    Uses components.html + window.top to escape any intermediate Streamlit
    iframes, injecting CSS and HTML directly into the top-level page document.
    """
    st.html("""
    <script>
    function arboInject() {
        try {
            var doc = window.top.document;
            if (!doc || !doc.body) {
                setTimeout(arboInject, 200);
                return;
            }

            // Remove previous instance to avoid duplicates on Streamlit rerun
            ['_arbo_fab_root', '_arbo_style_tag'].forEach(function(id) {
                var el = doc.getElementById(id);
                if (el) el.remove();
            });

            var style = doc.createElement('style');
            style.id = '_arbo_style_tag';
            style.textContent = [
                '@keyframes _arbo_glow{0%,100%{box-shadow:0 4px 20px rgba(102,126,234,.45)}50%{box-shadow:0 4px 32px rgba(118,75,162,.70)}}',
                '@keyframes _arbo_fadein{from{opacity:0;transform:scale(.93)}to{opacity:1;transform:scale(1)}}',
                '#_arbo_fab_root{position:fixed !important;bottom:32px !important;right:32px !important;z-index:2147483647 !important;pointer-events:none}',
                '#_arbo_fab_root *{pointer-events:auto}',
                '._arbo_fab{display:flex !important;position:relative;width:54px;height:54px;',
                '  background:linear-gradient(135deg,#667eea,#764ba2) !important;border-radius:50%;',
                '  border:none;cursor:pointer;align-items:center;justify-content:center;',
                '  font-size:22px;outline:none;animation:_arbo_glow 3s ease-in-out infinite;',
                '  transition:transform .18s ease;box-sizing:border-box}',
                '._arbo_fab:hover{transform:scale(1.1)}',
                '._arbo_fab:active{transform:scale(.96)}',
                '._arbo_tip{position:fixed;bottom:96px;right:24px;background:#232F3E;color:#fff;',
                '  font-size:12px;font-weight:500;padding:6px 12px;border-radius:8px;',
                '  white-space:nowrap;opacity:0;pointer-events:none;transition:opacity .2s;',
                '  z-index:2147483647;font-family:Arial,sans-serif}',
                '._arbo_tip::after{content:"";position:absolute;top:100%;right:16px;border:6px solid transparent;border-top-color:#232F3E}',
                '._arbo_fab:hover~._arbo_tip{opacity:1}',
                '._arbo_ov{display:none;position:fixed !important;inset:0;background:rgba(15,20,40,.55);',
                '  backdrop-filter:blur(3px);z-index:2147483646 !important;align-items:center;justify-content:center}',
                '._arbo_ov.on{display:flex;animation:_arbo_fadein .2s ease forwards}',
                '._arbo_card{background:#fff;border-radius:16px;padding:32px 28px 24px;',
                '  width:340px;box-shadow:0 24px 64px rgba(0,0,0,.22);text-align:center;font-family:Arial,sans-serif}',
                '._arbo_ico{width:56px;height:56px;background:linear-gradient(135deg,#667eea,#764ba2);',
                '  border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:26px;margin:0 auto 16px}',
                '._arbo_card h3{margin:0 0 8px;font-size:18px;font-weight:700;color:#1a1a2e}',
                '._arbo_card p{margin:0 0 24px;font-size:13px;color:#666;line-height:1.6}',
                '._arbo_row{display:flex;gap:10px}',
                '._arbo_no{flex:1;padding:10px;border:1.5px solid #ddd;border-radius:8px;background:#fff;color:#555;font-size:14px;font-weight:500;cursor:pointer;outline:none}',
                '._arbo_no:hover{background:#f5f5f5}',
                '._arbo_go{flex:1.4;padding:10px;border:none;border-radius:8px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;font-size:14px;font-weight:600;cursor:pointer;outline:none}',
                '._arbo_go:hover{opacity:.88}',
                '@media(max-width: 768px){',
                '  ._arbo_fab{width:44px;height:44px;font-size:18px}',
                '  #_arbo_fab_root{bottom:16px !important;right:16px !important}',
                '  ._arbo_card{width:300px;padding:24px 20px 20px}',
                '}'
            ].join('');
            doc.head.appendChild(style);

            var root = doc.createElement('div');
            root.id = '_arbo_fab_root';
            
            // Oculta no celular se estiver na página da IA
            if (window.top.location.pathname.includes('/ia') && window.innerWidth <= 768) {
                root.style.display = 'none';
            }
            
            root.innerHTML =
                '<button class="_arbo_fab">🧠</button>' +
                '<div class="_arbo_tip">Analista IA</div>' +
                '<div id="_arbo_ov" class="_arbo_ov">' +
                '  <div class="_arbo_card">' +
                '    <div class="_arbo_ico">🧠</div>' +
                '    <h3>Analista IA</h3>' +
                '    <p>Faca perguntas em linguagem natural sobre os dados' +
                '       epidemiologicos. A IA responde usando o Data Lake em tempo real.</p>' +
                '    <div class="_arbo_row">' +
                '      <button class="_arbo_no">Agora nao</button>' +
                '      <button class="_arbo_go">Abrir IA</button>' +
                '    </div>' +
                '  </div>' +
                '</div>';

            var fab = root.querySelector('._arbo_fab');
            var ov  = root.querySelector('._arbo_ov');
            var no  = root.querySelector('._arbo_no');
            var go  = root.querySelector('._arbo_go');

            fab.onclick = function() { ov.classList.add('on'); };
            no.onclick  = function() { ov.classList.remove('on'); };
            go.onclick  = function() {
                // Try direct navigation first; if sandbox blocks it, use anchor click in parent doc
                try {
                    window.top.location.href = '/ia';
                } catch(navErr) {
                    try {
                        var a = doc.createElement('a');
                        a.href = '/ia';
                        a.style.display = 'none';
                        doc.body.appendChild(a);
                        a.click();
                        setTimeout(function() { if (a.parentNode) a.parentNode.removeChild(a); }, 100);
                    } catch(e2) {
                        console.error('[ArboVigilancia] Navigation failed:', e2);
                    }
                }
            };
            ov.onclick  = function(e) { if (e.target === ov) ov.classList.remove('on'); };

            doc.body.appendChild(root);
            console.log('[ArboVigilancia] Floating button injected OK');
        } catch(e) {
            console.error('[ArboVigilancia] Injection failed:', e);
        }
    }
    arboInject();
    </script>
    """)
