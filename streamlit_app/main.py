####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Entry point for ArboVigilancia SP Dashboard.
#   Defines multi-page navigation with per-page icons.
#   Run: streamlit run main.py
####################################################################

import sys
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))

from config import APP_NAME, STREAMLIT_LAYOUT

st.set_page_config(
    page_title=APP_NAME,
    page_icon="🦟",
    layout=STREAMLIT_LAYOUT,
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    body { background-color: #FAFAFA; }
    .stApp { background-color: #FAFAFA; }
</style>
""", unsafe_allow_html=True)

pg = st.navigation([
    st.Page("pages/dados.py", title="Dados", icon="📊", url_path="dados"),
    st.Page("pages/observabilidade.py", title="Observabilidade", icon="🔍", url_path="observabilidade"),
    st.Page("pages/ia_analista.py", title="IA Analista", icon="🤖", url_path="ia"),
])
pg.run()
