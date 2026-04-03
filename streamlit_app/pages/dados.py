####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Dados page — epidemiological surveillance visualizations.
#   Tabs: Visao Geral, Serie Temporal, Ranking, Perfil Demografico
####################################################################

import sys
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.shared_ui import render_header, render_sidebar, render_footer, require_athena

render_header()
render_sidebar()

athena_service = require_athena()

tab1, tab2, tab3, tab4 = st.tabs([
    "Visao Geral",
    "Serie Temporal",
    "Ranking",
    "Perfil Demografico",
])

with tab1:
    from epidemio_analytics import render_epidemio_analytics
    render_epidemio_analytics(athena_service)

with tab2:
    from epidemic_timeseries import render_epidemic_timeseries
    render_epidemic_timeseries(athena_service)

with tab3:
    from epidemic_ranking import render_epidemic_ranking
    render_epidemic_ranking(athena_service)

with tab4:
    from epidemic_demographic import render_epidemic_demographic
    render_epidemic_demographic(athena_service)

render_footer()
