####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Observabilidade page — pipeline health monitoring and data quality.
#   Tabs: Observabilidade (Logs), Data Quality
####################################################################

import sys
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from components.shared.favicon import set_page_favicon
set_page_favicon("🔍")

from components.shared.ui import render_header, render_sidebar, render_footer, require_athena, render_floating_ia_button

render_header()
render_sidebar()
render_floating_ia_button()

athena_service = require_athena()

tab1, tab2 = st.tabs([
    "Observabilidade",
    "Data Quality",
])

with tab1:
    from components.observability.logs import render_logs_observability
    render_logs_observability(athena_service)

with tab2:
    from components.observability.data_quality import render_data_quality
    render_data_quality(athena_service)

render_footer()
