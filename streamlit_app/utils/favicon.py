####################################################################
# Author: Bruno William da Silva
# Date: 04/14/2026
#
# Description:
#   UI helper for dynamic favicon by emoji.
#   Injects SVG favicon into page head via HTML.
####################################################################

import streamlit as st


def set_page_favicon(emoji: str) -> None:
    """
    Dynamically set page favicon using emoji.
    
    Args:
        emoji: Single emoji character (e.g., "📊", "🔍", "🤖")
        
    Example:
        set_page_favicon("📊")
    """
    # Encode emoji properly for SVG
    favicon_svg = f"""data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='75' font-size='75'>{emoji}</text></svg>"""
    
    st.markdown(
        f"""<link rel="icon" href="{favicon_svg}" />""",
        unsafe_allow_html=True
    )
