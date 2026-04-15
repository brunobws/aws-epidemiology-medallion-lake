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
    favicon_svg = f"""
    <svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'>
        <text y='75' font-size='75' dominant-baseline='middle'>{emoji}</text>
    </svg>
    """
    
    st.markdown(
        f"""
        <link rel="icon" href="data:image/svg+xml,
        {favicon_svg.replace('"', '&quot;').replace('#', '%23')}" />
        """,
        unsafe_allow_html=True
    )
