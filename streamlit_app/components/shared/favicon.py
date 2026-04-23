####################################################################
# Author: Bruno William da Silva
# Date: 04/14/2026
#
# Description:
#   UI helper for dynamic favicon by emoji.
#   Injects SVG favicon into page head via JavaScript.
####################################################################

import streamlit as st
import streamlit.components.v1 as components


def set_page_favicon(emoji: str) -> None:
    """
    Dynamically set page favicon using emoji.
    
    Args:
        emoji: Single emoji character (e.g., "📊", "🔍", "🤖")
        
    Example:
        set_page_favicon("📊")
    """
    # Create SVG favicon as data URI
    favicon_svg = f"""data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='75' font-size='75'>{emoji}</text></svg>"""
    
    # Use JavaScript to set favicon in page head
    html_code = f"""
    <script>
    // Remove existing favicon if any
    const existingFavicon = document.querySelector("link[rel='icon']");
    if (existingFavicon) {{
        existingFavicon.remove();
    }}
    
    // Create and add new favicon link
    const link = document.createElement('link');
    link.rel = 'icon';
    link.href = '{favicon_svg}';
    document.head.appendChild(link);
    </script>
    """
    
    st.html(html_code)
