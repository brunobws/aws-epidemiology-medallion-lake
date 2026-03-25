####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Theme and styling configuration for professional corporate
#   dashboard appearance. Provides color palette, typography,
#   and Plotly chart templates inspired by AWS design language.
####################################################################

########### imports ################
import plotly.graph_objects as go
import plotly.express as px
###################################


####################################################################
# AWS-INSPIRED COLOR PALETTE
####################################################################
COLOR_DARK_GRAY = "#232F3E"
COLOR_LIGHT_GRAY = "#37475A"
COLOR_ORANGE = "#FF9900"
COLOR_WHITE = "#FFFFFF"
COLOR_TEXT = "#1A1A1A"
COLOR_BORDER = "#E0E0E0"
COLOR_SUCCESS = "#2CA02C"
COLOR_ERROR = "#D62728"
COLOR_WARNING = "#DAA520"
COLOR_INFO = "#1F77B4"


####################################################################
# TYPOGRAPHY
####################################################################
FONT_FAMILY = "Arial, sans-serif"
FONT_SIZE_TITLE = 28
FONT_SIZE_SUBTITLE = 20
FONT_SIZE_LABEL = 14
FONT_SIZE_VALUE = 32


####################################################################
# PLOTLY TEMPLATE
####################################################################
def get_plotly_template():
    """
    Create a professional Plotly template with minimal visual noise.
    
    Returns:
        dict: Plotly template configuration
    """
    return {
        "layout": {
            "font": {"family": FONT_FAMILY, "size": FONT_SIZE_LABEL, "color": COLOR_TEXT},
            "plot_bgcolor": COLOR_WHITE,
            "paper_bgcolor": COLOR_WHITE,
            "margin": {"l": 50, "r": 50, "t": 50, "b": 50},
            "xaxis": {
                "showgrid": False,
                "showline": True,
                "linewidth": 1,
                "linecolor": COLOR_BORDER,
                "color": COLOR_TEXT
            },
            "yaxis": {
                "showgrid": True,
                "gridwidth": 0.5,
                "gridcolor": "#F0F0F0",
                "showline": True,
                "linewidth": 1,
                "linecolor": COLOR_BORDER,
                "color": COLOR_TEXT
            },
            "hovermode": "x unified",
            "legend": {
                "orientation": "h",
                "yanchor": "bottom",
                "y": 1.02,
                "xanchor": "right",
                "x": 1,
                "bgcolor": "rgba(255,255,255,0.8)",
                "bordercolor": COLOR_BORDER,
                "borderwidth": 1
            }
        }
    }


def apply_professional_theme(fig):
    """
    Apply professional corporate theme to a Plotly figure.
    
    Args:
        fig: Plotly figure object
    
    Returns:
        go.Figure: Themed figure
    """
    template = get_plotly_template()
    fig.update_layout(template["layout"])
    
    fig.update_xaxes(
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=COLOR_BORDER,
        color=COLOR_TEXT
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=0.5,
        gridcolor="#F0F0F0",
        showline=True,
        linewidth=1,
        linecolor=COLOR_BORDER,
        color=COLOR_TEXT
    )
    
    return fig


####################################################################
# CARD STYLING UTILITIES
####################################################################
def card_css() -> str:
    """
    Get CSS for card-style containers.
    
    Returns:
        str: CSS styling for cards
    """
    return f"""
    <style>
    .card-container {{
        background-color: #FAFAFA;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        padding: 20px;
        margin-bottom: 16px;
    }}
    
    .card-title {{
        font-size: 16px;
        font-weight: 600;
        color: {COLOR_DARK_GRAY};
        margin-bottom: 12px;
    }}
    </style>
    """


def kpi_card_html(value: str, label: str, delta: str = None) -> str:
    """
    Generate HTML for a modern KPI card.
    
    Args:
        value: Main metric value
        label: Metric label
        delta: Optional delta indicator
    
    Returns:
        str: HTML for the card
    """
    delta_html = f"<span style='color: {COLOR_SUCCESS}; font-size: 12px;'>{delta}</span>" if delta else ""
    
    return f"""
    <div style='
        background: {COLOR_WHITE};
        border-radius: 10px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    '>
        <div style='
            font-size: 24px;
            font-weight: 700;
            color: {COLOR_DARK_GRAY};
            margin-bottom: 6px;
        '>{value}</div>
        <div style='
            font-size: 12px;
            color: {COLOR_LIGHT_GRAY};
            margin-bottom: 8px;
        '>{label}</div>
        {delta_html}
    </div>
    """
