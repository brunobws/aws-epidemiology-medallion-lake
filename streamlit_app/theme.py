####################################################################
# Author: Bruno William da Silva
# Date: 03/03/2026
#
# Description:
#   Theme and styling configuration for ArboVigilancia SP dashboard.
#   Provides color palette, typography, Plotly chart templates,
#   and epidemiological alert color utilities.
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
# EPIDEMIOLOGICAL ALERT PALETTE
####################################################################
ALERT_VERDE = "#2CA02C"
ALERT_AMARELO = "#DAA520"
ALERT_LARANJA = "#FF7F0E"
ALERT_VERMELHO = "#D62728"

ALERT_COLOR_MAP = {
    "verde": ALERT_VERDE,
    "amarelo": ALERT_AMARELO,
    "laranja": ALERT_LARANJA,
    "vermelho": ALERT_VERMELHO,
    1: ALERT_VERDE,
    2: ALERT_AMARELO,
    3: ALERT_LARANJA,
    4: ALERT_VERMELHO,
}

ALERT_SEQUENCE = [ALERT_VERDE, ALERT_AMARELO, ALERT_LARANJA, ALERT_VERMELHO]

DOENCA_COLORS = {
    "dengue": "#D62728",
    "chikungunya": "#FF7F0E",
    "zika": "#1F77B4",
}


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
                "color": COLOR_TEXT,
            },
            "yaxis": {
                "showgrid": True,
                "gridwidth": 0.5,
                "gridcolor": "#F0F0F0",
                "showline": True,
                "linewidth": 1,
                "linecolor": COLOR_BORDER,
                "color": COLOR_TEXT,
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
                "borderwidth": 1,
            },
        }
    }


def apply_professional_theme(fig):
    template = get_plotly_template()
    fig.update_layout(template["layout"])
    fig.update_xaxes(
        showgrid=False, showline=True, linewidth=1,
        linecolor=COLOR_BORDER, color=COLOR_TEXT,
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=0.5, gridcolor="#F0F0F0",
        showline=True, linewidth=1, linecolor=COLOR_BORDER, color=COLOR_TEXT,
    )
    return fig


####################################################################
# CARD STYLING UTILITIES
####################################################################
def card_css() -> str:
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
    delta_html = (
        f"<span style='color: {COLOR_SUCCESS}; font-size: 12px;'>{delta}</span>"
        if delta
        else ""
    )
    return f"""
    <div style='
        background: {COLOR_WHITE};
        border-radius: 10px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    '>
        <div style='font-size: 24px; font-weight: 700; color: {COLOR_DARK_GRAY};
            margin-bottom: 6px;'>{value}</div>
        <div style='font-size: 12px; color: {COLOR_LIGHT_GRAY};
            margin-bottom: 8px;'>{label}</div>
        {delta_html}
    </div>
    """


def kpi_card_with_sparkline(
    value: str,
    label: str,
    sparkline_values: list,
    color: str = COLOR_ORANGE,
) -> str:
    """KPI card with an inline SVG sparkline showing trend of last N values."""
    if not sparkline_values or len(sparkline_values) < 2:
        return kpi_card_html(value, label)

    vals = [float(v) for v in sparkline_values]
    mn, mx = min(vals), max(vals)
    rng = mx - mn if mx != mn else 1
    w, h = 100, 28
    points = []
    for i, v in enumerate(vals):
        x = i * w / (len(vals) - 1)
        y = h - ((v - mn) / rng) * h
        points.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(points)

    trend = vals[-1] - vals[0]
    trend_arrow = "&#9650;" if trend > 0 else ("&#9660;" if trend < 0 else "&#8212;")
    trend_color = COLOR_ERROR if trend > 0 else (COLOR_SUCCESS if trend < 0 else COLOR_TEXT)

    return f"""
    <div style='
        background: {COLOR_WHITE};
        border-radius: 10px;
        padding: 16px 16px 10px 16px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    '>
        <div style='font-size: 24px; font-weight: 700; color: {COLOR_DARK_GRAY};
            margin-bottom: 2px;'>{value}</div>
        <div style='font-size: 11px; color: {COLOR_LIGHT_GRAY};
            margin-bottom: 6px;'>{label}</div>
        <svg width='{w}' height='{h}' style='display:block;margin:0 auto;'>
            <polyline points='{polyline}' fill='none' stroke='{color}'
                stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/>
        </svg>
        <div style='font-size: 11px; color: {trend_color}; margin-top: 4px;'>
            {trend_arrow} ultimas {len(vals)} semanas
        </div>
    </div>
    """


####################################################################
# TYPOGRAPHY HELPERS
####################################################################
def section_title(text: str) -> str:
    """
    Estilo de título de seção (H2).
    Renderiza com espaçamento vertical e formatação markdown.
    
    Uso:
        st.markdown(section_title("Minha Seção"))
    """
    return f"""

## {text}

"""


def chart_title(text: str) -> str:
    """
    Estilo de título de gráfico (H3).
    Renderiza com espaçamento vertical e formatação markdown.
    
    Uso:
        st.markdown(chart_title("Meu Gráfico"))
    """
    return f"""

### {text}

"""
