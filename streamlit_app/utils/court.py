"""
court.py -- Euroleague Half-Court Drawing Helper (Plotly)

Coordinate system (matches Euroleague API shot data):
  100 units ~ 1 metre.  X: -750..750,  Y: 0..1400.
  Basket centre at approximately (0, 72).
"""

import numpy as np
import plotly.graph_objects as go


def draw_euroleague_court(fig=None):
    """Draw standard FIBA/Euroleague half-court lines on a Plotly figure."""
    if fig is None:
        fig = go.Figure()

    LINE = "rgba(255,255,255,0.35)"
    FAINT = "rgba(255,255,255,0.15)"
    W = 1.5
    BY = 72  # basket-centre Y

    # Outer court boundary
    fig.add_shape(
        type="rect", x0=-750, y0=0, x1=750, y1=1400,
        line=dict(color=LINE, width=W),
        fillcolor="rgba(15,15,35,0.5)",
    )

    # Paint / key  (4.9 m wide x 5.8 m deep)
    fig.add_shape(
        type="rect", x0=-245, y0=0, x1=245, y1=580,
        line=dict(color=LINE, width=W),
    )

    # Free-throw circle  (r = 1.8 m = 180 units)
    fig.add_shape(
        type="circle", x0=-180, y0=400, x1=180, y1=760,
        line=dict(color=FAINT, width=1),
    )

    # Restricted-area arc  (r = 1.25 m = 125 units)
    theta_ra = np.linspace(0, np.pi, 60)
    fig.add_trace(go.Scatter(
        x=(125 * np.cos(theta_ra)).tolist(),
        y=(125 * np.sin(theta_ra) + BY).tolist(),
        mode="lines",
        line=dict(color=FAINT, width=1, dash="dot"),
        showlegend=False, hoverinfo="skip",
    ))

    # Three-point arc  (r = 6.75 m = 675 units)
    theta_3p = np.linspace(0, np.pi, 120)
    arc_x = np.clip(675 * np.cos(theta_3p), -660, 660)
    arc_y = 675 * np.sin(theta_3p) + BY
    fig.add_trace(go.Scatter(
        x=arc_x.tolist(), y=arc_y.tolist(),
        mode="lines",
        line=dict(color=LINE, width=2),
        showlegend=False, hoverinfo="skip",
    ))

    # Three-point corner straight lines
    corner_y = BY + float(np.sqrt(max(675**2 - 660**2, 0)))
    for x_pos in (-660, 660):
        fig.add_shape(
            type="line", x0=x_pos, y0=0, x1=x_pos, y1=corner_y,
            line=dict(color=LINE, width=2),
        )

    # Backboard  (1.8 m wide = 90 units each side)
    fig.add_shape(
        type="line", x0=-90, y0=43, x1=90, y1=43,
        line=dict(color="rgba(255,255,255,0.3)", width=2),
    )

    # Basket / hoop
    fig.add_shape(
        type="circle", x0=-22, y0=50, x1=22, y1=94,
        line=dict(color="#ff6b35", width=2),
        fillcolor="rgba(255,107,53,0.2)",
    )

    return fig
