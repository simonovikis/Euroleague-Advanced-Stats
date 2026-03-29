"""
team_dna.py -- Team DNA & Stylistic Clustering Dashboard
=========================================================
Visualizes team stylistic identity using unsupervised ML clustering
of the Four Factors (eFG%, TOV%, ORB%, FT Rate) + Pace + 3PA Rate.

Visual 1 — PCA 2D scatter showing all teams colored by cluster.
Visual 2 — Radar chart of percentile ranks for a selected team.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, TEAM_COLORS, DEFAULT_ACCENT, TEAM_NAME_MAP, _cfg_default,
    render_page_header, skeleton_chart,
    is_feature_enabled, show_disabled_message,
    favorite_team_index, format_team_option,
)

CLUSTER_COLORS = {
    "Pace & Space": "#6366f1",
    "Defensive Grinders": "#ef4444",
    "Rebound Dominant": "#f59e0b",
    "Balanced / Elite": "#10b981",
}

FEATURE_LABELS = {
    "efg_pct": "eFG%",
    "tov_pct": "TOV% (inv.)",
    "orb_pct": "ORB%",
    "ft_rate": "FT Rate",
    "pace": "Pace",
    "three_pt_rate": "3PA Rate",
}


@st.cache_data(ttl=3600, show_spinner=False)
def _run_clustering_with_pca(season: int) -> tuple:
    from data_pipeline.team_dna import (
        extract_team_four_factors, cluster_teams, compute_percentile_ranks,
    )

    raw = extract_team_four_factors(season)
    if raw.empty:
        return pd.DataFrame(), None, None

    result, _kmeans, _scaler, _pca = cluster_teams(raw)
    result = compute_percentile_ranks(result)
    return result, _pca.explained_variance_ratio_, _pca.components_


def render():
    if not is_feature_enabled("ENABLE_ML_PREDICTIONS"):
        show_disabled_message("ENABLE_ML_PREDICTIONS")
        st.stop()

    render_page_header(
        t("hdr_team_dna", default="Team DNA"),
        t("sub_team_dna",
          default="Unsupervised clustering of Euroleague teams by the Four Factors, Pace, and 3-Point Attempt Rate."),
        icon="\U0001f9ec",
    )

    season = st.session_state.get("selected_season", _cfg_default)

    scatter_placeholder = st.empty()
    with scatter_placeholder.container():
        skeleton_chart(height=500)

    try:
        with st.status(
            "Computing team stylistic profiles...", expanded=True,
        ) as status:
            st.write("Extracting Four Factors for all teams...")
            result_df, ev, comps = _run_clustering_with_pca(season)
            status.update(label="Clustering complete.", state="complete", expanded=False)
    except Exception as e:
        scatter_placeholder.error(
            f"Could not compute team DNA. Error: {type(e).__name__}"
        )
        return

    if result_df.empty:
        scatter_placeholder.warning("No team data available for this season.")
        return

    result_df["team_name"] = result_df["team_code"].map(
        lambda c: TEAM_NAME_MAP.get(c, c)
    )

    # ================================================================
    # Visual 1 — PCA Scatter (The Landscape)
    # ================================================================
    with scatter_placeholder.container():
        st.markdown("### Stylistic Landscape")
        st.caption(
            "Teams projected onto 2 principal components derived from the "
            "Four Factors, Pace, and 3PA Rate. Color indicates cluster."
        )

        color_map = {
            name: CLUSTER_COLORS.get(name, "#8b5cf6")
            for name in result_df["cluster_name"].unique()
        }

        def _get_pc_label(pc_idx, ev_ratio):
            if ev is None or comps is None:
                return f"Principal Component {pc_idx+1}"
            comp = comps[pc_idx]
            max_idx = abs(comp).argmax()
            from data_pipeline.team_dna import FEATURE_COLS
            feat = FEATURE_COLS[max_idx]
            feat_name = FEATURE_LABELS.get(feat, feat)
            direction = "High" if comp[max_idx] > 0 else "Low"
            return f"PC{pc_idx+1}: {direction} {feat_name} ({ev_ratio*100:.1f}% var.)"

        pc1_label = _get_pc_label(0, ev[0]) if ev is not None else "Principal Component 1"
        pc2_label = _get_pc_label(1, ev[1]) if ev is not None else "Principal Component 2"

        fig = px.scatter(
            result_df,
            x="pc1",
            y="pc2",
            color="cluster_name",
            color_discrete_map=color_map,
            hover_name="team_name",
            hover_data={
                "pc1": False,
                "pc2": False,
                "cluster_name": True,
                "efg_pct": ":.3f",
                "tov_pct": ":.3f",
                "orb_pct": ":.3f",
                "pace": ":.1f",
                "three_pt_rate": ":.3f",
            },
            labels={
                "pc1": pc1_label,
                "pc2": pc2_label,
                "cluster_name": "Cluster",
                "efg_pct": "eFG%",
                "tov_pct": "TOV%",
                "orb_pct": "ORB%",
                "pace": "Pace",
                "three_pt_rate": "3PA Rate",
            },
        )
        fig.update_traces(
            marker=dict(size=14, opacity=0.85, line=dict(width=1, color="#ffffff")),
        )
        for _, row in result_df.iterrows():
            fig.add_annotation(
                x=row["pc1"],
                y=row["pc2"],
                text=row["team_code"],
                showarrow=False,
                yshift=18,
                font=dict(size=10, color="#e4e4f0"),
            )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#e4e4f0",
            height=550,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
                font=dict(size=12),
            ),
        )
        st.plotly_chart(fig, width="stretch")

    # ================================================================
    # Visual 2 — Radar Chart (The DNA Profile)
    # ================================================================
    st.markdown("---")
    st.markdown("### Team DNA Profile")

    teams = sorted(result_df["team_code"].tolist())
    default_idx = favorite_team_index(teams, fallback=0)

    selected_team = st.selectbox(
        "Select a team",
        teams,
        index=default_idx,
        format_func=format_team_option,
        key="dna_team_selector",
    )

    team_row = result_df[result_df["team_code"] == selected_team].iloc[0]
    cluster_name = team_row["cluster_name"]
    team_primary = TEAM_COLORS.get(selected_team, DEFAULT_ACCENT)[0]
    cluster_color = CLUSTER_COLORS.get(cluster_name, team_primary)

    def _hex_to_rgba(hex_color: str, alpha: float = 0.2) -> str:
        # Avoid malformed 8-character hex colors bleeding in (e.g. #10b98133)
        h = hex_color.lstrip("#")[:6]
        # Fallback if somehow not a valid hex string
        if len(h) < 6:
            h = "8b5cf6"
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"

    categories = list(FEATURE_LABELS.values())
    pctl_cols = [f"{c}_pctl" for c in FEATURE_LABELS.keys()]
    values = [team_row[c] for c in pctl_cols]
    values_closed = values + [values[0]]
    categories_closed = categories + [categories[0]]

    fig_radar = go.Figure()
    fig_radar.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=categories_closed,
        fill="toself",
        fillcolor=_hex_to_rgba(cluster_color, 0.2),
        line=dict(color=cluster_color, width=2),
        name=TEAM_NAME_MAP.get(selected_team, selected_team),
    ))
    fig_radar.add_trace(go.Scatterpolar(
        r=[50] * len(categories_closed),
        theta=categories_closed,
        line=dict(color="#6b7280", width=1, dash="dash"),
        name="League Average",
        fill=None,
    ))
    fig_radar.update_layout(
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickvals=[25, 50, 75, 100],
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color="#6b7280", size=9),
            ),
            angularaxis=dict(
                gridcolor="rgba(255,255,255,0.1)",
                tickfont=dict(color="#e4e4f0", size=11),
            ),
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#e4e4f0",
        height=480,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
        ),
    )
    st.plotly_chart(fig_radar, width="stretch")

    # ================================================================
    # Cluster Summary
    # ================================================================
    from data_pipeline.team_dna import get_cluster_description

    team_display = TEAM_NAME_MAP.get(selected_team, selected_team)
    desc = get_cluster_description(cluster_name)
    st.info(f"**{team_display}** belongs to the **'{cluster_name}'** cluster, {desc}")

    # ================================================================
    # Raw Data Expander
    # ================================================================
    with st.expander("Raw Four Factors Data", expanded=False):
        display_cols = (
            ["team_code", "team_name", "cluster_name"]
            + list(FEATURE_LABELS.keys())
        )
        show_df = result_df[display_cols].copy()
        show_df.columns = [
            "Code", "Team", "Cluster",
            "eFG%", "TOV%", "ORB%", "FT Rate", "Pace", "3PA Rate",
        ]
        st.dataframe(
            show_df.sort_values("Cluster"),
            hide_index=True,
            column_config={
                "eFG%": st.column_config.NumberColumn(format="%.3f"),
                "TOV%": st.column_config.NumberColumn(format="%.3f"),
                "ORB%": st.column_config.NumberColumn(format="%.3f"),
                "FT Rate": st.column_config.NumberColumn(format="%.3f"),
                "Pace": st.column_config.NumberColumn(format="%.1f"),
                "3PA Rate": st.column_config.NumberColumn(format="%.3f"),
            },
        )
