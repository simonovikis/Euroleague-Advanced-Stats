"""
lineup_optimizer.py -- Lineup Optimizer & Expected Net Rating Simulator
========================================================================
Interactive page where the user selects a team and 5 players, then
a pre-trained ML model predicts the Expected Net Rating. Includes a
radar chart for lineup balance and an "AI Suggest 5th Player" mode.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t,
    CFG,
    TEAM_COLORS,
    DEFAULT_ACCENT,
    _cfg_default,
    _cfg_seasons,
    is_feature_enabled,
    show_disabled_message,
    render_page_header,
    skeleton_kpi_row,
    skeleton_chart,
    skeleton_dataframe,
)
from streamlit_app.queries import fetch_season_schedule
from streamlit_app.utils.config_loader import get_supported_seasons


# ========================================================================
# Cached loaders
# ========================================================================
@st.cache_resource(show_spinner=False)
def _load_lineup_model():
    """Load the pre-trained lineup model from disk (once per process)."""
    from data_pipeline.ml_train import load_model
    return load_model()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_player_features(season: int):
    """Compute and cache player feature vectors for a season."""
    from data_pipeline.ml_train import _compute_player_season_features
    return _compute_player_season_features(season)


def _train_model_on_the_fly(training_seasons):
    """Train the model live if no serialized model exists."""
    from data_pipeline.ml_train import train_lineup_model
    return train_lineup_model(list(training_seasons))


# ========================================================================
# View render
# ========================================================================
def render():
    if not is_feature_enabled("ENABLE_ML_PREDICTIONS"):
        show_disabled_message("ENABLE_ML_PREDICTIONS")
        st.stop()

    render_page_header(
        t("lineup_title", default="Lineup Optimizer"),
        t("lineup_subtitle", default="ML-powered Expected Net Rating simulator for any 5-man combination"),
        icon="🧪",
    )

    current_season = st.session_state.get("selected_season", _cfg_default)
    schedule = fetch_season_schedule(current_season)
    if schedule.empty:
        st.warning("No schedule data available for this season.")
        st.stop()

    team_codes = sorted(
        set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())
    )
    team_name_map = {
        code: vals.get("name", code)
        for code, vals in CFG.get("ui", {}).get("team_colors", {}).items()
    }

    def _fmt_team(code):
        return f"{team_name_map.get(code, code)} ({code})"

    # ---- Team selection ----
    selected_team = st.selectbox(
        t("lineup_select_team", default="Select Team"),
        team_codes,
        format_func=_fmt_team,
        key="lineup_team",
    )

    _tc_primary = TEAM_COLORS.get(selected_team, DEFAULT_ACCENT)[0]
    _tc_secondary = TEAM_COLORS.get(selected_team, DEFAULT_ACCENT)[1]

    # ---- Load player features ----
    pf_placeholder = st.empty()
    with pf_placeholder.container():
        skeleton_kpi_row(columns=3)

    player_features = _load_player_features(current_season)
    pf_placeholder.empty()

    if player_features.empty:
        st.error("Could not compute player features for this season. The API may be unavailable.")
        st.stop()

    team_players = player_features[player_features["team_code"] == selected_team].copy()
    if team_players.empty:
        st.warning(f"No player data found for {selected_team} in season {current_season}.")
        st.stop()

    team_players = team_players.sort_values("minutes", ascending=False)
    roster = dict(zip(team_players["player_id"], team_players["player_name"]))

    # ---- Load or train model ----
    model = _load_lineup_model()
    if model is None:
        all_seasons = sorted(_cfg_seasons, reverse=True)
        training_seasons = tuple(s for s in all_seasons if s <= current_season)[:3]
        if not training_seasons:
            training_seasons = (current_season,)

        with st.status("Training lineup model on historical data...", expanded=True) as status:
            st.write(f"Seasons: {', '.join(str(s) for s in training_seasons)}")
            st.write("This may take a few minutes on first run...")
            model = _train_model_on_the_fly(training_seasons)
            if model is not None:
                from data_pipeline.ml_train import save_model
                save_model(model)
                status.update(label="Model trained and cached.", state="complete", expanded=False)
            else:
                status.update(label="Training failed", state="error")
                st.error("Insufficient historical data to train the lineup model.")
                st.stop()

    # ---- Player selection ----
    st.markdown("---")
    st.markdown(f"### {t('lineup_select_players', default='Select Players')}")

    selected_players = st.multiselect(
        t("lineup_pick_5", default="Pick exactly 5 players from the roster"),
        options=list(roster.keys()),
        format_func=lambda pid: roster.get(pid, pid),
        max_selections=5,
        key="lineup_player_select",
    )

    n_selected = len(selected_players)

    # ---- AI Suggest 5th Player ----
    if n_selected == 4:
        st.info(t("lineup_suggest_info", default="You selected 4 players. The AI will suggest the best 5th player."))

        suggest_placeholder = st.empty()
        with suggest_placeholder.container():
            skeleton_dataframe(rows=5, cols=3)

        from data_pipeline.ml_train import find_best_5th_player
        suggestions = find_best_5th_player(
            model, player_features, selected_players, list(roster.keys()),
        )
        suggest_placeholder.empty()

        if suggestions:
            st.markdown(f"#### {t('lineup_ai_suggestions', default='AI Suggestions for 5th Player')}")
            with st.container(border=True):
                sug_df = pd.DataFrame(suggestions[:10])
                sug_df = sug_df.rename(columns={
                    "player_name": "Player",
                    "predicted_net_rtg": "Expected Net Rtg",
                })
                st.dataframe(
                    sug_df[["Player", "Expected Net Rtg"]],
                    hide_index=True,
                    column_config={
                        "Expected Net Rtg": st.column_config.NumberColumn(format="%.1f"),
                    },
                )

                if suggestions:
                    best = suggestions[0]
                    st.success(
                        f"Best fit: **{best['player_name']}** "
                        f"(Expected Net Rtg: **{best['predicted_net_rtg']:+.1f}**)"
                    )
        else:
            st.warning("Could not generate suggestions. Not enough roster data.")

    elif n_selected < 4:
        remaining = 5 - n_selected
        st.caption(
            t("lineup_pick_more", default="Select {n} more player(s) to run the simulator.").format(n=remaining)
        )
        st.stop()

    # ---- Prediction (exactly 5 selected) ----
    if n_selected == 5:
        st.markdown("---")

        pred_placeholder = st.empty()
        with pred_placeholder.container():
            skeleton_kpi_row(columns=3)
            skeleton_chart(height=400)

        from data_pipeline.ml_train import (
            predict_lineup_net_rating,
            compute_lineup_radar_scores,
        )

        predicted_net_rtg = predict_lineup_net_rating(model, player_features, selected_players)
        radar_scores = compute_lineup_radar_scores(player_features, selected_players)

        pred_placeholder.empty()

        if predicted_net_rtg is None:
            st.error("Could not compute prediction. One or more players may be missing feature data.")
            st.stop()

        # ---- KPI Cards ----
        player_names = [roster.get(pid, pid) for pid in selected_players]

        with st.container(border=True):
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric(
                t("lineup_exp_net_rtg", default="Expected Net Rating"),
                f"{predicted_net_rtg:+.1f}",
                help="ML-predicted net rating per 100 possessions for this 5-man combination.",
            )

            label = "Elite" if predicted_net_rtg > 8 else "Strong" if predicted_net_rtg > 3 else "Average" if predicted_net_rtg > -3 else "Weak" if predicted_net_rtg > -8 else "Poor"
            color = "#10b981" if predicted_net_rtg > 3 else "#f59e0b" if predicted_net_rtg > -3 else "#ef4444"
            kpi2.metric(
                t("lineup_grade", default="Lineup Grade"),
                label,
            )

            off_score = radar_scores.get("Offense", 50)
            def_score = radar_scores.get("Defense", 50)
            balance = 100 - abs(off_score - def_score)
            kpi3.metric(
                t("lineup_balance", default="Off/Def Balance"),
                f"{balance:.0f}/100",
                help="100 = perfectly balanced offense and defense. Lower = lopsided.",
            )

        # ---- Lineup display ----
        with st.container(border=True):
            st.markdown(f"#### {t('lineup_selected', default='Selected Lineup')}")
            pcols = st.columns(5)
            for i, pid in enumerate(selected_players):
                name = roster.get(pid, pid)
                prow = team_players[team_players["player_id"] == pid]
                mpg = f"{prow['minutes_pg'].iloc[0]:.1f}" if not prow.empty and "minutes_pg" in prow.columns else "?"
                ts = f"{prow['ts_pct'].iloc[0]:.1%}" if not prow.empty and "ts_pct" in prow.columns else "?"
                with pcols[i]:
                    st.markdown(
                        f"<div style='text-align:center; padding:8px; "
                        f"background:linear-gradient(135deg, #1e1e3f, #2a2a5a); "
                        f"border-radius:10px; border:1px solid {_tc_primary}40;'>"
                        f"<div style='font-size:1rem; font-weight:600; color:{_tc_primary};'>{name}</div>"
                        f"<div style='color:#9ca3af; font-size:0.8rem;'>{mpg} MPG | {ts} TS%</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

        # ---- Radar Chart ----
        if radar_scores:
            st.markdown(f"#### {t('lineup_radar', default='Lineup Balance Radar')}")

            categories = list(radar_scores.keys())
            values = [radar_scores[c] for c in categories]
            values_closed = values + [values[0]]
            categories_closed = categories + [categories[0]]

            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=values_closed,
                theta=categories_closed,
                fill="toself",
                fillcolor=f"rgba({int(_tc_primary[1:3], 16)},{int(_tc_primary[3:5], 16)},{int(_tc_primary[5:7], 16)},0.25)",
                line=dict(color=_tc_primary, width=2),
                marker=dict(size=6, color=_tc_primary),
                name=selected_team,
            ))

            fig_radar.add_trace(go.Scatterpolar(
                r=[50] * (len(categories) + 1),
                theta=categories_closed,
                line=dict(color="#4b5563", width=1, dash="dash"),
                name="League Avg",
                fill=None,
            ))

            fig_radar.update_layout(
                polar=dict(
                    bgcolor="rgba(15,15,35,0.8)",
                    radialaxis=dict(
                        visible=True, range=[0, 100],
                        gridcolor="rgba(255,255,255,0.08)",
                        tickfont=dict(size=9, color="#6b7280"),
                    ),
                    angularaxis=dict(
                        gridcolor="rgba(255,255,255,0.08)",
                        tickfont=dict(size=12, color="#e4e4f0"),
                    ),
                ),
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e4e4f0"),
                height=450,
                showlegend=True,
                legend=dict(
                    orientation="h", yanchor="bottom", y=-0.15,
                    xanchor="center", x=0.5,
                ),
                margin=dict(t=40, b=60),
            )
            st.plotly_chart(fig_radar)

        # ---- Detailed Player Stats Table ----
        st.markdown(f"#### {t('lineup_player_breakdown', default='Player Feature Breakdown')}")
        with st.container(border=True):
            display_cols = ["player_name", "minutes_pg", "ts_pct", "true_usg_pct", "stop_rate", "off_rating", "assist_ratio", "three_pt_rate"]
            available_cols = [c for c in display_cols if c in team_players.columns]
            breakdown_df = team_players[team_players["player_id"].isin(selected_players)][available_cols].copy()

            rename_map = {
                "player_name": "Player",
                "minutes_pg": "MPG",
                "ts_pct": "TS%",
                "true_usg_pct": "tUSG%",
                "stop_rate": "Stop Rate",
                "off_rating": "ORtg",
                "assist_ratio": "AST Ratio",
                "three_pt_rate": "3PA Rate",
            }
            breakdown_df = breakdown_df.rename(columns=rename_map)

            pct_cols = {"TS%", "tUSG%", "Stop Rate", "AST Ratio", "3PA Rate"}
            col_config = {}
            for col in breakdown_df.columns:
                if col in pct_cols:
                    col_config[col] = st.column_config.NumberColumn(format="%.3f")
                elif col in ("MPG", "ORtg"):
                    col_config[col] = st.column_config.NumberColumn(format="%.1f")

            st.dataframe(breakdown_df, hide_index=True, column_config=col_config)

        st.caption(
            t("lineup_disclaimer", default=(
                "Predictions are based on a GradientBoosting regression model trained on "
                "historical lineup net ratings. Individual player features (TS%, tUSG%, Stop Rate, etc.) "
                "are aggregated to represent the 5-man unit. Results are estimates, not guarantees."
            ))
        )
