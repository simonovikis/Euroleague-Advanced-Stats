import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, CFG, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default, _cfg_seasons,
    is_feature_enabled, show_disabled_message,
)
from streamlit_app.queries import fetch_season_schedule, fetch_prediction_model, predict_game_outcome


def render():
    if not is_feature_enabled("ENABLE_ML_PREDICTIONS"):
        show_disabled_message("ENABLE_ML_PREDICTIONS")
        st.stop()

    st.markdown('<p class="section-header">Oracle: Upcoming Games</p>', unsafe_allow_html=True)
    st.markdown(
        "<p style='color:#9ca3af; font-size:0.9rem;'>"
        "ML-powered win probability predictions based on season net rating, "
        "recent form, rest days, and home court advantage.</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    current_season = st.session_state.get("selected_season", _cfg_default)

    # Collect team codes from the current season schedule
    oracle_schedule = fetch_season_schedule(current_season)
    if oracle_schedule.empty:
        st.warning("No schedule data available for this season.")
        st.stop()

    oracle_teams = sorted(
        list(
            set(oracle_schedule["home_code"].unique())
            | set(oracle_schedule["away_code"].unique())
        )
    )

    # Team name map from config
    team_name_map = {code: vals.get("name", code) for code, vals in CFG.get("ui", {}).get("team_colors", {}).items()}

    def _format_team(code):
        return f"{team_name_map.get(code, code)} ({code})"

    col_home, col_away = st.columns(2)
    with col_home:
        home_team = st.selectbox(
            "Home Team",
            oracle_teams,
            index=0,
            format_func=_format_team,
            key="oracle_home",
        )
    with col_away:
        default_away = 1 if len(oracle_teams) > 1 else 0
        away_team = st.selectbox(
            "Away Team",
            oracle_teams,
            index=default_away,
            format_func=_format_team,
            key="oracle_away",
        )

    if home_team == away_team:
        st.warning("Please select two different teams.")
        st.stop()

    if st.button("Predict Outcome", type="primary", key="oracle_predict"):
        # Determine training seasons (up to 3 prior seasons)
        all_seasons = sorted(_cfg_seasons, reverse=True)
        training_seasons = tuple(
            s for s in all_seasons if s < current_season
        )[:3]

        if not training_seasons:
            training_seasons = (current_season,)

        with st.spinner("Training prediction model on historical data..."):
            model = fetch_prediction_model(training_seasons)

        if model is None:
            st.error("Insufficient historical data to train the model. Try a more recent season.")
            st.stop()

        with st.spinner("Generating prediction..."):
            home_wp = predict_game_outcome(model, home_team, away_team, current_season)

        away_wp = 1.0 - home_wp

        home_name = team_name_map.get(home_team, home_team)
        away_name = team_name_map.get(away_team, away_team)

        home_clr = TEAM_COLORS.get(home_team, DEFAULT_ACCENT)[0]
        away_clr = TEAM_COLORS.get(away_team, DEFAULT_ACCENT)[0]

        # Gauge charts
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            fig_h = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(home_wp * 100, 1),
                title={"text": f"{home_name} Win %", "font": {"color": "#e4e4f0", "size": 16}},
                number={"suffix": "%", "font": {"color": home_clr, "size": 36}},
                gauge=dict(
                    axis=dict(range=[0, 100], tickcolor="#6b7280"),
                    bar=dict(color=home_clr),
                    bgcolor="rgba(30,30,63,0.8)",
                    steps=[
                        dict(range=[0, 50], color="rgba(239,68,68,0.15)"),
                        dict(range=[50, 100], color="rgba(99,102,241,0.15)"),
                    ],
                    threshold=dict(
                        line=dict(color="#f59e0b", width=3), value=50, thickness=0.8,
                    ),
                ),
            ))
            fig_h.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e4f0"),
                height=280, margin=dict(t=60, b=20, l=30, r=30),
            )
            st.plotly_chart(fig_h)

        with col_g2:
            fig_a = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(away_wp * 100, 1),
                title={"text": f"{away_name} Win %", "font": {"color": "#e4e4f0", "size": 16}},
                number={"suffix": "%", "font": {"color": away_clr, "size": 36}},
                gauge=dict(
                    axis=dict(range=[0, 100], tickcolor="#6b7280"),
                    bar=dict(color=away_clr),
                    bgcolor="rgba(30,30,63,0.8)",
                    steps=[
                        dict(range=[0, 50], color="rgba(239,68,68,0.15)"),
                        dict(range=[50, 100], color="rgba(99,102,241,0.15)"),
                    ],
                    threshold=dict(
                        line=dict(color="#f59e0b", width=3), value=50, thickness=0.8,
                    ),
                ),
            ))
            fig_a.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e4f0"),
                height=280, margin=dict(t=60, b=20, l=30, r=30),
            )
            st.plotly_chart(fig_a)

        # Verdict
        winner = home_name if home_wp > 0.5 else away_name
        wp_pct = max(home_wp, away_wp) * 100
        loser = away_name if home_wp > 0.5 else home_name
        venue = "at home" if home_wp > 0.5 else "on the road"
        st.markdown(
            f"<div style='text-align:center; padding:16px; "
            f"background:linear-gradient(135deg, #1e1e3f 0%, #2a2a5a 100%); "
            f"border-radius:12px; border:1px solid rgba(255,255,255,0.08);'>"
            f"<span style='font-size:1.3rem; color:#e4e4f0; font-weight:600;'>"
            f"**{winner}** has a **{wp_pct:.1f}%** probability of winning "
            f"{venue} against {loser}."
            f"</span></div>",
            unsafe_allow_html=True,
        )

        # Feature breakdown table
        st.markdown("---")
        st.markdown("#### Feature Breakdown")

        from data_pipeline.ml_pipeline import (
            _compute_recent_form,
            _compute_rest_days_latest,
            FEATURE_COLS,
        )
        from data_pipeline.extractors import (
            get_league_efficiency_landscape,
        )

        eff = get_league_efficiency_landscape(current_season)
        net_map = dict(zip(eff["team_code"], eff["net_rtg"])) if not eff.empty else {}

        max_round = oracle_schedule["round"].max() if not oracle_schedule.empty else 1

        breakdown = pd.DataFrame({
            "Feature": [
                "Season Net Rating",
                "Recent Form (Last 5 Games)",
                "Rest Days",
            ],
            home_name: [
                f"{net_map.get(home_team, 0.0):+.1f}",
                f"{_compute_recent_form(oracle_schedule, home_team, max_round + 1):+.1f}",
                f"{_compute_rest_days_latest(oracle_schedule, home_team)} days",
            ],
            away_name: [
                f"{net_map.get(away_team, 0.0):+.1f}",
                f"{_compute_recent_form(oracle_schedule, away_team, max_round + 1):+.1f}",
                f"{_compute_rest_days_latest(oracle_schedule, away_team)} days",
            ],
        })
        st.dataframe(breakdown, hide_index=True)

        st.caption(
            f"Model trained on {len(training_seasons)} historical season(s): "
            f"{', '.join(str(s) for s in training_seasons)}. "
            f"Uses LogisticRegression with StandardScaler."
        )
