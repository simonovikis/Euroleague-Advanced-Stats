import time as _time
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default,
    is_feature_enabled, show_disabled_message,
)


def render():
    if not is_feature_enabled("ENABLE_LIVE_MATCH"):
        show_disabled_message("ENABLE_LIVE_MATCH")
        st.stop()

    st.markdown(f'<p class="section-header">{t("card_live_title")}</p>', unsafe_allow_html=True)
    st.caption(f"🔄 {t('live_autorefresh')}")

    season_live = st.session_state.get("selected_season", _cfg_default)

    # --- Auto-refresh: rerun every 30 seconds ---
    if "live_last_refresh" not in st.session_state:
        st.session_state["live_last_refresh"] = _time.time()

    elapsed_since_refresh = _time.time() - st.session_state["live_last_refresh"]

    # Placeholder for countdown
    refresh_placeholder = st.empty()
    secs_until = max(0, int(30 - elapsed_since_refresh))
    refresh_placeholder.markdown(
        f"<p style='color:#6b7280; font-size:0.8rem;'>Next refresh in ~{secs_until}s</p>",
        unsafe_allow_html=True,
    )

    if elapsed_since_refresh >= 30:
        st.session_state["live_last_refresh"] = _time.time()
        if "live_cache" in st.session_state:
            del st.session_state["live_cache"]
        st.rerun()

    # --- Detect live games ---
    from streamlit_app.queries import fetch_live_games, fetch_live_game_data_fresh

    try:
        with st.spinner(t("live_checking")):
            if "live_cache" not in st.session_state:
                live_games = fetch_live_games(season_live)
                st.session_state["live_cache"] = live_games
            else:
                live_games = st.session_state["live_cache"]
    except Exception as e:
        st.error(f"Could not check for live games. The API may be temporarily unavailable. Error: {type(e).__name__}")
        live_games = []

    if not live_games:
        st.info(t("live_no_games"))
        st.markdown("---")
        st.markdown(
            "<p style='color:#9ca3af;'>If games are scheduled for today but haven't tipped off yet, "
            "they will appear here once play-by-play data becomes available.</p>",
            unsafe_allow_html=True,
        )
    else:
        # --- Game selector ---
        game_labels = [
            f"{g['home_code']} vs {g['away_code']} ({g['home_score']}-{g['away_score']})"
            for g in live_games
        ]
        selected_idx = st.selectbox(
            t("live_select_game"),
            range(len(game_labels)),
            format_func=lambda i: game_labels[i],
            key="live_game_select",
        )

        sel_game = live_games[selected_idx]
        sel_gc = sel_game["gamecode"]

        # --- Fetch fresh data ---
        try:
            with st.spinner(t("live_fetching_data")):
                live_data = fetch_live_game_data_fresh(season_live, sel_gc)
        except Exception as e:
            st.error(f"Could not fetch live game data. Error: {type(e).__name__}")
            st.stop()

        pbp_df = live_data.get("pbp", pd.DataFrame())
        boxscore_df = live_data.get("boxscore", pd.DataFrame())

        # --- Live Score Header ---
        from data_pipeline.live_extractor import get_live_score_and_time

        score_info = get_live_score_and_time(pbp_df)
        home_score = score_info["home_score"]
        away_score = score_info["away_score"]
        period = score_info["period"]
        time_rem = score_info["time_remaining"]

        period_label = (
            t("live_overtime", num=period - 4) if period > 4
            else t("live_period", period=period)
        )

        _live_home_clr = TEAM_COLORS.get(sel_game.get("home_code"), DEFAULT_ACCENT)[0]
        _live_away_clr = TEAM_COLORS.get(sel_game.get("away_code"), DEFAULT_ACCENT)[0]
        st.markdown(
            f'<div class="game-header">'
            f'  <div class="team-block">'
            f'    <span class="team-name" style="color:{_live_home_clr};">{sel_game["home_name"]}</span>'
            f'    <span style="color:#9ca3af; font-size:0.85rem;">{sel_game["home_code"]}</span>'
            f"  </div>"
            f'  <div style="text-align:center;">'
            f'    <span class="score">{home_score}<span class="dash"> — </span>{away_score}</span>'
            f'    <p style="color:#f59e0b; font-weight:600; margin:0;">{period_label} | {time_rem}</p>'
            f"  </div>"
            f'  <div class="team-block">'
            f'    <span class="team-name" style="color:{_live_away_clr};">{sel_game["away_name"]}</span>'
            f'    <span style="color:#9ca3af; font-size:0.85rem;">{sel_game["away_code"]}</span>'
            f"  </div>"
            f"</div>",
            unsafe_allow_html=True,
        )

        # --- Tabs ---
        tab_overview, tab_momentum, tab_wp = st.tabs([
            t("live_tab_overview"),
            t("live_tab_momentum"),
            t("live_tab_winprob"),
        ])

        # ------------------------------------------------------------------
        # TAB: Live Overview (Current Lineups + Active Run)
        # ------------------------------------------------------------------
        with tab_overview:
            st.markdown(f"### {t('live_current_lineups')}")

            from data_pipeline.live_metrics import get_current_lineups, detect_active_run

            lineups = get_current_lineups(pbp_df, boxscore_df)

            col_home, col_away = st.columns(2)

            for col, side, color in [
                (col_home, "home", _live_home_clr),
                (col_away, "away", _live_away_clr),
            ]:
                with col:
                    lu = lineups.get(side)
                    if lu is None:
                        st.info(f"No lineup data for {side} team.")
                        continue

                    st.markdown(
                        f"<h4 style='color:{color};'>{lu['team']} — {t('live_on_court')}</h4>",
                        unsafe_allow_html=True,
                    )

                    for player in lu["players"]:
                        st.markdown(f"- {player}")

                    net_color = "#10b981" if lu["net_rtg"] >= 0 else "#ef4444"
                    st.markdown(
                        f"<div style='margin-top:8px; padding:12px; "
                        f"background:linear-gradient(135deg,#1e1e3f,#2a2a5a); "
                        f"border-radius:8px; text-align:center;'>"
                        f"<span style='color:#9ca3af; font-size:0.85rem;'>{t('live_net_rtg')}</span><br>"
                        f"<span style='color:{net_color}; font-size:1.8rem; font-weight:700;'>"
                        f"{lu['net_rtg']:+.1f}</span><br>"
                        f"<span style='color:#6b7280; font-size:0.75rem;'>"
                        f"ORtg {lu['ortg']:.1f} | DRtg {lu['drtg']:.1f} | {lu['events']} poss</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Active scoring run
            st.markdown("---")
            st.markdown(f"### {t('live_momentum')}")

            run = detect_active_run(pbp_df)
            if run and run["run_points"] >= 4:
                run_color = "#f59e0b"
                st.markdown(
                    f"<div style='padding:16px; background:linear-gradient(135deg,#422006,#78350f); "
                    f"border:1px solid #f59e0b; border-radius:12px; text-align:center;'>"
                    f"<span style='font-size:1.4rem; font-weight:700; color:#fbbf24;'>🔥 "
                    f"{t('live_scoring_run', team=run['run_team'], pts=run['run_points'], dur=run['duration_str'])}"
                    f"</span></div>",
                    unsafe_allow_html=True,
                )
                with st.expander("Scoring plays in this run"):
                    for play in run["scoring_plays"]:
                        st.markdown(f"- {play}")
            else:
                st.markdown(
                    f"<p style='color:#6b7280;'>{t('live_no_run')}</p>",
                    unsafe_allow_html=True,
                )

            # Quick boxscore summary
            st.markdown("---")
            st.markdown(f"### {t('hdr_player_stats')}")
            if not boxscore_df.empty:
                from data_pipeline.transformers import compute_advanced_stats
                live_adv = compute_advanced_stats(boxscore_df)
                active_players = live_adv[live_adv["minutes"] > 0].copy()

                if not active_players.empty:
                    display_cols = [c for c in [
                        "player_name", "team_code", "minutes", "points",
                        "ts_pct", "off_rating", "def_rating", "plus_minus",
                    ] if c in active_players.columns]

                    st.dataframe(
                        active_players[display_cols]
                        .sort_values("points", ascending=False)
                        .round(3)
                        .rename(columns={
                            "player_name": t("col_player"), "team_code": t("col_team"),
                            "minutes": t("col_min"), "points": t("col_pts"),
                            "ts_pct": t("col_ts"), "off_rating": t("col_ortg"),
                            "def_rating": t("col_drtg"), "plus_minus": "+/-",
                        }),
                        use_container_width=True, hide_index=True, height=400,
                    )

        # ------------------------------------------------------------------
        # TAB: Momentum
        # ------------------------------------------------------------------
        with tab_momentum:
            st.markdown(f"### {t('live_score_diff_timeline')}")

            from data_pipeline.live_metrics import get_momentum_timeline

            timeline = get_momentum_timeline(pbp_df)
            if timeline.empty:
                st.info("No scoring events yet.")
            else:
                fig_mom = go.Figure()

                fig_mom.add_trace(go.Scatter(
                    x=list(range(len(timeline))),
                    y=timeline["score_diff"],
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(
                        size=6,
                        color=timeline["score_diff"].apply(
                            lambda d: "#6366f1" if d > 0 else "#ef4444" if d < 0 else "#6b7280"
                        ),
                    ),
                    fill="tozeroy",
                    fillcolor="rgba(99,102,241,0.1)",
                    hovertemplate=(
                        "Play #%{x}<br>"
                        "Score: %{customdata[0]}-%{customdata[1]}<br>"
                        "Diff: %{y:+d}<br>"
                        "%{customdata[2]} - %{customdata[3]}"
                        "<extra></extra>"
                    ),
                    customdata=list(zip(
                        timeline["home_score"], timeline["away_score"],
                        timeline["player"], timeline["play_type"],
                    )),
                ))

                fig_mom.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")

                home_code = sel_game["home_code"]
                away_code = sel_game["away_code"]
                fig_mom.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)",
                    height=400,
                    xaxis_title="Scoring Events",
                    yaxis_title=f"Score Diff ({home_code} perspective)",
                    font=dict(family="Inter"),
                    showlegend=False,
                )

                # Annotate quadrants
                max_diff = max(abs(timeline["score_diff"].max()), abs(timeline["score_diff"].min()), 1)
                fig_mom.add_annotation(
                    x=len(timeline) * 0.05, y=max_diff * 0.8,
                    text=f"{home_code} leading", showarrow=False,
                    font=dict(color="#6366f1", size=11), opacity=0.5,
                )
                fig_mom.add_annotation(
                    x=len(timeline) * 0.05, y=-max_diff * 0.8,
                    text=f"{away_code} leading", showarrow=False,
                    font=dict(color="#ef4444", size=11), opacity=0.5,
                )

                st.plotly_chart(fig_mom, use_container_width=True)

                # Run history table
                st.markdown("#### Scoring Run History")
                from data_pipeline.transformers import track_lineups as _tl
                pbp_lu = _tl(pbp_df, boxscore_df) if not pbp_df.empty and not boxscore_df.empty else pd.DataFrame()
                if not pbp_lu.empty:
                    from data_pipeline.transformers import detect_runs_and_stoppers
                    runs = detect_runs_and_stoppers(pbp_lu)
                    if not runs.empty:
                        st.dataframe(
                            runs[["run_points", "stopper_player", "stopper_team",
                                  "stopper_playtype", "period", "markertime"]].rename(
                                columns={
                                    "run_points": "Run (pts)", "stopper_player": "Broken By",
                                    "stopper_team": "Team", "stopper_playtype": "Play",
                                    "period": "Q", "markertime": "Time",
                                }
                            ),
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.info("No 8+ point runs detected yet.")

        # ------------------------------------------------------------------
        # TAB: Win Probability
        # ------------------------------------------------------------------
        with tab_wp:
            st.markdown(f"### {t('live_win_prob')}")

            if not is_feature_enabled("ENABLE_ML_PREDICTIONS"):
                show_disabled_message("ENABLE_ML_PREDICTIONS")
                st.stop()

            from data_pipeline.live_metrics import (
                compute_live_win_probability,
                compute_win_probability_timeline,
            )

            # Current win probability gauge
            home_wp = compute_live_win_probability(
                home_score, away_score,
                score_info["total_seconds_remaining"],
            )
            away_wp = 1.0 - home_wp

            col_wp1, col_wp2 = st.columns(2)
            with col_wp1:
                fig_gauge_h = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=home_wp * 100,
                    title={"text": f"{sel_game['home_code']} Win %",
                           "font": {"color": "#e4e4f0", "size": 16}},
                    number={"suffix": "%", "font": {"color": "#6366f1", "size": 32}},
                    gauge=dict(
                        axis=dict(range=[0, 100], tickcolor="#6b7280"),
                        bar=dict(color="#6366f1"),
                        bgcolor="rgba(30,30,63,0.8)",
                        steps=[
                            dict(range=[0, 50], color="rgba(239,68,68,0.15)"),
                            dict(range=[50, 100], color="rgba(99,102,241,0.15)"),
                        ],
                        threshold=dict(line=dict(color="#f59e0b", width=3), value=50, thickness=0.8),
                    ),
                ))
                fig_gauge_h.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e4f0"),
                    height=250, margin=dict(t=60, b=20, l=30, r=30),
                )
                st.plotly_chart(fig_gauge_h, use_container_width=True)

            with col_wp2:
                fig_gauge_a = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=away_wp * 100,
                    title={"text": f"{sel_game['away_code']} Win %",
                           "font": {"color": "#e4e4f0", "size": 16}},
                    number={"suffix": "%", "font": {"color": "#8b5cf6", "size": 32}},
                    gauge=dict(
                        axis=dict(range=[0, 100], tickcolor="#6b7280"),
                        bar=dict(color="#8b5cf6"),
                        bgcolor="rgba(30,30,63,0.8)",
                        steps=[
                            dict(range=[0, 50], color="rgba(239,68,68,0.15)"),
                            dict(range=[50, 100], color="rgba(139,92,246,0.15)"),
                        ],
                        threshold=dict(line=dict(color="#f59e0b", width=3), value=50, thickness=0.8),
                    ),
                ))
                fig_gauge_a.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e4f0"),
                    height=250, margin=dict(t=60, b=20, l=30, r=30),
                )
                st.plotly_chart(fig_gauge_a, use_container_width=True)

            # Win probability timeline
            st.markdown(f"### {t('live_wp_timeline')}")
            wp_timeline = compute_win_probability_timeline(pbp_df)

            if wp_timeline.empty:
                st.info("No scoring events yet for win probability timeline.")
            else:
                fig_wp = go.Figure()

                fig_wp.add_trace(go.Scatter(
                    x=list(range(len(wp_timeline))),
                    y=wp_timeline["home_wp"] * 100,
                    mode="lines",
                    name=sel_game["home_code"],
                    line=dict(color="#6366f1", width=2.5),
                    fill="tozeroy",
                    fillcolor="rgba(99,102,241,0.08)",
                    hovertemplate=(
                        "%{customdata[0]}-%{customdata[1]}<br>"
                        "Home WP: %{y:.1f}%<extra></extra>"
                    ),
                    customdata=list(zip(
                        wp_timeline["home_score"], wp_timeline["away_score"],
                    )),
                ))

                fig_wp.add_hline(y=50, line_dash="dash", line_color="rgba(245,158,11,0.5)",
                                 annotation_text="50%", annotation_font_color="#f59e0b")

                fig_wp.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)",
                    height=400,
                    yaxis=dict(title="Home Win Probability (%)", range=[0, 100]),
                    xaxis_title="Scoring Events",
                    font=dict(family="Inter"),
                    showlegend=True,
                    legend=dict(x=0.02, y=0.98),
                )
                st.plotly_chart(fig_wp, use_container_width=True)

    # Schedule auto-rerun using a short sleep to avoid blocking indefinitely.
    # Streamlit will rerun when the sleep ends; the elapsed check at the top
    # of this page handles the 30-second gate.
    if "live_last_refresh" in st.session_state:
        remaining = max(0, 30 - (_time.time() - st.session_state["live_last_refresh"]))
        if remaining > 0 and remaining <= 30:
            _time.sleep(remaining)
            st.session_state["live_last_refresh"] = _time.time()
            if "live_cache" in st.session_state:
                del st.session_state["live_cache"]
            st.rerun()
