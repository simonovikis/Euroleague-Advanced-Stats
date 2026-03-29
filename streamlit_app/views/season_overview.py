import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, render_aggrid, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default,
    render_team_sidebar, format_df_decimals, get_decimal_column_config,
    GLOBAL_DECIMALS, render_skeleton_loader, render_page_header,
    skeleton_kpi_row, skeleton_dataframe, skeleton_chart,
)
from streamlit_app.utils.config_loader import get_feature_toggle


def render():
    render_team_sidebar()
    schedule = st.session_state.get("schedule", pd.DataFrame())

    season_to_fetch = st.session_state.get("selected_season", _cfg_default)
    team_code = st.session_state.get("selected_team")
    valid_teams = st.session_state.get("season_team_codes", set())

    # --- Page Header ---
    render_page_header(
        t("hdr_season_overview", default="Season Overview"),
        t("sub_season_overview", default="Team performance metrics and league standings"),
        icon="📊",
    )

    if not team_code:
        st.warning(t("warn_select_team"))
        st.stop()

    _tc_primary = TEAM_COLORS.get(team_code, DEFAULT_ACCENT)[0]

    # --- League Efficiency Landscape ---
    with st.container(border=True):
        st.markdown(f"### {t('hdr_league_eff')}")
        st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_league_eff')}</p>", unsafe_allow_html=True)

        eff_placeholder = st.empty()
        with eff_placeholder.container():
            skeleton_chart(height=500)

        try:
            from streamlit_app.queries import fetch_league_efficiency_landscape, fetch_team_season_data
            eff_df = fetch_league_efficiency_landscape(season_to_fetch)
        except Exception as e:
            eff_placeholder.error(f"Could not load league efficiency data. Error: {type(e).__name__}")
            eff_df = pd.DataFrame()

    if eff_df.empty:
        eff_placeholder.warning(t("err_league_eff"))
    else:
        if valid_teams:
            eff_df = eff_df[eff_df["team_code"].isin(valid_teams)].copy()
        eff_df["color"] = np.where(eff_df["team_code"] == team_code, _tc_primary, "#4b5563")
        eff_df["size"] = np.where(eff_df["team_code"] == team_code, 15, 10)

        fig_eff = px.scatter(
            eff_df, x="ortg", y="drtg", hover_name="team_name",
            color="color", size="size", color_discrete_map="identity",
            labels={"ortg": t("lbl_ortg"), "drtg": t("lbl_drtg")},
        )
        fig_eff.update_yaxes(autorange="reversed")
        mean_ortg = eff_df["ortg"].mean()
        mean_drtg = eff_df["drtg"].mean()
        fig_eff.add_hline(y=mean_drtg, line_dash="dash", line_color="#374151")
        fig_eff.add_vline(x=mean_ortg, line_dash="dash", line_color="#374151")
        fig_eff.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e4e4f0"), showlegend=False, height=500,
        )
        # Replace skeleton with actual chart
        eff_placeholder.plotly_chart(fig_eff)

        # Pace vs Net Rating
        st.markdown(f"### {t('hdr_pace_eff')}")
        st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_pace_eff')}</p>", unsafe_allow_html=True)

        if "net_rtg" not in eff_df.columns:
            eff_df["net_rtg"] = eff_df["ortg"] - eff_df["drtg"]
        if "pace" not in eff_df.columns:
            n_teams = len(eff_df)
            total_games = len(schedule) if not schedule.empty else 1
            avg_games = max(total_games / max(n_teams, 1), 1)
            eff_df["pace"] = eff_df["poss_off"] / avg_games

        eff_df["color_pace"] = np.where(eff_df["team_code"] == team_code, _tc_primary, "#4b5563")
        eff_df["size_pace"] = np.where(eff_df["team_code"] == team_code, 15, 10)

        fig_pace = px.scatter(
            eff_df, x="pace", y="net_rtg", hover_name="team_name",
            color="color_pace", size="size_pace", color_discrete_map="identity",
            text="team_code",
            labels={"pace": t("lbl_pace"), "net_rtg": t("lbl_net_rtg")},
        )
        fig_pace.update_traces(textposition="top center", textfont_size=9)
        mean_pace = eff_df["pace"].mean()
        mean_net = eff_df["net_rtg"].mean()
        fig_pace.add_hline(y=mean_net, line_dash="dash", line_color="#374151")
        fig_pace.add_vline(x=mean_pace, line_dash="dash", line_color="#374151")

        x_range = eff_df["pace"].max() - eff_df["pace"].min()
        y_range = eff_df["net_rtg"].max() - eff_df["net_rtg"].min()
        quadrant_labels = [
            (mean_pace + x_range * 0.25, mean_net + y_range * 0.35, t("q_fast_eff")),
            (mean_pace - x_range * 0.25, mean_net + y_range * 0.35, t("q_slow_eff")),
            (mean_pace + x_range * 0.25, mean_net - y_range * 0.35, t("q_fast_ineff")),
            (mean_pace - x_range * 0.25, mean_net - y_range * 0.35, t("q_slow_ineff")),
        ]
        for qx, qy, qlabel in quadrant_labels:
            fig_pace.add_annotation(
                x=qx, y=qy, text=qlabel, showarrow=False,
                font=dict(size=11, color="#6b7280"), opacity=0.5,
            )
        fig_pace.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e4e4f0"), showlegend=False, height=520,
        )
        st.plotly_chart(fig_pace)

    st.markdown("---")

    # --- Situational Scoring ---
    st.markdown(f"### {t('hdr_sit_scoring')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_sit_scoring')}</p>", unsafe_allow_html=True)

    try:
        with st.status(t("fetching_sit_scoring"), expanded=True) as sit_status:
            st.write("⏳ Aggregating 2PT / 3PT / FT scoring distributions...")
            from streamlit_app.queries import fetch_situational_scoring
            sit_df = fetch_situational_scoring(season_to_fetch)
            sit_status.update(label="Situational scoring loaded.", state="complete", expanded=False)
    except Exception as e:
        st.error(f"Could not load situational scoring data. Error: {type(e).__name__}")
        sit_df = pd.DataFrame()

    if sit_df.empty:
        st.info(t("no_sit_scoring"))
    else:
        if valid_teams:
            sit_df = sit_df[sit_df["team_code"].isin(valid_teams)].copy()
        team_row = sit_df[sit_df["team_code"] == team_code]
        if team_row.empty:
            st.info(t("no_sit_scoring"))
        else:
            team_row = team_row.iloc[0]
            league_avg = sit_df[["pts_from_2pt_pct", "pts_from_3pt_pct", "pts_from_ft_pct",
                                  "steals_pg", "turnovers_pg", "off_reb_pg", "assists_pg"]].mean()

            categories = [t("lbl_2pt_pct"), t("lbl_3pt_pct"), t("lbl_ft_pct")]
            team_vals = [team_row["pts_from_2pt_pct"], team_row["pts_from_3pt_pct"], team_row["pts_from_ft_pct"]]
            league_vals = [league_avg["pts_from_2pt_pct"], league_avg["pts_from_3pt_pct"], league_avg["pts_from_ft_pct"]]

            fig_sit = go.Figure()
            fig_sit.add_trace(go.Bar(
                name=team_code, x=categories, y=team_vals, marker_color=_tc_primary,
                text=[f"{v:.1f}%" for v in team_vals], textposition="outside",
            ))
            fig_sit.add_trace(go.Bar(
                name=t("lbl_league_avg"), x=categories, y=league_vals, marker_color="#4b5563",
                text=[f"{v:.1f}%" for v in league_vals], textposition="outside",
            ))
            fig_sit.update_layout(
                barmode="group", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e4e4f0"), height=400, yaxis_title=t("lbl_pct_of_pts"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_sit)

            with st.container(border=True):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric(
                    t("lbl_steals_pg"), f"{team_row['steals_pg']:.1f}",
                    f"{team_row['steals_pg'] - league_avg['steals_pg']:+.1f} vs avg",
                    help=t("tooltip_steals_pg"),
                )
                c2.metric(
                    t("lbl_turnovers_pg"), f"{team_row['turnovers_pg']:.1f}",
                    f"{team_row['turnovers_pg'] - league_avg['turnovers_pg']:+.1f} vs avg",
                    delta_color="inverse",
                    help=t("tooltip_turnovers_pg"),
                )
                c3.metric(
                    t("lbl_off_reb_pg"), f"{team_row['off_reb_pg']:.1f}",
                    f"{team_row['off_reb_pg'] - league_avg['off_reb_pg']:+.1f} vs avg",
                    help=t("tooltip_off_reb_pg"),
                )
                c4.metric(
                    t("lbl_assists_pg"), f"{team_row['assists_pg']:.1f}",
                    f"{team_row['assists_pg'] - league_avg['assists_pg']:+.1f} vs avg",
                    help=t("tooltip_assists_pg"),
                )

    st.markdown("---")

    # --- Home vs. Away Performance ---
    st.markdown(f"### {t('hdr_home_away', default='Home vs. Away Performance')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_home_away', default='Compare team performance at home versus on the road.')}</p>", unsafe_allow_html=True)

    try:
        with st.status(t("fetching_home_away", default="Calculating Home/Away splits..."), expanded=True) as ha_status:
            st.write("⏳ Computing home vs. away offensive & defensive ratings...")
            from streamlit_app.queries import fetch_home_away_splits
            ha_df = fetch_home_away_splits(season_to_fetch)
            ha_status.update(label="Home/Away splits loaded.", state="complete", expanded=False)
    except Exception as e:
        st.error(f"Could not load Home/Away splits. Error: {type(e).__name__}")
        ha_df = pd.DataFrame()

    if ha_df.empty:
        st.info(t("no_home_away", default="No Home/Away data available yet."))
    else:
        if valid_teams:
            ha_df = ha_df[ha_df["team_code"].isin(valid_teams)].copy()
        ha_df = ha_df.sort_values("home_adv_diff", ascending=False)

        tab_net, tab_ortg, tab_drtg = st.tabs([
            t("tab_net_rtg", default="Net Rating"),
            t("tab_ortg", default="Offensive Rating"),
            t("tab_drtg", default="Defensive Rating"),
        ])

        def build_ha_chart(df, col_home, col_away, title_y):
            fig = go.Figure()
            c_home = [_tc_primary if c == team_code else "#6b7280" for c in df["team_code"]]
            c_away = [TEAM_COLORS.get(team_code, DEFAULT_ACCENT)[1] if c == team_code else "#4b5563" for c in df["team_code"]]
            fig.add_trace(go.Bar(name=t("lbl_home", default="Home"), x=df["team_code"], y=df[col_home], marker_color=c_home))
            fig.add_trace(go.Bar(name=t("lbl_away", default="Away"), x=df["team_code"], y=df[col_away], marker_color=c_away))
            min_y = min(df[col_home].min(), df[col_away].min()) - 3
            max_y = max(df[col_home].max(), df[col_away].max()) + 3
            fig.update_layout(
                barmode="group", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e4e4f0"), height=380, yaxis_title=title_y,
                yaxis=dict(range=[min_y, max_y]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            return fig

        with tab_net:
            st.plotly_chart(build_ha_chart(ha_df, "home_net", "away_net", t("lbl_net_rating", default="Net Rating")))
            st.caption(t("cap_home_adv", default="Teams are sorted left-to-right from largest Home Advantage to smallest."))
        with tab_ortg:
            st.plotly_chart(build_ha_chart(ha_df, "home_ortg", "away_ortg", t("lbl_ortg", default="ORtg")))
        with tab_drtg:
            st.plotly_chart(build_ha_chart(ha_df, "home_drtg", "away_drtg", t("lbl_drtg", default="DRtg")))

    st.markdown("---")

    # --- Clutch & Close Games ---
    st.markdown(f"### {t('hdr_clutch_close')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_clutch_close')}</p>", unsafe_allow_html=True)

    close_threshold = st.slider(
        t("lbl_close_threshold"), min_value=1, max_value=15, value=5, key="close_threshold",
    )

    try:
        with st.status(t("fetching_clutch_close"), expanded=True) as clutch_status:
            st.write("⏳ Identifying close games and computing clutch win rates...")
            from streamlit_app.queries import fetch_close_game_stats
            close_df = fetch_close_game_stats(season_to_fetch, close_threshold)
            clutch_status.update(label="Clutch/close game data loaded.", state="complete", expanded=False)
    except Exception as e:
        st.error(f"Could not load clutch/close game data. Error: {type(e).__name__}")
        close_df = pd.DataFrame()

    if close_df.empty:
        st.info(t("no_clutch_close"))
    else:
        if valid_teams:
            close_df = close_df[close_df["team_code"].isin(valid_teams)].copy()
        team_row = close_df[close_df["team_code"] == team_code]
        if not team_row.empty:
            tr = team_row.iloc[0]
            league_avg_val = tr["league_avg_close_win_pct"]
            with st.container(border=True):
                c1, c2, c3 = st.columns(3)
                c1.metric(
                    t("lbl_close_win_pct"),
                    f"{tr['close_win_pct']:.1f}%" if not pd.isna(tr["close_win_pct"]) else "N/A",
                    f"{tr['close_win_pct'] - league_avg_val:+.1f} vs avg" if not pd.isna(tr["close_win_pct"]) else None,
                    help=t("tooltip_close_win_pct", threshold=close_threshold),
                )
                c2.metric(
                    t("lbl_league_avg_close"), f"{league_avg_val:.1f}%",
                    help=t("tooltip_league_avg_close"),
                )
                c3.metric(
                    t("lbl_close_record"),
                    f"{int(tr['close_wins'])}-{int(tr['close_losses'])}",
                    f"{int(tr['close_games_played'])} close games",
                    help=t("tooltip_close_record", threshold=close_threshold),
                )

        plot_df = close_df[close_df["close_games_played"] > 0].copy()
        plot_df["is_selected"] = plot_df["team_code"] == team_code

        if len(plot_df) >= 2:
            league_avg_cw = plot_df["league_avg_close_win_pct"].iloc[0]

            # Clutch vs. Dominance
            st.markdown(f"#### {t('hdr_clutch_dominance')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_clutch_dominance')}</p>", unsafe_allow_html=True)

            fig_dom = go.Figure()
            others = plot_df[~plot_df["is_selected"]]
            _hover_dom = "%{customdata[0]}<br>" + t("hover_pt_diff") + ": %{x:.1f}<br>" + t("hover_close_win_pct") + ": %{y:.1f}%%<br>" + t("hover_close_gp") + ": %{customdata[1]}<extra></extra>"
            fig_dom.add_trace(go.Scatter(
                x=others["avg_point_diff"], y=others["close_win_pct"],
                mode="markers+text", text=others["team_code"],
                textposition="top center", textfont=dict(size=9, color="#9ca3af"),
                marker=dict(size=others["close_games_played"] * 3 + 8, color="#4b5563", opacity=0.7,
                            line=dict(width=1, color="rgba(255,255,255,0.2)")),
                hovertemplate=_hover_dom,
                customdata=list(zip(others["team_name"], others["close_games_played"])),
                showlegend=False,
            ))
            sel = plot_df[plot_df["is_selected"]]
            if not sel.empty:
                fig_dom.add_trace(go.Scatter(
                    x=sel["avg_point_diff"], y=sel["close_win_pct"],
                    mode="markers+text", text=sel["team_code"],
                    textposition="top center", textfont=dict(size=11, color="#f0f0ff"),
                    marker=dict(size=sel["close_games_played"].iloc[0] * 3 + 8, color=_tc_primary, opacity=1.0,
                                line=dict(width=2, color="#f0f0ff")),
                    hovertemplate=_hover_dom,
                    customdata=list(zip(sel["team_name"], sel["close_games_played"])),
                    showlegend=False,
                ))

            fig_dom.add_hline(y=league_avg_cw, line_dash="dash", line_color="#f59e0b",
                              annotation_text=f"League Avg: {league_avg_cw:.1f}%",
                              annotation_font_color="#f59e0b")
            fig_dom.add_vline(x=0, line_dash="dash", line_color="#374151")

            x_min, x_max = plot_df["avg_point_diff"].min(), plot_df["avg_point_diff"].max()
            fig_dom.add_annotation(x=x_max * 0.7, y=league_avg_cw + 20, text="Dominant & Clutch",
                                   showarrow=False, font=dict(size=10, color="#10b981"), opacity=0.5)
            fig_dom.add_annotation(x=x_min * 0.7, y=league_avg_cw + 20, text="Clutch DNA\n(close-game reliant)",
                                   showarrow=False, font=dict(size=10, color="#f59e0b"), opacity=0.5)
            fig_dom.add_annotation(x=x_max * 0.7, y=league_avg_cw - 20, text="Dominant but\nnot Clutch",
                                   showarrow=False, font=dict(size=10, color="#6366f1"), opacity=0.5)

            fig_dom.update_layout(
                xaxis_title=t("lbl_avg_pt_diff"), yaxis_title=t("lbl_close_win_pct"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e4e4f0"), height=520, showlegend=False,
            )
            st.plotly_chart(fig_dom)

            # Clutch vs. Overall
            st.markdown(f"#### {t('hdr_clutch_overall')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_clutch_overall')}</p>", unsafe_allow_html=True)

            fig_ov = go.Figure()
            others2 = plot_df[~plot_df["is_selected"]]
            _hover_ov = "%{customdata}<br>" + t("lbl_overall_win_pct") + ": %{x:.1f}%%<br>" + t("hover_close_win_pct") + ": %{y:.1f}%%<extra></extra>"
            fig_ov.add_trace(go.Scatter(
                x=others2["overall_win_pct"], y=others2["close_win_pct"],
                mode="markers+text", text=others2["team_code"],
                textposition="top center", textfont=dict(size=9, color="#9ca3af"),
                marker=dict(size=10, color="#4b5563", opacity=0.7,
                            line=dict(width=1, color="rgba(255,255,255,0.2)")),
                hovertemplate=_hover_ov,
                customdata=others2["team_name"], showlegend=False,
            ))
            sel2 = plot_df[plot_df["is_selected"]]
            if not sel2.empty:
                fig_ov.add_trace(go.Scatter(
                    x=sel2["overall_win_pct"], y=sel2["close_win_pct"],
                    mode="markers+text", text=sel2["team_code"],
                    textposition="top center", textfont=dict(size=11, color="#f0f0ff"),
                    marker=dict(size=14, color=_tc_primary, opacity=1.0,
                                line=dict(width=2, color="#f0f0ff")),
                    hovertemplate=_hover_ov,
                    customdata=sel2["team_name"], showlegend=False,
                ))

            fig_ov.add_shape(type="line", x0=0, y0=0, x1=100, y1=100,
                             line=dict(dash="dot", color="rgba(255,255,255,0.15)"))
            fig_ov.update_layout(
                xaxis_title=t("lbl_overall_win_pct"), yaxis_title=t("lbl_close_win_pct"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e4e4f0"), height=500, showlegend=False,
            )
            st.plotly_chart(fig_ov)
        else:
            st.info(t("no_clutch_close"))

    st.markdown("---")

    # --- Team Averages & Lineups ---
    with st.status(t("agg_season", team_code=team_code), expanded=True) as _season_status:
        try:
            _season_status.update(label=f"Fetching all game data for {team_code}...")
            from streamlit_app.queries import fetch_team_season_data
            season_data = fetch_team_season_data(season_to_fetch, team_code)
            _season_status.update(label=f"Aggregating player stats and lineup data for {team_code}...")
            _season_status.update(label="Season data loaded.", state="complete", expanded=False)
        except Exception as e:
            _season_status.update(label="Failed to load season data", state="error")
            st.error(f"Could not load season data for {team_code}. The API may be temporarily unavailable. Error: {type(e).__name__}")
            st.stop()

    if not season_data or season_data.get("player_season_stats").empty:
        st.warning(t("no_season_stats", team_code=team_code))
        st.stop()

    player_stats = season_data["player_season_stats"]
    lineup_stats = season_data["lineup_season_stats"]
    if not lineup_stats.empty and "team" in lineup_stats.columns:
        lineup_stats = lineup_stats[lineup_stats["team"] == team_code]

    st.markdown(t("hdr_player_usage", team_code=team_code))
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_player_usage')}</p>", unsafe_allow_html=True)

    fig_usage = px.scatter(
        player_stats, x="true_usg_pct", y="ts_pct", hover_name="player_name",
        size="minutes", color="points", color_continuous_scale="Viridis",
        labels={"true_usg_pct": t("lbl_tusg"), "ts_pct": t("lbl_ts"), "points": t("col_pts")},
    )
    fig_usage.layout.yaxis.tickformat = ".1%"
    mean_usg = player_stats["true_usg_pct"].mean()
    mean_ts = player_stats["ts_pct"].mean()
    fig_usage.add_hline(y=mean_ts, line_dash="dash", line_color="#374151")
    fig_usage.add_vline(x=mean_usg, line_dash="dash", line_color="#374151")
    fig_usage.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e4e4f0"), height=500,
    )
    st.plotly_chart(fig_usage)

    st.markdown("---")

    # Positional Scoring (Season) — Feature Toggle
    if get_feature_toggle("show_positional_scoring_chart", default=True):
        st.markdown(f"### {t('hdr_pos_scoring_season')}")
        st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_pos_scoring_season')}</p>", unsafe_allow_html=True)

        season_box = season_data.get("boxscore", pd.DataFrame())
        if not season_box.empty and "Points" in season_box.columns:
            from data_pipeline.transformers import compute_positional_scoring
            pos_season = compute_positional_scoring(season_box, team_code=team_code)
            if not pos_season.empty and pos_season["points"].sum() > 0:
                _tc_sec = TEAM_COLORS.get(team_code, DEFAULT_ACCENT)[1]
                pos_colors = {"Guard": _tc_primary, "Forward": "#06b6d4", "Center": _tc_sec}
                fig_donut = px.pie(
                    pos_season, names="position", values="points", color="position",
                    color_discrete_map=pos_colors, hole=0.5,
                )
                fig_donut.update_traces(textinfo="label+percent", textfont_size=14,
                                        hovertemplate="%{label}: %{value} pts (%{percent})")
                fig_donut.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#e4e4f0"), height=400, showlegend=False,
                )
                st.plotly_chart(fig_donut)
            else:
                st.info(t("no_pos_scoring"))
        else:
            st.info(t("no_pos_scoring"))

    st.markdown("---")

    # --- Player Form Tracker / Trends ---
    st.markdown(f"### {t('hdr_form_tracker')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_form_tracker')}</p>", unsafe_allow_html=True)

    per_game_stats = season_data.get("per_game_stats", pd.DataFrame())
    if not per_game_stats.empty and "player_name" in per_game_stats.columns:
        team_per_game = per_game_stats[per_game_stats["team_code"] == team_code].copy()
        form_players = sorted(team_per_game["player_name"].dropna().unique())

        if form_players:
            form_player = st.selectbox(
                t("form_select_player"), form_players, key="form_tracker_player",
            )

            form_metric = st.radio(
                t("form_select_metric"),
                ["TPC", "TS%"],
                horizontal=True,
                key="form_tracker_metric",
            )

            pdf = team_per_game[team_per_game["player_name"] == form_player].copy()
            pdf = pdf[pdf["minutes"] > 0].sort_values("Gamecode").reset_index(drop=True)

            if len(pdf) >= 2:
                pdf["game_num"] = range(1, len(pdf) + 1)
                game_label = [f"G{i}" for i in pdf["game_num"]]

                if form_metric == "TPC":
                    metric_col = "total_pts_created"
                    metric_label = t("lbl_tpc", default="Total Points Created")
                    fmt_func = lambda v: f"{v:.1f}"
                    is_pct = False
                else:
                    metric_col = "ts_pct"
                    metric_label = t("lbl_ts", default="TS%")
                    fmt_func = lambda v: f"{v:.1%}"
                    is_pct = True

                if metric_col not in pdf.columns:
                    st.info(t("form_no_data"))
                else:
                    pdf[metric_col] = pd.to_numeric(pdf[metric_col], errors="coerce")
                    pdf = pdf.dropna(subset=[metric_col])

                    if len(pdf) >= 2:
                        window = min(5, len(pdf))
                        pdf["rolling_avg"] = pdf[metric_col].rolling(window=window, min_periods=1).mean()
                        season_avg = pdf[metric_col].mean()

                        hot_threshold = season_avg * 1.10
                        pdf["is_hot"] = pdf["rolling_avg"] > hot_threshold

                        fig_form = go.Figure()

                        # Bar chart: game-by-game values
                        bar_colors = [
                            "#10b981" if h else "#6366f1" for h in pdf["is_hot"]
                        ]
                        hover_vals = [fmt_func(v) for v in pdf[metric_col]]
                        fig_form.add_trace(go.Bar(
                            x=pdf["game_num"],
                            y=pdf[metric_col],
                            name=t("form_game_value", default="Game Value"),
                            marker_color=bar_colors,
                            opacity=0.6,
                            hovertemplate="%{x}: %{customdata}<extra>" + metric_label + "</extra>",
                            customdata=hover_vals,
                        ))

                        # Rolling average line
                        fig_form.add_trace(go.Scatter(
                            x=pdf["game_num"],
                            y=pdf["rolling_avg"],
                            mode="lines+markers",
                            name=t("form_rolling_avg", default="5-Game Rolling Avg"),
                            line=dict(color="#f59e0b", width=3),
                            marker=dict(size=6),
                            hovertemplate="%{x}: " + ("%{y:.1%}" if is_pct else "%{y:.1f}") + "<extra>Rolling Avg</extra>",
                        ))

                        # Season average dashed line
                        fig_form.add_hline(
                            y=season_avg,
                            line_dash="dash",
                            line_color="#ef4444",
                            annotation_text=f"{t('form_season_avg', default='Season Avg')}: {fmt_func(season_avg)}",
                            annotation_font_color="#ef4444",
                            annotation_position="top left",
                        )

                        # Highlight hot streak zones
                        hot_starts = []
                        in_streak = False
                        for i, row in pdf.iterrows():
                            if row["is_hot"] and not in_streak:
                                hot_starts.append({"start": row["game_num"]})
                                in_streak = True
                            elif not row["is_hot"] and in_streak:
                                hot_starts[-1]["end"] = row["game_num"] - 1
                                in_streak = False
                        if in_streak and hot_starts:
                            hot_starts[-1]["end"] = pdf["game_num"].iloc[-1]

                        for streak in hot_starts:
                            fig_form.add_vrect(
                                x0=streak["start"] - 0.5,
                                x1=streak["end"] + 0.5,
                                fillcolor="rgba(16, 185, 129, 0.1)",
                                layer="below",
                                line_width=0,
                                annotation_text=t("form_hot_streak", default="Hot"),
                                annotation_position="top left",
                                annotation_font_color="#10b981",
                                annotation_font_size=10,
                            )

                        y_format = ".0%" if is_pct else None
                        fig_form.update_layout(
                            template="plotly_dark",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(15,15,35,0.8)",
                            height=450,
                            font=dict(family="Inter"),
                            xaxis_title=t("form_game_number", default="Game #"),
                            yaxis_title=metric_label,
                            yaxis_tickformat=y_format,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                            xaxis=dict(
                                tickmode="array",
                                tickvals=pdf["game_num"].tolist(),
                                ticktext=game_label,
                            ),
                        )
                        st.plotly_chart(fig_form)

                        # Summary metrics
                        last_5 = pdf.tail(window)
                        last_5_avg = last_5[metric_col].mean()
                        trend = last_5_avg - season_avg
                        trend_pct = (trend / season_avg * 100) if season_avg != 0 else 0

                        with st.container(border=True):
                            mc1, mc2, mc3, mc4 = st.columns(4)
                            mc1.metric(
                                t("form_season_avg", default="Season Avg"),
                                fmt_func(season_avg),
                                help=t("tooltip_season_avg", metric=metric_label),
                            )
                            mc2.metric(
                                t("form_last_n_avg", n=window),
                                fmt_func(last_5_avg),
                                f"{trend_pct:+.1f}%",
                                help=t("tooltip_rolling_avg", n=window),
                            )
                            mc3.metric(
                                t("form_best_game", default="Best Game"),
                                fmt_func(pdf[metric_col].max()),
                                help=t("tooltip_best_game", metric=metric_label),
                            )
                            mc4.metric(
                                t("form_games_played", default="Games Played"),
                                str(len(pdf)),
                                help=t("tooltip_games_played"),
                            )
                    else:
                        st.info(t("form_insufficient_games"))
            else:
                st.info(t("form_insufficient_games"))
        else:
            st.info(t("form_no_data"))
    else:
        st.info(t("form_no_data"))

    st.markdown("---")

    st.markdown(t("hdr_most_used", team_code=team_code))
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_most_used')}</p>", unsafe_allow_html=True)

    if lineup_stats.empty:
        st.info(t("no_lineups"))
    else:
        _lineup_cols = ["lineup_str", "events", "ortg", "drtg", "net_rtg"]
        _lineup_rename = {
            "lineup_str": t("col_lineup"), "events": t("col_poss"),
            "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")
        }
        _numeric_cols = [t("col_ortg"), t("col_drtg"), t("col_netrtg")]
        _col_config = get_decimal_column_config(_numeric_cols)

        col1, col2 = st.columns(2)
        with col1:
            with st.container(border=True):
                st.markdown(f"#### {t('hdr_best_net')}")
                _best_df = format_df_decimals(lineup_stats.head(5)[_lineup_cols]).rename(columns=_lineup_rename)
                st.dataframe(_best_df, hide_index=True, column_config=_col_config)
        with col2:
            with st.container(border=True):
                st.markdown(f"#### {t('hdr_worst_net')}")
                _worst_df = format_df_decimals(lineup_stats.tail(5)[_lineup_cols]).rename(columns=_lineup_rename)
                st.dataframe(_worst_df, hide_index=True, column_config=_col_config)
