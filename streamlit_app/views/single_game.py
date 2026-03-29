import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, render_aggrid, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default,
    ensure_game_data, apply_clutch_filter, render_game_header,
    render_game_sidebar,
)


def render():
    gamecode = render_game_sidebar()
    if gamecode is None:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        st.stop()

    data = ensure_game_data(gamecode)
    if st.session_state.get("clutch_mode"):
        data = apply_clutch_filter(data)
    render_game_header()

    tab_stats, tab_shots, tab_radar, tab_lineups, tab_assist, tab_rotations = st.tabs([
        t("nav_player_stats"),
        t("nav_shot_chart"),
        t("nav_radar"),
        t("nav_lineups"),
        t("nav_assist"),
        t("nav_rotations"),
    ])

    # ------------------------------------------------------------------
    # TAB: Player Advanced Stats
    # ------------------------------------------------------------------
    with tab_stats:
        st.markdown(f'<p class="section-header">{t("hdr_player_stats")}</p>', unsafe_allow_html=True)

        adv = data["advanced_stats"]
        if adv.empty:
            st.warning(t("no_adv_stats"))
        else:
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                teams = sorted(adv["team_code"].dropna().unique())
                sel_team = st.selectbox(t("filter_team"), [t("filter_all")] + teams, key="adv_team")
            with col_f2:
                players = sorted(adv["player_name"].dropna().unique())
                sel_player = st.selectbox(t("filter_player"), [t("filter_all")] + players, key="adv_player")

            filtered = adv.copy()
            if sel_team != t("filter_all"):
                filtered = filtered[filtered["team_code"] == sel_team]
            if sel_player != t("filter_all"):
                filtered = filtered[filtered["player_name"] == sel_player]

            active = filtered[filtered["minutes"] > 0].copy()

            if not active.empty:
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric(
                    t("metric_players", default="Players"), len(active),
                    help=t("tooltip_players_count"),
                )
                c2.metric(
                    f"{t('metric_avg', default='Avg')} {t('lbl_ts', default='TS%')}",
                    f"{active['ts_pct'].mean():.1%}",
                    help=t("tooltip_avg_ts"),
                )
                c3.metric(
                    f"{t('metric_avg', default='Avg')} {t('lbl_ortg', default='ORtg')}",
                    f"{active['off_rating'].mean():.1f}",
                    help=t("tooltip_avg_ortg"),
                )
                c4.metric(
                    f"{t('metric_avg', default='Avg')} {t('lbl_drtg', default='DRtg')}",
                    f"{active['def_rating'].mean():.1f}",
                    help=t("tooltip_avg_drtg"),
                )
                if "true_usg_pct" in active.columns:
                    c5.metric(
                        f"{t('metric_avg', default='Avg')} {t('lbl_tusg', default='tUSG%')}",
                        f"{active['true_usg_pct'].mean():.1%}",
                        help=t("tooltip_tusg"),
                    )
                else:
                    c5.metric(
                        f"{t('metric_avg', default='Avg')} {t('col_poss', default='Poss')}",
                        f"{active['possessions'].mean():.1f}",
                        help=t("tooltip_poss"),
                    )

            display_cols = [
                "player_name", "team_code", "minutes", "points",
                "pts_from_assists", "total_pts_created",
                "possessions", "ts_pct", "off_rating", "def_rating",
            ]
            if "true_usg_pct" in active.columns:
                display_cols += ["true_usg_pct", "stop_rate"]
            display_cols = [c for c in display_cols if c in active.columns]

            sort_col = "total_pts_created" if "total_pts_created" in active.columns else "points"
            grid_df = active[display_cols].round(3).sort_values(sort_col, ascending=False).copy()
            if "off_rating" in grid_df.columns and "def_rating" in grid_df.columns:
                grid_df["net_rating"] = (grid_df["off_rating"] - grid_df["def_rating"]).round(1)
            grid_df = grid_df.rename(columns={
                "player_name": t("col_player"), "team_code": t("col_team"), "minutes": t("col_min"),
                "points": t("col_pts"), "pts_from_assists": t("col_pts_from_assists"),
                "total_pts_created": t("col_total_pts_created"),
                "possessions": t("col_poss"), "ts_pct": t("col_ts"),
                "off_rating": t("col_ortg"), "def_rating": t("col_drtg"),
                "true_usg_pct": t("col_tusg"), "stop_rate": t("col_stop_rate"),
                "net_rating": t("col_net_rtg_short"),
            })
            render_aggrid(
                grid_df,
                pin_cols=[t("col_player"), t("col_team")],
                heatmap_cols=["Net Rtg"],
                height=400,
                key="single_game_stats",
            )

            # --- Total Points Produced Stacked Bar Chart ---
            if "total_pts_created" in active.columns:
                st.markdown(f"#### {t('hdr_tpc_breakdown')}")
                chart_team = sel_team if sel_team != t("filter_all") else None
                chart_df = active.copy()
                if chart_team:
                    chart_df = chart_df[chart_df["team_code"] == chart_team]
                else:
                    teams_in_game = sorted(active["team_code"].unique())
                    if teams_in_game:
                        chart_team = st.selectbox(
                            t("lbl_select_team_chart"), teams_in_game, key="tpc_chart_team",
                        )
                        chart_df = active[active["team_code"] == chart_team]

                chart_df = chart_df[chart_df["total_pts_created"] > 0].sort_values(
                    "total_pts_created", ascending=True,
                )
                if not chart_df.empty:
                    fig_tpc = go.Figure()
                    fig_tpc.add_trace(go.Bar(
                        y=chart_df["player_name"], x=chart_df["points"],
                        name=t("lbl_own_points"), orientation="h",
                        marker_color="#6366f1",
                        hovertemplate="%{y}: %{x} pts<extra>" + t("lbl_own_points") + "</extra>",
                    ))
                    fig_tpc.add_trace(go.Bar(
                        y=chart_df["player_name"], x=chart_df["pts_from_assists"],
                        name=t("lbl_pts_via_ast"), orientation="h",
                        marker_color="#f59e0b",
                        hovertemplate="%{y}: %{x} pts<extra>" + t("lbl_pts_via_ast") + "</extra>",
                    ))
                    fig_tpc.update_layout(
                        barmode="stack",
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)",
                        height=max(350, len(chart_df) * 38),
                        xaxis_title=t("lbl_total_pts_produced"),
                        font=dict(family="Inter"),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    )
                    st.plotly_chart(fig_tpc, use_container_width=True)

            st.markdown(f"#### ⚔️ {t('lbl_ortg')} vs {t('lbl_drtg')}")
            scatter_df = active.dropna(subset=["off_rating", "def_rating"])
            if not scatter_df.empty:
                fig = px.scatter(
                    scatter_df, x="def_rating", y="off_rating", color="team_code",
                    text="player_name", size="minutes", size_max=20,
                    hover_data=["points", "ts_pct", "possessions"],
                    labels={"off_rating": t("lbl_offensive_rating"), "def_rating": t("lbl_defensive_rating")},
                    color_discrete_sequence=["#6366f1", "#f59e0b", "#10b981", "#ef4444"],
                )
                fig.update_traces(textposition="top center", textfont_size=9)
                fig.update_layout(
                    template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)", height=500, font=dict(family="Inter"),
                )
                avg_ortg = scatter_df["off_rating"].mean()
                avg_drtg = scatter_df["def_rating"].mean()
                fig.add_hline(y=avg_ortg, line_dash="dash", line_color="rgba(255,255,255,0.2)")
                fig.add_vline(x=avg_drtg, line_dash="dash", line_color="rgba(255,255,255,0.2)")
                st.plotly_chart(fig, use_container_width=True)

            # Positional Scoring Distribution
            st.markdown("---")
            st.markdown(f"### {t('hdr_pos_scoring')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_pos_scoring')}</p>", unsafe_allow_html=True)

            boxscore_raw = data.get("boxscore", pd.DataFrame())
            if not boxscore_raw.empty and "Points" in boxscore_raw.columns:
                from data_pipeline.transformers import compute_positional_scoring

                teams_in_game = sorted(boxscore_raw["Team"].dropna().unique())
                fig_pos = go.Figure()
                pos_colors = {"Guard": "#8b5cf6", "Forward": "#06b6d4", "Center": "#f59e0b"}

                for tm in teams_in_game:
                    pos_df = compute_positional_scoring(boxscore_raw, team_code=tm)
                    for _, row in pos_df.iterrows():
                        fig_pos.add_trace(go.Bar(
                            name=row["position"], x=[tm], y=[row["pct"]],
                            marker_color=pos_colors.get(row["position"], "#6b7280"),
                            text=f"{row['pct']:.1f}%", textposition="inside",
                            legendgroup=row["position"], showlegend=(tm == teams_in_game[0]),
                        ))

                fig_pos.update_layout(
                    barmode="stack", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#e4e4f0"), height=380, yaxis_title=t("lbl_pct_of_pts"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig_pos, use_container_width=True)
            else:
                st.info(t("no_pos_scoring"))

    # ------------------------------------------------------------------
    # TAB: Player Shot Charts (contextual, per-game)
    # ------------------------------------------------------------------
    with tab_shots:
        from streamlit_app.utils.court import draw_euroleague_court

        st.markdown(f'<p class="section-header">{t("nav_shot_chart")}</p>', unsafe_allow_html=True)

        shots = data.get("shots", pd.DataFrame())
        shot_quality = data.get("shot_quality", pd.DataFrame())
        boxscore_raw = data.get("boxscore", pd.DataFrame())

        if shots.empty:
            st.warning(t("no_shot_data", default="No shot data available for this game."))
        else:
            _FG_ACTIONS_GAME = {"2FGA", "2FGM", "3FGA", "3FGM"}
            if "ID_ACTION" in shots.columns:
                shots = shots[shots["ID_ACTION"].isin(_FG_ACTIONS_GAME)].copy()

            # Build roster from both teams using boxscore
            roster_options = []
            if not boxscore_raw.empty and "Player" in boxscore_raw.columns:
                roster_options = sorted(boxscore_raw["Player"].dropna().unique())
            if not roster_options:
                roster_options = sorted(shots["PLAYER"].dropna().unique())

            _sc_col1, _sc_col2 = st.columns([3, 1])
            with _sc_col1:
                sel_shooter = st.selectbox(
                    t("filter_player"), [t("filter_all")] + roster_options, key="shot_player",
                )
            with _sc_col2:
                _sc_heatmap = st.toggle("Density Heatmap", value=False, key="shot_heatmap")

            shot_df = shots.copy()
            if sel_shooter != t("filter_all"):
                shot_df = shot_df[shot_df["PLAYER"] == sel_shooter]

            if shot_df.empty:
                st.info(t("err_no_shots", default="No shots for this selection."))
            else:
                has_coords = (
                    "COORD_X" in shot_df.columns
                    and "COORD_Y" in shot_df.columns
                    and not shot_df["COORD_X"].isna().all()
                )

                if has_coords:
                    shot_df = shot_df.dropna(subset=["COORD_X", "COORD_Y"])
                    shot_df["Outcome"] = shot_df["POINTS"].apply(
                        lambda p: t("lbl_made", default="Made") if p > 0 else t("lbl_missed", default="Missed")
                    )

                    # Shooting summary
                    _sc_total = len(shot_df)
                    _sc_made = (shot_df["Outcome"] == t("lbl_made", default="Made")).sum()
                    _sc_fg_pct = _sc_made / _sc_total if _sc_total > 0 else 0
                    _sc_id = shot_df.get("ID_ACTION", pd.Series(dtype=str))
                    _sc_twos = shot_df[_sc_id.isin({"2FGA", "2FGM"})] if not _sc_id.empty else pd.DataFrame()
                    _sc_threes = shot_df[_sc_id.isin({"3FGA", "3FGM"})] if not _sc_id.empty else pd.DataFrame()
                    _sc_fg2_pct = _sc_twos["POINTS"].gt(0).sum() / len(_sc_twos) if len(_sc_twos) > 0 else 0
                    _sc_fg3_pct = _sc_threes["POINTS"].gt(0).sum() / len(_sc_threes) if len(_sc_threes) > 0 else 0

                    # Side-by-side: Court + Summary
                    col_chart, col_summary = st.columns([3, 1])

                    with col_chart:
                        fig = draw_euroleague_court()

                        if not _sc_heatmap:
                            made = shot_df[shot_df["Outcome"] == t("lbl_made", default="Made")]
                            missed = shot_df[shot_df["Outcome"] == t("lbl_missed", default="Missed")]

                            fig.add_trace(go.Scatter(
                                x=made["COORD_X"], y=made["COORD_Y"], mode="markers",
                                marker=dict(color="#10b981", size=10, symbol="circle",
                                            line=dict(width=1, color="white"), opacity=0.85),
                                name=t("lbl_made", default="Made"), text=made["ACTION"],
                                hovertemplate="%{text}<br>" + t("hover_zone") + ": %{customdata}<extra></extra>",
                                customdata=made["ZONE"],
                            ))
                            fig.add_trace(go.Scatter(
                                x=missed["COORD_X"], y=missed["COORD_Y"], mode="markers",
                                marker=dict(color="#ef4444", size=8, symbol="x",
                                            line=dict(width=1, color="white"), opacity=0.65),
                                name=t("lbl_missed", default="Missed"), text=missed["ACTION"],
                                hovertemplate="%{text}<br>" + t("hover_zone") + ": %{customdata}<extra></extra>",
                                customdata=missed["ZONE"],
                            ))
                        else:
                            fig.add_trace(go.Histogram2dContour(
                                x=shot_df["COORD_X"], y=shot_df["COORD_Y"],
                                colorscale=[
                                    [0, "rgba(15,15,35,0)"], [0.2, "#312e81"],
                                    [0.4, "#6366f1"], [0.6, "#a78bfa"],
                                    [0.8, "#f59e0b"], [1.0, "#ef4444"],
                                ],
                                showscale=True,
                                colorbar=dict(title="Density"),
                                contours=dict(coloring="heatmap"),
                                ncontours=20,
                                hoverinfo="skip",
                            ))

                        fig.update_layout(
                            template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(15,15,35,0.9)", height=650,
                            xaxis=dict(range=[-800, 800], showgrid=False, zeroline=False,
                                       showticklabels=False, scaleanchor="y"),
                            yaxis=dict(range=[-50, 1050], showgrid=False, zeroline=False, showticklabels=False),
                            legend=dict(x=0.02, y=0.98, font=dict(size=12)),
                            font=dict(family="Inter"), margin=dict(l=20, r=20, t=30, b=20),
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    with col_summary:
                        st.markdown("#### Shooting Summary")
                        _sc_label = sel_shooter if sel_shooter != t("filter_all") else "All Players"
                        st.markdown(f"**{_sc_label}**")
                        st.metric("Total Shots", _sc_total)
                        st.metric("FG%", f"{_sc_fg_pct:.1%}")
                        st.metric("2PT%", f"{_sc_fg2_pct:.1%}",
                                  help=f"{len(_sc_twos)} attempts" if len(_sc_twos) > 0 else None)
                        st.metric("3PT%", f"{_sc_fg3_pct:.1%}",
                                  help=f"{len(_sc_threes)} attempts" if len(_sc_threes) > 0 else None)

                    # Shot Quality table below
                    if not shot_quality.empty:
                        st.markdown(f"#### {t('hdr_shot_quality')}")
                        sq = shot_quality.copy()
                        if sel_shooter != t("filter_all"):
                            sq = sq[sq["PLAYER"] == sel_shooter]
                        if not sq.empty:
                            sq_display = sq[[
                                "PLAYER", "TEAM", "total_shots", "makes", "fg_pct",
                                "actual_pts", "total_expected_pts", "shot_quality_diff",
                            ]].rename(columns={
                                "PLAYER": "Player", "TEAM": "Team", "total_shots": "Shots",
                                "makes": "Makes", "fg_pct": "FG%", "actual_pts": "Actual Pts",
                                "total_expected_pts": "Expected Pts", "shot_quality_diff": "Pts vs Expected",
                            })
                            st.dataframe(sq_display.round(2), use_container_width=True, hide_index=True)
                else:
                    st.info(t("lbl_no_coords"))
                    zone_summary = (
                        shot_df.groupby("ZONE")
                        .agg(shots=("POINTS", "size"), made=("POINTS", lambda x: (x > 0).sum()))
                        .reset_index()
                    )
                    zone_summary["fg_pct"] = zone_summary["made"] / zone_summary["shots"]
                    st.dataframe(zone_summary.round(3), use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # TAB: Player Comparison Radar
    # ------------------------------------------------------------------
    with tab_radar:
        st.markdown(f'<p class="section-header">{t("nav_radar")}</p>', unsafe_allow_html=True)

        adv = data["advanced_stats"]
        active = adv[adv["minutes"] > 0].copy() if not adv.empty else pd.DataFrame()

        if active.empty:
            st.warning(t("no_adv_stats"))
        else:
            players = sorted(active["player_name"].dropna().unique())
            col1, col2 = st.columns(2)
            with col1:
                p1 = st.selectbox(t("lbl_player", default="Player") + " 1", players, index=0, key="radar_p1")
            with col2:
                p2 = st.selectbox(t("lbl_player", default="Player") + " 2", players, index=min(1, len(players) - 1), key="radar_p2")

            radar_cols = ["off_rating", "def_rating", "ts_pct"]
            radar_labels = ["ORtg", "DRtg (inv)", "TS%"]
            if "true_usg_pct" in active.columns:
                radar_cols.append("true_usg_pct")
                radar_labels.append("tUSG%")
            if "stop_rate" in active.columns:
                radar_cols.append("stop_rate")
                radar_labels.append("Stop Rate")

            p1_data = active[active["player_name"] == p1][radar_cols].mean()
            p2_data = active[active["player_name"] == p2][radar_cols].mean()

            all_vals = active[radar_cols].copy()
            mins = all_vals.min()
            maxs = all_vals.max()
            ranges = (maxs - mins).replace(0, 1)

            p1_norm = ((p1_data - mins) / ranges).values.tolist()
            p2_norm = ((p2_data - mins) / ranges).values.tolist()

            drtg_idx = radar_labels.index("DRtg (inv)")
            p1_norm[drtg_idx] = 1 - p1_norm[drtg_idx]
            p2_norm[drtg_idx] = 1 - p2_norm[drtg_idx]

            p1_norm.append(p1_norm[0])
            p2_norm.append(p2_norm[0])
            labels_closed = radar_labels + [radar_labels[0]]

            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=p1_norm, theta=labels_closed, fill="toself", name=p1,
                fillcolor="rgba(99,102,241,0.25)", line=dict(color="#6366f1", width=2),
            ))
            fig.add_trace(go.Scatterpolar(
                r=p2_norm, theta=labels_closed, fill="toself", name=p2,
                fillcolor="rgba(245,158,11,0.25)", line=dict(color="#f59e0b", width=2),
            ))
            fig.update_layout(
                polar=dict(
                    bgcolor="rgba(15,15,35,0.8)",
                    radialaxis=dict(visible=True, range=[0, 1], showticklabels=False,
                                    gridcolor="rgba(255,255,255,0.1)"),
                    angularaxis=dict(gridcolor="rgba(255,255,255,0.1)",
                                     tickfont=dict(size=12, color="#e4e4f0")),
                ),
                template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=500,
                font=dict(family="Inter"),
                legend=dict(x=0.35, y=-0.1, orientation="h", font=dict(size=13)),
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown(f"#### {t('hdr_raw_comparison')}")
            comparison = pd.DataFrame({
                t("col_metric"): radar_labels,
                p1: [round(p1_data.iloc[i], 3) for i in range(len(radar_labels))],
                p2: [round(p2_data.iloc[i], 3) for i in range(len(radar_labels))],
            })
            st.dataframe(comparison, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # TAB: Lineup & Synergy
    # ------------------------------------------------------------------
    with tab_lineups:
        st.markdown(f'<p class="section-header">{t("nav_lineups")}</p>', unsafe_allow_html=True)

        sub_lu, sub_duo, sub_trio, sub_on_off = st.tabs([
            t("tab_5man", default="🏅 5-Man Lineups"),
            t("tab_duo", default="👥 Duo Synergy"),
            t("tab_trio", default="🔺 Trio Synergy"),
            t("tab_on_off", default="📊 On/Off Court"),
        ])

        with sub_lu:
            lu_stats = data.get("lineup_stats", pd.DataFrame())
            if lu_stats.empty:
                st.info(t("no_lineups", default="No lineup data available."))
            else:
                lu_teams = sorted(lu_stats["team"].unique())
                sel_lu_team = st.selectbox(t("col_team", default="Team"), [t("filter_all")] + lu_teams, key="lu_team")
                lu_filtered = lu_stats if sel_lu_team == t("filter_all") else lu_stats[lu_stats["team"] == sel_lu_team]

                min_events = st.slider(t("lbl_min_events", default="Min PBP events"), 5, 100, 20, key="lu_min_events")
                lu_filtered = lu_filtered[lu_filtered["events"] >= min_events]

                if lu_filtered.empty:
                    st.info(t("no_lineups", default="No lineups meet the minimum events threshold."))
                else:
                    st.markdown(f"##### {t('hdr_best_net')}")
                    best = lu_filtered.head(5)[["team", "lineup_str", "events", "pts_for", "pts_against", "ortg", "drtg", "net_rtg"]].rename(
                        columns={"team": t("col_team", default="Team"), "lineup_str": t("col_lineup"), "events": t("col_poss"),
                                 "pts_for": t("col_pts"), "pts_against": t("col_pts_ag", default="Opp Pts"),
                                 "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                    )
                    st.dataframe(best, use_container_width=True, hide_index=True)

                    st.markdown(f"##### {t('hdr_worst_net')}")
                    worst = lu_filtered.tail(5).sort_values("net_rtg")[["team", "lineup_str", "events", "pts_for", "pts_against", "ortg", "drtg", "net_rtg"]].rename(
                        columns={"team": t("col_team", default="Team"), "lineup_str": t("col_lineup"), "events": t("col_poss"),
                                 "pts_for": t("col_pts"), "pts_against": t("col_pts_ag", default="Opp Pts"),
                                 "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                    )
                    st.dataframe(worst, use_container_width=True, hide_index=True)

                    fig = px.bar(
                        lu_filtered.head(15), x="net_rtg", y="lineup_str", orientation="h",
                        color="net_rtg", color_continuous_scale=["#ef4444", "#6b7280", "#10b981"],
                        color_continuous_midpoint=0, labels={"net_rtg": t("lbl_net_rtg"), "lineup_str": t("col_lineup")},
                    )
                    fig.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)", height=450,
                        font=dict(family="Inter"), yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig, use_container_width=True)

        with sub_duo:
            duo = data.get("duo_synergy", pd.DataFrame())
            if duo.empty:
                st.info(t("no_duo_data"))
            else:
                duo_teams = sorted(duo["team"].unique())
                sel_duo_team = st.selectbox(t("col_team"), [t("filter_all")] + duo_teams, key="duo_team")
                duo_f = duo if sel_duo_team == t("filter_all") else duo[duo["team"] == sel_duo_team]
                duo_f = duo_f[duo_f["events_together"] >= 10]

                st.markdown(f"##### {t('hdr_best_duos')}")
                st.dataframe(
                    duo_f.head(10)[["team", "combo_names", "events_together", "net_rtg_together", "net_rtg_apart", "synergy"]].rename(
                        columns={"team": t("col_team"), "combo_names": t("col_lineup"), "events_together": t("col_poss"),
                                 "net_rtg_together": t("col_netrtg"), "net_rtg_apart": f"{t('col_netrtg')} ({t('lbl_away')})", "synergy": "Synergy"}
                    ),
                    use_container_width=True, hide_index=True,
                )

                st.markdown(f"##### {t('hdr_worst_duos')}")
                st.dataframe(
                    duo_f.tail(5).sort_values("synergy")[["team", "combo_names", "events_together", "net_rtg_together", "net_rtg_apart", "synergy"]].rename(
                        columns={"team": t("col_team"), "combo_names": t("col_lineup"), "events_together": t("col_poss"),
                                 "net_rtg_together": t("col_netrtg"), "net_rtg_apart": f"{t('col_netrtg')} ({t('lbl_away')})", "synergy": "Synergy"}
                    ),
                    use_container_width=True, hide_index=True,
                )

        with sub_trio:
            trio = data.get("trio_synergy", pd.DataFrame())
            if trio.empty:
                st.info(t("no_trio_data"))
            else:
                trio_teams = sorted(trio["team"].unique())
                sel_trio_team = st.selectbox(t("col_team"), [t("filter_all")] + trio_teams, key="trio_team")
                trio_f = trio if sel_trio_team == t("filter_all") else trio[trio["team"] == sel_trio_team]
                trio_f = trio_f[trio_f["events_together"] >= 10]

                st.markdown(f"##### {t('hdr_best_trios')}")
                st.dataframe(
                    trio_f.head(10)[["team", "combo_names", "events_together", "net_rtg_together", "net_rtg_apart", "synergy"]].rename(
                        columns={"team": t("col_team"), "combo_names": t("col_lineup"), "events_together": t("col_poss"),
                                 "net_rtg_together": t("col_netrtg"), "net_rtg_apart": f"{t('col_netrtg')} ({t('lbl_away')})", "synergy": "Synergy"}
                    ),
                    use_container_width=True, hide_index=True,
                )

        with sub_on_off:
            st.markdown(f"#### {t('hdr_on_off')}")
            st.markdown(
                f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_on_off')}</p>",
                unsafe_allow_html=True,
            )

            on_off = data.get("on_off_splits", pd.DataFrame())
            if on_off.empty:
                st.info(t("no_on_off"))
            else:
                oo_teams = sorted(on_off["team"].unique())
                sel_oo_team = st.selectbox(
                    t("col_team"), [t("filter_all")] + oo_teams, key="on_off_team",
                )
                oo_f = on_off if sel_oo_team == t("filter_all") else on_off[on_off["team"] == sel_oo_team]

                if oo_f.empty:
                    st.info(t("no_on_off"))
                else:
                    # Metric cards for top 3 impact players
                    st.markdown(f"##### {t('hdr_on_off_top')}")
                    top3 = oo_f.head(3)
                    cols = st.columns(len(top3))
                    for col, (_, row) in zip(cols, top3.iterrows()):
                        with col:
                            st.metric(
                                label=f"{row['player_name']} ({row['team']})",
                                value=f"{row['on_net_rtg']:+.1f}",
                                delta=f"{row['on_off_diff']:+.1f} diff",
                                help=(
                                    f"On Court: ORtg {row['on_ortg']:.1f} / DRtg {row['on_drtg']:.1f} / "
                                    f"NetRtg {row['on_net_rtg']:+.1f}\n"
                                    f"Off Court: ORtg {row['off_ortg']:.1f} / DRtg {row['off_drtg']:.1f} / "
                                    f"NetRtg {row['off_net_rtg']:+.1f}"
                                ),
                            )

                    # Horizontal bar chart: On/Off differential for all players
                    fig_oo = go.Figure()

                    oo_sorted = oo_f.sort_values("on_off_diff", ascending=True)

                    colors = [
                        "#10b981" if v > 0 else "#ef4444"
                        for v in oo_sorted["on_off_diff"]
                    ]

                    fig_oo.add_trace(go.Bar(
                        y=oo_sorted["player_name"],
                        x=oo_sorted["on_off_diff"],
                        orientation="h",
                        marker_color=colors,
                        text=[f"{v:+.1f}" for v in oo_sorted["on_off_diff"]],
                        textposition="outside",
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            "On/Off Diff: %{x:+.1f}<br>"
                            "<extra></extra>"
                        ),
                        customdata=oo_sorted[["on_ortg", "on_drtg", "on_net_rtg",
                                               "off_ortg", "off_drtg", "off_net_rtg"]].values,
                    ))

                    fig_oo.update_traces(
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            "On Court: ORtg %{customdata[0]:.1f} / DRtg %{customdata[1]:.1f} / "
                            "NetRtg %{customdata[2]:+.1f}<br>"
                            "Off Court: ORtg %{customdata[3]:.1f} / DRtg %{customdata[4]:.1f} / "
                            "NetRtg %{customdata[5]:+.1f}<br>"
                            "Diff: %{x:+.1f}<extra></extra>"
                        ),
                    )

                    fig_oo.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.3)")
                    fig_oo.update_layout(
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)",
                        height=max(400, len(oo_sorted) * 32 + 80),
                        font=dict(family="Inter"),
                        xaxis=dict(
                            title=t("col_on_off_diff"),
                            showgrid=True,
                            gridcolor="rgba(255,255,255,0.06)",
                            zeroline=False,
                        ),
                        yaxis=dict(showgrid=False),
                        margin=dict(l=120, t=20, r=60, b=40),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_oo, use_container_width=True)

                    # Full data table
                    display_cols = [
                        "player_name", "team",
                        "on_ortg", "on_drtg", "on_net_rtg",
                        "off_ortg", "off_drtg", "off_net_rtg",
                        "on_off_diff",
                    ]
                    display_df = oo_f[display_cols].rename(columns={
                        "player_name": t("col_player"),
                        "team": t("col_team"),
                        "on_ortg": t("col_on_ortg"),
                        "on_drtg": t("col_on_drtg"),
                        "on_net_rtg": t("col_on_net"),
                        "off_ortg": t("col_off_ortg"),
                        "off_drtg": t("col_off_drtg"),
                        "off_net_rtg": t("col_off_net"),
                        "on_off_diff": t("col_on_off_diff"),
                    })
                    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # TAB: Assist Network
    # ------------------------------------------------------------------
    with tab_assist:
        st.markdown(f'<p class="section-header">{t("nav_assist")}</p>', unsafe_allow_html=True)

        assists = data.get("assist_network", pd.DataFrame())
        if assists.empty:
            st.warning(t("no_adv_stats", default="No assist data available."))
        else:
            ast_teams = sorted(assists["team"].unique())
            sel_ast_team = st.selectbox(t("team_dropdown", default="Select Team"), ast_teams, key="ast_team")
            team_assists = assists[assists["team"] == sel_ast_team]

            if team_assists.empty:
                st.info(t("err_no_shots", default="No data for this team."))
            else:
                passers = sorted(team_assists["assister_name"].unique())
                scorers = sorted(team_assists["scorer_name"].unique())
                all_players = sorted(set(passers) | set(scorers))

                matrix = pd.DataFrame(0, index=all_players, columns=all_players)
                for _, row in team_assists.iterrows():
                    matrix.loc[row["assister_name"], row["scorer_name"]] += row["count"]

                fig = go.Figure(data=go.Heatmap(
                    z=matrix.values, x=matrix.columns.tolist(), y=matrix.index.tolist(),
                    colorscale=[
                        [0, "rgba(15,15,35,0.9)"], [0.25, "#312e81"],
                        [0.5, "#6366f1"], [0.75, "#a78bfa"], [1.0, "#f59e0b"],
                    ],
                    hovertemplate=t("col_passer", default="Passer") + ": %{y}<br>" + t("col_shooter", default="Scorer") + ": %{x}<br>" + t("col_ast", default="Assists") + ": %{z}<extra></extra>",
                    showscale=True, colorbar=dict(title=t("col_ast", default="Assists")),
                ))
                fig.update_layout(
                    template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)", height=500,
                    xaxis=dict(title="Scorer", tickangle=45, tickfont=dict(size=10)),
                    yaxis=dict(title="Passer", tickfont=dict(size=10), autorange="reversed"),
                    font=dict(family="Inter"), margin=dict(l=120, b=120),
                )
                st.plotly_chart(fig, use_container_width=True)

                st.markdown(f"#### {t('hdr_top_ast_connections')}")
                st.dataframe(
                    team_assists[["assister_name", "scorer_name", "count", "play_types"]].rename(
                        columns={"assister_name": t("lbl_passer_col"), "scorer_name": t("lbl_scorer_col"),
                                 "count": t("col_ast"), "play_types": t("col_shot_types")}
                    ),
                    use_container_width=True, hide_index=True,
                )

    # ------------------------------------------------------------------
    # TAB: Player Rotations (Gantt Chart)
    # ------------------------------------------------------------------
    with tab_rotations:
        st.markdown(f'<p class="section-header">{t("nav_rotations")}</p>', unsafe_allow_html=True)
        st.markdown(
            f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_rotations')}</p>",
            unsafe_allow_html=True,
        )

        from data_pipeline.transformers import compute_player_stints

        rot_box = data.get("boxscore", pd.DataFrame())
        rot_pbp = data.get("pbp", pd.DataFrame())

        if rot_box.empty or rot_pbp.empty:
            st.warning(t("no_adv_stats", default="No data available."))
        else:
            home_players = rot_box[rot_box["Home"] == 1]
            away_players = rot_box[rot_box["Home"] == 0]
            home_team_rot = home_players["Team"].iloc[0] if not home_players.empty else "HOME"
            away_team_rot = away_players["Team"].iloc[0] if not away_players.empty else "AWAY"

            sel_rot_team = st.selectbox(
                t("team_dropdown", default="Select Team"),
                [home_team_rot, away_team_rot],
                key="rot_team",
            )

            with st.status("Computing Rotations...", expanded=True) as rot_status:
                st.write("⏳ Parsing play-by-play and identifying player stints...")
                stints_df = compute_player_stints(rot_pbp, rot_box, sel_rot_team)
                rot_status.update(label="Rotations Ready!", state="complete", expanded=False)

            if stints_df.empty:
                st.info(t("rot_no_stints"))
            else:
                # Order players by total minutes (most minutes at top)
                player_minutes = (
                    stints_df.groupby("player_name")["duration_sec"]
                    .sum()
                    .sort_values(ascending=True)
                )
                player_order = player_minutes.index.tolist()

                # Determine game length for x-axis
                max_sec = stints_df["end_sec"].max()
                n_periods = max(int(np.ceil(max_sec / 600)), 4)

                # Build color scale: +/- mapped to red-gray-green
                pm_abs_max = max(abs(stints_df["plus_minus"].max()), abs(stints_df["plus_minus"].min()), 1)

                fig_rot = go.Figure()

                for _, stint in stints_df.iterrows():
                    pm = stint["plus_minus"]
                    # Normalize +/- to [-1, 1] for color mapping
                    norm = max(min(pm / pm_abs_max, 1.0), -1.0)
                    if norm > 0:
                        r = int(99 + (16 - 99) * norm)
                        g = int(102 + (185 - 102) * norm)
                        b = int(241 + (129 - 241) * norm)
                    elif norm < 0:
                        r = int(99 + (239 - 99) * abs(norm))
                        g = int(102 + (68 - 102) * abs(norm))
                        b = int(241 + (68 - 241) * abs(norm))
                    else:
                        r, g, b = 99, 102, 241

                    color = f"rgb({r},{g},{b})"

                    start_min = stint["start_sec"] / 60
                    end_min = stint["end_sec"] / 60
                    dur_min = stint["duration_sec"] / 60

                    fig_rot.add_trace(go.Bar(
                        y=[stint["player_name"]],
                        x=[dur_min],
                        base=[start_min],
                        orientation="h",
                        marker_color=color,
                        marker_line=dict(width=0.5, color="rgba(255,255,255,0.15)"),
                        hovertemplate=(
                            f"<b>{stint['player_name']}</b><br>"
                            f"In: {start_min:.1f} min — Out: {end_min:.1f} min<br>"
                            f"Duration: {dur_min:.1f} min<br>"
                            f"+/−: {pm:+d}<extra></extra>"
                        ),
                        showlegend=False,
                    ))

                # Period separator lines
                for p in range(1, n_periods + 1):
                    fig_rot.add_vline(
                        x=p * 10, line_dash="dot",
                        line_color="rgba(255,255,255,0.25)", line_width=1,
                    )

                # Quarter labels
                period_labels = []
                for p in range(1, n_periods + 1):
                    label = f"Q{p}" if p <= 4 else f"OT{p - 4}"
                    period_labels.append(
                        dict(
                            x=(p - 0.5) * 10, y=1.02, xref="x", yref="paper",
                            text=label, showarrow=False,
                            font=dict(size=11, color="#9ca3af"),
                        )
                    )

                fig_rot.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)",
                    height=max(350, len(player_order) * 36 + 80),
                    font=dict(family="Inter"),
                    xaxis=dict(
                        title=t("rot_game_minutes", default="Game Minutes"),
                        range=[0, n_periods * 10],
                        dtick=5,
                        showgrid=True,
                        gridcolor="rgba(255,255,255,0.06)",
                    ),
                    yaxis=dict(
                        categoryorder="array",
                        categoryarray=player_order,
                        showgrid=False,
                    ),
                    barmode="stack",
                    bargap=0.3,
                    annotations=period_labels,
                    margin=dict(l=120, t=40, r=20, b=50),
                )
                st.plotly_chart(fig_rot, use_container_width=True)

                # Color legend
                st.markdown(
                    "<p style='color:#9ca3af; font-size:0.8rem; text-align:center;'>"
                    "<span style='color:#10b981;'>■</span> "
                    + t("rot_positive_pm", default="Positive +/−") +
                    " &nbsp;&nbsp; "
                    "<span style='color:#6366f1;'>■</span> "
                    + t("rot_neutral_pm", default="Neutral") +
                    " &nbsp;&nbsp; "
                    "<span style='color:#ef4444;'>■</span> "
                    + t("rot_negative_pm", default="Negative +/−") +
                    "</p>",
                    unsafe_allow_html=True,
                )

                # Summary stats table
                st.markdown(f"#### {t('rot_stint_summary', default='Stint Summary')}")
                summary = (
                    stints_df.groupby("player_name")
                    .agg(
                        total_min=("duration_sec", lambda x: x.sum() / 60),
                        stints=("duration_sec", "count"),
                        avg_stint_min=("duration_sec", lambda x: x.mean() / 60),
                        total_pm=("plus_minus", "sum"),
                    )
                    .sort_values("total_min", ascending=False)
                    .reset_index()
                )
                summary.columns = [
                    t("col_player"), t("col_min"),
                    t("rot_stints", default="Stints"),
                    t("rot_avg_stint", default="Avg Stint (min)"),
                    t("rot_total_pm", default="Total +/−"),
                ]
                summary[t("col_min")] = summary[t("col_min")].round(1)
                summary[t("rot_avg_stint", default="Avg Stint (min)")] = summary[t("rot_avg_stint", default="Avg Stint (min)")].round(1)

                st.dataframe(summary, use_container_width=True, hide_index=True)
