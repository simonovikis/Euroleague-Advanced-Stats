import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default,
    ensure_game_data, apply_clutch_filter, render_game_header,
    render_game_sidebar,
)


def render():
    gamecode = render_game_sidebar()
    if gamecode is None:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        st.stop()

    data = ensure_game_data(gamecode)
    render_game_header()

    clutch_mode = st.toggle(
        t("clutch_toggle_label", default="Isolate Clutch Time Only"),
        value=st.session_state.get("clutch_mode", False),
        key="clutch_toggle_advanced",
        help=t("clutch_toggle_help", default="Recalculate all stats for Clutch Time only: last 5 min of Q4/OT, score within 5 pts."),
    )
    st.session_state["clutch_mode"] = clutch_mode
    if clutch_mode:
        st.caption(t("clutch_caption", default="Showing clutch-time stats only (Q4/OT, <=5 min, <=5 pt diff)"))
        data = apply_clutch_filter(data)

    tab_playmaking, tab_clutch_mom = st.tabs([
        t("nav_playmaking"),
        t("nav_clutch"),
    ])

    # ------------------------------------------------------------------
    # SUB-TAB: Playmaking & AAQ
    # ------------------------------------------------------------------
    with tab_playmaking:
        st.markdown(f'<p class="section-header">{t("hdr_playmaking")}</p>', unsafe_allow_html=True)
        st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_playmaking')}</p>", unsafe_allow_html=True)

        aaq_df = data.get("playmaking_aaq", pd.DataFrame())
        axp_df = data.get("playmaking_axp", pd.DataFrame())
        duos_df = data.get("playmaking_duos", pd.DataFrame())
        assist_links = data.get("assist_shot_links", pd.DataFrame())

        sub_aaq, sub_axp, sub_duos, sub_network = st.tabs([
            t("tab_aaq"), t("tab_axp"), t("tab_duos"), t("tab_duo_network"),
        ])

        with sub_aaq:
            st.markdown(f"#### {t('hdr_aaq')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_aaq')}</p>", unsafe_allow_html=True)
            if aaq_df.empty:
                st.info(t("no_playmaking"))
            else:
                display_aaq = aaq_df[["passer_name", "team", "total_assists", "aaq"]].rename(columns={
                    "passer_name": t("col_passer"), "team": t("col_team"),
                    "total_assists": t("col_total_ast"), "aaq": "AAQ (xP)",
                })
                st.dataframe(display_aaq, hide_index=True)

                if len(aaq_df) >= 2:
                    fig_aaq = px.bar(
                        aaq_df.head(15), x="passer_name", y="aaq",
                        color="aaq", color_continuous_scale=["#312e81", "#6366f1", "#f59e0b"],
                        labels={"passer_name": t("col_passer"), "aaq": "AAQ (xP)"}, text="aaq",
                    )
                    fig_aaq.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                    fig_aaq.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)", height=420,
                        font=dict(family="Inter"), showlegend=False, xaxis_tickangle=-45,
                    )
                    st.plotly_chart(fig_aaq)

        with sub_axp:
            st.markdown(f"#### {t('hdr_axp')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_axp')}</p>", unsafe_allow_html=True)
            if axp_df.empty:
                st.info(t("no_playmaking"))
            else:
                display_axp = axp_df[["shooter_name", "team", "assisted_shots", "axp_total", "axp_avg"]].rename(columns={
                    "shooter_name": t("col_shooter"), "team": t("col_team"),
                    "assisted_shots": t("col_ast_shots"), "axp_total": "AxP Total", "axp_avg": "AxP Avg",
                })
                st.dataframe(display_axp, hide_index=True)

                if len(axp_df) >= 2:
                    fig_axp = px.scatter(
                        axp_df, x="assisted_shots", y="axp_avg", size="axp_total",
                        hover_name="shooter_name", color="team", text="shooter_name",
                        labels={"assisted_shots": t("col_ast_shots"), "axp_avg": "AxP Avg", "axp_total": "AxP Total"},
                        color_discrete_sequence=["#6366f1", "#f59e0b", "#10b981", "#ef4444"],
                    )
                    fig_axp.update_traces(textposition="top center", textfont_size=9)
                    fig_axp.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)", height=450, font=dict(family="Inter"),
                    )
                    st.plotly_chart(fig_axp)

        with sub_duos:
            st.markdown(f"#### {t('hdr_duos_xp')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_duos_xp')}</p>", unsafe_allow_html=True)
            if duos_df.empty:
                st.info(t("no_playmaking"))
            else:
                duo_teams = sorted(duos_df["team"].unique())
                sel_duo_team_xp = st.selectbox(t("col_team"), [t("filter_all")] + duo_teams, key="duo_xp_team")
                duos_f = duos_df if sel_duo_team_xp == t("filter_all") else duos_df[duos_df["team"] == sel_duo_team_xp]

                display_duos = duos_f[["passer_name", "shooter_name", "team", "assists", "duo_xp"]].rename(columns={
                    "passer_name": t("col_passer"), "shooter_name": t("col_shooter"),
                    "team": t("col_team"), "assists": t("col_ast"), "duo_xp": t("col_duo_xp"),
                })
                st.dataframe(display_duos, hide_index=True)

        with sub_network:
            st.markdown(f"#### {t('hdr_duo_heatmap')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_duo_heatmap')}</p>", unsafe_allow_html=True)

            if assist_links.empty:
                st.info(t("no_playmaking"))
            else:
                net_teams = sorted(assist_links["team"].unique())
                sel_net_team = st.selectbox(t("team_dropdown"), net_teams, key="xp_net_team")
                team_links = assist_links[assist_links["team"] == sel_net_team]

                if team_links.empty:
                    st.info(t("no_playmaking"))
                else:
                    duo_agg = (
                        team_links.groupby(["passer_name", "shooter_name"])
                        .agg(total_xp=("xp", "sum"), count=("xp", "size"))
                        .reset_index()
                    )
                    passers = sorted(duo_agg["passer_name"].unique())
                    shooters = sorted(duo_agg["shooter_name"].unique())
                    all_names = sorted(set(passers) | set(shooters))

                    matrix = pd.DataFrame(0.0, index=all_names, columns=all_names)
                    for _, row in duo_agg.iterrows():
                        matrix.loc[row["passer_name"], row["shooter_name"]] = row["total_xp"]

                    fig_hm = go.Figure(data=go.Heatmap(
                        z=matrix.values, x=matrix.columns.tolist(), y=matrix.index.tolist(),
                        colorscale=[
                            [0, "rgba(15,15,35,0.9)"], [0.25, "#312e81"],
                            [0.5, "#6366f1"], [0.75, "#a78bfa"], [1.0, "#f59e0b"],
                        ],
                        hovertemplate=t("col_passer") + ": %{y}<br>" + t("col_shooter") + ": %{x}<br>xP: %{z:.2f}<extra></extra>",
                        showscale=True, colorbar=dict(title="xP"),
                    ))
                    fig_hm.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)", height=500,
                        xaxis=dict(title=t("col_shooter"), tickangle=45, tickfont=dict(size=10)),
                        yaxis=dict(title=t("col_passer"), tickfont=dict(size=10), autorange="reversed"),
                        font=dict(family="Inter"), margin=dict(l=120, b=120),
                    )
                    st.plotly_chart(fig_hm)

    # ------------------------------------------------------------------
    # SUB-TAB: Clutch & Momentum
    # ------------------------------------------------------------------
    with tab_clutch_mom:
        st.markdown(f'<p class="section-header">{t("nav_clutch")}</p>', unsafe_allow_html=True)

        sub_clutch, sub_runs, sub_fouls = st.tabs([
            t("tab_clutch", default="Clutch Stats"),
            t("tab_runs", default="Run Stoppers"),
            t("tab_fouls", default="Foul Trouble Impact"),
        ])

        with sub_clutch:
            clutch = data.get("clutch_stats", pd.DataFrame())
            if clutch.empty:
                st.info(t("info_no_clutch", default="No clutch situations detected."))
            else:
                st.markdown(t("desc_clutch", default="Clutch = last 5 min of Q4/OT, score within 5 points."))
                st.dataframe(
                    clutch[["player_name", "team", "clutch_actions", "clutch_points",
                             "clutch_fga", "clutch_turnovers", "clutch_ts_pct", "clutch_usage"]]
                    .rename(columns={
                        "player_name": t("col_player", default="Player"), "team": t("col_team", default="Team"),
                        "clutch_actions": t("col_actions", default="Actions"), "clutch_points": t("col_pts"),
                        "clutch_fga": t("col_fga", default="FGA"), "clutch_turnovers": t("col_tov", default="TOV"),
                        "clutch_ts_pct": t("col_ts"), "clutch_usage": t("col_usage", default="Usage%"),
                    }).round(3),
                    hide_index=True,
                )

        with sub_runs:
            stoppers = data.get("run_stoppers", pd.DataFrame())
            if stoppers.empty:
                st.info(t("info_no_runs", default="No 8+ point scoring runs detected in this game."))
            else:
                st.markdown(t("desc_runs", default="Scoring runs of **8+ unanswered points** and the player who broke them."))
                st.dataframe(
                    stoppers[["run_points", "stopper_player", "stopper_team",
                               "stopper_playtype", "period", "markertime"]]
                    .rename(columns={
                        "run_points": t("col_run_pts", default="Run (pts)"), "stopper_player": t("col_stopper", default="Stopper"),
                        "stopper_team": t("col_team", default="Team"), "stopper_playtype": t("col_play_type", default="Play Type"),
                        "period": t("col_period", default="Period"), "markertime": t("col_time", default="Time"),
                    }),
                    hide_index=True,
                )

        with sub_fouls:
            foul_trouble = data.get("foul_trouble", pd.DataFrame())
            if foul_trouble.empty:
                st.info(t("info_no_fouls", default="No foul trouble detected for high-usage players."))
            else:
                st.markdown(t("desc_fouls", default="Impact on team ratings when the **highest-usage player** gets 2+ fouls in the first half."))
                for _, ft in foul_trouble.iterrows():
                    st.markdown(f"**{ft['team']}** — {ft['star_player']} ({t('lbl_foul2', default='2nd foul in Q')}{ft['foul_period']})")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric(
                        f"{t('lbl_ortg')} {t('lbl_before', default='Before')}", f"{ft['ortg_before']:.1f}",
                        help=t("tooltip_ortg_before_foul"),
                    )
                    c2.metric(
                        f"{t('lbl_ortg')} {t('lbl_after', default='After')}", f"{ft['ortg_after']:.1f}",
                        delta=f"{ft['ortg_impact']:+.1f}",
                        help=t("tooltip_ortg_after_foul"),
                    )
                    c3.metric(
                        f"{t('lbl_drtg')} {t('lbl_before', default='Before')}", f"{ft['drtg_before']:.1f}",
                        help=t("tooltip_drtg_before_foul"),
                    )
                    c4.metric(
                        f"{t('lbl_drtg')} {t('lbl_after', default='After')}", f"{ft['drtg_after']:.1f}",
                        delta=f"{ft['drtg_impact']:+.1f}", delta_color="inverse",
                        help=t("tooltip_drtg_after_foul"),
                    )
                    st.markdown("---")
