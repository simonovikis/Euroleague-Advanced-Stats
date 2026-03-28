import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default, _cfg_seasons,
    is_feature_enabled, show_disabled_message,
)


def render():
    if not is_feature_enabled("ENABLE_SCOUTING"):
        show_disabled_message("ENABLE_SCOUTING")
        st.stop()

    st.markdown(f'<p class="section-header">{t("card_scouting_title")}</p>', unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('scout_method_note')}</p>",
        unsafe_allow_html=True,
    )

    season_scout = st.session_state.get("selected_season", _cfg_default)

    from streamlit_app.queries import fetch_scouting_player_pool
    from data_pipeline.scouting_engine import (
        find_similar_players,
        build_multi_radar,
        get_player_feature_vector,
        FEATURE_COLUMNS,
        FEATURE_LABELS,
        MIN_MINUTES_PG,
    )

    with st.status(t("scout_loading_pool"), expanded=True) as _scout_status:
        try:
            _scout_status.update(label="Fetching season player data and computing scouting features...")
            player_pool = fetch_scouting_player_pool(season_scout)
            _scout_status.update(label="Player pool loaded.", state="complete", expanded=False)
        except Exception as e:
            _scout_status.update(label="Failed to load player pool", state="error")
            st.error(f"Could not load scouting data. The API may be temporarily unavailable. Error: {type(e).__name__}")
            st.stop()

    if player_pool.empty:
        st.warning(t("err_no_schedule", season=season_scout))
        st.stop()

    # --- Filters row ---
    col_sel, col_mpg, col_pos = st.columns([3, 1, 1])

    with col_mpg:
        min_mpg = st.slider(
            t("scout_min_filter"),
            min_value=5, max_value=25, value=int(MIN_MINUTES_PG),
            key="scout_min_mpg",
        )

    with col_pos:
        pos_options = [
            t("scout_position_all"),
            t("scout_position_guard"),
            t("scout_position_forward"),
            t("scout_position_center"),
        ]
        pos_choice = st.selectbox(
            t("scout_position_filter"), pos_options, index=0, key="scout_pos",
        )
        pos_map = {
            pos_options[1]: "Guard",
            pos_options[2]: "Forward",
            pos_options[3]: "Center",
        }
        active_pos_filter = pos_map.get(pos_choice)

    filtered_pool = player_pool[player_pool["minutes_pg"] >= min_mpg].copy()

    player_names = sorted(filtered_pool["player_name"].dropna().unique())
    if not player_names:
        st.info("No players meet the current minutes filter.")
        st.stop()

    with col_sel:
        target_player = st.selectbox(
            t("scout_select_player"),
            player_names,
            key="scout_target",
        )

    # --- Target player profile card ---
    target_row = filtered_pool[filtered_pool["player_name"] == target_player]
    if target_row.empty:
        st.warning(t("scout_no_player"))
        st.stop()

    tr = target_row.iloc[0]
    target_pos = tr.get("position", "")
    st.markdown(f"### {t('scout_profile')}: {target_player}")

    prof_cols = st.columns(7)
    prof_cols[0].metric(t("col_team"), tr["team_code"])
    prof_cols[1].metric(t("scout_position_label"), target_pos, help=t("tooltip_position"))
    prof_cols[2].metric("GP", f"{int(tr['games_played'])}", help=t("tooltip_gp"))
    prof_cols[3].metric("MPG", f"{tr['minutes_pg']:.1f}", help=t("tooltip_mpg"))
    prof_cols[4].metric("PPG", f"{tr['points_pg']:.1f}", help=t("tooltip_ppg"))
    prof_cols[5].metric("TS%", f"{tr['ts_pct']:.1%}", help=t("tooltip_ts_scout"))
    prof_cols[6].metric("tUSG%", f"{tr['true_usg_pct']:.1%}", help=t("tooltip_tusg_scout"))

    st.markdown("---")

    # --- Find similar players ---
    top_n = 5
    similar_df = find_similar_players(
        target_player, filtered_pool, top_n=top_n,
        position_filter=active_pos_filter,
    )

    # --- PDF Scouting Report Download ---
    from streamlit_app.utils.pdf_report import generate_player_report
    from streamlit_app.queries import fetch_team_season_data

    recent_form_df = None
    try:
        team_code_for_report = tr["team_code"]
        season_data = fetch_team_season_data(season_scout, team_code_for_report)
        per_game = season_data.get("per_game_stats", pd.DataFrame())
        if not per_game.empty and "player_name" in per_game.columns:
            player_games = per_game[
                (per_game["player_name"] == target_player) & (per_game["minutes"] > 0)
            ].sort_values("Gamecode")
            if not player_games.empty:
                recent_form_df = player_games.tail(5)
    except Exception:
        pass

    try:
        pdf_buf = generate_player_report(
            player_name=target_player,
            season=season_scout,
            player_pool=filtered_pool,
            similar_df=similar_df,
            recent_form_df=recent_form_df,
        )
        safe_name = target_player.replace(" ", "_").replace(",", "")
        st.download_button(
            label="\U0001F4C4 Download PDF Scouting Report",
            data=pdf_buf,
            file_name=f"scouting_report_{safe_name}_{season_scout}.pdf",
            mime="application/pdf",
            key="scout_pdf_download",
        )
    except Exception as e:
        st.warning(f"Could not generate PDF report: {type(e).__name__}")

    if similar_df.empty:
        st.warning(t("scout_no_player"))
        st.stop()

    st.markdown(f"### {t('scout_similar_title', n=top_n, player=target_player)}")

    # ================================================================
    # MATCH CARDS — Top 5 in styled columns
    # ================================================================
    pct_feats = ["ts_pct", "true_usg_pct", "stop_rate", "assist_ratio",
                 "orb_pct", "drb_pct", "three_pt_rate", "ft_rate"]

    card_cols = st.columns(top_n)
    for ci, (_, row) in enumerate(similar_df.iterrows()):
        sim_pct = row["similarity_pct"]
        # Gradient colours: high similarity -> amber, lower -> indigo
        if sim_pct >= 90:
            accent = "#f59e0b"
        elif sim_pct >= 80:
            accent = "#6366f1"
        else:
            accent = "#64748b"

        pos_label = row.get("position", "")
        with card_cols[ci]:
            st.markdown(
                f'<div style="background:linear-gradient(135deg,#1e1e3f,#2a2a5a);'
                f'border:1px solid {accent}55;border-radius:14px;padding:18px 14px;'
                f'text-align:center;">'
                f'<div style="font-size:2rem;font-weight:800;color:{accent};'
                f'line-height:1;">{sim_pct:.1f}%</div>'
                f'<div style="color:#e4e4f0;font-size:0.72rem;margin-bottom:8px;">'
                f'{t("scout_similarity")}</div>'
                f'<div style="color:#fff;font-weight:700;font-size:0.95rem;">'
                f'{row["player_name"]}</div>'
                f'<div style="color:#9ca3af;font-size:0.8rem;">'
                f'{row["team_name"]}</div>'
                f'<div style="color:#6366f1;font-size:0.75rem;margin-top:4px;">'
                f'{pos_label} · {row["points_pg"]:.1f} PPG · {row["minutes_pg"]:.1f} MPG</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("")

    # --- Similarity bar chart ---
    sim_bar = similar_df[["player_name", "similarity_pct"]].copy()
    fig_sim = go.Figure(go.Bar(
        x=sim_bar["similarity_pct"],
        y=sim_bar["player_name"],
        orientation="h",
        marker=dict(
            color=sim_bar["similarity_pct"],
            colorscale=[[0, "#312e81"], [0.5, "#6366f1"], [1.0, "#f59e0b"]],
        ),
        text=sim_bar["similarity_pct"].apply(lambda x: f"{x:.1f}%"),
        textposition="outside",
        hovertemplate="%{y}: %{x:.1f}% " + t("hover_similarity") + "<extra></extra>",
    ))
    fig_sim.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,15,35,0.8)", height=280,
        xaxis=dict(title="Cosine Similarity (%)", range=[0, 105]),
        yaxis=dict(autorange="reversed"),
        font=dict(family="Inter"), margin=dict(l=150, r=60, t=20, b=40),
    )
    st.plotly_chart(fig_sim, use_container_width=True)

    st.markdown("---")

    # ================================================================
    # SIDE-BY-SIDE COMPARISON MATRIX with Delta Highlighting
    # ================================================================
    st.markdown(f"### {t('scout_comparison_matrix_title')}")

    target_feats = get_player_feature_vector(target_player, filtered_pool)
    if target_feats:
        matrix_rows = []
        for feat in FEATURE_COLUMNS:
            label = FEATURE_LABELS[feat]
            is_pct = feat in pct_feats
            tv = target_feats[feat]
            row_data = {"Metric": label, target_player: f"{tv:.1%}" if is_pct else f"{tv:.2f}"}
            for _, sr in similar_df.iterrows():
                sv = sr[feat]
                delta = sv - tv
                if is_pct:
                    val_str = f"{sv:.1%}"
                    delta_str = f"{delta:+.1%}"
                else:
                    val_str = f"{sv:.2f}"
                    delta_str = f"{delta:+.2f}"
                # Flag significant delta (absolute > 5 pp for pct, > 0.5 for raw)
                threshold = 0.05 if is_pct else 0.5
                if abs(delta) > threshold:
                    delta_str = f"**{delta_str}**"
                row_data[sr["player_name"]] = f"{val_str} ({delta_str})"
            matrix_rows.append(row_data)

        matrix_df = pd.DataFrame(matrix_rows)
        st.dataframe(matrix_df, use_container_width=True, hide_index=True)

        # Key insight callout for #1 match
        best_match_name = similar_df.iloc[0]["player_name"]
        best_sim_pct = similar_df.iloc[0]["similarity_pct"]
        best_feats = {feat: similar_df.iloc[0][feat] for feat in FEATURE_COLUMNS}
        big_diffs = []
        for feat in FEATURE_COLUMNS:
            d = best_feats[feat] - target_feats[feat]
            is_pct = feat in pct_feats
            thr = 0.05 if is_pct else 0.5
            if abs(d) > thr:
                direction = "higher" if d > 0 else "lower"
                if is_pct:
                    big_diffs.append(f"{FEATURE_LABELS[feat]} is {abs(d):.1%} {direction}")
                else:
                    big_diffs.append(f"{FEATURE_LABELS[feat]} is {abs(d):.2f} {direction}")

        if big_diffs:
            diff_bullets = ", ".join(big_diffs[:4])
            st.info(
                f"**{best_match_name}** is a {best_sim_pct:.1f}% match, "
                f"but: {diff_bullets}."
            )

    st.markdown("---")

    # ================================================================
    # EXPANDED RADAR CHART — Multi-player overlay
    # ================================================================
    st.markdown(f"### {t('scout_radar_title')}")

    match_names = similar_df["player_name"].tolist()
    default_sel = match_names[:3]
    radar_selection = st.multiselect(
        t("scout_select_radar_players"),
        match_names,
        default=default_sel,
        key="scout_radar_sel",
    )

    if radar_selection:
        radar_data = build_multi_radar(target_player, radar_selection, filtered_pool)
        if radar_data is None:
            st.info("Cannot build radar comparison.")
        else:
            labels = radar_data["labels"]
            labels_closed = labels + [labels[0]]
            t_vals = radar_data["target_values"]
            t_closed = t_vals + [t_vals[0]]

            # Colour palette for overlays
            overlay_colors = [
                ("#f59e0b", "rgba(245,158,11,0.18)"),
                ("#10b981", "rgba(16,185,129,0.18)"),
                ("#ef4444", "rgba(239,68,68,0.18)"),
                ("#3b82f6", "rgba(59,130,246,0.18)"),
                ("#a855f7", "rgba(168,85,247,0.18)"),
            ]

            fig_radar = go.Figure()
            # Target player trace
            fig_radar.add_trace(go.Scatterpolar(
                r=t_closed, theta=labels_closed, fill="toself",
                name=target_player,
                fillcolor="rgba(99,102,241,0.25)",
                line=dict(color="#6366f1", width=2.5),
            ))
            # Overlay selected matches
            for i, name in enumerate(radar_selection):
                if name not in radar_data["players"]:
                    continue
                p = radar_data["players"][name]
                vals_closed = p["norm"] + [p["norm"][0]]
                lc, fc = overlay_colors[i % len(overlay_colors)]
                fig_radar.add_trace(go.Scatterpolar(
                    r=vals_closed, theta=labels_closed, fill="toself",
                    name=name, fillcolor=fc,
                    line=dict(color=lc, width=2),
                ))

            fig_radar.update_layout(
                polar=dict(
                    bgcolor="rgba(15,15,35,0.8)",
                    radialaxis=dict(visible=True, range=[0, 1], showticklabels=False,
                                    gridcolor="rgba(255,255,255,0.1)"),
                    angularaxis=dict(gridcolor="rgba(255,255,255,0.1)",
                                     tickfont=dict(size=11, color="#e4e4f0")),
                ),
                template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=560,
                font=dict(family="Inter"),
                legend=dict(x=0.15, y=-0.12, orientation="h", font=dict(size=12)),
            )
            st.plotly_chart(fig_radar, use_container_width=True)
    else:
        st.info(t("scout_select_radar_players"))
