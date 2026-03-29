import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default,
    render_page_header, GLOBAL_DECIMALS, TEAM_NAME_MAP,
)


_POSITION_MAP = {"Guard": "G", "Forward": "F", "Center": "C"}
_POSITION_COLORS = {"G": "#6366f1", "F": "#06b6d4", "C": "#f59e0b"}


def render():
    render_page_header(
        t("scout_finder_title", default="Scout Finder"),
        t("scout_finder_subtitle", default="Find undervalued players using biometrics and advanced efficiency metrics"),
        icon="💰",
    )

    season = st.session_state.get("selected_season", _cfg_default)
    _d = GLOBAL_DECIMALS

    from streamlit_app.queries import fetch_scout_targets

    with st.status(t("scout_finder_loading", default="Loading player pool..."), expanded=True) as _status:
        try:
            _status.update(label="Querying player stats, metadata and on/off splits...")
            pool = fetch_scout_targets(season)
            _status.update(label="Player pool loaded.", state="complete", expanded=False)
        except Exception as e:
            _status.update(label="Failed to load player pool", state="error")
            st.error(f"Could not load scout data. Error: {type(e).__name__}")
            st.stop()

    if pool.empty:
        st.warning(t("scout_finder_no_data", default="No player data available. Ensure the database is synced and player metadata enriched."))
        st.stop()

    pool["pos_short"] = pool["position"].map(_POSITION_MAP).fillna("?")

    # ------------------------------------------------------------------
    # FILTERS
    # ------------------------------------------------------------------
    with st.popover("🔎 " + t("scout_finder_filters", default="Filter Players"), use_container_width=True):
        f_col1, f_col2 = st.columns(2)

        with f_col1:
            age_range = st.slider(
                t("lbl_age_range", default="Age Range"),
                min_value=16, max_value=42,
                value=(18, 40),
                key="sf_age",
            )
            height_range = st.slider(
                t("lbl_height_range", default="Height Range (cm)"),
                min_value=170, max_value=230,
                value=(170, 230),
                step=1,
                key="sf_height",
            )
            positions = st.multiselect(
                t("lbl_positions", default="Positions"),
                options=["G", "F", "C"],
                default=["G", "F", "C"],
                key="sf_positions",
            )

        with f_col2:
            min_minutes = st.number_input(
                t("lbl_min_minutes", default="Min Minutes/Game"),
                min_value=0.0, max_value=40.0, value=5.0, step=1.0,
                key="sf_min_minutes",
            )
            min_ts = st.slider(
                t("lbl_min_ts", default="Min True Shooting %"),
                min_value=0, max_value=100, value=0, step=1,
                key="sf_min_ts",
            )
            max_usg = st.slider(
                t("lbl_max_usg", default="Max Usage Rate"),
                min_value=0.0, max_value=4.0, value=4.0, step=0.1,
                format="%.1f",
                key="sf_max_usg",
                help=t("help_usg", default="Usage rate measures a player's share of team possessions. Typical range: 0.5 (role player) to 2.5 (primary option)."),
            )
            min_games = st.number_input(
                t("lbl_min_games", default="Min Games Played"),
                min_value=1, max_value=50, value=3, step=1,
                key="sf_min_games",
            )

    # ------------------------------------------------------------------
    # APPLY FILTERS
    # ------------------------------------------------------------------
    df = pool.copy()

    if "age" in df.columns and df["age"].notna().any():
        has_age = df["age"].notna()
        df = df[has_age & (df["age"] >= age_range[0]) & (df["age"] <= age_range[1])]

    if "height" in df.columns and df["height"].notna().any():
        has_h = df["height"].notna()
        df = df[has_h & (df["height"] >= height_range[0]) & (df["height"] <= height_range[1])]

    df = df[df["pos_short"].isin(positions)]
    df = df[df["minutes_pg"] >= min_minutes]
    df = df[df["games"] >= min_games]

    if "ts_pct" in df.columns:
        df = df[(df["ts_pct"].isna()) | (df["ts_pct"] >= (min_ts / 100.0))]

    if "true_usg_pct" in df.columns:
        df = df[(df["true_usg_pct"].isna()) | (df["true_usg_pct"] <= max_usg)]

    if df.empty:
        st.warning(t("scout_finder_empty", default="No players match the current filters. Try relaxing the criteria."))
        st.stop()

    st.caption(f"{len(df)} players match filters")

    # ------------------------------------------------------------------
    # SCATTER PLOT: Age vs On/Off Net Rating
    # ------------------------------------------------------------------
    st.markdown(f"### {t('hdr_scout_scatter', default='Impact vs Age')}")
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>"
        f"{t('sub_scout_scatter', default='Young players in the top-left quadrant are high-impact and still developing. Bubble size = minutes played.')}"
        f"</p>",
        unsafe_allow_html=True,
    )

    scatter_df = df.dropna(subset=["on_off_diff", "age"]).copy()

    if scatter_df.empty:
        st.info(t("scout_finder_no_on_off", default="On/Off data not available. Run season aggregations first."))
    else:
        scatter_df["minutes_size"] = scatter_df["minutes_pg"].clip(lower=5)

        fig_scatter = px.scatter(
            scatter_df,
            x="age",
            y="on_off_diff",
            size="minutes_size",
            color="pos_short",
            color_discrete_map=_POSITION_COLORS,
            hover_name="player_name",
            hover_data={
                "team_code": True,
                "age": ":.0f",
                "height": True,
                "on_off_diff": ":.1f",
                "minutes_pg": ":.1f",
                "ts_pct": ":.1%",
                "true_usg_pct": ":.1%",
                "minutes_size": False,
                "pos_short": False,
            },
            labels={
                "age": t("lbl_age", default="Age"),
                "on_off_diff": t("lbl_on_off_diff", default="On/Off Net Rating"),
                "pos_short": t("lbl_position", default="Position"),
                "minutes_pg": t("lbl_min_pg", default="Min/Game"),
            },
        )

        mean_age = scatter_df["age"].mean()
        mean_onoff = scatter_df["on_off_diff"].mean()
        fig_scatter.add_hline(y=0, line_dash="dash", line_color="#374151")
        fig_scatter.add_vline(x=mean_age, line_dash="dash", line_color="#374151")

        fig_scatter.add_annotation(
            x=scatter_df["age"].min() + 1, y=scatter_df["on_off_diff"].max() * 0.85,
            text="Young & High-Impact",
            showarrow=False, font=dict(size=10, color="#10b981"), opacity=0.6,
        )
        fig_scatter.add_annotation(
            x=scatter_df["age"].max() - 1, y=scatter_df["on_off_diff"].min() * 0.85,
            text="Aging & Low-Impact",
            showarrow=False, font=dict(size=10, color="#ef4444"), opacity=0.6,
        )

        fig_scatter.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e4e4f0"),
            height=560,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                title_text="",
            ),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("---")

    # ------------------------------------------------------------------
    # KPI ROW — pool summary
    # ------------------------------------------------------------------
    with st.container(border=True):
        k1, k2, k3, k4 = st.columns(4)
        k1.metric(t("lbl_players_found", default="Players Found"), len(df))
        k2.metric(
            t("lbl_avg_age", default="Avg Age"),
            f"{df['age'].mean():.1f}" if "age" in df.columns and df["age"].notna().any() else "N/A",
        )
        k3.metric(
            t("lbl_avg_ts", default="Avg TS%"),
            f"{df['ts_pct'].mean():.1%}" if "ts_pct" in df.columns else "N/A",
        )
        avg_onoff = df["on_off_diff"].mean() if "on_off_diff" in df.columns and df["on_off_diff"].notna().any() else None
        k4.metric(
            t("lbl_avg_on_off", default="Avg On/Off"),
            f"{avg_onoff:+.1f}" if avg_onoff is not None else "N/A",
        )

    # ------------------------------------------------------------------
    # INTERACTIVE TABLE
    # ------------------------------------------------------------------
    st.markdown(f"### {t('hdr_scout_table', default='Player Database')}")

    display = df[[
        "player_name", "team_code", "pos_short", "age", "height", "country",
        "games", "minutes_pg", "points_pg", "ts_pct", "true_usg_pct",
        "off_rating", "assist_ratio", "stop_rate", "three_pt_rate",
        "rebounds_pg", "assists_pg", "steals_pg",
        "on_off_diff",
    ]].copy()

    display.columns = [
        "Player", "Team", "Pos", "Age", "Height", "Country",
        "GP", "Min/G", "Pts/G", "TS%", "USG%",
        "ORtg", "AST Ratio", "Stop Rate", "3PT Rate",
        "Reb/G", "Ast/G", "Stl/G",
        "On/Off",
    ]

    sort_col = st.selectbox(
        t("lbl_sort_by", default="Sort by"),
        options=display.columns.tolist(),
        index=display.columns.tolist().index("On/Off") if "On/Off" in display.columns else 0,
        key="sf_sort",
    )
    ascending = sort_col in ("Age", "USG%")
    display = display.sort_values(sort_col, ascending=ascending, na_position="last")

    col_config = {
        "TS%": st.column_config.ProgressColumn(
            "TS%", format="%.1f%%", min_value=0, max_value=1,
        ),
        "USG%": st.column_config.ProgressColumn(
            "USG%", format="%.1f%%", min_value=0, max_value=1,
        ),
        "3PT Rate": st.column_config.ProgressColumn(
            "3PT Rate", format="%.1f%%", min_value=0, max_value=1,
        ),
        "Stop Rate": st.column_config.ProgressColumn(
            "Stop Rate", format="%.1f%%", min_value=0, max_value=0.6,
        ),
        "AST Ratio": st.column_config.ProgressColumn(
            "AST Ratio", format="%.1f%%", min_value=0, max_value=0.5,
        ),
        "On/Off": st.column_config.NumberColumn(
            "On/Off", format="%+.1f",
        ),
        "Min/G": st.column_config.NumberColumn("Min/G", format="%.1f"),
        "Pts/G": st.column_config.NumberColumn("Pts/G", format="%.1f"),
        "ORtg": st.column_config.NumberColumn("ORtg", format="%.1f"),
        "Reb/G": st.column_config.NumberColumn("Reb/G", format="%.1f"),
        "Ast/G": st.column_config.NumberColumn("Ast/G", format="%.1f"),
        "Stl/G": st.column_config.NumberColumn("Stl/G", format="%.1f"),
        "Height": st.column_config.NumberColumn("Height", format="%d cm"),
        "Age": st.column_config.NumberColumn("Age", format="%d"),
        "GP": st.column_config.NumberColumn("GP", format="%d"),
    }

    st.dataframe(
        display,
        column_config=col_config,
        hide_index=True,
        use_container_width=True,
        height=min(700, 40 + len(display) * 35),
    )
