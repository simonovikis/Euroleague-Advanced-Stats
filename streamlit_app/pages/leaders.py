import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.shared import (
    t, render_aggrid, TEAM_COLORS, DEFAULT_ACCENT, _cfg_default,
)
from streamlit_app.utils.config_loader import get_leaders_defaults


def render():
    st.markdown(f'<p class="section-header">{t("leaders_title")}</p>', unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('leaders_subtitle')}</p>",
        unsafe_allow_html=True,
    )

    season_leaders = st.session_state.get("selected_season", _cfg_default)

    from streamlit_app.queries import fetch_league_leaders

    with st.status(t("leaders_loading"), expanded=True) as _leaders_status:
        try:
            _leaders_status.update(label="Fetching per-game and accumulated player stats from API...")
            leaders_data = fetch_league_leaders(season_leaders)
            _leaders_status.update(label="Leaderboard data loaded.", state="complete", expanded=False)
        except Exception as e:
            _leaders_status.update(label="Failed to load leaders", state="error")
            st.error(f"Could not fetch league leader stats. The API may be temporarily unavailable. Error: {type(e).__name__}")
            st.stop()

    per_game_df = leaders_data.get("per_game", pd.DataFrame())
    totals_df = leaders_data.get("totals", pd.DataFrame())

    if per_game_df.empty and totals_df.empty:
        st.warning(t("err_no_schedule", season=season_leaders))
        st.stop()

    # --- View toggle ---
    view_mode = st.radio(
        t("leaders_view_by"),
        [t("leaders_per_game"), t("leaders_totals")],
        horizontal=True,
        key="leaders_view_mode",
    )
    is_per_game = view_mode == t("leaders_per_game")
    df = per_game_df if is_per_game else totals_df

    if df.empty:
        st.warning(t("leaders_no_data"))
        st.stop()

    # --- Configurable filters (from config.yaml) ---
    _leaders_cfg = get_leaders_defaults()
    MIN_GAMES_DEFAULT = _leaders_cfg["min_games"]
    MIN_FGA2_DEFAULT = _leaders_cfg["min_fga2"]
    MIN_FGA3_DEFAULT = _leaders_cfg["min_fga3"]
    MIN_FTA_DEFAULT = _leaders_cfg["min_fta"]

    with st.expander(t("leaders_filters_label"), expanded=False):
        fc1, fc2, fc3, fc4 = st.columns(4)
        min_games = fc1.number_input(
            t("leaders_min_games"), min_value=1, max_value=40,
            value=MIN_GAMES_DEFAULT, key="leaders_min_gp",
        )
        min_fga2 = fc2.number_input(
            t("leaders_min_fga2"), min_value=1, max_value=200,
            value=MIN_FGA2_DEFAULT, key="leaders_min_fga2",
        )
        min_fga3 = fc3.number_input(
            t("leaders_min_fga3"), min_value=1, max_value=200,
            value=MIN_FGA3_DEFAULT, key="leaders_min_fga3",
        )
        min_fta = fc4.number_input(
            t("leaders_min_fta"), min_value=1, max_value=200,
            value=MIN_FTA_DEFAULT, key="leaders_min_fta",
        )

    # Apply games filter to all volume leaderboards
    vol_df = df[df["games"] >= min_games].copy()

    # For percentage thresholds we need accumulated attempts.
    # When viewing PerGame, attempts are per-game averages; multiply by games.
    if is_per_game:
        vol_df["_total_fga2"] = vol_df["fga2"] * vol_df["games"]
        vol_df["_total_fga3"] = vol_df["fga3"] * vol_df["games"]
        vol_df["_total_fta"] = vol_df["fta"] * vol_df["games"]
    else:
        vol_df["_total_fga2"] = vol_df["fga2"]
        vol_df["_total_fga3"] = vol_df["fga3"]
        vol_df["_total_fta"] = vol_df["fta"]

    pct2_df = vol_df[vol_df["_total_fga2"] >= min_fga2]
    pct3_df = vol_df[vol_df["_total_fga3"] >= min_fga3]
    pctft_df = vol_df[vol_df["_total_fta"] >= min_fta]

    st.markdown("---")

    # --- Helper to render a Top-10 leaderboard card ---
    def _render_leaderboard(data: pd.DataFrame, stat_col: str, title: str,
                            fmt: str = ".1f", suffix: str = "", top_n: int = 10):
        """Render a Top-N leaderboard as styled HTML inside a column."""
        if data.empty or stat_col not in data.columns:
            st.info(f"No data for {title}")
            return
        board = data.nlargest(top_n, stat_col)[["player_name", "team_code", stat_col]].reset_index(drop=True)
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#1e1e3f,#2a2a5a);'
            f'border:1px solid rgba(255,255,255,0.08);border-radius:14px;'
            f'padding:16px 14px;margin-bottom:12px;">'
            f'<div style="color:#6366f1;font-weight:700;font-size:1.05rem;'
            f'margin-bottom:10px;text-align:center;">{title}</div>',
            unsafe_allow_html=True,
        )
        rows_html = ""
        for rank, (_, r) in enumerate(board.iterrows(), 1):
            val = r[stat_col]
            if fmt == "pct":
                val_str = f"{val:.1f}%"
            else:
                val_str = f"{val:{fmt}}{suffix}"
            bg = "rgba(99,102,241,0.15)" if rank <= 3 else "transparent"
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"<span style='color:#6b7280;'>{rank}.</span>")
            rows_html += (
                f'<div style="display:flex;align-items:center;justify-content:space-between;'
                f'padding:6px 8px;background:{bg};border-radius:6px;margin-bottom:3px;">'
                f'<span style="color:#e4e4f0;font-size:0.88rem;">'
                f'{medal} <b>{r["player_name"]}</b> '
                f'<span style="color:#6b7280;font-size:0.75rem;">{r["team_code"]}</span></span>'
                f'<span style="color:#f59e0b;font-weight:700;font-size:0.92rem;">{val_str}</span>'
                f'</div>'
            )
        st.markdown(rows_html + "</div>", unsafe_allow_html=True)

    # ================================================================
    # VOLUME STAT LEADERBOARDS
    # ================================================================
    st.markdown(f"### {t('leaders_volume_title')}")
    v1, v2, v3, v4 = st.columns(4)
    with v1:
        _render_leaderboard(vol_df, "points", t("leaders_top_scorers"))
    with v2:
        _render_leaderboard(vol_df, "rebounds", t("leaders_top_rebounders"))
    with v3:
        _render_leaderboard(vol_df, "assists", t("leaders_top_assists"))
    with v4:
        _render_leaderboard(vol_df, "turnovers", t("leaders_top_turnovers"))

    st.markdown("---")

    # ================================================================
    # SHOOTING EFFICIENCY LEADERBOARDS
    # ================================================================
    st.markdown(f"### {t('leaders_efficiency_title')}")
    e1, e2, e3 = st.columns(3)
    with e1:
        _render_leaderboard(pct2_df, "fg2_pct", t("leaders_top_fg2"), fmt="pct")
    with e2:
        _render_leaderboard(pct3_df, "fg3_pct", t("leaders_top_fg3"), fmt="pct")
    with e3:
        _render_leaderboard(pctft_df, "ft_pct", t("leaders_top_ft"), fmt="pct")

    # ================================================================
    # FULL INTERACTIVE STATS TABLE (AgGrid)
    # ================================================================
    st.markdown("---")
    st.markdown(f"### {t('leaders_full_table', default='Full Stats Table')}")
    leaders_table_cols = [c for c in [
        "player_name", "team_code", "games", "minutes", "points",
        "rebounds", "assists", "steals", "blocks", "turnovers",
        "fgm2", "fga2", "fg2_pct", "fgm3", "fga3", "fg3_pct",
        "ftm", "fta", "ft_pct",
    ] if c in vol_df.columns]
    leaders_grid_df = vol_df[leaders_table_cols].round(1).copy()
    leaders_grid_df = leaders_grid_df.rename(columns={
        "player_name": t("col_player"), "team_code": t("col_team"),
        "games": "GP", "minutes": t("col_min"), "points": t("col_pts"),
        "rebounds": "REB", "assists": "AST", "steals": "STL",
        "blocks": "BLK", "turnovers": "TOV",
        "fgm2": "2PM", "fga2": "2PA", "fg2_pct": "2P%",
        "fgm3": "3PM", "fga3": "3PA", "fg3_pct": "3P%",
        "ftm": "FTM", "fta": "FTA", "ft_pct": "FT%",
    })
    render_aggrid(
        leaders_grid_df,
        pin_cols=[t("col_player"), t("col_team")],
        pagination=True,
        page_size=20,
        height=600,
        key="league_leaders_table",
    )
