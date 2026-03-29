"""
shared.py — Shared helpers and state for all Streamlit pages.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from streamlit_app.queries import fetch_season_schedule
from streamlit_app.utils.config_loader import (
    get_config,
    get_supported_seasons,
    get_default_season,
    get_default_language,
    get_language_map,
    get_team_colors,
    get_default_accent,
    get_leaders_defaults,
    get_team_name_map,
    get_global_decimals,
)
from streamlit_app.utils.feature_flags import is_feature_enabled, show_disabled_message
from streamlit_app.utils.secrets_manager import OPENAI_API_KEY, REQUIRE_LOGIN

CFG = get_config()
_cfg_seasons = get_supported_seasons()
_cfg_default = get_default_season()

TEAM_COLORS = get_team_colors()
DEFAULT_ACCENT = get_default_accent()
TEAM_NAME_MAP = get_team_name_map()
GLOBAL_DECIMALS = get_global_decimals()


def get_team_logo_map() -> dict:
    """Return a dict of team_code -> logo_url from the schedule in session state."""
    logo_map = st.session_state.get("_team_logo_map")
    if logo_map is not None:
        return logo_map
    logo_map = {}
    schedule = st.session_state.get("schedule", pd.DataFrame())
    if not schedule.empty:
        for _, row in schedule.iterrows():
            hc = row.get("home_code", "")
            ac = row.get("away_code", "")
            hl = row.get("home_logo")
            al = row.get("away_logo")
            if hc and pd.notna(hl) and hl:
                logo_map[hc] = str(hl)
            if ac and pd.notna(al) and al:
                logo_map[ac] = str(al)
    st.session_state["_team_logo_map"] = logo_map
    return logo_map


def get_team_logo_url(team_code: str) -> str:
    """Get logo URL for a team code, with avatar fallback."""
    logo_map = get_team_logo_map()
    url = logo_map.get(team_code, "")
    if not url:
        url = (
            f"https://ui-avatars.com/api/?name={team_code}"
            f"&background=2a2a5a&color=e4e4f0&size=128&rounded=true&bold=true"
        )
    return url


def add_logo_images_to_figure(
    fig, df, x_col, y_col, team_col="team_code", size=0.04,
    selected_team=None, ring_size=50,
):
    """
    Overlay team logo images on a Plotly scatter figure.
    The *selected_team* gets a colored ring (circle border) around its logo.

    Parameters
    ----------
    size : float
        Logo size as a fraction of the axis range.
    selected_team : str or None
        Team code to highlight with a colored ring.
    ring_size : int
        Marker diameter (px) for the ring on the selected team.
    """
    logo_map = get_team_logo_map()
    x_range = df[x_col].max() - df[x_col].min()
    y_range = df[y_col].max() - df[y_col].min()
    sx = x_range * size if x_range > 0 else 1
    sy = y_range * size if y_range > 0 else 1

    # Ring only for the selected team
    if selected_team is not None:
        sel_rows = df[df[team_col] == selected_team]
        if not sel_rows.empty:
            color = TEAM_COLORS.get(selected_team, DEFAULT_ACCENT)[0]
            fig.add_trace(go.Scatter(
                x=sel_rows[x_col], y=sel_rows[y_col],
                mode="markers",
                marker=dict(
                    size=ring_size,
                    color="rgba(0,0,0,0)",
                    line=dict(width=3, color=color),
                ),
                hoverinfo="skip",
                showlegend=False,
            ))

    # Logo images
    for _, row in df.iterrows():
        tc = row[team_col]
        url = logo_map.get(tc, "")
        if not url:
            continue
        fig.add_layout_image(
            dict(
                source=url,
                xref="x", yref="y",
                x=row[x_col], y=row[y_col],
                sizex=sx, sizey=sy,
                xanchor="center", yanchor="middle",
                layer="above",
            )
        )
    return fig


# ========================================================================
# TRANSLATIONS
# ========================================================================
def load_translations():
    with open(Path(_project_root) / "streamlit_app" / "translations.json", "r", encoding="utf-8") as f:
        return json.load(f)


TRANSLATIONS = load_translations()
_FALLBACK_LANG = CFG["app"]["default_language"]


def t(key: str, **kwargs) -> str:
    lang = st.session_state.get("lang", _FALLBACK_LANG)
    default = kwargs.pop("default", key)
    text = TRANSLATIONS.get(key, {}).get(lang, TRANSLATIONS.get(key, {}).get(_FALLBACK_LANG, default))
    return text.format(**kwargs) if kwargs else text


# ========================================================================
# FAVORITE TEAM — Persistence & Selection Dialog
# ========================================================================
import logging as _logging

_fav_logger = _logging.getLogger(__name__)


def _supabase_has_session() -> bool:
    """Return True if the Supabase client holds a valid user session."""
    try:
        from streamlit_app.utils.auth import get_supabase_client
        client = get_supabase_client()
        session = client.auth.get_session()
        return session is not None and session.access_token is not None
    except Exception:
        return False


def _fetch_favorite_team_from_db(email: str) -> str | None:
    """Load favorite_team_code from user_profiles (Supabase)."""
    if not _supabase_has_session():
        return None
    try:
        from streamlit_app.utils.auth import get_supabase_client
        client = get_supabase_client()
        resp = (
            client.table("user_profiles")
            .select("favorite_team_code")
            .eq("user_email", email)
            .limit(1)
            .execute()
        )
        if resp.data:
            return resp.data[0].get("favorite_team_code")
    except Exception as exc:
        _fav_logger.debug("Could not fetch favorite team from DB: %s", exc)
    return None


def _save_favorite_team_to_db(email: str, team_code: str | None) -> bool:
    """Upsert favorite_team_code into user_profiles (Supabase)."""
    if not _supabase_has_session():
        _fav_logger.debug("Skipping favorite team save — no active Supabase session")
        return False
    try:
        from streamlit_app.utils.auth import get_supabase_client
        client = get_supabase_client()
        client.table("user_profiles").upsert(
            {"user_email": email, "favorite_team_code": team_code},
            on_conflict="user_email",
        ).execute()
        return True
    except Exception as exc:
        _fav_logger.debug("Could not save favorite team to DB: %s", exc)
        return False


def init_favorite_team() -> None:
    """Initialise ``st.session_state.favorite_team`` on app load.

    Priority: 1) already set  2) DB (logged-in user)  3) query param ``fav_team``
    """
    if "favorite_team" in st.session_state:
        return

    email = st.session_state.get("user_email")
    if email and REQUIRE_LOGIN:
        db_fav = _fetch_favorite_team_from_db(email)
        if db_fav:
            st.session_state["favorite_team"] = db_fav
            return

    qp_fav = st.query_params.get("fav_team")
    if qp_fav:
        st.session_state["favorite_team"] = qp_fav
        return

    st.session_state["favorite_team"] = None


def save_favorite_team(team_code: str | None) -> None:
    """Persist a favorite-team choice to session + DB + query params."""
    st.session_state["favorite_team"] = team_code
    email = st.session_state.get("user_email")
    if email and REQUIRE_LOGIN:
        _save_favorite_team_to_db(email, team_code)
    if team_code:
        st.query_params["fav_team"] = team_code
    else:
        st.query_params.pop("fav_team", None)


def get_favorite_team() -> str | None:
    """Return the current favorite team code (or None)."""
    return st.session_state.get("favorite_team")


def _build_team_display_name(code: str) -> str:
    """Return ``'CODE — Team Name'`` when a name is known, else just the code."""
    name = TEAM_NAME_MAP.get(code)
    if name:
        return f"{code} — {name}"
    return code


@st.dialog(t("fav_dialog_title", default="Pick Your Favorite Team"))
def show_favorite_team_selector(team_options: list[str] | None = None) -> None:
    """Modal dialog that lets the user pick (or skip) a favorite team."""
    st.markdown(
        f"<p style='color:#9ca3af;'>{t('fav_dialog_subtitle', default='Personalise your dashboard by choosing a favorite team. It will be pre-selected across all views.')}</p>",
        unsafe_allow_html=True,
    )

    if team_options is None:
        schedule = st.session_state.get("schedule", pd.DataFrame())
        if not schedule.empty:
            team_options = sorted(
                list(
                    set(schedule["home_code"].unique())
                    | set(schedule["away_code"].unique())
                )
            )
        else:
            team_options = sorted(TEAM_NAME_MAP.keys()) if TEAM_NAME_MAP else []

    if not team_options:
        st.warning(t("fav_no_teams", default="No teams available yet. Please select a season first."))
        return

    display_map = {code: _build_team_display_name(code) for code in team_options}

    chosen = st.selectbox(
        t("fav_select_label", default="Choose a team"),
        team_options,
        format_func=lambda c: display_map.get(c, c),
        key="_fav_team_dialog_select",
    )

    col_save, col_skip = st.columns(2)
    with col_save:
        if st.button(t("fav_save_btn", default="Save Preference"), type="primary", use_container_width=True):
            save_favorite_team(chosen)
            st.rerun()
    with col_skip:
        if st.button(t("fav_skip_btn", default="Skip for now"), use_container_width=True):
            st.session_state["favorite_team_skipped"] = True
            st.rerun()


def favorite_team_index(team_options: list[str], fallback: int = 0) -> int:
    """Return the index of the favorite team in *team_options*, or *fallback*."""
    fav = get_favorite_team()
    if fav and fav in team_options:
        return team_options.index(fav)
    return fallback


def format_team_option(code: str) -> str:
    """Format a team code for display, prepending a star if it is the favorite."""
    fav = get_favorite_team()
    name = TEAM_NAME_MAP.get(code, "")
    star = "⭐ " if code == fav else ""
    if name:
        return f"{star}{code} — {name}"
    return f"{star}{code}"


# ========================================================================
# AGGRID HELPERS
# ========================================================================
_HEATMAP_JSCODE = JsCode("""
function(params) {
    if (params.value == null) return {};
    var val = parseFloat(params.value);
    if (isNaN(val)) return {};
    if (val > 0) {
        var t = Math.min(val / 15, 1);
        return {
            backgroundColor: 'rgba(16,185,129,' + (0.15 + 0.50 * t) + ')',
            color: '#e4e4f0'
        };
    } else if (val < 0) {
        var t = Math.min(Math.abs(val) / 15, 1);
        return {
            backgroundColor: 'rgba(239,68,68,' + (0.15 + 0.50 * t) + ')',
            color: '#e4e4f0'
        };
    }
    return {color: '#e4e4f0'};
}
""")


def format_df_decimals(df: pd.DataFrame, decimals: int = None) -> pd.DataFrame:
    """Round all float columns in a DataFrame to GLOBAL_DECIMALS (or custom).

    This provides a single source of truth for decimal formatting across the app.
    Change GLOBAL_DECIMALS in config.yaml to adjust globally.

    Args:
        df: DataFrame to format
        decimals: Override decimal places (uses GLOBAL_DECIMALS if None)

    Returns:
        DataFrame with rounded float columns
    """
    if df.empty:
        return df
    decimals = decimals if decimals is not None else GLOBAL_DECIMALS
    df_copy = df.copy()
    float_cols = df_copy.select_dtypes(include=["float64", "float32"]).columns
    for col in float_cols:
        df_copy[col] = df_copy[col].round(decimals)
    return df_copy


def get_decimal_column_config(columns: list[str], decimals: int = None) -> dict:
    """Generate Streamlit column_config for NumberColumn formatting.

    Use with st.dataframe(df, column_config=get_decimal_column_config([...]))

    Args:
        columns: List of column names to format
        decimals: Override decimal places (uses GLOBAL_DECIMALS if None)

    Returns:
        Dict suitable for st.dataframe column_config parameter
    """
    decimals = decimals if decimals is not None else GLOBAL_DECIMALS
    fmt = f"%.{decimals}f"
    return {col: st.column_config.NumberColumn(format=fmt) for col in columns}


def render_aggrid(df, pin_cols=None, pagination=False, page_size=20,
                  heatmap_cols=None, height=400, key="aggrid"):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filterable=True, sortable=True, resizable=True,
        wrapHeaderText=True, autoHeaderHeight=True,
    )
    if pin_cols:
        for col in pin_cols:
            if col in df.columns:
                gb.configure_column(col, pinned="left")
    if pagination:
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size)
    if heatmap_cols:
        for col in heatmap_cols:
            if col in df.columns:
                gb.configure_column(col, cellStyle=_HEATMAP_JSCODE)

    grid_options = gb.build()

    AgGrid(
        df,
        gridOptions=grid_options,
        height=height,
        theme="streamlit",
        enable_enterprise_modules=False,
        allow_unsafe_jscode=True,
        update_mode=GridUpdateMode.NO_UPDATE,
        key=key,
    )
    st.download_button(
        label="📥 Export CSV",
        data=df.to_csv(index=False),
        file_name="euroleague_stats.csv",
        mime="text/csv",
        key=f"csv_{key}",
    )


# ========================================================================
# TEAM BRANDING
# ========================================================================
def get_team_accent() -> tuple:
    team = st.session_state.get("selected_team") or st.session_state.get("_active_home_team")
    if team and team in TEAM_COLORS:
        return TEAM_COLORS[team]
    return DEFAULT_ACCENT


# ========================================================================
# PAGE-SPECIFIC SIDEBAR HELPERS  (used by views under st.navigation)
# ========================================================================
def render_game_sidebar():
    """Render round / game / clutch sidebar controls. Returns the selected gamecode."""
    schedule = st.session_state.get("schedule")
    if schedule is None or (hasattr(schedule, "empty") and schedule.empty):
        st.session_state["game_info_cache"] = None
        return None

    with st.sidebar:
        rounds = sorted(schedule["round"].unique())
        if st.session_state.get("selected_round") not in rounds:
            played_games = schedule[schedule["home_score"].notna() & schedule["away_score"].notna()] if "home_score" in schedule.columns else pd.DataFrame()
            if not played_games.empty:
                st.session_state.selected_round = int(played_games["round"].max())
            else:
                st.session_state.selected_round = rounds[-1] if rounds else 1

        def _fmt_round(r):
            if "round_name" not in schedule.columns:
                return f"Round {r}"
            round_name = schedule[schedule["round"] == r]["round_name"].iloc[0]
            if not round_name:
                return f"Round {r}"
            return f"{round_name}" if "Round" in round_name else f"Round {r} ({round_name})"

        selected_round = st.selectbox(
            t("round_dropdown"),
            rounds,
            index=rounds.index(st.session_state.selected_round)
            if st.session_state.selected_round in rounds
            else 0,
            format_func=_fmt_round,
            key="round_picker",
        )
        st.session_state.selected_round = selected_round

        round_games = schedule[schedule["round"] == selected_round].copy()

        def _fmt_matchup(row):
            home = row.get("home_code", row.get("home_team", "???"))
            away = row.get("away_code", row.get("away_team", "???"))
            if pd.notna(row.get("home_score")) and pd.notna(row.get("away_score")):
                return f"{home}-{away} [{int(row['home_score'])}-{int(row['away_score'])}]"
            return f"{home}-{away} [{t('lbl_upcoming', default='Upcoming')}]"

        round_games["matchup_label"] = round_games.apply(_fmt_matchup, axis=1)
        matchup_dict = {row["matchup_label"]: row.to_dict() for _, row in round_games.iterrows()}
        labels = list(matchup_dict.keys())

        _default_idx = 0
        _url_gc = st.session_state.pop("_url_gamecode", None)
        if _url_gc is not None and "matchup_picker" not in st.session_state:
            for i, (_lbl, ginfo) in enumerate(matchup_dict.items()):
                if ginfo.get("gamecode") == _url_gc:
                    _default_idx = i
                    break
        elif "matchup_picker" not in st.session_state:
            fav = get_favorite_team()
            if fav:
                for i, (_lbl, ginfo) in enumerate(matchup_dict.items()):
                    if fav in (ginfo.get("home_code"), ginfo.get("away_code")):
                        _default_idx = i
                        break

        selected_label = st.selectbox(
            t("matchup_dropdown"),
            labels,
            index=_default_idx,
            key="matchup_picker",
        )

        gamecode = None
        if selected_label:
            selected_game = matchup_dict[selected_label]
            gamecode = selected_game["gamecode"]
            st.session_state["game_info_cache"] = selected_game
            st.session_state["_active_home_team"] = selected_game.get("home_code")
        else:
            st.session_state["game_info_cache"] = None
            st.session_state["_active_home_team"] = None

    return gamecode


def render_team_sidebar():
    """Render team-selector sidebar. Returns the selected team code."""
    schedule = st.session_state.get("schedule")
    if schedule is None or (hasattr(schedule, "empty") and schedule.empty):
        return None

    with st.sidebar:
        euroleague_games = (
            schedule[schedule["played"] == True]
            if "played" in schedule.columns
            else schedule
        )
        if not euroleague_games.empty:
            team_options = sorted(
                list(
                    set(euroleague_games["home_code"].unique())
                    | set(euroleague_games["away_code"].unique())
                )
            )
        else:
            team_options = sorted(
                list(
                    set(schedule["home_code"].unique())
                    | set(schedule["away_code"].unique())
                )
            )
        st.session_state["season_team_codes"] = set(team_options)

        _default_idx = favorite_team_index(team_options, fallback=0)
        _url_team = st.session_state.pop("_url_team", None)
        if _url_team and _url_team in team_options and "team_picker" not in st.session_state:
            _default_idx = team_options.index(_url_team)

        st.session_state["selected_team"] = st.selectbox(
            t("team_dropdown"),
            team_options,
            index=_default_idx,
            format_func=format_team_option,
            key="team_picker",
        )

    return st.session_state["selected_team"]


# ========================================================================
# GAME DATA HELPERS
# ========================================================================
def ensure_game_data(gc: int) -> dict:
    data_needs_update = (
        "game_data" not in st.session_state
        or st.session_state.get("season") != st.session_state.get("selected_season", _cfg_default)
        or st.session_state.get("gamecode") != gc
    )
    if data_needs_update:
        season_to_fetch = st.session_state.get("selected_season", _cfg_default)
        with st.status("Analyzing Game Data...", expanded=True) as status:
            try:
                st.write("⏳ Fetching Play-by-Play from DB...")
                from streamlit_app.queries import _use_db, _get_repository
                from data_pipeline.transformers import (
                    compute_advanced_stats, track_lineups, compute_lineup_stats,
                    compute_duo_trio_synergy, compute_clutch_stats,
                    detect_runs_and_stoppers, foul_trouble_impact,
                    build_assist_network, compute_shot_quality,
                    link_assists_to_shots, compute_playmaking_metrics,
                    compute_total_points_created, compute_on_off_splits,
                )

                repo = _get_repository()
                if repo.db_available() and repo.is_game_cached(season_to_fetch, gc):
                    st.write("✅ Game found in database cache (parallel fetch).")
                    raw = repo.load_game_data_concurrent(season_to_fetch, gc)
                else:
                    st.write("⏳ Downloading boxscore & play-by-play from API...")
                    from data_pipeline.extractors import extract_game_data
                    raw = extract_game_data(season_to_fetch, gc)
                    if repo.db_available():
                        repo._save_raw_to_db(raw, season_to_fetch, gc)

                boxscore_df = raw["boxscore"]
                pbp_df = raw["pbp"]
                shots_df = raw["shots"]
                game_info_df = raw["game_info"]

                st.write("⏳ Reconstructing on-court lineups...")
                pbp_lu = track_lineups(pbp_df, boxscore_df)

                st.write("⏳ Calculating Advanced Metrics...")
                advanced_df = compute_advanced_stats(boxscore_df)
                lineup_stats = compute_lineup_stats(pbp_lu, boxscore_df)
                duo_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=2)
                trio_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=3)
                on_off_splits = compute_on_off_splits(pbp_lu, boxscore_df)

                st.write("⏳ Analyzing clutch situations & momentum...")
                clutch = compute_clutch_stats(pbp_df, boxscore_df)
                stoppers = detect_runs_and_stoppers(pbp_lu)
                foul_impact = foul_trouble_impact(pbp_df, boxscore_df)

                st.write("⏳ Building assist networks & shot quality...")
                assists = build_assist_network(pbp_df)
                shot_quality = compute_shot_quality(shots_df)
                assist_shot_links = link_assists_to_shots(pbp_df, shots_df)
                playmaking = compute_playmaking_metrics(assist_shot_links, min_assists=1)
                advanced_df = compute_total_points_created(advanced_df, assist_shot_links)

                game_data = {
                    "boxscore": boxscore_df,
                    "pbp": pbp_df,
                    "shots": shots_df,
                    "game_info": game_info_df,
                    "advanced_stats": advanced_df,
                    "pbp_with_lineups": pbp_lu,
                    "lineup_stats": lineup_stats,
                    "assist_network": assists,
                    "clutch_stats": clutch,
                    "run_stoppers": stoppers,
                    "foul_trouble": foul_impact,
                    "duo_synergy": duo_synergy,
                    "trio_synergy": trio_synergy,
                    "on_off_splits": on_off_splits,
                    "shot_quality": shot_quality,
                    "assist_shot_links": assist_shot_links,
                    "playmaking_aaq": playmaking["aaq"],
                    "playmaking_axp": playmaking["axp"],
                    "playmaking_duos": playmaking["duos"],
                }

                st.session_state["game_data"] = game_data
                st.session_state["season"] = season_to_fetch
                st.session_state["gamecode"] = gc
                status.update(label="Analysis Complete!", state="complete", expanded=False)
            except ConnectionError:
                status.update(label="Connection failed", state="error", expanded=True)
                st.error("Could not connect to the Euroleague API. Please check your internet connection and try again.")
                st.stop()
            except Exception as e:
                status.update(label="Data load failed", state="error", expanded=True)
                st.error(f"Failed to load game data for Game {gc}. The API may be temporarily unavailable. Error: {type(e).__name__}")
                st.stop()
    return st.session_state["game_data"]


def apply_clutch_filter(data: dict) -> dict:
    from data_pipeline.transformers import (
        filter_clutch_time, filter_clutch_shots, build_clutch_boxscore,
        compute_advanced_stats, track_lineups, compute_lineup_stats,
        compute_duo_trio_synergy, compute_clutch_stats, detect_runs_and_stoppers,
        foul_trouble_impact, build_assist_network, compute_shot_quality,
        link_assists_to_shots, compute_playmaking_metrics, compute_total_points_created,
        compute_on_off_splits,
    )

    with st.status("Applying Clutch Filter...", expanded=True) as status:
        pbp_df = data.get("pbp", pd.DataFrame())
        shots_df = data.get("shots", pd.DataFrame())
        original_box = data.get("boxscore", pd.DataFrame())

        st.write("⏳ Reconstructing on-court lineups for full game...")
        pbp_with_lineups = track_lineups(pbp_df, original_box)

        st.write("⏳ Filtering to clutch-time plays...")
        clutch_pbp = filter_clutch_time(pbp_with_lineups)
        clutch_shots = filter_clutch_shots(shots_df)

        if clutch_pbp.empty:
            status.update(label="No clutch-time data found", state="complete", expanded=False)
            st.warning("No clutch-time plays found in this game (Q4/OT, ≤5 min left, ≤5 pt differential).")
            return data

        clutch_box = build_clutch_boxscore(clutch_pbp, original_box)
        if clutch_box.empty:
            status.update(label="No clutch-time data found", state="complete", expanded=False)
            return data

        st.write("⏳ Calculating clutch advanced metrics...")
        advanced_df = compute_advanced_stats(clutch_box)
        pbp_lu = clutch_pbp
        lineup_stats = compute_lineup_stats(pbp_lu, clutch_box)
        duo_synergy = compute_duo_trio_synergy(pbp_lu, clutch_box, combo_size=2)
        trio_synergy = compute_duo_trio_synergy(pbp_lu, clutch_box, combo_size=3)
        on_off_splits = compute_on_off_splits(pbp_lu, clutch_box)

        st.write("⏳ Detecting runs, stoppers & foul impact...")
        clutch_stats = compute_clutch_stats(clutch_pbp, clutch_box)
        stoppers = detect_runs_and_stoppers(pbp_lu)
        foul_impact = foul_trouble_impact(clutch_pbp, clutch_box)

        st.write("⏳ Building clutch assist network & shot quality...")
        assists = build_assist_network(clutch_pbp)
        shot_quality = compute_shot_quality(clutch_shots)
        assist_shot_links = link_assists_to_shots(clutch_pbp, clutch_shots)
        playmaking = compute_playmaking_metrics(assist_shot_links, min_assists=1)
        advanced_df = compute_total_points_created(advanced_df, assist_shot_links)

        status.update(label="Clutch Analysis Complete!", state="complete", expanded=False)

    return {
        "boxscore": clutch_box, "pbp": clutch_pbp, "shots": clutch_shots,
        "game_info": data.get("game_info", pd.DataFrame()),
        "advanced_stats": advanced_df, "pbp_with_lineups": pbp_lu,
        "lineup_stats": lineup_stats, "assist_network": assists,
        "clutch_stats": clutch_stats, "run_stoppers": stoppers,
        "foul_trouble": foul_impact, "duo_synergy": duo_synergy,
        "trio_synergy": trio_synergy, "on_off_splits": on_off_splits,
        "shot_quality": shot_quality,
        "assist_shot_links": assist_shot_links,
        "playmaking_aaq": playmaking.get("aaq", pd.DataFrame()),
        "playmaking_axp": playmaking.get("axp", pd.DataFrame()),
        "playmaking_duos": playmaking.get("duos", pd.DataFrame()),
    }


def _resolve_team_name(code: str, name_from_data: str = None) -> str:
    """Resolve a team display name using data first, then config fallback."""
    if name_from_data and name_from_data not in ("???", "", None) and name_from_data != code:
        return name_from_data
    return TEAM_NAME_MAP.get(code, code)


def render_game_header():
    if "game_info_cache" in st.session_state and st.session_state["game_info_cache"]:
        gi = st.session_state["game_info_cache"]
        home_code = gi.get("home_code", "HOM")
        away_code = gi.get("away_code", "AWA")
        home_name = _resolve_team_name(home_code, gi.get("home_name"))
        away_name = _resolve_team_name(away_code, gi.get("away_name"))
        hs = f"{int(gi['home_score'])}" if pd.notna(gi.get("home_score")) else "-"
        as_ = f"{int(gi['away_score'])}" if pd.notna(gi.get("away_score")) else "-"
        _hl = gi.get("home_logo")
        _al = gi.get("away_logo")
        home_logo = str(_hl) if pd.notna(_hl) and _hl else ""
        away_logo = str(_al) if pd.notna(_al) and _al else ""
    else:
        data = st.session_state.get("game_data", {})
        gi_df = data.get("game_info", pd.DataFrame())
        local_gi = gi_df.iloc[0] if not gi_df.empty else {}
        home_code = local_gi.get("home_team", "???")
        away_code = local_gi.get("away_team", "???")
        home_name = _resolve_team_name(home_code)
        away_name = _resolve_team_name(away_code)
        hs = local_gi.get("home_score", "-")
        as_ = local_gi.get("away_score", "-")
        home_logo, away_logo = "", ""

    fb_home = f"https://ui-avatars.com/api/?name={home_code}&background=2a2a5a&color=e4e4f0&size=128&rounded=true&bold=true"
    fb_away = f"https://ui-avatars.com/api/?name={away_code}&background=2a2a5a&color=e4e4f0&size=128&rounded=true&bold=true"
    hl = home_logo or fb_home
    al = away_logo or fb_away

    home_clr = TEAM_COLORS.get(home_code, DEFAULT_ACCENT)[0]
    away_clr = TEAM_COLORS.get(away_code, DEFAULT_ACCENT)[0]

    st.markdown(
        f'<div class="game-header">'
        f'  <div class="team-block">'
        f'    <img src="{hl}" class="team-logo" alt="{home_code}" onerror="this.onerror=null; this.src=\'{fb_home}\';">'
        f'    <span class="team-name" style="color:{home_clr};">{home_name}</span>'
        f"  </div>"
        f'  <span class="score">{hs}<span class="dash"> — </span>{as_}</span>'
        f'  <div class="team-block">'
        f'    <img src="{al}" class="team-logo" alt="{away_code}" onerror="this.onerror=null; this.src=\'{fb_away}\';">'
        f'    <span class="team-name" style="color:{away_clr};">{away_name}</span>'
        f"  </div>"
        f"</div>"
        f'<p style="text-align:center; color:#6b7280; margin-top:-8px;">Season {st.session_state.get("season","")} &bull; Game {st.session_state.get("gamecode","")}</p>',
        unsafe_allow_html=True,
    )
    st.markdown("")


# ========================================================================
# UI COMPONENTS: Page Headers & Skeleton Loaders
# ========================================================================
def render_page_header(title: str, subtitle: str = None, icon: str = None) -> None:
    """Render a consistent page header with optional subtitle.

    Args:
        title: Main page title
        subtitle: Optional subtitle/description
        icon: Optional emoji icon (prepended to title)

    Example:
        render_page_header("Season Overview", "Team performance across the season", "📊")
    """
    display_title = f"{icon} {title}" if icon else title
    st.markdown(
        f'<h1 class="section-header" style="font-size: 2rem; margin-bottom: 0.25rem;">{display_title}</h1>',
        unsafe_allow_html=True,
    )
    if subtitle:
        st.markdown(
            f'<p style="color: #9ca3af; font-size: 0.95rem; margin-top: 0; margin-bottom: 1.5rem;">{subtitle}</p>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown("<div style='margin-bottom: 1rem;'></div>", unsafe_allow_html=True)


_SKELETON_CSS_INJECTED = False


def render_skeleton_loader(
    height: int = 200,
    width: str = "100%",
    border_radius: int = 8,
    count: int = 1,
    gap: int = 12,
) -> None:
    """Render a shimmering skeleton placeholder for loading states.

    Args:
        height: Height of each skeleton block in pixels
        width: Width (CSS value, e.g., "100%", "300px")
        border_radius: Corner radius in pixels
        count: Number of skeleton blocks to render
        gap: Gap between multiple blocks in pixels

    Example:
        placeholder = st.empty()
        with placeholder.container():
            render_skeleton_loader(height=300)
        # ... fetch data ...
        placeholder.plotly_chart(fig)
    """
    global _SKELETON_CSS_INJECTED

    css = ""
    if not _SKELETON_CSS_INJECTED:
        css = """
        <style>
            @keyframes skeleton-shimmer {
                0% {
                    background-position: -200% 0;
                }
                100% {
                    background-position: 200% 0;
                }
            }
            .skeleton-loader {
                background: linear-gradient(
                    90deg,
                    rgba(30, 30, 63, 0.8) 0%,
                    rgba(50, 50, 90, 0.9) 20%,
                    rgba(70, 70, 120, 1) 40%,
                    rgba(50, 50, 90, 0.9) 60%,
                    rgba(30, 30, 63, 0.8) 100%
                );
                background-size: 200% 100%;
                animation: skeleton-shimmer 1.5s ease-in-out infinite;
                border-radius: var(--skeleton-radius, 8px);
            }
            .skeleton-container {
                display: flex;
                flex-direction: column;
            }
        </style>
        """
        _SKELETON_CSS_INJECTED = True

    skeletons = "".join([
        f'<div class="skeleton-loader" style="'
        f'height: {height}px; '
        f'width: {width}; '
        f'--skeleton-radius: {border_radius}px; '
        f'margin-bottom: {gap if i < count - 1 else 0}px;'
        f'"></div>'
        for i in range(count)
    ])

    st.markdown(
        f'{css}<div class="skeleton-container">{skeletons}</div>',
        unsafe_allow_html=True,
    )


def render_skeleton_table(rows: int = 5, cols: int = 4, row_height: int = 40) -> None:
    """Render a table-shaped skeleton loader.

    Args:
        rows: Number of skeleton rows
        cols: Number of columns
        row_height: Height of each row in pixels
    """
    global _SKELETON_CSS_INJECTED

    css = ""
    if not _SKELETON_CSS_INJECTED:
        css = """
        <style>
            @keyframes skeleton-shimmer {
                0% { background-position: -200% 0; }
                100% { background-position: 200% 0; }
            }
            .skeleton-loader {
                background: linear-gradient(
                    90deg,
                    rgba(30, 30, 63, 0.8) 0%,
                    rgba(50, 50, 90, 0.9) 20%,
                    rgba(70, 70, 120, 1) 40%,
                    rgba(50, 50, 90, 0.9) 60%,
                    rgba(30, 30, 63, 0.8) 100%
                );
                background-size: 200% 100%;
                animation: skeleton-shimmer 1.5s ease-in-out infinite;
            }
        </style>
        """
        _SKELETON_CSS_INJECTED = True

    header_cells = "".join([
        f'<div class="skeleton-loader" style="height: 20px; border-radius: 4px;"></div>'
        for _ in range(cols)
    ])
    header_row = f'<div style="display: grid; grid-template-columns: repeat({cols}, 1fr); gap: 12px; padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.1);">{header_cells}</div>'

    body_rows = ""
    for _ in range(rows):
        cells = "".join([
            f'<div class="skeleton-loader" style="height: 16px; border-radius: 4px;"></div>'
            for _ in range(cols)
        ])
        body_rows += f'<div style="display: grid; grid-template-columns: repeat({cols}, 1fr); gap: 12px; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">{cells}</div>'

    st.markdown(
        f'{css}<div style="background: rgba(15,15,35,0.5); border-radius: 8px; padding: 8px 16px;">{header_row}{body_rows}</div>',
        unsafe_allow_html=True,
    )


# ========================================================================
# ADVANCED CONTENT-AWARE SKELETON LOADERS
# ========================================================================
_ADV_SKELETON_CSS = """
<style>
    @keyframes skel-shimmer {
        0%   { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }
    .skel-bar {
        background: linear-gradient(
            90deg,
            rgba(30,30,63,0.8) 0%,
            rgba(50,50,90,0.9) 20%,
            rgba(70,70,120,1.0) 40%,
            rgba(50,50,90,0.9) 60%,
            rgba(30,30,63,0.8) 100%
        );
        background-size: 200% 100%;
        animation: skel-shimmer 1.5s ease-in-out infinite;
        border-radius: 4px;
    }
    .skel-kpi-card {
        background: linear-gradient(135deg, #1e1e3f 0%, #2a2a5a 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 20px 16px;
        display: flex;
        flex-direction: column;
        gap: 10px;
    }
    .skel-kpi-row {
        display: grid;
        gap: 16px;
    }
    .skel-table-wrap {
        background: rgba(15,15,35,0.5);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 8px;
        padding: 12px 16px;
        overflow: hidden;
    }
    .skel-tr {
        display: grid;
        gap: 12px;
        padding: 10px 0;
    }
    .skel-tr-header { border-bottom: 2px solid rgba(99,102,241,0.25); }
    .skel-tr-body   { border-bottom: 1px solid rgba(255,255,255,0.04); }
    .skel-chart-wrap {
        background: linear-gradient(135deg, #1e1e3f 0%, #2a2a5a 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 20px;
        position: relative;
        overflow: hidden;
    }
    .skel-chart-yaxis {
        position: absolute;
        left: 20px;
        top: 50px;
        bottom: 50px;
        width: 6px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .skel-chart-yaxis .skel-bar { width: 30px; height: 8px; opacity: 0.4; }
    .skel-chart-xaxis {
        position: absolute;
        left: 60px;
        right: 20px;
        bottom: 20px;
        height: 8px;
        display: flex;
        gap: 12px;
        justify-content: space-around;
    }
    .skel-chart-xaxis .skel-bar { flex: 1; height: 8px; opacity: 0.4; }
    .skel-chart-bars {
        display: flex;
        align-items: flex-end;
        gap: 8px;
        margin-left: 44px;
        margin-right: 16px;
        margin-bottom: 30px;
    }
    .skel-chart-bars .skel-bar { flex: 1; border-radius: 4px 4px 0 0; }
</style>
"""

_ADV_SKELETON_CSS_INJECTED = False


def _ensure_adv_skeleton_css() -> str:
    global _ADV_SKELETON_CSS_INJECTED
    if not _ADV_SKELETON_CSS_INJECTED:
        _ADV_SKELETON_CSS_INJECTED = True
        return _ADV_SKELETON_CSS
    return ""


def skeleton_kpi_row(columns: int = 3) -> None:
    """Render a shimmer skeleton that mimics a row of st.metric KPI cards.

    Args:
        columns: Number of KPI card placeholders to show (1-6).
    """
    css = _ensure_adv_skeleton_css()
    cards = ""
    for _ in range(columns):
        cards += (
            '<div class="skel-kpi-card">'
            '  <div class="skel-bar" style="width:55%;height:12px;opacity:0.5;"></div>'
            '  <div class="skel-bar" style="width:40%;height:28px;"></div>'
            '  <div class="skel-bar" style="width:35%;height:10px;opacity:0.4;"></div>'
            '</div>'
        )
    html = (
        f'{css}'
        f'<div class="skel-kpi-row" style="grid-template-columns:repeat({columns},1fr);">'
        f'{cards}'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def skeleton_dataframe(rows: int = 5, cols: int = 5) -> None:
    """Render a shimmer skeleton that mimics a data table with header and rows.

    Args:
        rows: Number of body rows.
        cols: Number of columns.
    """
    css = _ensure_adv_skeleton_css()
    col_tpl = f"repeat({cols}, 1fr)"

    header_cells = "".join(
        f'<div class="skel-bar" style="height:14px;opacity:0.6;"></div>'
        for _ in range(cols)
    )
    header = (
        f'<div class="skel-tr skel-tr-header" style="grid-template-columns:{col_tpl};">'
        f'{header_cells}'
        f'</div>'
    )

    body = ""
    for i in range(rows):
        width_variation = [("90%", "0.5"), ("75%", "0.45"), ("85%", "0.5"), ("65%", "0.4"), ("80%", "0.45")]
        cells = ""
        for c in range(cols):
            w, o = width_variation[c % len(width_variation)]
            cells += f'<div class="skel-bar" style="height:12px;width:{w};opacity:{o};"></div>'
        body += (
            f'<div class="skel-tr skel-tr-body" style="grid-template-columns:{col_tpl};">'
            f'{cells}'
            f'</div>'
        )

    html = (
        f'{css}'
        f'<div class="skel-table-wrap">'
        f'{header}{body}'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def skeleton_chart(height: int = 300) -> None:
    """Render a shimmer skeleton that mimics a Plotly/Altair chart area.

    Includes faux Y-axis ticks, X-axis labels, and randomised bar heights
    to give the illusion of a real chart loading.

    Args:
        height: Total height of the chart skeleton in pixels.
    """
    css = _ensure_adv_skeleton_css()
    usable = height - 70  # space for axis labels and padding

    bar_heights = [int(usable * p) for p in (0.65, 0.85, 0.45, 0.72, 0.55, 0.90, 0.38)]
    bars = "".join(
        f'<div class="skel-bar" style="height:{h}px;"></div>'
        for h in bar_heights
    )

    y_ticks = "".join(
        '<div class="skel-bar"></div>' for _ in range(5)
    )
    x_ticks = "".join(
        '<div class="skel-bar"></div>' for _ in range(7)
    )

    # Title placeholder
    title_bar = '<div class="skel-bar" style="width:35%;height:14px;margin-bottom:12px;opacity:0.5;"></div>'

    html = (
        f'{css}'
        f'<div class="skel-chart-wrap" style="height:{height}px;">'
        f'  {title_bar}'
        f'  <div class="skel-chart-yaxis">{y_ticks}</div>'
        f'  <div class="skel-chart-bars" style="height:{usable}px;">{bars}</div>'
        f'  <div class="skel-chart-xaxis">{x_ticks}</div>'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)
