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
)
from streamlit_app.utils.feature_flags import is_feature_enabled, show_disabled_message
from streamlit_app.utils.secrets_manager import OPENAI_API_KEY, REQUIRE_LOGIN

CFG = get_config()
_cfg_seasons = get_supported_seasons()
_cfg_default = get_default_season()

TEAM_COLORS = get_team_colors()
DEFAULT_ACCENT = get_default_accent()
TEAM_NAME_MAP = get_team_name_map()


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
    text = TRANSLATIONS.get(key, {}).get(lang, TRANSLATIONS.get(key, {}).get(_FALLBACK_LANG, key))
    return text.format(**kwargs) if kwargs else text


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
            st.session_state.selected_round = rounds[0] if rounds else 1

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

        st.markdown("---")
        clutch_mode = st.toggle(
            "🧊 Isolate Clutch Time Only",
            value=st.session_state.get("clutch_mode", False),
            key="clutch_toggle",
            help="Recalculate all stats for Clutch Time only: last 5 min of Q4/OT, score within 5 pts.",
        )
        st.session_state["clutch_mode"] = clutch_mode
        if clutch_mode:
            st.caption("Showing clutch-time stats only (Q4/OT, ≤5 min, ≤5 pt diff)")

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

        _default_idx = 0
        _url_team = st.session_state.pop("_url_team", None)
        if _url_team and _url_team in team_options and "team_picker" not in st.session_state:
            _default_idx = team_options.index(_url_team)

        st.session_state["selected_team"] = st.selectbox(
            t("team_dropdown"),
            team_options,
            index=_default_idx,
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
