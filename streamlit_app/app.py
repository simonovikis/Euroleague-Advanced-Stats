"""
app.py — Euroleague Advanced Analytics Dashboard (Router)
==========================================================
Thin router that handles page config, authentication, navigation,
and sidebar filters, then dispatches to per-page modules under
streamlit_app/pages/.

Launch:  streamlit run streamlit_app/app.py
"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from streamlit_app.shared import (
    CFG, TEAM_COLORS, DEFAULT_ACCENT,
    _cfg_seasons, _cfg_default,
    t, render_aggrid, get_team_accent,
    ensure_game_data, apply_clutch_filter, render_game_header,
    fetch_season_schedule,
    is_feature_enabled, show_disabled_message,
    REQUIRE_LOGIN, OPENAI_API_KEY,
)
from streamlit_app.utils.config_loader import (
    get_default_language, get_language_map,
)
from streamlit_app.utils.auth import init_auth_state, render_auth_page, render_user_sidebar

# ========================================================================
# PAGE CONFIG
# ========================================================================
st.set_page_config(
    page_title=CFG["app"]["page_title"],
    page_icon=CFG["app"]["page_icon"],
    layout=CFG["app"]["layout"],
    initial_sidebar_state="expanded",
)

# ========================================================================
# CUSTOM CSS
# ========================================================================
st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .stApp { font-family: 'Inter', sans-serif; }

    /* Sidebar */
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0f0f23 0%, #1a1a3e 100%); }
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 { color: #e4e4f0; }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e1e3f 0%, #2a2a5a 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    [data-testid="stMetric"] label { color: #9ca3af !important; font-size: 0.85rem !important; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { color: #f0f0ff !important; font-weight: 700 !important; }

    .stDataFrame { border-radius: 8px; overflow: hidden; }

    .section-header {
        background: linear-gradient(90deg, #6366f1 0%, #8b5cf6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }

    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] { border-radius: 8px 8px 0 0; padding: 8px 20px; }

    /* Game header */
    .game-header { display: flex; align-items: center; justify-content: center; gap: 24px; padding: 16px 0; }
    .game-header .team-block { display: flex; flex-direction: column; align-items: center; gap: 6px; }
    .game-header .team-logo { width: 64px; height: 64px; object-fit: contain; }
    .game-header .team-name { font-size: 1.1rem; font-weight: 600; }
    .game-header .score { font-size: 2.4rem; font-weight: 700; color: #e4e4f0; letter-spacing: 2px; }
    .game-header .dash { color: #6b7280; font-size: 2rem; margin: 0 4px; }

    /* Landing page cards */
    .landing-card {
        background: linear-gradient(135deg, #1e1e3f 0%, #2a2a5a 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 28px 20px;
        text-align: center;
        min-height: 220px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .landing-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 30px rgba(99, 102, 241, 0.25);
        border-color: rgba(99, 102, 241, 0.3);
    }
    .landing-card .card-icon { font-size: 2.5rem; margin-bottom: 12px; }
    .landing-card h3 { color: #e4e4f0 !important; font-size: 1.15rem; font-weight: 600; margin-bottom: 8px; }
    .landing-card p { color: #9ca3af; font-size: 0.88rem; line-height: 1.5; margin: 0; }
</style>
""",
    unsafe_allow_html=True,
)

# ========================================================================
# AUTHENTICATION GATE
# ========================================================================
if REQUIRE_LOGIN:
    init_auth_state()
    if not st.session_state.get("authenticated"):
        render_auth_page()
        st.stop()


# ========================================================================
# DYNAMIC TEAM BRANDING
# ========================================================================
def _inject_team_css(primary: str, secondary: str):
    def hex_to_rgba(h, a=0.25):
        h = h.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{a})"

    pri_light = hex_to_rgba(primary, 0.15)
    pri_glow = hex_to_rgba(primary, 0.25)

    st.markdown(
        f"""
<style>
    .section-header {{
        background: linear-gradient(90deg, {primary} 0%, {secondary} 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
    }}
    .stTabs [data-baseweb="tab-highlight"] {{ background-color: {primary} !important; }}
    .stApp hr {{ border-color: {hex_to_rgba(primary, 0.35)} !important; }}
    .stApp button[kind="secondary"]:hover,
    .stApp .stButton > button:hover {{
        border-color: {primary} !important;
        color: {primary} !important;
        box-shadow: 0 0 8px {pri_glow};
    }}
    .stApp [data-baseweb="select"] [data-baseweb="input"]:focus-within {{
        border-color: {primary} !important;
        box-shadow: 0 0 0 1px {primary} !important;
    }}
    [data-testid="stMetric"]:hover {{
        border-color: {hex_to_rgba(primary, 0.40)} !important;
        box-shadow: 0 4px 20px {pri_light} !important;
    }}
    [data-testid="stMetricDelta"] svg[data-testid="stMetricDeltaIcon-Up"] {{ fill: {primary} !important; }}
    [data-testid="stMetricDelta"][style*="color"] {{ color: {primary} !important; }}
    .landing-card:hover {{
        box-shadow: 0 8px 30px {pri_glow} !important;
        border-color: {hex_to_rgba(primary, 0.3)} !important;
    }}
    .nav-link-selected {{ background-color: {primary} !important; }}
    .stDataFrame [data-testid="glideDataEditor"] .dvn-scroller .dvn-header {{
        background-color: {hex_to_rgba(primary, 0.10)} !important;
    }}
</style>
""",
        unsafe_allow_html=True,
    )


# ========================================================================
# SIDEBAR — Language Selector
# ========================================================================
with st.sidebar:
    lang_map = get_language_map()
    _default_lang = get_default_language()
    default_lang_idx = list(lang_map.values()).index(st.session_state.get("lang", _default_lang))

    selected_lang_label = st.selectbox(
        t("lang_selection"),
        list(lang_map.keys()),
        index=default_lang_idx,
        label_visibility="collapsed",
    )
    if st.session_state.get("lang") != lang_map[selected_lang_label]:
        st.session_state["lang"] = lang_map[selected_lang_label]
        st.rerun()

    st.markdown(f"# {t('app_title')}")
    if REQUIRE_LOGIN:
        render_user_sidebar()
    st.markdown("---")

# ========================================================================
# TOP NAVIGATION
# ========================================================================
NAV_HOME = t("nav_home_label")
NAV_SINGLE = t("nav_single_label")
NAV_SEASON = t("nav_season_label")
NAV_ADVANCED = t("nav_advanced_label")
NAV_LIVE = t("nav_live_label")
NAV_LEADERS = t("nav_leaders_label")
NAV_SCOUTING = t("nav_scouting_label")
NAV_REFEREE = t("nav_referee_label")
NAV_CHAT = t("nav_chat_label")
NAV_GLOSSARY = t("nav_glossary_label")
NAV_ORACLE = "Oracle"

_ALL_NAV = [
    (NAV_HOME,     "house-fill",              None),
    (NAV_SINGLE,   "trophy-fill",             None),
    (NAV_SEASON,   "bar-chart-line-fill",     None),
    (NAV_ADVANCED, "lightning-charge-fill",    None),
    (NAV_LIVE,     "broadcast",               "ENABLE_LIVE_MATCH"),
    (NAV_LEADERS,  "award-fill",              None),
    (NAV_SCOUTING, "search",                  "ENABLE_SCOUTING"),
    (NAV_ORACLE,   "eye",                     "ENABLE_ML_PREDICTIONS"),
    (NAV_REFEREE,  "clipboard-check",         None),
    (NAV_CHAT,     "chat-dots-fill",          "ENABLE_LLM_CHAT"),
    (NAV_GLOSSARY, "book-half",               None),
]

NAV_OPTIONS = [label for label, _, flag in _ALL_NAV if flag is None or is_feature_enabled(flag)]
NAV_ICONS = [icon for _, icon, flag in _ALL_NAV if flag is None or is_feature_enabled(flag)]

selected_nav = option_menu(
    menu_title=None,
    options=NAV_OPTIONS,
    icons=NAV_ICONS,
    default_index=0,
    orientation="horizontal",
    key="main_nav",
    styles={
        "container": {"padding": "0!important", "background-color": "#0f0f23", "border-bottom": "1px solid rgba(255,255,255,0.06)"},
        "icon": {"color": "#a78bfa", "font-size": "16px"},
        "nav-link": {
            "font-size": "14px",
            "text-align": "center",
            "margin": "0px",
            "--hover-color": "#1a1a3e",
            "color": "#9ca3af",
            "padding": "10px 16px",
        },
        "nav-link-selected": {"background-color": "#312e81", "color": "#f0f0ff", "font-weight": "600"},
    },
)

needs_game_data = selected_nav in (NAV_SINGLE, NAV_ADVANCED)
needs_team_filter = selected_nav in (NAV_SEASON, NAV_REFEREE)

# ========================================================================
# SIDEBAR — Context-sensitive Filters
# ========================================================================
gamecode = None

with st.sidebar:
    st.markdown(f"### 📅 {t('selection_header')}")

    if "selected_season" not in st.session_state:
        st.session_state.selected_season = _cfg_default
    if "selected_round" not in st.session_state:
        st.session_state.selected_round = 1

    def on_season_change():
        st.session_state.selected_round = 1

    seasons = _cfg_seasons
    selected_season_input = st.selectbox(
        t("season_dropdown"),
        seasons,
        index=seasons.index(st.session_state.selected_season) if st.session_state.selected_season in seasons else 0,
        key="season_picker",
        on_change=on_season_change,
    )
    st.session_state.selected_season = selected_season_input

    schedule = fetch_season_schedule(st.session_state.selected_season)

    if schedule.empty:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        st.session_state["game_info_cache"] = None
    else:
        if needs_game_data:
            rounds = sorted(schedule["round"].unique())
            if st.session_state.selected_round not in rounds:
                st.session_state.selected_round = rounds[0] if rounds else 1

            def format_round(r):
                round_name = schedule[schedule["round"] == r]["round_name"].iloc[0]
                if not round_name:
                    return f"Round {r}"
                return f"{round_name}" if "Round" in round_name else f"Round {r} ({round_name})"

            selected_round_input = st.selectbox(
                t("round_dropdown"),
                rounds,
                index=rounds.index(st.session_state.selected_round) if st.session_state.selected_round in rounds else 0,
                format_func=format_round,
                key="round_picker",
            )
            st.session_state.selected_round = selected_round_input

            round_games = schedule[schedule["round"] == st.session_state.selected_round].copy()

            def format_matchup(row):
                home = row.get("home_code", row.get("home_team", "???"))
                away = row.get("away_code", row.get("away_team", "???"))
                if pd.notna(row.get("home_score")) and pd.notna(row.get("away_score")):
                    return f"{home}-{away} [{int(row['home_score'])}-{int(row['away_score'])}]"
                return f"{home}-{away} [{t('lbl_upcoming', default='Upcoming')}]"

            round_games["matchup_label"] = round_games.apply(format_matchup, axis=1)
            matchup_dict = {row["matchup_label"]: row.to_dict() for _, row in round_games.iterrows()}
            labels = list(matchup_dict.keys())

            selected_label = st.selectbox(t("matchup_dropdown"), labels, key="matchup_picker")

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

        if needs_team_filter:
            euroleague_games = schedule[schedule["played"] == True] if "played" in schedule.columns else schedule
            if not euroleague_games.empty:
                team_options = sorted(list(set(euroleague_games["home_code"].unique()) | set(euroleague_games["away_code"].unique())))
            else:
                team_options = sorted(list(set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())))
            st.session_state["season_team_codes"] = set(team_options)
            st.session_state["selected_team"] = st.selectbox(
                t("team_dropdown"), team_options, key="team_picker"
            )

    st.markdown("---")
    st.markdown(
        "<p style='color:#6b7280; font-size:0.75rem;'>"
        "Data: euroleague-api &bull; Built with Streamlit &amp; Plotly</p>",
        unsafe_allow_html=True,
    )

# Inject dynamic team-branded CSS
_team_primary, _team_secondary = get_team_accent()
_inject_team_css(_team_primary, _team_secondary)


# ========================================================================
# PAGE ROUTING
# ========================================================================
if selected_nav == NAV_HOME:
    from streamlit_app.views.home import render
    render()

elif selected_nav == NAV_SINGLE:
    from streamlit_app.views.single_game import render
    render(gamecode)

elif selected_nav == NAV_SEASON:
    from streamlit_app.views.season_overview import render
    render(schedule)

elif selected_nav == NAV_ADVANCED:
    from streamlit_app.views.advanced_analytics import render
    render(gamecode)

elif selected_nav == NAV_LIVE:
    from streamlit_app.views.live_match import render
    render()

elif selected_nav == NAV_LEADERS:
    from streamlit_app.views.leaders import render
    render()

elif selected_nav == NAV_SCOUTING:
    from streamlit_app.views.scouting import render
    render()

elif selected_nav == NAV_ORACLE:
    from streamlit_app.views.oracle import render
    render()

elif selected_nav == NAV_REFEREE:
    from streamlit_app.views.referee import render
    render()

elif selected_nav == NAV_CHAT:
    from streamlit_app.views.chat import render
    render()

elif selected_nav == NAV_GLOSSARY:
    from streamlit_app.views.glossary import render
    render()


# ========================================================================
# SIDEBAR — Admin: Database Sync Manager
# ========================================================================
if not REQUIRE_LOGIN or st.session_state.get("is_admin"):
    with st.sidebar:
        with st.expander("🔧 Database Sync Manager", expanded=False):
            from streamlit_app.queries import _get_repository

            _repo = _get_repository()

            if _repo.db_available():
                st.markdown(
                    "<span style='color:#10b981;'>● Database connected</span>",
                    unsafe_allow_html=True,
                )

                _sync_season = st.session_state.get("selected_season", _cfg_default)
                cached_codes = _repo.get_cached_gamecodes(_sync_season)
                st.caption(f"Season {_sync_season}: **{len(cached_codes)}** games cached")

                if st.button(f"Sync missing games for {_sync_season}", key="btn_sync"):
                    missing = _repo.get_missing_gamecodes(_sync_season)
                    if not missing:
                        st.success("Database is already up to date!")
                    else:
                        progress_bar = st.progress(0, text=f"Syncing {len(missing)} games...")

                        def _update_progress(current, total):
                            progress_bar.progress(
                                current / total,
                                text=f"Syncing game {current}/{total}...",
                            )

                        result = _repo.sync_missing_games(
                            _sync_season, progress_callback=_update_progress,
                        )
                        progress_bar.empty()
                        st.success(
                            f"Done! Synced **{result['synced']}** / {result['total']} games."
                            + (f" ({result['failed']} failed)" if result["failed"] else "")
                        )
            else:
                st.markdown(
                    "<span style='color:#ef4444;'>● Database offline</span>",
                    unsafe_allow_html=True,
                )
                st.caption("Running in API-only mode. Start PostgreSQL to enable caching.")
