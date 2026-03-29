"""
app.py — Euroleague Advanced Analytics Dashboard (st.navigation router)
========================================================================
Thin router that handles page config, authentication, common sidebar
(language & season), then dispatches to per-page modules via
Streamlit's native st.navigation + st.Page APIs.

Launch:  streamlit run streamlit_app/app.py
"""

import sys
from pathlib import Path

import streamlit as st

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from streamlit_app.shared import (
    CFG, get_team_accent,
    _cfg_seasons, _cfg_default,
    t, fetch_season_schedule,
    is_feature_enabled,
    REQUIRE_LOGIN,
    init_favorite_team,
    get_favorite_team,
    show_favorite_team_selector,
    format_team_option,
)
from streamlit_app.utils.config_loader import get_default_language, get_language_map
from streamlit_app.utils.auth import init_auth_state, render_auth_page, render_user_sidebar, flush_pending_cookies

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

    /* Sidebar — force light text on dark background regardless of theme */
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0f0f23 0%, #1a1a3e 100%); }
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] * { color: #e4e4f0 !important; }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] .stCheckbox label,
    [data-testid="stSidebar"] .stSlider label,
    [data-testid="stSidebar"] .stNumberInput label,
    [data-testid="stSidebar"] .stTextInput label,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 { color: #e4e4f0 !important; }
    [data-testid="stSidebar"] [data-baseweb="select"] { background-color: rgba(255,255,255,0.08) !important; }
    [data-testid="stSidebar"] [data-baseweb="select"] * { color: #e4e4f0 !important; }
    [data-testid="stSidebar"] svg { fill: #e4e4f0 !important; }

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
    flush_pending_cookies()
    if not st.session_state.get("authenticated"):
        render_auth_page()
        st.stop()

# ========================================================================
# FAVORITE TEAM STATE
# ========================================================================
init_favorite_team()

# Flush any pending favorite-team cookie (no-login mode)
if not REQUIRE_LOGIN:
    _pending_fav = st.session_state.pop("_pending_fav_cookie", None)
    if _pending_fav is not None:
        from streamlit_app.shared import _write_fav_cookie
        _write_fav_cookie(_pending_fav if _pending_fav else None)

# ========================================================================
# DEEP LINKING: Initialize state from URL query parameters
# ========================================================================
if "_deep_link_applied" not in st.session_state:
    _qp = st.query_params
    if "season" in _qp:
        try:
            _url_season = int(_qp["season"])
            if _url_season in _cfg_seasons:
                st.session_state["selected_season"] = _url_season
        except (ValueError, TypeError):
            pass
    if "round" in _qp:
        try:
            st.session_state["selected_round"] = int(_qp["round"])
        except (ValueError, TypeError):
            pass
    if "gamecode" in _qp:
        try:
            st.session_state["_url_gamecode"] = int(_qp["gamecode"])
        except (ValueError, TypeError):
            pass
    if "team" in _qp:
        st.session_state["_url_team"] = _qp["team"]
    st.session_state["_deep_link_applied"] = True


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
    .stDataFrame [data-testid="glideDataEditor"] .dvn-scroller .dvn-header {{
        background-color: {hex_to_rgba(primary, 0.10)} !important;
    }}
</style>
""",
        unsafe_allow_html=True,
    )


# ========================================================================
# SIDEBAR — Branding, Language & Season (common to all pages)
# ========================================================================
lang_map = get_language_map()
_default_lang = get_default_language()
default_lang_idx = list(lang_map.values()).index(st.session_state.get("lang", _default_lang))

with st.sidebar:
    st.markdown(
        "<h1 style='text-align:center; background: linear-gradient(90deg, #6366f1 0%, #8b5cf6 100%); "
        "-webkit-background-clip: text; -webkit-text-fill-color: transparent; "
        "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.5rem;'>Euroleague Analytics</h1>",
        unsafe_allow_html=True,
    )

    # Language selector (below branding)
    selected_lang_label = st.selectbox(
        t("lang_selection"),
        list(lang_map.keys()),
        index=default_lang_idx,
        label_visibility="collapsed",
        key="sidebar_lang_selector",
    )
    if st.session_state.get("lang") != lang_map[selected_lang_label]:
        st.session_state["lang"] = lang_map[selected_lang_label]
        st.rerun()

    if REQUIRE_LOGIN:
        render_user_sidebar()
    st.markdown("---")

    st.markdown(f"### 📅 {t('selection_header')}")

    if "selected_season" not in st.session_state:
        st.session_state.selected_season = _cfg_default
    if "selected_round" not in st.session_state:
        st.session_state.selected_round = -1

    def on_season_change():
        st.session_state.selected_round = -1

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
    st.session_state["schedule"] = schedule

    if schedule.empty:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))

    # Deep link: resolve gamecode -> round
    _url_gc = st.session_state.get("_url_gamecode")
    if _url_gc is not None and not schedule.empty:
        _gc_match = schedule[schedule["gamecode"] == _url_gc]
        if not _gc_match.empty:
            st.session_state.selected_round = int(_gc_match.iloc[0]["round"])

    st.markdown("---")

    # --- Favorite Team quick-view & change button ---
    _fav = get_favorite_team()
    if _fav:
        st.markdown(
            f"<p style='font-size:0.85rem;'>⭐ {t('fav_sidebar_label', default='Favorite')}: "
            f"<strong>{format_team_option(_fav).lstrip('⭐ ')}</strong></p>",
            unsafe_allow_html=True,
        )
    if st.button(
        t("fav_change_btn", default="Change Favorite Team") if _fav
        else t("fav_set_btn", default="Set Favorite Team"),
        key="sidebar_change_fav",
        width="stretch",
    ):
        show_favorite_team_selector()

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
# PAGE DEFINITIONS  (lazy imports for isolation)
# ========================================================================
def _page_home():
    from streamlit_app.views.home import render
    render()

def _page_single_game():
    from streamlit_app.views.single_game import render
    render()

def _page_season():
    from streamlit_app.views.season_overview import render
    render()

def _page_advanced():
    from streamlit_app.views.advanced_analytics import render
    render()

def _page_live():
    from streamlit_app.views.live_match import render
    render()

def _page_leaders():
    from streamlit_app.views.leaders import render
    render()

def _page_scouting():
    from streamlit_app.views.scouting import render
    render()

def _page_oracle():
    from streamlit_app.views.oracle import render
    render()

def _page_referee():
    from streamlit_app.views.referee import render
    render()

def _page_chat():
    from streamlit_app.views.chat import render
    render()

def _page_glossary():
    from streamlit_app.views.glossary import render
    render()

def _page_lineup_optimizer():
    from streamlit_app.views.lineup_optimizer import render
    render()

def _page_scout_finder():
    from streamlit_app.views.scout_finder import render
    render()

def _page_playoff_picture():
    from streamlit_app.views.playoff_probabilities import render
    render()


# ========================================================================
# NAVIGATION  (grouped into Main / Analytics / Tools)
# ========================================================================
main_pages = [
    st.Page(_page_home, title=t("nav_home_label"), icon="🏠", default=True, url_path="home"),
    st.Page(_page_single_game, title=t("nav_single_label"), icon="🏆", url_path="single-game"),
    st.Page(_page_season, title=t("nav_season_label"), icon="📊", url_path="season"),
]

analytics_pages = [
    st.Page(_page_advanced, title=t("nav_advanced_label"), icon="⚡", url_path="advanced"),
]
if is_feature_enabled("ENABLE_LIVE_MATCH"):
    analytics_pages.append(
        st.Page(_page_live, title=t("nav_live_label"), icon="📡", url_path="live"),
    )
analytics_pages.append(
    st.Page(_page_leaders, title=t("nav_leaders_label"), icon="🏅", url_path="leaders"),
)
if is_feature_enabled("ENABLE_SCOUTING"):
    analytics_pages.append(
        st.Page(_page_scouting, title=t("nav_scouting_label"), icon="🔍", url_path="scouting"),
    )
analytics_pages.append(
    st.Page(_page_scout_finder, title=t("nav_scout_finder", default="Scout Finder"), icon="💰", url_path="scout-finder"),
)
if is_feature_enabled("ENABLE_ML_PREDICTIONS"):
    analytics_pages.append(
        st.Page(_page_playoff_picture, title=t("nav_playoff_picture", default="Playoff Picture"), icon="🎯", url_path="playoff-picture"),
    )
    analytics_pages.append(
        st.Page(_page_lineup_optimizer, title=t("nav_lineup_label", default="Lineup Optimizer"), icon="🧪", url_path="lineup-optimizer"),
    )

tools_pages = []
if is_feature_enabled("ENABLE_ML_PREDICTIONS"):
    tools_pages.append(
        st.Page(_page_oracle, title="Oracle", icon="👁️", url_path="oracle"),
    )
tools_pages.append(
    st.Page(_page_referee, title=t("nav_referee_label"), icon="📋", url_path="referee"),
)
if is_feature_enabled("ENABLE_LLM_CHAT"):
    tools_pages.append(
        st.Page(_page_chat, title=t("nav_chat_label"), icon="💬", url_path="chat"),
    )
tools_pages.append(
    st.Page(_page_glossary, title=t("nav_glossary_label"), icon="📖", url_path="glossary"),
)

# Flatten pages into a single list to show them all as top-level tabs
all_pages = main_pages + analytics_pages + tools_pages
nav = st.navigation(all_pages, position="top")
nav.run()


# ========================================================================
# SIDEBAR — Admin: Database Sync Manager (rendered after page content)
# ========================================================================
_show_db_sync = CFG.get("data", {}).get("show_db_sync_status", False)

if _show_db_sync and (not REQUIRE_LOGIN or st.session_state.get("is_admin")):
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
