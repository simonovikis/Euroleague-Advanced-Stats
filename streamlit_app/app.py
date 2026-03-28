"""
app.py — Euroleague Advanced Analytics Dashboard
==================================================
Multi-page Streamlit dashboard with top horizontal navigation and
context-sensitive sidebar filters.

Navigation Tabs:
  1. Home/Hub        — Landing page with visual directory cards
  2. Single Game     — Player Stats, Shot Chart, Radar, Lineups, Assist Network
  3. Season Overview — League efficiency, team usage, home/away, clutch close games
  4. Advanced Analytics — Playmaking & AAQ, Clutch & Momentum
  5. Referees        — Win/loss records per official
  6. Glossary        — Advanced metrics explainer

Launch:  streamlit run streamlit_app/app.py
"""

import json
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from streamlit_app.queries import fetch_game_data_live, fetch_season_schedule

# ========================================================================
# PAGE CONFIG
# ========================================================================
st.set_page_config(
    page_title="Euroleague Advanced Analytics",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)


def load_translations():
    with open(Path(_project_root) / "streamlit_app" / "translations.json", "r", encoding="utf-8") as f:
        return json.load(f)


TRANSLATIONS = load_translations()


def t(key: str, **kwargs) -> str:
    lang = st.session_state.get("lang", "en")
    text = TRANSLATIONS.get(key, {}).get(lang, TRANSLATIONS.get(key, {}).get("en", key))
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
    """Render an interactive AgGrid table with filtering, sorting, pinning, and optional heatmap."""
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
# DYNAMIC TEAM BRANDING
# ========================================================================
# (primary, secondary) — primary is the dominant accent; secondary is contrast/text
TEAM_COLORS = {
    "OLY": ("#D31145", "#FFFFFF"),   # Olympiacos
    "PAO": ("#007A33", "#FFFFFF"),   # Panathinaikos
    "RMB": ("#00529F", "#FEBE10"),   # Real Madrid
    "FCB": ("#A50044", "#004D98"),   # FC Barcelona
    "FEN": ("#FFED00", "#09235A"),   # Fenerbahce
    "EFS": ("#003DA5", "#FFFFFF"),   # Anadolu Efes
    "MTA": ("#FFD130", "#0057B8"),   # Maccabi Tel Aviv
    "ASM": ("#CC0000", "#FFFFFF"),   # AS Monaco
    "BKN": ("#003DA5", "#E30613"),   # Baskonia
    "CZV": ("#CC0000", "#FFFFFF"),   # Crvena Zvezda (Red Star)
    "MIL": ("#C8102E", "#FFFFFF"),   # Olimpia Milano
    "BER": ("#003DA5", "#FFED00"),   # Alba Berlin
    "ZAL": ("#006633", "#FFFFFF"),   # Zalgiris Kaunas
    "VIR": ("#000000", "#E2001A"),   # Virtus Bologna
    "PRS": ("#0055A4", "#FFFFFF"),   # Paris Basketball
    "BAY": ("#E30613", "#FFFFFF"),   # Bayern Munich
    "PAR": ("#9B2335", "#FFFFFF"),   # Partizan
    "VAL": ("#FF6600", "#000000"),   # Valencia
    "ALB": ("#003DA5", "#FFED00"),   # Alba Berlin alias
    "MON": ("#CC0000", "#FFFFFF"),   # Monaco alias
    "PER": ("#00A651", "#FFFFFF"),   # Peristeri
    "LDP": ("#0055A4", "#FFFFFF"),   # LDLC ASVEL / Paris alias
}

DEFAULT_ACCENT = ("#6366f1", "#8b5cf6")  # Indigo fallback


def _get_team_accent() -> tuple:
    """Return (primary, secondary) hex colors for the currently active team."""
    team = st.session_state.get("selected_team") or st.session_state.get("_active_home_team")
    if team and team in TEAM_COLORS:
        return TEAM_COLORS[team]
    return DEFAULT_ACCENT


def _inject_team_css(primary: str, secondary: str):
    """Inject dynamic CSS overrides that theme the UI to the active team's colors."""
    # Compute a translucent version for backgrounds
    def hex_to_rgba(h, a=0.25):
        h = h.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{a})"

    pri_light = hex_to_rgba(primary, 0.15)
    pri_medium = hex_to_rgba(primary, 0.30)
    pri_glow = hex_to_rgba(primary, 0.25)

    st.markdown(
        f"""
<style>
    /* Section header gradient → team primary */
    .section-header {{
        background: linear-gradient(90deg, {primary} 0%, {secondary} 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
    }}

    /* Tab underline indicator */
    .stTabs [data-baseweb="tab-highlight"] {{
        background-color: {primary} !important;
    }}

    /* Horizontal rule */
    .stApp hr {{
        border-color: {hex_to_rgba(primary, 0.35)} !important;
    }}

    /* Button hover */
    .stApp button[kind="secondary"]:hover,
    .stApp .stButton > button:hover {{
        border-color: {primary} !important;
        color: {primary} !important;
        box-shadow: 0 0 8px {pri_glow};
    }}

    /* Selectbox border focus */
    .stApp [data-baseweb="select"] [data-baseweb="input"]:focus-within {{
        border-color: {primary} !important;
        box-shadow: 0 0 0 1px {primary} !important;
    }}

    /* Metric card accent border on hover */
    [data-testid="stMetric"]:hover {{
        border-color: {hex_to_rgba(primary, 0.40)} !important;
        box-shadow: 0 4px 20px {pri_light} !important;
    }}

    /* Metric positive delta */
    [data-testid="stMetricDelta"] svg[data-testid="stMetricDeltaIcon-Up"] {{
        fill: {primary} !important;
    }}
    [data-testid="stMetricDelta"][style*="color"] {{
        color: {primary} !important;
    }}

    /* Landing cards hover */
    .landing-card:hover {{
        box-shadow: 0 8px 30px {pri_glow} !important;
        border-color: {hex_to_rgba(primary, 0.3)} !important;
    }}

    /* Selected option-menu item */
    .nav-link-selected {{
        background-color: {primary} !important;
    }}

    /* Dataframe header tint (via Streamlit theming) */
    .stDataFrame [data-testid="glideDataEditor"] .dvn-scroller .dvn-header {{
        background-color: {hex_to_rgba(primary, 0.10)} !important;
    }}
</style>
""",
        unsafe_allow_html=True,
    )


# ========================================================================
# SIDEBAR — Language Selector (always first)
# ========================================================================
with st.sidebar:
    lang_map = {"🇬🇧 English": "en", "🇬🇷 Ελληνικά": "el", "🇩🇪 Deutsch": "de", "🇪🇸 Español": "es"}
    default_lang_idx = list(lang_map.values()).index(st.session_state.get("lang", "en"))

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
    st.markdown("---")

# ========================================================================
# TOP NAVIGATION — Horizontal option menu
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

NAV_OPTIONS = [NAV_HOME, NAV_SINGLE, NAV_SEASON, NAV_ADVANCED, NAV_LIVE, NAV_LEADERS, NAV_SCOUTING, NAV_REFEREE, NAV_CHAT, NAV_GLOSSARY]
NAV_ICONS = ["house-fill", "trophy-fill", "bar-chart-line-fill", "lightning-charge-fill", "broadcast", "award-fill", "search", "clipboard-check", "chat-dots-fill", "book-half"]

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
is_live_page = selected_nav == NAV_LIVE

# ========================================================================
# SIDEBAR — Context-sensitive Filters
# ========================================================================
gamecode = None

with st.sidebar:
    st.markdown(f"### 📅 {t('selection_header')}")

    if "selected_season" not in st.session_state:
        st.session_state.selected_season = 2025
    if "selected_round" not in st.session_state:
        st.session_state.selected_round = 1

    def on_season_change():
        st.session_state.selected_round = 1

    seasons = list(range(2025, 2010, -1))
    selected_season_input = st.selectbox(
        t("season_dropdown"),
        seasons,
        index=seasons.index(st.session_state.selected_season),
        key="season_picker",
        on_change=on_season_change,
    )
    st.session_state.selected_season = selected_season_input

    schedule = fetch_season_schedule(st.session_state.selected_season)

    if schedule.empty:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        st.session_state["game_info_cache"] = None
    else:
        # --- Round / Matchup selectors (Single Game or Advanced Analytics) ---
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

        # --- Team selector (Season Overview or Referees) ---
        if needs_team_filter:
            team_options = sorted(list(set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())))
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
_team_primary, _team_secondary = _get_team_accent()
_inject_team_css(_team_primary, _team_secondary)


# ========================================================================
# HELPERS — Game data loading & header rendering
# ========================================================================
def _ensure_game_data(gc: int) -> dict:
    """Load game data into session_state if stale, return the data dict."""
    data_needs_update = (
        "game_data" not in st.session_state
        or st.session_state.get("season") != st.session_state.get("selected_season", 2025)
        or st.session_state.get("gamecode") != gc
    )
    if data_needs_update:
        with st.spinner(t("loading_data")):
            try:
                season_to_fetch = st.session_state.get("selected_season", 2025)
                st.session_state["game_data"] = fetch_game_data_live(season_to_fetch, gc)
                st.session_state["season"] = season_to_fetch
                st.session_state["gamecode"] = gc
            except Exception as e:
                st.error(f"Failed to load game data (Game {gc}): {e}")
                st.stop()
    return st.session_state["game_data"]


def _render_game_header():
    """Render the game score banner with team logos."""
    if "game_info_cache" in st.session_state and st.session_state["game_info_cache"]:
        gi = st.session_state["game_info_cache"]
        home_name = gi.get("home_name", "???")
        away_name = gi.get("away_name", "???")
        hs = f"{int(gi['home_score'])}" if pd.notna(gi.get("home_score")) else "-"
        as_ = f"{int(gi['away_score'])}" if pd.notna(gi.get("away_score")) else "-"
        home_logo = gi.get("home_logo", "")
        away_logo = gi.get("away_logo", "")
        home_code = gi.get("home_code", "HOM")
        away_code = gi.get("away_code", "AWA")
    else:
        data = st.session_state.get("game_data", {})
        gi_df = data.get("game_info", pd.DataFrame())
        local_gi = gi_df.iloc[0] if not gi_df.empty else {}
        home_code = local_gi.get("home_team", "???")
        away_code = local_gi.get("away_team", "???")
        home_name, away_name = home_code, away_code
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
# PAGE: HOME / LANDING
# ========================================================================
if selected_nav == NAV_HOME:
    st.markdown("")
    st.markdown(
        f'<h1 style="text-align:center; color:#e4e4f0; margin-bottom:4px;">{t("home_welcome_title")}</h1>'
        f'<p style="text-align:center; color:#9ca3af; font-size:1.05rem; margin-bottom:2rem;">{t("home_welcome_sub")}</p>',
        unsafe_allow_html=True,
    )

    row1 = st.columns(3)
    cards_row1 = [
        ("🏀", t("card_single_title"), t("card_single_desc")),
        ("📊", t("card_season_title"), t("card_season_desc")),
        ("🧠", t("card_advanced_title"), t("card_advanced_desc")),
    ]
    for col, (icon, title, desc) in zip(row1, cards_row1):
        with col:
            st.markdown(
                f'<div class="landing-card">'
                f'  <div class="card-icon">{icon}</div>'
                f"  <h3>{title}</h3>"
                f"  <p>{desc}</p>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("")
    row2 = st.columns(3)
    cards_row2 = [
        ("📡", t("card_live_title"), t("card_live_desc")),
        ("🏅", t("card_leaders_title"), t("card_leaders_desc")),
        ("🔍", t("card_scouting_title"), t("card_scouting_desc")),
    ]
    for col, (icon, title, desc) in zip(row2, cards_row2):
        with col:
            st.markdown(
                f'<div class="landing-card">'
                f'  <div class="card-icon">{icon}</div>'
                f"  <h3>{title}</h3>"
                f"  <p>{desc}</p>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("")
    row3 = st.columns(4)
    cards_row3 = [
        ("⚖️", t("card_referee_title"), t("card_referee_desc")),
        ("💬", t("card_chat_title"), t("card_chat_desc")),
        ("📖", t("card_glossary_title"), t("card_glossary_desc")),
    ]
    for col, (icon, title, desc) in zip(row3, cards_row3):
        with col:
            st.markdown(
                f'<div class="landing-card">'
                f'  <div class="card-icon">{icon}</div>'
                f"  <h3>{title}</h3>"
                f"  <p>{desc}</p>"
                f"</div>",
                unsafe_allow_html=True,
            )


# ========================================================================
# PAGE: SINGLE GAME
# ========================================================================
elif selected_nav == NAV_SINGLE:
    if gamecode is None:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        st.stop()

    data = _ensure_game_data(gamecode)
    _render_game_header()

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
                c1.metric(t("metric_players", default="Players"), len(active))
                c2.metric(f"{t('metric_avg', default='Avg')} {t('lbl_ts', default='TS%')}", f"{active['ts_pct'].mean():.1%}")
                c3.metric(f"{t('metric_avg', default='Avg')} {t('lbl_ortg', default='ORtg')}", f"{active['off_rating'].mean():.1f}")
                c4.metric(f"{t('metric_avg', default='Avg')} {t('lbl_drtg', default='DRtg')}", f"{active['def_rating'].mean():.1f}")
                if "true_usg_pct" in active.columns:
                    c5.metric(f"{t('metric_avg', default='Avg')} {t('lbl_tusg', default='tUSG%')}", f"{active['true_usg_pct'].mean():.1%}")
                else:
                    c5.metric(f"{t('metric_avg', default='Avg')} {t('col_poss', default='Poss')}", f"{active['possessions'].mean():.1f}")

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
                "points": t("col_pts"), "pts_from_assists": "PTS via Assists",
                "total_pts_created": "Total PTS Produced",
                "possessions": t("col_poss"), "ts_pct": t("col_ts"),
                "off_rating": t("col_ortg"), "def_rating": t("col_drtg"),
                "true_usg_pct": t("col_tusg"), "stop_rate": "Stop Rate",
                "net_rating": "Net Rtg",
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
                st.markdown("#### Total Points Produced Breakdown")
                chart_team = sel_team if sel_team != t("filter_all") else None
                chart_df = active.copy()
                if chart_team:
                    chart_df = chart_df[chart_df["team_code"] == chart_team]
                else:
                    teams_in_game = sorted(active["team_code"].unique())
                    if teams_in_game:
                        chart_team = st.selectbox(
                            "Select team for TPC chart", teams_in_game, key="tpc_chart_team",
                        )
                        chart_df = active[active["team_code"] == chart_team]

                chart_df = chart_df[chart_df["total_pts_created"] > 0].sort_values(
                    "total_pts_created", ascending=True,
                )
                if not chart_df.empty:
                    fig_tpc = go.Figure()
                    fig_tpc.add_trace(go.Bar(
                        y=chart_df["player_name"], x=chart_df["points"],
                        name="Own Points", orientation="h",
                        marker_color="#6366f1",
                        hovertemplate="%{y}: %{x} pts<extra>Own Points</extra>",
                    ))
                    fig_tpc.add_trace(go.Bar(
                        y=chart_df["player_name"], x=chart_df["pts_from_assists"],
                        name="PTS via Assists", orientation="h",
                        marker_color="#f59e0b",
                        hovertemplate="%{y}: %{x} pts<extra>PTS via Assists</extra>",
                    ))
                    fig_tpc.update_layout(
                        barmode="stack",
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)",
                        height=max(350, len(chart_df) * 38),
                        xaxis_title="Total Points Produced",
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
                    labels={"off_rating": "Offensive Rating", "def_rating": "Defensive Rating"},
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
    # TAB: Shot Chart
    # ------------------------------------------------------------------
    with tab_shots:
        st.markdown(f'<p class="section-header">{t("nav_shot_chart")}</p>', unsafe_allow_html=True)

        shots = data.get("shots", pd.DataFrame())
        shot_quality = data.get("shot_quality", pd.DataFrame())

        if shots.empty:
            st.warning(t("no_shot_data", default="No shot data available for this game."))
        else:
            all_shooters = sorted(shots["PLAYER"].dropna().unique())
            sel_shooter = st.selectbox(t("filter_player"), [t("filter_all")] + all_shooters, key="shot_player")

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
                    shot_df["Outcome"] = shot_df["POINTS"].apply(
                        lambda p: t("lbl_made", default="Made") if p > 0 else t("lbl_missed", default="Missed")
                    )

                    fig = go.Figure()
                    court_shapes = [
                        dict(type="rect", x0=-750, y0=0, x1=750, y1=1400,
                             line=dict(color="rgba(255,255,255,0.3)", width=1), fillcolor="rgba(15,15,35,0.5)"),
                        dict(type="rect", x0=-250, y0=0, x1=250, y1=580,
                             line=dict(color="rgba(255,255,255,0.2)", width=1)),
                        dict(type="circle", x0=-180, y0=400, x1=180, y1=760,
                             line=dict(color="rgba(255,255,255,0.15)", width=1)),
                        dict(type="rect", x0=-675, y0=0, x1=675, y1=50,
                             line=dict(color="rgba(255,255,255,0.1)", width=0)),
                        dict(type="circle", x0=-22, y0=50, x1=22, y1=94,
                             line=dict(color="#ff6b35", width=2), fillcolor="rgba(255,107,53,0.2)"),
                    ]
                    fig.update_layout(shapes=court_shapes)

                    theta = np.linspace(0, np.pi, 100)
                    arc_x = np.clip(675 * np.cos(theta), -660, 660)
                    arc_y = 675 * np.sin(theta) + 72
                    fig.add_trace(go.Scatter(
                        x=arc_x, y=arc_y, mode="lines",
                        line=dict(color="rgba(255,255,255,0.25)", width=2),
                        showlegend=False, hoverinfo="skip",
                    ))

                    made = shot_df[shot_df["Outcome"] == t("lbl_made", default="Made")]
                    missed = shot_df[shot_df["Outcome"] == t("lbl_missed", default="Missed")]

                    fig.add_trace(go.Scatter(
                        x=made["COORD_X"], y=made["COORD_Y"], mode="markers",
                        marker=dict(color="#10b981", size=10, symbol="circle",
                                    line=dict(width=1, color="white"), opacity=0.85),
                        name=t("lbl_made", default="Made"), text=made["ACTION"],
                        hovertemplate="%{text}<br>Zone: %{customdata}<extra></extra>",
                        customdata=made["ZONE"],
                    ))
                    fig.add_trace(go.Scatter(
                        x=missed["COORD_X"], y=missed["COORD_Y"], mode="markers",
                        marker=dict(color="#ef4444", size=8, symbol="x",
                                    line=dict(width=1, color="white"), opacity=0.65),
                        name=t("lbl_missed", default="Missed"), text=missed["ACTION"],
                        hovertemplate="%{text}<br>Zone: %{customdata}<extra></extra>",
                        customdata=missed["ZONE"],
                    ))

                    fig.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.9)", height=650, width=700,
                        xaxis=dict(range=[-800, 800], showgrid=False, zeroline=False,
                                   showticklabels=False, scaleanchor="y"),
                        yaxis=dict(range=[-50, 1000], showgrid=False, zeroline=False, showticklabels=False),
                        legend=dict(x=0.02, y=0.98, font=dict(size=12)),
                        font=dict(family="Inter"), margin=dict(l=20, r=20, t=30, b=20),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    if not shot_quality.empty:
                        st.markdown("#### 📊 Shot Quality Analysis")
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
                    st.info("No X/Y coordinates available — showing zone breakdown instead.")
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

            st.markdown("#### 📋 Raw Values Comparison")
            comparison = pd.DataFrame({
                "Metric": radar_labels,
                p1: [round(p1_data.iloc[i], 3) for i in range(len(radar_labels))],
                p2: [round(p2_data.iloc[i], 3) for i in range(len(radar_labels))],
            })
            st.dataframe(comparison, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------------
    # TAB: Lineup & Synergy
    # ------------------------------------------------------------------
    with tab_lineups:
        st.markdown(f'<p class="section-header">{t("nav_lineups")}</p>', unsafe_allow_html=True)

        sub_lu, sub_duo, sub_trio = st.tabs([
            t("tab_5man", default="🏅 5-Man Lineups"),
            t("tab_duo", default="👥 Duo Synergy"),
            t("tab_trio", default="🔺 Trio Synergy"),
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
                        color_continuous_midpoint=0, labels={"net_rtg": "Net Rating", "lineup_str": "Lineup"},
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
                st.info("No duo synergy data.")
            else:
                duo_teams = sorted(duo["team"].unique())
                sel_duo_team = st.selectbox("Team", ["All"] + duo_teams, key="duo_team")
                duo_f = duo if sel_duo_team == "All" else duo[duo["team"] == sel_duo_team]
                duo_f = duo_f[duo_f["events_together"] >= 10]

                st.markdown("##### 🔝 Best Duos (by Synergy)")
                st.dataframe(
                    duo_f.head(10)[["team", "combo_names", "events_together", "net_rtg_together", "net_rtg_apart", "synergy"]],
                    use_container_width=True, hide_index=True,
                )

                st.markdown("##### ⬇️ Worst Duos (by Synergy)")
                st.dataframe(
                    duo_f.tail(5).sort_values("synergy")[["team", "combo_names", "events_together", "net_rtg_together", "net_rtg_apart", "synergy"]],
                    use_container_width=True, hide_index=True,
                )

        with sub_trio:
            trio = data.get("trio_synergy", pd.DataFrame())
            if trio.empty:
                st.info("No trio synergy data.")
            else:
                trio_teams = sorted(trio["team"].unique())
                sel_trio_team = st.selectbox("Team", ["All"] + trio_teams, key="trio_team")
                trio_f = trio if sel_trio_team == "All" else trio[trio["team"] == sel_trio_team]
                trio_f = trio_f[trio_f["events_together"] >= 10]

                st.markdown("##### 🔝 Best Trios (by Synergy)")
                st.dataframe(
                    trio_f.head(10)[["team", "combo_names", "events_together", "net_rtg_together", "net_rtg_apart", "synergy"]],
                    use_container_width=True, hide_index=True,
                )

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
                    hovertemplate="Passer: %{y}<br>Scorer: %{x}<br>Assists: %{z}<extra></extra>",
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

                st.markdown("#### 🏅 Top Assist Connections")
                st.dataframe(
                    team_assists[["assister_name", "scorer_name", "count", "play_types"]].rename(
                        columns={"assister_name": "Passer", "scorer_name": "Scorer",
                                 "count": "Assists", "play_types": "Shot Types"}
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

            stints_df = compute_player_stints(rot_pbp, rot_box, sel_rot_team)

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


# ========================================================================
# PAGE: SEASON OVERVIEW
# ========================================================================
elif selected_nav == NAV_SEASON:
    st.markdown(f'<p class="section-header">{t("hdr_season_overview")}</p>', unsafe_allow_html=True)

    season_to_fetch = st.session_state.get("selected_season", 2025)
    team_code = st.session_state.get("selected_team")

    if not team_code:
        st.warning(t("warn_select_team"))
        st.stop()

    _tc_primary = TEAM_COLORS.get(team_code, DEFAULT_ACCENT)[0]

    # --- League Efficiency Landscape ---
    st.markdown(f"### {t('hdr_league_eff')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_league_eff')}</p>", unsafe_allow_html=True)

    with st.spinner(t("fetching_league_eff")):
        from streamlit_app.queries import fetch_league_efficiency_landscape, fetch_team_season_data
        eff_df = fetch_league_efficiency_landscape(season_to_fetch)

    if eff_df.empty:
        st.warning(t("err_league_eff"))
    else:
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
        st.plotly_chart(fig_eff, use_container_width=True)

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
        st.plotly_chart(fig_pace, use_container_width=True)

    st.markdown("---")

    # --- Situational Scoring ---
    st.markdown(f"### {t('hdr_sit_scoring')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_sit_scoring')}</p>", unsafe_allow_html=True)

    with st.spinner(t("fetching_sit_scoring")):
        from streamlit_app.queries import fetch_situational_scoring
        sit_df = fetch_situational_scoring(season_to_fetch)

    if sit_df.empty:
        st.info(t("no_sit_scoring"))
    else:
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
            st.plotly_chart(fig_sit, use_container_width=True)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric(t("lbl_steals_pg"), f"{team_row['steals_pg']:.1f}",
                      f"{team_row['steals_pg'] - league_avg['steals_pg']:+.1f} vs avg")
            c2.metric(t("lbl_turnovers_pg"), f"{team_row['turnovers_pg']:.1f}",
                      f"{team_row['turnovers_pg'] - league_avg['turnovers_pg']:+.1f} vs avg", delta_color="inverse")
            c3.metric(t("lbl_off_reb_pg"), f"{team_row['off_reb_pg']:.1f}",
                      f"{team_row['off_reb_pg'] - league_avg['off_reb_pg']:+.1f} vs avg")
            c4.metric(t("lbl_assists_pg"), f"{team_row['assists_pg']:.1f}",
                      f"{team_row['assists_pg'] - league_avg['assists_pg']:+.1f} vs avg")

    st.markdown("---")

    # --- Home vs. Away Performance ---
    st.markdown(f"### {t('hdr_home_away', default='Home vs. Away Performance')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_home_away', default='Compare team performance at home versus on the road.')}</p>", unsafe_allow_html=True)

    with st.spinner(t("fetching_home_away", default="Calculating Home/Away splits...")):
        from streamlit_app.queries import fetch_home_away_splits
        ha_df = fetch_home_away_splits(season_to_fetch)

    if ha_df.empty:
        st.info(t("no_home_away", default="No Home/Away data available yet."))
    else:
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
            st.plotly_chart(build_ha_chart(ha_df, "home_net", "away_net", t("lbl_net_rating", default="Net Rating")), use_container_width=True)
            st.caption(t("cap_home_adv", default="Teams are sorted left-to-right from largest Home Advantage to smallest."))
        with tab_ortg:
            st.plotly_chart(build_ha_chart(ha_df, "home_ortg", "away_ortg", t("lbl_ortg", default="ORtg")), use_container_width=True)
        with tab_drtg:
            st.plotly_chart(build_ha_chart(ha_df, "home_drtg", "away_drtg", t("lbl_drtg", default="DRtg")), use_container_width=True)

    st.markdown("---")

    # --- Clutch & Close Games ---
    st.markdown(f"### {t('hdr_clutch_close')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_clutch_close')}</p>", unsafe_allow_html=True)

    close_threshold = st.slider(
        t("lbl_close_threshold"), min_value=1, max_value=15, value=5, key="close_threshold",
    )

    with st.spinner(t("fetching_clutch_close")):
        from streamlit_app.queries import fetch_close_game_stats
        close_df = fetch_close_game_stats(season_to_fetch, close_threshold)

    if close_df.empty:
        st.info(t("no_clutch_close"))
    else:
        team_row = close_df[close_df["team_code"] == team_code]
        if not team_row.empty:
            tr = team_row.iloc[0]
            league_avg_val = tr["league_avg_close_win_pct"]
            c1, c2, c3 = st.columns(3)
            c1.metric(
                t("lbl_close_win_pct"),
                f"{tr['close_win_pct']:.1f}%" if not pd.isna(tr["close_win_pct"]) else "N/A",
                f"{tr['close_win_pct'] - league_avg_val:+.1f} vs avg" if not pd.isna(tr["close_win_pct"]) else None,
            )
            c2.metric(t("lbl_league_avg_close"), f"{league_avg_val:.1f}%")
            c3.metric(
                t("lbl_close_record"),
                f"{int(tr['close_wins'])}-{int(tr['close_losses'])}",
                f"{int(tr['close_games_played'])} close games",
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
            fig_dom.add_trace(go.Scatter(
                x=others["avg_point_diff"], y=others["close_win_pct"],
                mode="markers+text", text=others["team_code"],
                textposition="top center", textfont=dict(size=9, color="#9ca3af"),
                marker=dict(size=others["close_games_played"] * 3 + 8, color="#4b5563", opacity=0.7,
                            line=dict(width=1, color="rgba(255,255,255,0.2)")),
                hovertemplate="%{customdata[0]}<br>Pt Diff: %{x:.1f}<br>Close W%%: %{y:.1f}%%<br>Close GP: %{customdata[1]}<extra></extra>",
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
                    hovertemplate="%{customdata[0]}<br>Pt Diff: %{x:.1f}<br>Close W%%: %{y:.1f}%%<br>Close GP: %{customdata[1]}<extra></extra>",
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
            st.plotly_chart(fig_dom, use_container_width=True)

            # Clutch vs. Overall
            st.markdown(f"#### {t('hdr_clutch_overall')}")
            st.markdown(f"<p style='color:#9ca3af; font-size:0.85rem;'>{t('desc_clutch_overall')}</p>", unsafe_allow_html=True)

            fig_ov = go.Figure()
            others2 = plot_df[~plot_df["is_selected"]]
            fig_ov.add_trace(go.Scatter(
                x=others2["overall_win_pct"], y=others2["close_win_pct"],
                mode="markers+text", text=others2["team_code"],
                textposition="top center", textfont=dict(size=9, color="#9ca3af"),
                marker=dict(size=10, color="#4b5563", opacity=0.7,
                            line=dict(width=1, color="rgba(255,255,255,0.2)")),
                hovertemplate="%{customdata}<br>Win%%: %{x:.1f}%%<br>Close W%%: %{y:.1f}%%<extra></extra>",
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
                    hovertemplate="%{customdata}<br>Win%%: %{x:.1f}%%<br>Close W%%: %{y:.1f}%%<extra></extra>",
                    customdata=sel2["team_name"], showlegend=False,
                ))

            fig_ov.add_shape(type="line", x0=0, y0=0, x1=100, y1=100,
                             line=dict(dash="dot", color="rgba(255,255,255,0.15)"))
            fig_ov.update_layout(
                xaxis_title=t("lbl_overall_win_pct"), yaxis_title=t("lbl_close_win_pct"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e4e4f0"), height=500, showlegend=False,
            )
            st.plotly_chart(fig_ov, use_container_width=True)
        else:
            st.info(t("no_clutch_close"))

    st.markdown("---")

    # --- Team Averages & Lineups ---
    with st.spinner(t("agg_season", team_code=team_code)):
        season_data = fetch_team_season_data(season_to_fetch, team_code)

    if not season_data or season_data.get("player_season_stats").empty:
        st.warning(t("no_season_stats", team_code=team_code))
        st.stop()

    player_stats = season_data["player_season_stats"]
    lineup_stats = season_data["lineup_season_stats"]

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
    st.plotly_chart(fig_usage, use_container_width=True)

    st.markdown("---")

    # Positional Scoring (Season)
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
            st.plotly_chart(fig_donut, use_container_width=True)
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
                        st.plotly_chart(fig_form, use_container_width=True)

                        # Summary metrics
                        last_5 = pdf.tail(window)
                        last_5_avg = last_5[metric_col].mean()
                        trend = last_5_avg - season_avg
                        trend_pct = (trend / season_avg * 100) if season_avg != 0 else 0

                        mc1, mc2, mc3, mc4 = st.columns(4)
                        mc1.metric(
                            t("form_season_avg", default="Season Avg"),
                            fmt_func(season_avg),
                        )
                        mc2.metric(
                            t("form_last_n_avg", n=window),
                            fmt_func(last_5_avg),
                            f"{trend_pct:+.1f}%",
                        )
                        mc3.metric(
                            t("form_best_game", default="Best Game"),
                            fmt_func(pdf[metric_col].max()),
                        )
                        mc4.metric(
                            t("form_games_played", default="Games Played"),
                            str(len(pdf)),
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
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"#### {t('hdr_best_net')}")
            st.dataframe(
                lineup_stats.head(5)[["lineup_str", "events", "ortg", "drtg", "net_rtg"]].rename(
                    columns={"lineup_str": t("col_lineup"), "events": t("col_poss"),
                             "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                ),
                use_container_width=True, hide_index=True,
            )
        with col2:
            st.markdown(f"#### {t('hdr_worst_net')}")
            st.dataframe(
                lineup_stats.tail(5)[["lineup_str", "events", "ortg", "drtg", "net_rtg"]].rename(
                    columns={"lineup_str": t("col_lineup"), "events": t("col_poss"),
                             "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                ),
                use_container_width=True, hide_index=True,
            )


# ========================================================================
# PAGE: ADVANCED ANALYTICS (Playmaking + Clutch)
# ========================================================================
elif selected_nav == NAV_ADVANCED:
    if gamecode is None:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        st.stop()

    data = _ensure_game_data(gamecode)
    _render_game_header()

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
                st.dataframe(display_aaq, use_container_width=True, hide_index=True)

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
                    st.plotly_chart(fig_aaq, use_container_width=True)

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
                st.dataframe(display_axp, use_container_width=True, hide_index=True)

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
                    st.plotly_chart(fig_axp, use_container_width=True)

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
                st.dataframe(display_duos, use_container_width=True, hide_index=True)

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
                        hovertemplate="Passer: %{y}<br>Scorer: %{x}<br>Total xP: %{z:.2f}<extra></extra>",
                        showscale=True, colorbar=dict(title="xP"),
                    ))
                    fig_hm.update_layout(
                        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(15,15,35,0.8)", height=500,
                        xaxis=dict(title=t("col_shooter"), tickangle=45, tickfont=dict(size=10)),
                        yaxis=dict(title=t("col_passer"), tickfont=dict(size=10), autorange="reversed"),
                        font=dict(family="Inter"), margin=dict(l=120, b=120),
                    )
                    st.plotly_chart(fig_hm, use_container_width=True)

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
                    use_container_width=True, hide_index=True,
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
                    use_container_width=True, hide_index=True,
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
                    c1.metric(f"{t('lbl_ortg')} {t('lbl_before', default='Before')}", f"{ft['ortg_before']:.1f}")
                    c2.metric(f"{t('lbl_ortg')} {t('lbl_after', default='After')}", f"{ft['ortg_after']:.1f}", delta=f"{ft['ortg_impact']:+.1f}")
                    c3.metric(f"{t('lbl_drtg')} {t('lbl_before', default='Before')}", f"{ft['drtg_before']:.1f}")
                    c4.metric(f"{t('lbl_drtg')} {t('lbl_after', default='After')}", f"{ft['drtg_after']:.1f}",
                              delta=f"{ft['drtg_impact']:+.1f}", delta_color="inverse")
                    st.markdown("---")


# ========================================================================
# PAGE: LIVE MATCH CENTER
# ========================================================================
elif selected_nav == NAV_LIVE:
    import time as _time

    st.markdown(f'<p class="section-header">{t("card_live_title")}</p>', unsafe_allow_html=True)
    st.caption(f"🔄 {t('live_autorefresh')}")

    season_live = st.session_state.get("selected_season", 2025)

    # --- Auto-refresh: rerun every 30 seconds ---
    if "live_last_refresh" not in st.session_state:
        st.session_state["live_last_refresh"] = _time.time()

    elapsed_since_refresh = _time.time() - st.session_state["live_last_refresh"]

    # Placeholder for countdown
    refresh_placeholder = st.empty()
    secs_until = max(0, int(30 - elapsed_since_refresh))
    refresh_placeholder.markdown(
        f"<p style='color:#6b7280; font-size:0.8rem;'>Next refresh in ~{secs_until}s</p>",
        unsafe_allow_html=True,
    )

    if elapsed_since_refresh >= 30:
        st.session_state["live_last_refresh"] = _time.time()
        if "live_cache" in st.session_state:
            del st.session_state["live_cache"]
        st.rerun()

    # --- Detect live games ---
    from streamlit_app.queries import fetch_live_games, fetch_live_game_data_fresh

    with st.spinner(t("live_checking")):
        if "live_cache" not in st.session_state:
            live_games = fetch_live_games(season_live)
            st.session_state["live_cache"] = live_games
        else:
            live_games = st.session_state["live_cache"]

    if not live_games:
        st.info(t("live_no_games"))
        st.markdown("---")
        st.markdown(
            "<p style='color:#9ca3af;'>If games are scheduled for today but haven't tipped off yet, "
            "they will appear here once play-by-play data becomes available.</p>",
            unsafe_allow_html=True,
        )
    else:
        # --- Game selector ---
        game_labels = [
            f"{g['home_code']} vs {g['away_code']} ({g['home_score']}-{g['away_score']})"
            for g in live_games
        ]
        selected_idx = st.selectbox(
            t("live_select_game"),
            range(len(game_labels)),
            format_func=lambda i: game_labels[i],
            key="live_game_select",
        )

        sel_game = live_games[selected_idx]
        sel_gc = sel_game["gamecode"]

        # --- Fetch fresh data ---
        with st.spinner(t("live_fetching_data")):
            live_data = fetch_live_game_data_fresh(season_live, sel_gc)

        pbp_df = live_data.get("pbp", pd.DataFrame())
        boxscore_df = live_data.get("boxscore", pd.DataFrame())

        # --- Live Score Header ---
        from data_pipeline.live_extractor import get_live_score_and_time

        score_info = get_live_score_and_time(pbp_df)
        home_score = score_info["home_score"]
        away_score = score_info["away_score"]
        period = score_info["period"]
        time_rem = score_info["time_remaining"]

        period_label = (
            t("live_overtime", num=period - 4) if period > 4
            else t("live_period", period=period)
        )

        _live_home_clr = TEAM_COLORS.get(sel_game.get("home_code"), DEFAULT_ACCENT)[0]
        _live_away_clr = TEAM_COLORS.get(sel_game.get("away_code"), DEFAULT_ACCENT)[0]
        st.markdown(
            f'<div class="game-header">'
            f'  <div class="team-block">'
            f'    <span class="team-name" style="color:{_live_home_clr};">{sel_game["home_name"]}</span>'
            f'    <span style="color:#9ca3af; font-size:0.85rem;">{sel_game["home_code"]}</span>'
            f"  </div>"
            f'  <div style="text-align:center;">'
            f'    <span class="score">{home_score}<span class="dash"> — </span>{away_score}</span>'
            f'    <p style="color:#f59e0b; font-weight:600; margin:0;">{period_label} | {time_rem}</p>'
            f"  </div>"
            f'  <div class="team-block">'
            f'    <span class="team-name" style="color:{_live_away_clr};">{sel_game["away_name"]}</span>'
            f'    <span style="color:#9ca3af; font-size:0.85rem;">{sel_game["away_code"]}</span>'
            f"  </div>"
            f"</div>",
            unsafe_allow_html=True,
        )

        # --- Tabs ---
        tab_overview, tab_momentum, tab_wp = st.tabs([
            t("live_tab_overview"),
            t("live_tab_momentum"),
            t("live_tab_winprob"),
        ])

        # ------------------------------------------------------------------
        # TAB: Live Overview (Current Lineups + Active Run)
        # ------------------------------------------------------------------
        with tab_overview:
            st.markdown(f"### {t('live_current_lineups')}")

            from data_pipeline.live_metrics import get_current_lineups, detect_active_run

            lineups = get_current_lineups(pbp_df, boxscore_df)

            col_home, col_away = st.columns(2)

            for col, side, color in [
                (col_home, "home", _live_home_clr),
                (col_away, "away", _live_away_clr),
            ]:
                with col:
                    lu = lineups.get(side)
                    if lu is None:
                        st.info(f"No lineup data for {side} team.")
                        continue

                    st.markdown(
                        f"<h4 style='color:{color};'>{lu['team']} — {t('live_on_court')}</h4>",
                        unsafe_allow_html=True,
                    )

                    for player in lu["players"]:
                        st.markdown(f"- {player}")

                    net_color = "#10b981" if lu["net_rtg"] >= 0 else "#ef4444"
                    st.markdown(
                        f"<div style='margin-top:8px; padding:12px; "
                        f"background:linear-gradient(135deg,#1e1e3f,#2a2a5a); "
                        f"border-radius:8px; text-align:center;'>"
                        f"<span style='color:#9ca3af; font-size:0.85rem;'>{t('live_net_rtg')}</span><br>"
                        f"<span style='color:{net_color}; font-size:1.8rem; font-weight:700;'>"
                        f"{lu['net_rtg']:+.1f}</span><br>"
                        f"<span style='color:#6b7280; font-size:0.75rem;'>"
                        f"ORtg {lu['ortg']:.1f} | DRtg {lu['drtg']:.1f} | {lu['events']} poss</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Active scoring run
            st.markdown("---")
            st.markdown(f"### {t('live_momentum')}")

            run = detect_active_run(pbp_df)
            if run and run["run_points"] >= 4:
                run_color = "#f59e0b"
                st.markdown(
                    f"<div style='padding:16px; background:linear-gradient(135deg,#422006,#78350f); "
                    f"border:1px solid #f59e0b; border-radius:12px; text-align:center;'>"
                    f"<span style='font-size:1.4rem; font-weight:700; color:#fbbf24;'>🔥 "
                    f"{t('live_scoring_run', team=run['run_team'], pts=run['run_points'], dur=run['duration_str'])}"
                    f"</span></div>",
                    unsafe_allow_html=True,
                )
                with st.expander("Scoring plays in this run"):
                    for play in run["scoring_plays"]:
                        st.markdown(f"- {play}")
            else:
                st.markdown(
                    f"<p style='color:#6b7280;'>{t('live_no_run')}</p>",
                    unsafe_allow_html=True,
                )

            # Quick boxscore summary
            st.markdown("---")
            st.markdown(f"### {t('hdr_player_stats')}")
            if not boxscore_df.empty:
                from data_pipeline.transformers import compute_advanced_stats
                live_adv = compute_advanced_stats(boxscore_df)
                active_players = live_adv[live_adv["minutes"] > 0].copy()

                if not active_players.empty:
                    display_cols = [c for c in [
                        "player_name", "team_code", "minutes", "points",
                        "ts_pct", "off_rating", "def_rating", "plus_minus",
                    ] if c in active_players.columns]

                    st.dataframe(
                        active_players[display_cols]
                        .sort_values("points", ascending=False)
                        .round(3)
                        .rename(columns={
                            "player_name": t("col_player"), "team_code": t("col_team"),
                            "minutes": t("col_min"), "points": t("col_pts"),
                            "ts_pct": t("col_ts"), "off_rating": t("col_ortg"),
                            "def_rating": t("col_drtg"), "plus_minus": "+/-",
                        }),
                        use_container_width=True, hide_index=True, height=400,
                    )

        # ------------------------------------------------------------------
        # TAB: Momentum
        # ------------------------------------------------------------------
        with tab_momentum:
            st.markdown(f"### {t('live_score_diff_timeline')}")

            from data_pipeline.live_metrics import get_momentum_timeline

            timeline = get_momentum_timeline(pbp_df)
            if timeline.empty:
                st.info("No scoring events yet.")
            else:
                fig_mom = go.Figure()

                fig_mom.add_trace(go.Scatter(
                    x=list(range(len(timeline))),
                    y=timeline["score_diff"],
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(
                        size=6,
                        color=timeline["score_diff"].apply(
                            lambda d: "#6366f1" if d > 0 else "#ef4444" if d < 0 else "#6b7280"
                        ),
                    ),
                    fill="tozeroy",
                    fillcolor="rgba(99,102,241,0.1)",
                    hovertemplate=(
                        "Play #%{x}<br>"
                        "Score: %{customdata[0]}-%{customdata[1]}<br>"
                        "Diff: %{y:+d}<br>"
                        "%{customdata[2]} - %{customdata[3]}"
                        "<extra></extra>"
                    ),
                    customdata=list(zip(
                        timeline["home_score"], timeline["away_score"],
                        timeline["player"], timeline["play_type"],
                    )),
                ))

                fig_mom.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")

                home_code = sel_game["home_code"]
                away_code = sel_game["away_code"]
                fig_mom.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)",
                    height=400,
                    xaxis_title="Scoring Events",
                    yaxis_title=f"Score Diff ({home_code} perspective)",
                    font=dict(family="Inter"),
                    showlegend=False,
                )

                # Annotate quadrants
                max_diff = max(abs(timeline["score_diff"].max()), abs(timeline["score_diff"].min()), 1)
                fig_mom.add_annotation(
                    x=len(timeline) * 0.05, y=max_diff * 0.8,
                    text=f"{home_code} leading", showarrow=False,
                    font=dict(color="#6366f1", size=11), opacity=0.5,
                )
                fig_mom.add_annotation(
                    x=len(timeline) * 0.05, y=-max_diff * 0.8,
                    text=f"{away_code} leading", showarrow=False,
                    font=dict(color="#ef4444", size=11), opacity=0.5,
                )

                st.plotly_chart(fig_mom, use_container_width=True)

                # Run history table
                st.markdown("#### Scoring Run History")
                from data_pipeline.transformers import track_lineups as _tl
                pbp_lu = _tl(pbp_df, boxscore_df) if not pbp_df.empty and not boxscore_df.empty else pd.DataFrame()
                if not pbp_lu.empty:
                    from data_pipeline.transformers import detect_runs_and_stoppers
                    runs = detect_runs_and_stoppers(pbp_lu)
                    if not runs.empty:
                        st.dataframe(
                            runs[["run_points", "stopper_player", "stopper_team",
                                  "stopper_playtype", "period", "markertime"]].rename(
                                columns={
                                    "run_points": "Run (pts)", "stopper_player": "Broken By",
                                    "stopper_team": "Team", "stopper_playtype": "Play",
                                    "period": "Q", "markertime": "Time",
                                }
                            ),
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.info("No 8+ point runs detected yet.")

        # ------------------------------------------------------------------
        # TAB: Win Probability
        # ------------------------------------------------------------------
        with tab_wp:
            st.markdown(f"### {t('live_win_prob')}")

            from data_pipeline.live_metrics import (
                compute_live_win_probability,
                compute_win_probability_timeline,
            )

            # Current win probability gauge
            home_wp = compute_live_win_probability(
                home_score, away_score,
                score_info["total_seconds_remaining"],
            )
            away_wp = 1.0 - home_wp

            col_wp1, col_wp2 = st.columns(2)
            with col_wp1:
                fig_gauge_h = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=home_wp * 100,
                    title={"text": f"{sel_game['home_code']} Win %",
                           "font": {"color": "#e4e4f0", "size": 16}},
                    number={"suffix": "%", "font": {"color": "#6366f1", "size": 32}},
                    gauge=dict(
                        axis=dict(range=[0, 100], tickcolor="#6b7280"),
                        bar=dict(color="#6366f1"),
                        bgcolor="rgba(30,30,63,0.8)",
                        steps=[
                            dict(range=[0, 50], color="rgba(239,68,68,0.15)"),
                            dict(range=[50, 100], color="rgba(99,102,241,0.15)"),
                        ],
                        threshold=dict(line=dict(color="#f59e0b", width=3), value=50, thickness=0.8),
                    ),
                ))
                fig_gauge_h.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e4f0"),
                    height=250, margin=dict(t=60, b=20, l=30, r=30),
                )
                st.plotly_chart(fig_gauge_h, use_container_width=True)

            with col_wp2:
                fig_gauge_a = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=away_wp * 100,
                    title={"text": f"{sel_game['away_code']} Win %",
                           "font": {"color": "#e4e4f0", "size": 16}},
                    number={"suffix": "%", "font": {"color": "#8b5cf6", "size": 32}},
                    gauge=dict(
                        axis=dict(range=[0, 100], tickcolor="#6b7280"),
                        bar=dict(color="#8b5cf6"),
                        bgcolor="rgba(30,30,63,0.8)",
                        steps=[
                            dict(range=[0, 50], color="rgba(239,68,68,0.15)"),
                            dict(range=[50, 100], color="rgba(139,92,246,0.15)"),
                        ],
                        threshold=dict(line=dict(color="#f59e0b", width=3), value=50, thickness=0.8),
                    ),
                ))
                fig_gauge_a.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#e4e4f0"),
                    height=250, margin=dict(t=60, b=20, l=30, r=30),
                )
                st.plotly_chart(fig_gauge_a, use_container_width=True)

            # Win probability timeline
            st.markdown(f"### {t('live_wp_timeline')}")
            wp_timeline = compute_win_probability_timeline(pbp_df)

            if wp_timeline.empty:
                st.info("No scoring events yet for win probability timeline.")
            else:
                fig_wp = go.Figure()

                fig_wp.add_trace(go.Scatter(
                    x=list(range(len(wp_timeline))),
                    y=wp_timeline["home_wp"] * 100,
                    mode="lines",
                    name=sel_game["home_code"],
                    line=dict(color="#6366f1", width=2.5),
                    fill="tozeroy",
                    fillcolor="rgba(99,102,241,0.08)",
                    hovertemplate=(
                        "%{customdata[0]}-%{customdata[1]}<br>"
                        "Home WP: %{y:.1f}%<extra></extra>"
                    ),
                    customdata=list(zip(
                        wp_timeline["home_score"], wp_timeline["away_score"],
                    )),
                ))

                fig_wp.add_hline(y=50, line_dash="dash", line_color="rgba(245,158,11,0.5)",
                                 annotation_text="50%", annotation_font_color="#f59e0b")

                fig_wp.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)",
                    height=400,
                    yaxis=dict(title="Home Win Probability (%)", range=[0, 100]),
                    xaxis_title="Scoring Events",
                    font=dict(family="Inter"),
                    showlegend=True,
                    legend=dict(x=0.02, y=0.98),
                )
                st.plotly_chart(fig_wp, use_container_width=True)

    # Schedule auto-rerun using a short sleep to avoid blocking indefinitely.
    # Streamlit will rerun when the sleep ends; the elapsed check at the top
    # of this page handles the 30-second gate.
    if "live_last_refresh" in st.session_state:
        remaining = max(0, 30 - (_time.time() - st.session_state["live_last_refresh"]))
        if remaining > 0 and remaining <= 30:
            _time.sleep(remaining)
            st.session_state["live_last_refresh"] = _time.time()
            if "live_cache" in st.session_state:
                del st.session_state["live_cache"]
            st.rerun()


# ========================================================================
# PAGE: LEAGUE LEADERS
# ========================================================================
elif selected_nav == NAV_LEADERS:
    st.markdown(f'<p class="section-header">{t("leaders_title")}</p>', unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('leaders_subtitle')}</p>",
        unsafe_allow_html=True,
    )

    season_leaders = st.session_state.get("selected_season", 2025)

    from streamlit_app.queries import fetch_league_leaders

    with st.spinner(t("leaders_loading")):
        leaders_data = fetch_league_leaders(season_leaders)

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

    # --- Configurable filters ---
    MIN_GAMES_DEFAULT = 10
    MIN_FGA2_DEFAULT = 40
    MIN_FGA3_DEFAULT = 30
    MIN_FTA_DEFAULT = 30

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


# ========================================================================
# PAGE: AI SCOUTING ENGINE
# ========================================================================
elif selected_nav == NAV_SCOUTING:
    st.markdown(f'<p class="section-header">{t("card_scouting_title")}</p>', unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('scout_method_note')}</p>",
        unsafe_allow_html=True,
    )

    season_scout = st.session_state.get("selected_season", 2025)

    from streamlit_app.queries import fetch_scouting_player_pool
    from data_pipeline.scouting_engine import (
        find_similar_players,
        build_multi_radar,
        get_player_feature_vector,
        FEATURE_COLUMNS,
        FEATURE_LABELS,
        MIN_MINUTES_PG,
    )

    with st.spinner(t("scout_loading_pool")):
        player_pool = fetch_scouting_player_pool(season_scout)

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
    prof_cols[1].metric(t("scout_position_label"), target_pos)
    prof_cols[2].metric("GP", f"{int(tr['games_played'])}")
    prof_cols[3].metric("MPG", f"{tr['minutes_pg']:.1f}")
    prof_cols[4].metric("PPG", f"{tr['points_pg']:.1f}")
    prof_cols[5].metric("TS%", f"{tr['ts_pct']:.1%}")
    prof_cols[6].metric("tUSG%", f"{tr['true_usg_pct']:.1%}")

    st.markdown("---")

    # --- Find similar players ---
    top_n = 5
    similar_df = find_similar_players(
        target_player, filtered_pool, top_n=top_n,
        position_filter=active_pos_filter,
    )

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
        hovertemplate="%{y}: %{x:.1f}% similarity<extra></extra>",
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


# ========================================================================
# PAGE: REFEREE ANALYTICS
# ========================================================================
elif selected_nav == NAV_REFEREE:
    st.markdown(f'<p class="section-header">{t("hdr_referee_stats")}</p>', unsafe_allow_html=True)
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_referee_stats')}</p>", unsafe_allow_html=True)
    st.markdown("---")

    season_to_fetch = st.session_state.get("selected_season", 2025)
    selected_team = st.session_state.get("selected_team")

    if not selected_team:
        st.warning(t("warn_select_team"))
        st.stop()

    min_ref_games = st.slider(
        t("lbl_min_ref_games"), min_value=1, max_value=10, value=3, key="min_ref_games",
    )

    with st.spinner(t("fetching_referee_stats")):
        from streamlit_app.queries import fetch_referee_stats
        ref_stats = fetch_referee_stats(season_to_fetch, selected_team, min_games=min_ref_games)

    if ref_stats.empty:
        st.info(t("no_referee_stats"))
    else:
        best_pct = ref_stats["win_pct"].max()
        worst_pct = ref_stats["win_pct"].min()

        c1, c2, c3 = st.columns(3)
        c1.metric(t("metric_total_refs"), len(ref_stats))
        c2.metric(t("metric_best_ref"), f"{best_pct:.1f}%")
        c3.metric(t("metric_worst_ref"), f"{worst_pct:.1f}%")

        st.markdown("---")

        win_pct_label = t("col_win_pct")
        display_df = ref_stats.rename(columns={
            "referee": t("col_referee"), "games": t("col_games"),
            "wins": t("col_wins"), "losses": t("col_losses"), "win_pct": win_pct_label,
        })

        def highlight_win_pct(row):
            styles = [""] * len(row)
            if win_pct_label in row.index:
                idx = list(row.index).index(win_pct_label)
                if row.iloc[idx] == best_pct:
                    styles[idx] = "background-color: rgba(16,185,129,0.25); color: #10b981; font-weight: bold"
                elif row.iloc[idx] == worst_pct:
                    styles[idx] = "background-color: rgba(239,68,68,0.25); color: #ef4444; font-weight: bold"
            return styles

        styled = display_df.style.apply(highlight_win_pct, axis=1).format(precision=1)
        st.dataframe(styled, use_container_width=True, hide_index=True, height=450)


# ========================================================================
# PAGE: ASK THE DATA (LLM Chat Agent)
# ========================================================================
elif selected_nav == NAV_CHAT:
    st.markdown(f'<p class="section-header">{t("chat_title")}</p>', unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('chat_subtitle')}</p>",
        unsafe_allow_html=True,
    )

    # --- Check for API key ---
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        st.error(t("chat_no_api_key"))
        st.stop()

    # --- Load data for the agent ---
    season_chat = st.session_state.get("selected_season", 2025)

    @st.cache_data(ttl=3600, show_spinner=False)
    def _load_chat_dataframes(season: int):
        from streamlit_app.queries import fetch_scouting_player_pool, fetch_league_efficiency_landscape
        player_df = fetch_scouting_player_pool(season)
        team_df = fetch_league_efficiency_landscape(season)
        return player_df, team_df

    with st.spinner(t("chat_loading_data")):
        player_df, team_df = _load_chat_dataframes(season_chat)

    if player_df.empty and team_df.empty:
        st.warning(t("chat_no_data"))
        st.stop()

    # --- Build agent (cached in session state) ---
    agent_cache_key = f"chat_agent_{season_chat}"
    if agent_cache_key not in st.session_state:
        try:
            from streamlit_app.chat_agent import build_chat_agent
            st.session_state[agent_cache_key] = build_chat_agent(player_df, team_df)
        except Exception as e:
            st.error(f"{t('chat_agent_error')}: {e}")
            st.stop()

    agent = st.session_state[agent_cache_key]

    # --- Initialize chat history ---
    if "chat_messages" not in st.session_state:
        st.session_state["chat_messages"] = []

    # --- Display existing messages ---
    for msg in st.session_state["chat_messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # --- Chat input ---
    if prompt := st.chat_input(t("chat_placeholder")):
        st.session_state["chat_messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner(t("chat_thinking")):
                from streamlit_app.chat_agent import ask_agent
                answer = ask_agent(agent, prompt)
            st.markdown(answer)

        st.session_state["chat_messages"].append({"role": "assistant", "content": answer})

    # --- Suggested questions ---
    if not st.session_state["chat_messages"]:
        st.markdown(f"#### {t('chat_suggestions_title')}")
        suggestions = [
            t("chat_suggestion_1"),
            t("chat_suggestion_2"),
            t("chat_suggestion_3"),
            t("chat_suggestion_4"),
        ]
        cols = st.columns(2)
        for i, suggestion in enumerate(suggestions):
            with cols[i % 2]:
                st.markdown(
                    f'<div style="background:linear-gradient(135deg,#1e1e3f,#2a2a5a); '
                    f'border:1px solid rgba(255,255,255,0.08); border-radius:12px; '
                    f'padding:16px; margin-bottom:12px; color:#9ca3af; font-size:0.9rem;">'
                    f'💡 {suggestion}</div>',
                    unsafe_allow_html=True,
                )


# ========================================================================
# PAGE: METRICS GLOSSARY
# ========================================================================
elif selected_nav == NAV_GLOSSARY:
    st.markdown('<p class="section-header">Advanced Metrics Glossary</p>', unsafe_allow_html=True)
    st.markdown("---")

    st.markdown(f"""
### {t('gloss_ortg_title')}
{t('gloss_ortg_desc')}

### {t('gloss_drtg_title')}
{t('gloss_drtg_desc')}

### {t('gloss_net_title')}
{t('gloss_net_desc')}

### {t('gloss_ts_title')}
{t('gloss_ts_desc')}

### {t('gloss_tusg_title')}
{t('gloss_tusg_desc')}

### {t('gloss_stop_title')}
{t('gloss_stop_desc')}

### {t('gloss_ast_title')}
{t('gloss_ast_desc')}

### {t('gloss_tov_title')}
{t('gloss_tov_desc')}
""")

    st.info(f"{t('gloss_tip_title')} {t('gloss_tip_desc')}")
