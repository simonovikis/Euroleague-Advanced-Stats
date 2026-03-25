"""
app.py — Euroleague Advanced Analytics Dashboard
==================================================
Multi-page Streamlit dashboard with:
  1. Player Advanced Stats (sortable table + ORtg vs DRtg scatter)
  2. Shot Chart (half-court Plotly scatter with X/Y coordinates)
  3. Player Comparison Radar (select 2 players, 5-axis radar chart)
  4. Lineup & Synergy Analytics (5-man lineup tables, duo/trio combos)
  5. Assist Network (heatmap of passer→scorer relationships)
  6. Clutch & Momentum (clutch stats, run-stopping events, foul trouble)

Runs in LIVE MODE by default — fetches directly from the Euroleague API.
No database required.  Data is cached in session_state after first load.

Launch:  streamlit run streamlit_app/app.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure project root is on the Python path
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

@st.cache_data
def load_translations():
    with open(Path(_project_root) / "streamlit_app" / "translations.json", "r", encoding="utf-8") as f:
        return json.load(f)

TRANSLATIONS = load_translations()

def t(key: str, **kwargs) -> str:
    """Helper to fetch translated strings from translations.json based on session state lang."""
    lang = st.session_state.get('lang', 'en')
    text = TRANSLATIONS.get(key, {}).get(lang, TRANSLATIONS.get(key, {}).get('en', key))
    return text.format(**kwargs) if kwargs else text


# ========================================================================
# CUSTOM CSS — Premium dark-mode styling
# ========================================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global styling */
    .stApp {
        font-family: 'Inter', sans-serif;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f23 0%, #1a1a3e 100%);
    }
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2,
    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #e4e4f0;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e1e3f 0%, #2a2a5a 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    [data-testid="stMetric"] label {
        color: #9ca3af !important;
        font-size: 0.85rem !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f0f0ff !important;
        font-weight: 700 !important;
    }

    /* DataFrames */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }

    /* Section headers */
    .section-header {
        background: linear-gradient(90deg, #6366f1 0%, #8b5cf6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 8px 20px;
    }

    /* Game header with logos */
    .game-header {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 24px;
        padding: 16px 0;
    }
    .game-header .team-block {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 6px;
    }
    .game-header .team-logo {
        width: 64px;
        height: 64px;
        object-fit: contain;
    }
    .game-header .team-name {
        font-size: 1.1rem;
        font-weight: 600;
    }
    .game-header .score {
        font-size: 2.4rem;
        font-weight: 700;
        color: #e4e4f0;
        letter-spacing: 2px;
    }
    .game-header .dash {
        color: #6b7280;
        font-size: 2rem;
        margin: 0 4px;
    }
</style>
""", unsafe_allow_html=True)


# ========================================================================
# SIDEBAR — Game Selection & Navigation
# ========================================================================
with st.sidebar:
    # 1. Language Selector
    lang_map = {"🇬🇧 English": "en", "🇬🇷 Ελληνικά": "el", "🇩🇪 Deutsch": "de", "🇪🇸 Español": "es"}
    default_lang_idx = list(lang_map.values()).index(st.session_state.get('lang', 'en'))
    
    selected_lang_label = st.selectbox(
        t("lang_selection"), 
        list(lang_map.keys()), 
        index=default_lang_idx,
        label_visibility="collapsed"
    )
    if st.session_state.get('lang') != lang_map[selected_lang_label]:
        st.session_state['lang'] = lang_map[selected_lang_label]
        st.rerun()

    st.markdown(f"# {t('app_title')}")
    st.markdown("---")

    st.markdown(f"### 🔍 {t('analysis_mode')}")
    
    mode_options = [t("mode_single"), t("mode_season"), t("mode_glossary")]
    selected_mode = st.radio("Analysis Mode", mode_options, label_visibility="collapsed")
    
    if selected_mode == t("mode_single"):
        view_mode = "Single Game Analysis"
    elif selected_mode == t("mode_season"):
        view_mode = "Season Overview"
    else:
        view_mode = "Metrics Glossary"
    st.markdown("---")

    st.markdown(f"### 📅 {t('selection_header')}")
    
    # Session state for cascading selection
    if "selected_season" not in st.session_state:
        st.session_state.selected_season = 2025
    if "selected_round" not in st.session_state:
        st.session_state.selected_round = 1

    def on_season_change():
        st.session_state.selected_round = 1

    seasons = list(range(2025, 2010, -1))
    
    # 1. Season Dropdown
    selected_season_input = st.selectbox(
        t("season_dropdown"), 
        seasons, 
        index=seasons.index(st.session_state.selected_season),
        key="season_picker",
        on_change=on_season_change
    )
    st.session_state.selected_season = selected_season_input
    
    # Fetch cached schedule
    schedule = fetch_season_schedule(st.session_state.selected_season)
    
    if schedule.empty:
        st.warning(t("err_no_schedule", season=st.session_state.selected_season))
        gamecode = 1
        st.session_state["game_info_cache"] = None
        page = None
    else:
        if view_mode == "Single Game Analysis":
            # 2. Round Dropdown
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
                key="round_picker"
            )
            st.session_state.selected_round = selected_round_input
            
            # 3. Matchup Dropdown
            round_games = schedule[schedule["round"] == st.session_state.selected_round].copy()
            
            def format_matchup(row):
                home = row.get('home_code', row.get('home_team', '???'))
                away = row.get('away_code', row.get('away_team', '???'))
                if pd.notna(row.get('home_score')) and pd.notna(row.get('away_score')):
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
            else:
                gamecode = 1
                st.session_state["game_info_cache"] = None

            st.markdown("---")
            st.markdown(f"### 📊 {t('dashboard_pages')}")
            page = st.radio(
                t("navigate_to"),
                [
                    t("nav_player_stats"),
                    t("nav_shot_chart"),
                    t("nav_radar"),
                    t("nav_lineups"),
                    t("nav_assist"),
                    t("nav_clutch"),
                ],
                label_visibility="collapsed",
            )
        elif view_mode == "Season Overview":
            # Season Overview Mode Sidebar
            team_options = sorted(list(set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())))
            st.session_state["selected_team"] = st.selectbox(t("team_dropdown"), team_options, key="team_picker")
            page = "Season Overview"
        else:
            # Metrics Glossary Mode Sidebar
            page = "Metrics Glossary"

    st.markdown("---")
    st.markdown(
        "<p style='color:#6b7280; font-size:0.75rem;'>"
        "Data: euroleague-api • Built with Streamlit & Plotly</p>",
        unsafe_allow_html=True,
    )


# ========================================================================
# PAGE: SEASON OVERVIEW
# ========================================================================
if view_mode == "Season Overview":
    st.markdown(f'<p class="section-header">{t("hdr_season_overview")}</p>', unsafe_allow_html=True)
    
    season_to_fetch = st.session_state.get("selected_season", 2025)
    team_code = st.session_state.get("selected_team")
    
    if not team_code:
        st.warning(t("warn_select_team"))
        st.stop()
        
    # --- 1. League Efficiency Landscape ---
    st.markdown(f"### {t('hdr_league_eff')}")
    st.markdown(f"<p style='color:#9ca3af; font-size:0.9rem;'>{t('sub_league_eff')}</p>", unsafe_allow_html=True)
    
    with st.spinner(t("fetching_league_eff")):
        from streamlit_app.queries import fetch_league_efficiency_landscape, fetch_team_season_data
        eff_df = fetch_league_efficiency_landscape(season_to_fetch)
        
    if eff_df.empty:
        st.warning(t("err_league_eff"))
    else:
        # Create color column to highlight selected team
        eff_df["color"] = np.where(eff_df["team_code"] == team_code, "#8b5cf6", "#4b5563")
        eff_df["size"] = np.where(eff_df["team_code"] == team_code, 15, 10)
        
        fig_eff = px.scatter(
            eff_df, x="ortg", y="drtg", hover_name="team_name",
            color="color", size="size", color_discrete_map="identity",
            labels={"ortg": t("lbl_ortg"), "drtg": t("lbl_drtg")}
        )
        # Top right quadrant = high ORtg, LOW DRtg, so we invert Y!
        fig_eff.update_yaxes(autorange="reversed")
        
        # Add quadrant lines (average ORtg/DRtg)
        mean_ortg = eff_df["ortg"].mean()
        mean_drtg = eff_df["drtg"].mean()
        fig_eff.add_hline(y=mean_drtg, line_dash="dash", line_color="#374151")
        fig_eff.add_vline(x=mean_ortg, line_dash="dash", line_color="#374151")
        
        fig_eff.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e4e4f0"), showlegend=False, height=500
        )
        st.plotly_chart(fig_eff, use_container_width=True)
        
    st.markdown("---")
    
    # --- 2. Team Averages & Lineups ---
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
        labels={"true_usg_pct": t("lbl_tusg"), "ts_pct": t("lbl_ts"), "points": t("col_pts")}
    )
    
    # Format Y axis as percentage
    fig_usage.layout.yaxis.tickformat = '.1%'
    
    # Mean axis lines
    mean_usg = player_stats["true_usg_pct"].mean()
    mean_ts = player_stats["ts_pct"].mean()
    fig_usage.add_hline(y=mean_ts, line_dash="dash", line_color="#374151")
    fig_usage.add_vline(x=mean_usg, line_dash="dash", line_color="#374151")
    
    fig_usage.update_layout(
         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
         font=dict(color="#e4e4f0"), height=500
    )
    st.plotly_chart(fig_usage, use_container_width=True)
    
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
                    columns={"lineup_str": t("col_lineup"), "events": t("col_poss"), "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                ),
                use_container_width=True,
                hide_index=True
            )
        with col2:
            st.markdown(f"#### {t('hdr_worst_net')}")
            st.dataframe(
                lineup_stats.tail(5)[["lineup_str", "events", "ortg", "drtg", "net_rtg"]].rename(
                    columns={"lineup_str": t("col_lineup"), "events": t("col_poss"), "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                ),
                use_container_width=True,
                hide_index=True
            )
            
    # Stop execution here so Single Game views don't render!
    st.stop()


# ========================================================================
# PAGE: METRICS GLOSSARY
# ========================================================================
if view_mode == "Metrics Glossary":
    st.markdown('<p class="section-header">📖 Advanced Metrics Glossary</p>', unsafe_allow_html=True)
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
    
    st.stop()


# ========================================================================
# DATA LOADING — Cached in session_state (Single Game Mode)
# ========================================================================
# Auto-load when season or gamecode changes
data_needs_update = (
    "game_data" not in st.session_state or 
    st.session_state.get("season") != st.session_state.get("selected_season", 2025) or 
    st.session_state.get("gamecode") != gamecode
)

if data_needs_update:
    with st.spinner("⏳ Fetching advanced insights from Euroleague API..."):
        try:
            season_to_fetch = st.session_state.get("selected_season", 2025)
            st.session_state["game_data"] = fetch_game_data_live(season_to_fetch, gamecode)
            st.session_state["season"] = season_to_fetch
            st.session_state["gamecode"] = gamecode
        except Exception as e:
            st.error(f"❌ Failed to load game data (Game {gamecode}): {e}")
            st.stop()

data = st.session_state["game_data"]

# Game header with logos and full team names
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
    # Fallback to local game_info data if schedule API failed
    local_gi = data["game_info"].iloc[0] if not data["game_info"].empty else {}
    home_code = local_gi.get("home_team", "???")
    away_code = local_gi.get("away_team", "???")
    home_name = home_code
    away_name = away_code
    hs = local_gi.get("home_score", "-")
    as_ = local_gi.get("away_score", "-")
    home_logo = ""
    away_logo = ""

# Indestructible UI-Avatars fallback if image 404s
fb_home = f"https://ui-avatars.com/api/?name={home_code}&background=2a2a5a&color=e4e4f0&size=128&rounded=true&bold=true"
fb_away = f"https://ui-avatars.com/api/?name={away_code}&background=2a2a5a&color=e4e4f0&size=128&rounded=true&bold=true"

hl = home_logo if home_logo else fb_home
al = away_logo if away_logo else fb_away

home_logo_html = f'<img src="{hl}" class="team-logo" alt="{home_code}" onerror="this.onerror=null; this.src=\'{fb_home}\';">'
away_logo_html = f'<img src="{al}" class="team-logo" alt="{away_code}" onerror="this.onerror=null; this.src=\'{fb_away}\';">'

st.markdown(
    f'<div class="game-header">'
    f'  <div class="team-block">'
    f'    {home_logo_html}'
    f'    <span class="team-name" style="color:#6366f1;">{home_name}</span>'
    f'  </div>'
    f'  <span class="score">{hs}<span class="dash"> — </span>{as_}</span>'
    f'  <div class="team-block">'
    f'    {away_logo_html}'
    f'    <span class="team-name" style="color:#8b5cf6;">{away_name}</span>'
    f'  </div>'
    f'</div>'
    f'<p style="text-align:center; color:#6b7280; margin-top:-8px;">Season {st.session_state.get("season","")} • Game {st.session_state.get("gamecode","")}</p>',
    unsafe_allow_html=True,
)
st.markdown("")


# ========================================================================
# PAGE 1: PLAYER ADVANCED STATS
# ========================================================================
if page == t("nav_player_stats"):
    st.markdown(f'<p class="section-header">{t("hdr_player_stats")}</p>', unsafe_allow_html=True)

    adv = data["advanced_stats"]
    if adv.empty:
        st.warning(t("no_adv_stats"))
    else:
        # Filters
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

        # Active players (minutes > 0)
        active = filtered[filtered["minutes"] > 0].copy()

        # KPI metrics row
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

        # Stats table
        display_cols = [
            "player_name", "team_code", "minutes", "points",
            "possessions", "ts_pct", "off_rating", "def_rating",
        ]
        if "true_usg_pct" in active.columns:
            display_cols += ["true_usg_pct", "stop_rate"]
        display_cols = [c for c in display_cols if c in active.columns]

        st.dataframe(
            active[display_cols].round(3).sort_values("points", ascending=False).rename(
                columns={
                    "player_name": t("col_player"), "team_code": t("col_team"), "minutes": t("col_min"),
                    "points": t("col_pts"), "possessions": t("col_poss"), "ts_pct": t("col_ts"),
                    "off_rating": t("col_ortg"), "def_rating": t("col_drtg"), "true_usg_pct": t("col_tusg"),
                }
            ),
            use_container_width=True,
            hide_index=True,
            height=400,
        )

        # ORtg vs DRtg scatter plot
        st.markdown(f"#### ⚔️ {t('lbl_ortg')} vs {t('lbl_drtg')}")
        st.caption("Lower DRtg = better defense. Higher ORtg = better offense. Top-left quadrant = elite.")

        scatter_df = active.dropna(subset=["off_rating", "def_rating"])
        if not scatter_df.empty:
            fig = px.scatter(
                scatter_df,
                x="def_rating",
                y="off_rating",
                color="team_code",
                text="player_name",
                size="minutes",
                size_max=20,
                hover_data=["points", "ts_pct", "possessions"],
                labels={"off_rating": "Offensive Rating", "def_rating": "Defensive Rating"},
                color_discrete_sequence=["#6366f1", "#f59e0b", "#10b981", "#ef4444"],
            )
            fig.update_traces(textposition="top center", textfont_size=9)
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(15,15,35,0.8)",
                height=500,
                font=dict(family="Inter"),
            )
            # Add quadrant lines at the average
            avg_ortg = scatter_df["off_rating"].mean()
            avg_drtg = scatter_df["def_rating"].mean()
            fig.add_hline(y=avg_ortg, line_dash="dash", line_color="rgba(255,255,255,0.2)")
            fig.add_vline(x=avg_drtg, line_dash="dash", line_color="rgba(255,255,255,0.2)")
            st.plotly_chart(fig, use_container_width=True)


# ========================================================================
# PAGE 2: SHOT CHART
# ========================================================================
elif page == t("nav_shot_chart"):
    st.markdown(f'<p class="section-header">{t("nav_shot_chart")}</p>', unsafe_allow_html=True)

    shots = data.get("shots", pd.DataFrame())
    shot_quality = data.get("shot_quality", pd.DataFrame())

    if shots.empty:
        st.warning(t("no_shot_data", default="No shot data available for this game."))
    else:
        # Player filter
        all_shooters = sorted(shots["PLAYER"].dropna().unique())
        sel_shooter = st.selectbox(t("filter_player"), [t("filter_all")] + all_shooters, key="shot_player")

        shot_df = shots.copy()
        if sel_shooter != t("filter_all"):
            shot_df = shot_df[shot_df["PLAYER"] == sel_shooter]

        if shot_df.empty:
            st.info(t("err_no_shots", default="No shots for this selection."))
        else:
            # Check for coordinate data
            has_coords = (
                "COORD_X" in shot_df.columns
                and "COORD_Y" in shot_df.columns
                and not shot_df["COORD_X"].isna().all()
            )

            if has_coords:
                # Color by outcome: made shots (POINTS > 0) vs misses
                shot_df["Outcome"] = shot_df["POINTS"].apply(
                    lambda p: t("lbl_made", default="Made") if p > 0 else t("lbl_missed", default="Missed")
                )
                shot_df["shot_type"] = shot_df["ID_ACTION"].apply(
                    lambda x: "3PT" if "3FG" in str(x) else "2PT"
                )

                fig = go.Figure()

                # Draw half-court outline (simplified)
                # Court boundary
                court_shapes = [
                    # Outer boundary
                    dict(type="rect", x0=-750, y0=0, x1=750, y1=1400,
                         line=dict(color="rgba(255,255,255,0.3)", width=1),
                         fillcolor="rgba(15,15,35,0.5)"),
                    # Paint / key area
                    dict(type="rect", x0=-250, y0=0, x1=250, y1=580,
                         line=dict(color="rgba(255,255,255,0.2)", width=1)),
                    # Free-throw circle
                    dict(type="circle", x0=-180, y0=400, x1=180, y1=760,
                         line=dict(color="rgba(255,255,255,0.15)", width=1)),
                    # 3-point arc (simplified as rectangle — actual arc via path)
                    dict(type="rect", x0=-675, y0=0, x1=675, y1=50,
                         line=dict(color="rgba(255,255,255,0.1)", width=0)),
                    # Rim
                    dict(type="circle", x0=-22, y0=50, x1=22, y1=94,
                         line=dict(color="#ff6b35", width=2),
                         fillcolor="rgba(255,107,53,0.2)"),
                ]
                fig.update_layout(shapes=court_shapes)

                # 3-point arc as a scatter path (FIBA radius 6.75m from hoop center)
                theta = np.linspace(0, np.pi, 100)
                arc_x = 675 * np.cos(theta)
                arc_y = 675 * np.sin(theta) + 72
                # Clip to European max court width (6.60m) to create the straight corner 3 lines!
                arc_x = np.clip(arc_x, -660, 660)
                
                fig.add_trace(go.Scatter(
                    x=arc_x, y=arc_y, mode="lines",
                    line=dict(color="rgba(255,255,255,0.25)", width=2),
                    showlegend=False, hoverinfo="skip",
                ))

                # Plot shots
                made = shot_df[shot_df["Outcome"] == "Made"]
                missed = shot_df[shot_df["Outcome"] == "Missed"]

                fig.add_trace(go.Scatter(
                    x=made["COORD_X"], y=made["COORD_Y"],
                    mode="markers",
                    marker=dict(color="#10b981", size=10, symbol="circle",
                                line=dict(width=1, color="white"), opacity=0.85),
                    name=t("lbl_made", default="Made"),
                    text=made["ACTION"],
                    hovertemplate="%{text}<br>Zone: %{customdata}<extra></extra>",
                    customdata=made["ZONE"],
                ))
                fig.add_trace(go.Scatter(
                    x=missed["COORD_X"], y=missed["COORD_Y"],
                    mode="markers",
                    marker=dict(color="#ef4444", size=8, symbol="x",
                                line=dict(width=1, color="white"), opacity=0.65),
                    name=t("lbl_missed", default="Missed"),
                    text=missed["ACTION"],
                    hovertemplate="%{text}<br>Zone: %{customdata}<extra></extra>",
                    customdata=missed["ZONE"],
                ))

                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.9)",
                    height=650,
                    width=700,
                    xaxis=dict(range=[-800, 800], showgrid=False, zeroline=False,
                               showticklabels=False, scaleanchor="y"),
                    yaxis=dict(range=[-50, 1000], showgrid=False, zeroline=False,
                               showticklabels=False),
                    legend=dict(x=0.02, y=0.98, font=dict(size=12)),
                    font=dict(family="Inter"),
                    margin=dict(l=20, r=20, t=30, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

                # Shot quality summary below chart
                if not shot_quality.empty:
                    st.markdown("#### 📊 Shot Quality Analysis")
                    sq = shot_quality.copy()
                    if sel_shooter != "All Players":
                        sq = sq[sq["PLAYER"] == sel_shooter]
                    if not sq.empty:
                        sq_display = sq[[
                            "PLAYER", "TEAM", "total_shots", "makes", "fg_pct",
                            "actual_pts", "total_expected_pts", "shot_quality_diff",
                        ]].rename(columns={
                            "PLAYER": "Player", "TEAM": "Team",
                            "total_shots": "Shots", "makes": "Makes",
                            "fg_pct": "FG%", "actual_pts": "Actual Pts",
                            "total_expected_pts": "Expected Pts",
                            "shot_quality_diff": "Pts vs Expected",
                        })
                        st.dataframe(sq_display.round(2), use_container_width=True, hide_index=True)
            else:
                st.info("⚠️ No X/Y coordinates available — showing zone breakdown instead.")
                zone_summary = (
                    shot_df.groupby("ZONE")
                    .agg(shots=("POINTS", "size"), made=("POINTS", lambda x: (x > 0).sum()))
                    .reset_index()
                )
                zone_summary["fg_pct"] = zone_summary["made"] / zone_summary["shots"]
                st.dataframe(zone_summary.round(3), use_container_width=True, hide_index=True)


# ========================================================================
# PAGE 3: PLAYER COMPARISON RADAR
# ========================================================================
elif page == t("nav_radar"):
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
            p2 = st.selectbox(t("lbl_player", default="Player") + " 2", players, index=min(1, len(players)-1), key="radar_p2")

        # Radar axes: ORtg, DRtg (inverted), tUSG%, Stop Rate, TS%
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

        # Normalise to 0-1 range for radar
        all_vals = active[radar_cols].copy()
        mins = all_vals.min()
        maxs = all_vals.max()
        ranges = maxs - mins
        ranges = ranges.replace(0, 1)  # avoid div by zero

        p1_norm = ((p1_data - mins) / ranges).values.tolist()
        p2_norm = ((p2_data - mins) / ranges).values.tolist()

        # Invert DRtg (lower = better, so invert the normalised value)
        drtg_idx = radar_labels.index("DRtg (inv)")
        p1_norm[drtg_idx] = 1 - p1_norm[drtg_idx]
        p2_norm[drtg_idx] = 1 - p2_norm[drtg_idx]

        # Close the radar polygon
        p1_norm.append(p1_norm[0])
        p2_norm.append(p2_norm[0])
        labels_closed = radar_labels + [radar_labels[0]]

        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=p1_norm, theta=labels_closed, fill="toself",
            name=p1, fillcolor="rgba(99,102,241,0.25)",
            line=dict(color="#6366f1", width=2),
        ))
        fig.add_trace(go.Scatterpolar(
            r=p2_norm, theta=labels_closed, fill="toself",
            name=p2, fillcolor="rgba(245,158,11,0.25)",
            line=dict(color="#f59e0b", width=2),
        ))
        fig.update_layout(
            polar=dict(
                bgcolor="rgba(15,15,35,0.8)",
                radialaxis=dict(visible=True, range=[0, 1], showticklabels=False,
                                gridcolor="rgba(255,255,255,0.1)"),
                angularaxis=dict(gridcolor="rgba(255,255,255,0.1)",
                                 tickfont=dict(size=12, color="#e4e4f0")),
            ),
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            height=500,
            font=dict(family="Inter"),
            legend=dict(x=0.35, y=-0.1, orientation="h", font=dict(size=13)),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Raw values comparison table
        st.markdown("#### 📋 Raw Values Comparison")
        comparison = pd.DataFrame({
            "Metric": radar_labels,
            p1: [round(p1_data.iloc[i], 3) for i in range(len(radar_labels))],
            p2: [round(p2_data.iloc[i], 3) for i in range(len(radar_labels))],
        })
        st.dataframe(comparison, use_container_width=True, hide_index=True)


# ========================================================================
# PAGE 4: LINEUP & SYNERGY
# ========================================================================
elif page == t("nav_lineups"):
    st.markdown(f'<p class="section-header">{t("nav_lineups")}</p>', unsafe_allow_html=True)

    tab_lu, tab_duo, tab_trio = st.tabs([t("tab_5man", default="🏅 5-Man Lineups"), t("tab_duo", default="👥 Duo Synergy"), t("tab_trio", default="🔺 Trio Synergy")])

    # --- 5-man lineups ---
    with tab_lu:
        lu_stats = data.get("lineup_stats", pd.DataFrame())
        if lu_stats.empty:
            st.info(t("no_lineups", default="No lineup data available."))
        else:
            # Filter by team
            lu_teams = sorted(lu_stats["team"].unique())
            sel_lu_team = st.selectbox(t("col_team", default="Team"), [t("filter_all")] + lu_teams, key="lu_team")
            lu_filtered = lu_stats if sel_lu_team == t("filter_all") else lu_stats[lu_stats["team"] == sel_lu_team]

            # Min events filter to exclude noise
            min_events = st.slider(t("lbl_min_events", default="Min PBP events"), 5, 100, 20, key="lu_min_events")
            lu_filtered = lu_filtered[lu_filtered["events"] >= min_events]

            if lu_filtered.empty:
                st.info(t("no_lineups", default="No lineups meet the minimum events threshold."))
            else:
                st.markdown(f"##### {t('hdr_best_net')}")
                best = lu_filtered.head(5)[["team", "lineup_str", "events", "pts_for", "pts_against", "ortg", "drtg", "net_rtg"]].rename(
                    columns={"team": t("col_team", default="Team"), "lineup_str": t("col_lineup"), "events": t("col_poss"), "pts_for": t("col_pts"), "pts_against": t("col_pts_ag", default="Opp Pts"), "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                )
                st.dataframe(best, use_container_width=True, hide_index=True)

                st.markdown(f"##### {t('hdr_worst_net')}")
                worst = lu_filtered.tail(5).sort_values("net_rtg")[["team", "lineup_str", "events", "pts_for", "pts_against", "ortg", "drtg", "net_rtg"]].rename(
                    columns={"team": t("col_team", default="Team"), "lineup_str": t("col_lineup"), "events": t("col_poss"), "pts_for": t("col_pts"), "pts_against": t("col_pts_ag", default="Opp Pts"), "ortg": t("col_ortg"), "drtg": t("col_drtg"), "net_rtg": t("col_netrtg")}
                )
                st.dataframe(worst, use_container_width=True, hide_index=True)

                # Bar chart of all lineups net rating
                fig = px.bar(
                    lu_filtered.head(15),
                    x="net_rtg", y="lineup_str", orientation="h",
                    color="net_rtg",
                    color_continuous_scale=["#ef4444", "#6b7280", "#10b981"],
                    color_continuous_midpoint=0,
                    labels={"net_rtg": "Net Rating", "lineup_str": "Lineup"},
                )
                fig.update_layout(
                    template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,15,35,0.8)", height=450,
                    font=dict(family="Inter"),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig, use_container_width=True)

    # --- Duo Synergy ---
    with tab_duo:
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

    # --- Trio Synergy ---
    with tab_trio:
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


# ========================================================================
# PAGE 5: ASSIST NETWORK
# ========================================================================
elif page == t("nav_assist"):
    st.markdown(f'<p class="section-header">{t("nav_assist")}</p>', unsafe_allow_html=True)

    assists = data.get("assist_network", pd.DataFrame())
    if assists.empty:
        st.warning(t("no_adv_stats", default="No assist data available."))
    else:
        # Team filter
        ast_teams = sorted(assists["team"].unique())
        sel_ast_team = st.selectbox(t("team_dropdown", default="Select Team"), ast_teams, key="ast_team")
        team_assists = assists[assists["team"] == sel_ast_team]

        if team_assists.empty:
            st.info(t("err_no_shots", default="No data for this team."))
        else:
            # Build assist matrix for heatmap
            passers = sorted(team_assists["assister_name"].unique())
            scorers = sorted(team_assists["scorer_name"].unique())
            all_players = sorted(set(passers) | set(scorers))

            matrix = pd.DataFrame(0, index=all_players, columns=all_players)
            for _, row in team_assists.iterrows():
                matrix.loc[row["assister_name"], row["scorer_name"]] += row["count"]

            # Plotly heatmap
            fig = go.Figure(data=go.Heatmap(
                z=matrix.values,
                x=matrix.columns.tolist(),
                y=matrix.index.tolist(),
                colorscale=[
                    [0, "rgba(15,15,35,0.9)"],
                    [0.25, "#312e81"],
                    [0.5, "#6366f1"],
                    [0.75, "#a78bfa"],
                    [1.0, "#f59e0b"],
                ],
                hovertemplate="Passer: %{y}<br>Scorer: %{x}<br>Assists: %{z}<extra></extra>",
                showscale=True,
                colorbar=dict(title=t("col_ast", default="Assists")),
            ))
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(15,15,35,0.8)",
                height=500,
                xaxis=dict(title="Scorer", tickangle=45, tickfont=dict(size=10)),
                yaxis=dict(title="Passer", tickfont=dict(size=10), autorange="reversed"),
                font=dict(family="Inter"),
                margin=dict(l=120, b=120),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Top connections table
            st.markdown("#### 🏅 Top Assist Connections")
            st.dataframe(
                team_assists[["assister_name", "scorer_name", "count", "play_types"]]
                .rename(columns={
                    "assister_name": "Passer", "scorer_name": "Scorer",
                    "count": "Assists", "play_types": "Shot Types",
                }),
                use_container_width=True, hide_index=True,
            )


# ========================================================================
# PAGE 6: CLUTCH & MOMENTUM
# ========================================================================
elif page == t("nav_clutch"):
    st.markdown(f'<p class="section-header">{t("nav_clutch")}</p>', unsafe_allow_html=True)

    tab_clutch, tab_runs, tab_fouls = st.tabs([
        t("tab_clutch", default="⏰ Clutch Stats"), t("tab_runs", default="🏃 Run Stoppers"), t("tab_fouls", default="⚠️ Foul Trouble Impact")
    ])

    # --- Clutch ---
    with tab_clutch:
        clutch = data.get("clutch_stats", pd.DataFrame())
        if clutch.empty:
            st.info(t("info_no_clutch", default="ℹ️ No clutch situations detected (score diff > 5 or game didn't reach 4th quarter crunch time)."))
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

    # --- Run Stoppers ---
    with tab_runs:
        stoppers = data.get("run_stoppers", pd.DataFrame())
        if stoppers.empty:
            st.info(t("info_no_runs", default="ℹ️ No 8+ point scoring runs detected in this game."))
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

    # --- Foul Trouble ---
    with tab_fouls:
        foul_trouble = data.get("foul_trouble", pd.DataFrame())
        if foul_trouble.empty:
            st.info(t("info_no_fouls", default="ℹ️ No foul trouble detected for high-usage players in the first half."))
        else:
            st.markdown(t("desc_fouls", default="Impact on team ratings when the **highest-usage player** gets 2+ fouls in the first half."))
            for _, ft in foul_trouble.iterrows():
                st.markdown(f"**{ft['team']}** — ⚡ {ft['star_player']} ({t('lbl_foul2', default='2nd foul in Q')}{ft['foul_period']})")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric(f"{t('lbl_ortg')} {t('lbl_before', default='Before')}", f"{ft['ortg_before']:.1f}")
                c2.metric(f"{t('lbl_ortg')} {t('lbl_after', default='After')}", f"{ft['ortg_after']:.1f}", delta=f"{ft['ortg_impact']:+.1f}")
                c3.metric(f"{t('lbl_drtg')} {t('lbl_before', default='Before')}", f"{ft['drtg_before']:.1f}")
                c4.metric(f"{t('lbl_drtg')} {t('lbl_after', default='After')}", f"{ft['drtg_after']:.1f}", delta=f"{ft['drtg_impact']:+.1f}", delta_color="inverse")
                st.markdown("---")
