"""
queries.py — Data Access Layer for the Streamlit Dashboard
============================================================
Provides two modes of data access:

  1. LIVE MODE (default): Fetches data directly from the Euroleague API
     via the extractors and processes it through the transformers.
     No database required — works immediately.

  2. DB MODE: Queries the PostgreSQL database for pre-loaded data.
     Requires running the ETL pipeline first (load_to_db.run_pipeline).

The dashboard uses LIVE MODE by default, falling back to cached data
in st.session_state to avoid redundant API calls.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

COMPETITION = "E"

logger = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")


# ========================================================================
# LIVE MODE — Direct API extraction + transformation
# ========================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_season_schedule(
    season: int,
    competition: str = "E",
) -> pd.DataFrame:
    """
    Fetch the full schedule for a season and cache it for 1 hour.
    Used to populate the cascading Season/Round/Matchup dropdowns.
    """
    from data_pipeline.extractors import get_season_schedule, apply_team_aliases
    schedule = get_season_schedule(season, competition)
    if not schedule.empty:
        schedule = apply_team_aliases(schedule, ["home_code", "away_code"])
    return schedule


def fetch_game_data_live(
    season: int,
    gamecode: int,
    competition: str = "E",
) -> Dict[str, pd.DataFrame]:
    """
    Fetch and process all data for a single game directly from the API.

    Returns a dict with:
      - boxscore: raw boxscore DataFrame
      - pbp: raw play-by-play DataFrame
      - shots: raw shot data with COORD_X/COORD_Y
      - game_info: game metadata
      - advanced_stats: computed player-level advanced stats
      - pbp_with_lineups: PBP enriched with lineup tracking
      - lineup_stats: per-lineup net rating
      - assist_network: passer→scorer relationships
      - clutch_stats: clutch situation player stats
      - run_stoppers: run-breaking events
      - foul_trouble: foul trouble impact analysis
      - duo_synergy: 2-player combo performance
      - trio_synergy: 3-player combo performance
      - shot_quality: per-player shot quality vs. expected
    """
    from data_pipeline.extractors import extract_game_data
    from data_pipeline.transformers import (
        compute_advanced_stats,
        track_lineups,
        compute_lineup_stats,
        compute_duo_trio_synergy,
        compute_clutch_stats,
        detect_runs_and_stoppers,
        foul_trouble_impact,
        build_assist_network,
        compute_shot_quality,
    )

    # 1. Extract raw data
    raw = extract_game_data(season, gamecode, competition)
    boxscore_df = raw["boxscore"]
    pbp_df = raw["pbp"]
    shots_df = raw["shots"]
    game_info_df = raw["game_info"]

    # 2. Compute base + custom advanced stats
    advanced_df = compute_advanced_stats(boxscore_df)

    # 3. PBP analytics — lineup tracking
    pbp_lu = track_lineups(pbp_df, boxscore_df)

    # 4. Lineup stats & synergy
    lineup_stats = compute_lineup_stats(pbp_lu, boxscore_df)
    duo_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=2)
    trio_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=3)

    # 5. Clutch, runs, foul trouble
    clutch = compute_clutch_stats(pbp_df, boxscore_df)
    stoppers = detect_runs_and_stoppers(pbp_lu)
    foul_impact = foul_trouble_impact(pbp_df, boxscore_df)

    # 6. Assist network
    assists = build_assist_network(pbp_df)

    # 7. Shot quality
    shot_quality = compute_shot_quality(shots_df)

    return {
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
        "shot_quality": shot_quality,
    }


@st.cache_data(ttl=3600)
def fetch_league_efficiency_landscape(season: int, competition: str = COMPETITION) -> pd.DataFrame:
    """Fetch and calculate all 18 teams' ORtg and DRtg for the given season."""
    from data_pipeline.extractors import get_league_efficiency_landscape
    return get_league_efficiency_landscape(season, competition)


@st.cache_data(ttl=3600)
def fetch_team_season_data(season: int, team_code: str, competition: str = COMPETITION) -> Dict[str, pd.DataFrame]:
    """
    Fetch all boxscores/PBPs for a team's season, and aggregate into mathematically
    accurate season-level player stats and lineup stats.
    """
    from data_pipeline.extractors import extract_team_season_data
    from data_pipeline.transformers import (
        compute_advanced_stats,
        track_lineups,
        compute_lineup_stats,
        compute_season_player_stats,
    )
    import pandas as pd
    
    raw = extract_team_season_data(season, team_code, competition)
    if raw["boxscore"].empty:
        return {}

    adv_df = compute_advanced_stats(raw["boxscore"])
    season_player_stats = compute_season_player_stats(adv_df, team_code)
    
    # Track lineups per game, then concat
    pbp_lu_list = []
    if not raw["pbp"].empty and "Gamecode" in raw["pbp"].columns:
        for gamecode in raw["pbp"]["Gamecode"].unique():
            g_pbp = raw["pbp"][raw["pbp"]["Gamecode"] == gamecode]
            g_box = raw["boxscore"][raw["boxscore"]["Gamecode"] == gamecode]
            if not g_pbp.empty and not g_box.empty:
                pbp_lu_list.append(track_lineups(g_pbp, g_box))
                
    pbp_lu = pd.concat(pbp_lu_list, ignore_index=True) if pbp_lu_list else pd.DataFrame()
    season_lineup_stats = compute_lineup_stats(pbp_lu, raw["boxscore"], min_events=20)
    
    return {
        "player_season_stats": season_player_stats,
        "lineup_season_stats": season_lineup_stats,
    }


# ========================================================================
# DB MODE — SQL queries for pre-loaded data
# ========================================================================

def _get_db_engine():
    """Get SQLAlchemy engine for database queries."""
    try:
        from data_pipeline.load_to_db import get_engine
        return get_engine()
    except Exception as e:
        logger.warning(f"Database not available: {e}")
        return None


SQL_PLAYER_STATS = """
    SELECT
        pas.season, pas.gamecode,
        pas.player_id, pas.player_name, pas.team_code, pas.is_home,
        pas.minutes, pas.points,
        pas.fgm2, pas.fga2, pas.fgm3, pas.fga3,
        pas.ftm, pas.fta,
        pas.off_rebounds, pas.def_rebounds, pas.total_rebounds,
        pas.assists, pas.steals, pas.turnovers,
        pas.blocks_favour, pas.blocks_against,
        pas.fouls_committed, pas.fouls_received, pas.plus_minus,
        pas.possessions, pas.ts_pct, pas.off_rating, pas.def_rating
    FROM player_advanced_stats pas
    WHERE 1=1
"""

SQL_TEAM_STATS = """
    SELECT
        pas.team_code,
        COUNT(DISTINCT pas.gamecode) AS games_played,
        AVG(pas.points) AS avg_points,
        AVG(pas.ts_pct) AS avg_ts_pct,
        AVG(pas.off_rating) AS avg_off_rating,
        AVG(pas.def_rating) AS avg_def_rating,
        AVG(pas.possessions) AS avg_possessions
    FROM player_advanced_stats pas
    WHERE pas.minutes > 0
    GROUP BY pas.team_code
    ORDER BY avg_off_rating DESC
"""


def query_player_stats_db(
    season: Optional[int] = None,
    team_code: Optional[str] = None,
    player_name: Optional[str] = None,
) -> pd.DataFrame:
    """Query player advanced stats from the database with optional filters."""
    engine = _get_db_engine()
    if engine is None:
        return pd.DataFrame()

    query = SQL_PLAYER_STATS
    params = {}

    if season:
        query += " AND pas.season = :season"
        params["season"] = season
    if team_code:
        query += " AND pas.team_code = :team_code"
        params["team_code"] = team_code
    if player_name:
        query += " AND pas.player_name ILIKE :player_name"
        params["player_name"] = f"%{player_name}%"

    query += " ORDER BY pas.points DESC"

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def query_team_stats_db() -> pd.DataFrame:
    """Query aggregated team stats from the database."""
    engine = _get_db_engine()
    if engine is None:
        return pd.DataFrame()

    with engine.connect() as conn:
        return pd.read_sql(SQL_TEAM_STATS, conn)
