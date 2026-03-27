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


def _use_db() -> bool:
    """Check if the dashboard should read from the PostgreSQL DB instead of the live API."""
    return os.getenv("USE_DB", "false").lower() == "true"


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_season_schedule(
    season: int,
    competition: str = "E",
) -> pd.DataFrame:
    """
    Fetch the full schedule for a season and cache it for 1 hour.
    Used to populate the cascading Season/Round/Matchup dropdowns.
    """
    from data_pipeline.extractors import apply_team_aliases
    
    if _use_db():
        engine = _get_db_engine()
        if engine:
            import pandas as pd
            query = f"""
                SELECT 
                    season, gamecode, 
                    home_team AS home_code, away_team AS away_code, 
                    home_score, away_score, 
                    game_date, round, played, 
                    referee1, referee2, referee3 
                FROM games 
                WHERE season = {season}
                ORDER BY round ASC, game_date ASC
            """
            with engine.connect() as conn:
                schedule = pd.read_sql(query, conn)
            if not schedule.empty:
                return apply_team_aliases(schedule, ["home_code", "away_code"])

    # Fallback to Live API
    from data_pipeline.extractors import get_season_schedule
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
        link_assists_to_shots,
        compute_playmaking_metrics,
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

    # 8. Playmaking metrics (AAQ, AxP, Duos)
    assist_shot_links = link_assists_to_shots(pbp_df, shots_df)
    playmaking = compute_playmaking_metrics(assist_shot_links, min_assists=1)

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
        "assist_shot_links": assist_shot_links,
        "playmaking_aaq": playmaking["aaq"],
        "playmaking_axp": playmaking["axp"],
        "playmaking_duos": playmaking["duos"],
    }


@st.cache_data(ttl=3600)
def fetch_league_efficiency_landscape(season: int, competition: str = COMPETITION) -> pd.DataFrame:
    """Fetch and calculate all 18 teams' ORtg and DRtg for the given season."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            import pandas as pd
            from sqlalchemy import text
            query = text("""
                WITH team_poss AS (
                    SELECT team_code, SUM(possessions) AS poss, MAX(team_name) AS team_name
                    FROM player_advanced_stats
                    WHERE season = :season
                    GROUP BY team_code
                ),
                team_pts AS (
                    SELECT
                        t.team_code,
                        SUM(CASE WHEN g.home_team = t.team_code THEN g.home_score ELSE g.away_score END) AS pts_scored,
                        SUM(CASE WHEN g.home_team = t.team_code THEN g.away_score ELSE g.home_score END) AS pts_allowed,
                        COUNT(g.gamecode) AS games
                    FROM teams t
                    JOIN games g ON t.team_code = g.home_team OR t.team_code = g.away_team
                    WHERE g.season = :season AND g.played = TRUE
                    GROUP BY t.team_code
                )
                SELECT
                    p.team_code, p.team_name,
                    t.games,
                    p.poss AS possessions,
                    (p.poss / t.games) AS pace,
                    (t.pts_scored / p.poss * 100) AS ortg,
                    (t.pts_allowed / p.poss * 100) AS drtg,
                    ((t.pts_scored / p.poss * 100) - (t.pts_allowed / p.poss * 100)) AS net_rtg
                FROM team_poss p
                JOIN team_pts t ON p.team_code = t.team_code
                ORDER BY net_rtg DESC
            """)
            with engine.connect() as conn:
                 df = pd.read_sql(query, conn, params={"season": season})
            if not df.empty:
                 return df

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


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_season_game_metadata(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch per-game metadata (including referees) for a full season."""
    if _use_db():
        return fetch_season_schedule(season, competition)
    from data_pipeline.extractors import get_season_game_metadata
    return get_season_game_metadata(season, competition)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_referee_stats(
    season: int,
    team_code: str,
    min_games: int = 3,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch and compute referee win/loss stats for a team in a season."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            import pandas as pd
            from sqlalchemy import text
            query = text("""
                WITH ref_games AS (
                    SELECT gamecode, home_team, away_team, home_score, away_score, referee1 AS referee FROM games WHERE season = :season AND played = TRUE AND (home_team = :team OR away_team = :team) AND referee1 IS NOT NULL
                    UNION ALL
                    SELECT gamecode, home_team, away_team, home_score, away_score, referee2 FROM games WHERE season = :season AND played = TRUE AND (home_team = :team OR away_team = :team) AND referee2 IS NOT NULL
                    UNION ALL
                    SELECT gamecode, home_team, away_team, home_score, away_score, referee3 FROM games WHERE season = :season AND played = TRUE AND (home_team = :team OR away_team = :team) AND referee3 IS NOT NULL
                )
                SELECT
                    referee,
                    COUNT(gamecode) AS games,
                    SUM(CASE WHEN (home_team = :team AND home_score > away_score) OR (away_team = :team AND away_score > home_score) THEN 1 ELSE 0 END) AS wins
                FROM ref_games
                GROUP BY referee
                HAVING COUNT(gamecode) >= :min_games
            """)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"season": season, "team": team_code, "min_games": min_games})
            if not df.empty:
                df["losses"] = df["games"] - df["wins"]
                df["win_pct"] = (df["wins"] / df["games"] * 100).round(1)
                return df.sort_values("win_pct", ascending=False).reset_index(drop=True)

    metadata_df = fetch_season_game_metadata(season, competition)
    if metadata_df.empty:
        return pd.DataFrame()
    from data_pipeline.transformers import compute_referee_stats
    return compute_referee_stats(metadata_df, team_code, min_games)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_close_game_stats(
    season: int,
    close_threshold: int = 5,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Compute close-game / clutch DNA stats for all teams in a season."""
    schedule = fetch_season_schedule(season, competition)
    if schedule.empty:
        return pd.DataFrame()
    from data_pipeline.transformers import compute_close_game_stats
    return compute_close_game_stats(schedule, close_threshold)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_situational_scoring(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch team-level situational scoring profile for a season."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            import pandas as pd
            from sqlalchemy import text
            query = text("""
                SELECT
                    team_code,
                    SUM(fgm2 * 2) AS pts_from_2pt,
                    SUM(fgm3 * 3) AS pts_from_3pt,
                    SUM(ftm) AS pts_from_ft,
                    SUM(points) AS total_pts,
                    (SUM(fgm2 * 2)::FLOAT / SUM(points) * 100) AS pts_from_2pt_pct,
                    (SUM(fgm3 * 3)::FLOAT / SUM(points) * 100) AS pts_from_3pt_pct,
                    (SUM(ftm)::FLOAT / SUM(points) * 100) AS pts_from_ft_pct,
                    COUNT(DISTINCT gamecode) AS games,
                    SUM(steals)::FLOAT / COUNT(DISTINCT gamecode) AS steals_pg,
                    SUM(turnovers)::FLOAT / COUNT(DISTINCT gamecode) AS turnovers_pg,
                    SUM(off_rebounds)::FLOAT / COUNT(DISTINCT gamecode) AS off_reb_pg,
                    SUM(assists)::FLOAT / COUNT(DISTINCT gamecode) AS assists_pg
                FROM player_advanced_stats
                WHERE season = :season AND points > 0
                GROUP BY team_code
            """)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"season": season})
            if not df.empty:
                return df

    import importlib
    import data_pipeline.extractors as _ext_mod
    importlib.reload(_ext_mod)
    return _ext_mod.get_situational_scoring(season, competition)


# ========================================================================
# LIVE GAME QUERIES (no caching — always fetch fresh data)
# ========================================================================

def fetch_live_games(season: int, competition: str = COMPETITION) -> list:
    """Detect games currently in progress. Not cached — hits API each call."""
    from data_pipeline.live_extractor import detect_live_games
    return detect_live_games(season, competition)


def fetch_live_game_data_fresh(
    season: int, gamecode: int, competition: str = COMPETITION
) -> dict:
    """Fetch fresh boxscore + PBP for a live game. Never cached."""
    from data_pipeline.live_extractor import fetch_live_game_data
    return fetch_live_game_data(season, gamecode, competition)


# ========================================================================
# SCOUTING ENGINE QUERIES
# ========================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_scouting_player_pool(
    season: int, competition: str = COMPETITION
) -> pd.DataFrame:
    """Fetch and cache league-wide player stats for the scouting engine."""
    from data_pipeline.scouting_engine import fetch_league_player_stats
    return fetch_league_player_stats(season, competition)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_league_leaders(
    season: int,
    competition: str = COMPETITION,
) -> dict:
    """
    Fetch PerGame and Accumulated traditional player stats for a full season
    and return both views ready for leaderboard rendering.

    Returns
    -------
    dict with keys:
        "per_game" : pd.DataFrame  — per-game averages, one row per player
        "totals"   : pd.DataFrame  — accumulated season totals
    Both DataFrames have unified column names.
    """
    from euroleague_api.player_stats import PlayerStats
    from data_pipeline.extractors import apply_team_aliases
    from data_pipeline.transformers import format_player_name

    ps = PlayerStats(competition)

    COL_MAP = {
        "player.code": "player_code",
        "player.name": "player_name_raw",
        "player.team.code": "team_code",
        "player.team.name": "team_name",
        "player.imageUrl": "image_url",
    }

    def _fetch_and_clean(statistic_mode: str) -> pd.DataFrame:
        try:
            df = ps.get_player_stats_single_season(
                endpoint="traditional",
                season=season,
                phase_type_code="RS",
                statistic_mode=statistic_mode,
            )
        except Exception as e:
            logger.error(f"Failed to fetch {statistic_mode} player stats: {e}")
            return pd.DataFrame()

        if df.empty:
            return df

        df = df.rename(columns=COL_MAP)

        # Parse percentage strings -> float
        for pct_col in ["twoPointersPercentage", "threePointersPercentage", "freeThrowsPercentage"]:
            if pct_col in df.columns:
                df[pct_col] = (
                    df[pct_col].astype(str)
                    .str.replace("%", "", regex=False)
                    .pipe(pd.to_numeric, errors="coerce")
                )

        df = apply_team_aliases(df, ["team_code"])

        # Normalise column names
        df = df.rename(columns={
            "gamesPlayed": "games",
            "minutesPlayed": "minutes",
            "pointsScored": "points",
            "twoPointersMade": "fgm2",
            "twoPointersAttempted": "fga2",
            "twoPointersPercentage": "fg2_pct",
            "threePointersMade": "fgm3",
            "threePointersAttempted": "fga3",
            "threePointersPercentage": "fg3_pct",
            "freeThrowsMade": "ftm",
            "freeThrowsAttempted": "fta",
            "freeThrowsPercentage": "ft_pct",
            "totalRebounds": "rebounds",
            "assists": "assists",
            "turnovers": "turnovers",
            "steals": "steals",
            "blocks": "blocks",
        })

        for col in ["games", "minutes", "points", "fgm2", "fga2", "fgm3", "fga3",
                     "ftm", "fta", "rebounds", "assists", "turnovers", "steals",
                     "blocks", "fg2_pct", "fg3_pct", "ft_pct"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "player_name_raw" in df.columns:
            df["player_name"] = df["player_name_raw"].apply(format_player_name)
        else:
            df["player_name"] = df.get("player_code", "")

        return df

    per_game = _fetch_and_clean("PerGame")
    totals = _fetch_and_clean("Accumulated")

    return {"per_game": per_game, "totals": totals}


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_home_away_splits(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch Home vs Away advanced splits (ORtg, DRtg, Net Rating) for all teams."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            import pandas as pd
            from sqlalchemy import text
            query = text("""
                WITH team_poss AS (
                    SELECT team_code, is_home, SUM(possessions) AS poss FROM player_advanced_stats WHERE season = :season GROUP BY team_code, is_home
                ),
                team_games AS (
                    SELECT home_team AS team_code, TRUE AS is_home, COUNT(*) AS games, SUM(home_score) AS pts_for, SUM(away_score) AS pts_against, SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS wins FROM games WHERE season = :season AND played = TRUE GROUP BY home_team
                    UNION ALL
                    SELECT away_team AS team_code, FALSE AS is_home, COUNT(*) AS games, SUM(away_score) AS pts_for, SUM(home_score) AS pts_against, SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) AS wins FROM games WHERE season = :season AND played = TRUE GROUP BY away_team
                )
                SELECT
                    h.team_code,
                    h.games AS home_games,
                    (h.wins::FLOAT / h.games * 100) AS home_win_pct,
                    (h.pts_for / hp.poss * 100) AS home_ortg,
                    (h.pts_against / hp.poss * 100) AS home_drtg,
                    (h.pts_for / hp.poss * 100) - (h.pts_against / hp.poss * 100) AS home_net,
                    a.games AS away_games,
                    (a.wins::FLOAT / a.games * 100) AS away_win_pct,
                    (a.pts_for / ap.poss * 100) AS away_ortg,
                    (a.pts_against / ap.poss * 100) AS away_drtg,
                    (a.pts_for / ap.poss * 100) - (a.pts_against / ap.poss * 100) AS away_net,
                    ((h.pts_for / hp.poss * 100) - (h.pts_against / hp.poss * 100)) - ((a.pts_for / ap.poss * 100) - (a.pts_against / ap.poss * 100)) AS home_adv_diff
                FROM team_games h
                JOIN team_poss hp ON h.team_code = hp.team_code AND h.is_home = TRUE AND hp.is_home = TRUE
                JOIN team_games a ON h.team_code = a.team_code AND a.is_home = FALSE
                JOIN team_poss ap ON a.team_code = ap.team_code AND ap.is_home = FALSE
                ORDER BY home_adv_diff DESC
            """)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"season": season})
            if not df.empty:
                return df

    import importlib
    import data_pipeline.extractors as _ext_mod
    importlib.reload(_ext_mod)
    return _ext_mod.get_home_away_splits(season, competition)
