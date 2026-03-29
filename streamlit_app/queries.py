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
from typing import Dict, Optional

import pandas as pd
import streamlit as st

from streamlit_app.utils.secrets_manager import USE_DB

logger = logging.getLogger(__name__)

from streamlit_app.utils.config_loader import get_default_competition, get_cache_ttl

COMPETITION = get_default_competition()
_CACHE_TTL = get_cache_ttl()


def _use_db() -> bool:
    """Check if the dashboard should read from the PostgreSQL DB instead of the live API."""
    return USE_DB


def _get_repository():
    """Return the shared DataRepository singleton (lazy-initialised)."""
    if "data_repository" not in st.session_state:
        from data_pipeline.data_repository import DataRepository
        st.session_state["data_repository"] = DataRepository()
    return st.session_state["data_repository"]


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
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
            try:
                from sqlalchemy import text
                query = text("""
                    SELECT 
                        g.season, g.gamecode, 
                        g.home_team AS home_code, g.away_team AS away_code,
                        ht.team_name AS home_name, at.team_name AS away_name,
                        ht.logo_url  AS home_logo, at.logo_url  AS away_logo,
                        g.home_score, g.away_score, 
                        g.game_date AS date, g.round, g.played, 
                        g.referee1, g.referee2, g.referee3 
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team = ht.team_code
                    LEFT JOIN teams at ON g.away_team = at.team_code
                    WHERE g.season = :season
                    ORDER BY g.round ASC, g.game_date ASC
                """)
                with engine.connect() as conn:
                    schedule = pd.read_sql(query, conn, params={"season": season})
                if not schedule.empty:
                    return apply_team_aliases(schedule, ["home_code", "away_code"])
            except Exception as e:
                logger.warning("DB fetch failed for fetch_season_schedule(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

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
    Fetch and process all data for a single game.

    Uses the DB-first cache-aside pattern via DataRepository:
      1. If the game exists in PostgreSQL, load raw data instantly.
      2. On cache miss, fetch from the Euroleague API, persist to DB,
         then return — so the user is only delayed once per game.
      3. If the database is unreachable, falls back to pure API mode.

    Returns a dict with:
      - boxscore, pbp, shots, game_info (raw)
      - advanced_stats, lineup_stats, assist_network, clutch_stats,
        run_stoppers, foul_trouble, duo_synergy, trio_synergy,
        shot_quality, playmaking_aaq, playmaking_axp, playmaking_duos
    """
    repo = _get_repository()
    if repo.db_available():
        try:
            return repo.get_game_data(season, gamecode, competition)
        except Exception as e:
            logger.warning("DB fetch failed for fetch_game_data_live(%s, %s). Falling back to API. Reason: %s: %s", season, gamecode, type(e).__name__, e)

    # Fallback: pure API mode (original path, no DB)
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
        compute_total_points_created,
        compute_on_off_splits,
    )

    raw = extract_game_data(season, gamecode, competition)
    boxscore_df = raw["boxscore"]
    pbp_df = raw["pbp"]
    shots_df = raw["shots"]
    game_info_df = raw["game_info"]

    advanced_df = compute_advanced_stats(boxscore_df)
    pbp_lu = track_lineups(pbp_df, boxscore_df)
    lineup_stats = compute_lineup_stats(pbp_lu, boxscore_df)
    duo_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=2)
    trio_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=3)
    on_off_splits = compute_on_off_splits(pbp_lu, boxscore_df)
    clutch = compute_clutch_stats(pbp_df, boxscore_df)
    stoppers = detect_runs_and_stoppers(pbp_lu)
    foul_impact = foul_trouble_impact(pbp_df, boxscore_df)
    assists = build_assist_network(pbp_df)
    shot_quality = compute_shot_quality(shots_df)
    assist_shot_links = link_assists_to_shots(pbp_df, shots_df)
    playmaking = compute_playmaking_metrics(assist_shot_links, min_assists=1)
    advanced_df = compute_total_points_created(advanced_df, assist_shot_links)

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
        "on_off_splits": on_off_splits,
        "shot_quality": shot_quality,
        "assist_shot_links": assist_shot_links,
        "playmaking_aaq": playmaking["aaq"],
        "playmaking_axp": playmaking["axp"],
        "playmaking_duos": playmaking["duos"],
    }


@st.cache_data(ttl=_CACHE_TTL)
def fetch_league_efficiency_landscape(season: int, competition: str = COMPETITION) -> pd.DataFrame:
    """Fetch and calculate all 18 teams' ORtg and DRtg for the given season."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
                from sqlalchemy import text
                query = text("""
                    WITH team_poss AS (
                        SELECT pa.team_code, SUM(pa.possessions) AS poss,
                               MAX(tm.team_name) AS team_name,
                               MAX(tm.logo_url) AS logo_url
                        FROM player_advanced_stats pa
                        JOIN teams tm ON pa.team_code = tm.team_code
                        WHERE pa.season = :season
                        GROUP BY pa.team_code
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
                        p.team_code, p.team_name, p.logo_url,
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
            except Exception as e:
                logger.warning("DB fetch failed for fetch_league_efficiency_landscape(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

    from data_pipeline.extractors import get_league_efficiency_landscape
    return get_league_efficiency_landscape(season, competition)


@st.cache_data(ttl=_CACHE_TTL)
def fetch_team_season_data(season: int, team_code: str, competition: str = COMPETITION) -> Dict[str, pd.DataFrame]:
    """
    Fetch all boxscores/PBPs for a team's season, and aggregate into mathematically
    accurate season-level player stats and lineup stats.
    Also returns per-game advanced stats with TPC for the Form Tracker.

    DB-first: reads player_advanced_stats, boxscores, and play_by_play from
    PostgreSQL. Falls back to the live Euroleague API on any DB error.
    """
    from data_pipeline.transformers import (
        compute_advanced_stats,
        track_lineups,
        compute_lineup_stats,
        compute_season_player_stats,
        link_assists_to_shots,
        compute_total_points_created,
    )
    import pandas as pd

    # ------------------------------------------------------------------
    # DB-first path
    # ------------------------------------------------------------------
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
                result = _fetch_team_season_from_db(
                    engine, season, team_code,
                    compute_season_player_stats,
                    track_lineups,
                    compute_lineup_stats,
                    link_assists_to_shots,
                    compute_total_points_created,
                )
                if result:
                    return result
                logger.info("DB returned no data for %s/%s, falling back to API", season, team_code)
            except Exception as e:
                logger.warning(
                    "DB fetch failed for team season data (%s/%s). "
                    "Falling back to API. Reason: %s: %s",
                    season, team_code, type(e).__name__, e,
                )

    # ------------------------------------------------------------------
    # API fallback
    # ------------------------------------------------------------------
    from data_pipeline.extractors import extract_team_season_data
    raw = extract_team_season_data(season, team_code, competition)
    if raw["boxscore"].empty:
        return {}

    adv_df = compute_advanced_stats(raw["boxscore"])
    season_player_stats = compute_season_player_stats(adv_df, team_code)

    per_game_adv_frames = []
    pbp_df = raw.get("pbp", pd.DataFrame())
    shots_df = raw.get("shots", pd.DataFrame())
    if not adv_df.empty and "Gamecode" in adv_df.columns:
        for gc in adv_df["Gamecode"].unique():
            g_adv = adv_df[adv_df["Gamecode"] == gc].copy()
            g_pbp = pbp_df[pbp_df["Gamecode"] == gc] if not pbp_df.empty and "Gamecode" in pbp_df.columns else pd.DataFrame()
            g_shots = shots_df[shots_df["Gamecode"] == gc] if not shots_df.empty and "Gamecode" in shots_df.columns else pd.DataFrame()
            if not g_pbp.empty and not g_shots.empty:
                assist_links = link_assists_to_shots(g_pbp, g_shots)
                g_adv = compute_total_points_created(g_adv, assist_links)
            else:
                g_adv["pts_from_assists"] = 0
                g_adv["total_pts_created"] = g_adv["points"].fillna(0)
            per_game_adv_frames.append(g_adv)
    per_game_stats = pd.concat(per_game_adv_frames, ignore_index=True) if per_game_adv_frames else pd.DataFrame()

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
        "per_game_stats": per_game_stats,
    }


def _fetch_team_season_from_db(
    engine, season, team_code,
    compute_season_player_stats,
    track_lineups,
    compute_lineup_stats,
    link_assists_to_shots,
    compute_total_points_created,
) -> Optional[Dict[str, pd.DataFrame]]:
    """DB-mode implementation for fetch_team_season_data."""
    from sqlalchemy import text

    # 1. Player advanced stats (per-game rows) — feeds season aggregation
    adv_query = text("""
        SELECT
            season    AS "Season",
            gamecode  AS "Gamecode",
            player_id, player_name, team_code, is_home,
            minutes, points,
            fgm2, fga2, fgm3, fga3, ftm, fta,
            off_rebounds, def_rebounds, total_rebounds,
            assists, steals, turnovers,
            blocks_favour, blocks_against,
            fouls_committed, fouls_received, plus_minus,
            possessions, ts_pct, off_rating, def_rating
        FROM player_advanced_stats
        WHERE season = :season
          AND team_code = :team
          AND minutes > 0
        ORDER BY gamecode, player_name
    """)
    with engine.connect() as conn:
        adv_df = pd.read_sql(adv_query, conn, params={"season": season, "team": team_code})

    if adv_df.empty:
        return None

    season_player_stats = compute_season_player_stats(adv_df, team_code)

    # 2. Per-game stats for the Form Tracker (with TPC)
    per_game_frames = []
    shots_query = text("""
        SELECT gamecode AS "Gamecode", team, id_player AS "PLAYER_ID",
               player AS "PLAYER", id_action, action, points,
               coord_x, coord_y, zone, minute
        FROM shots
        WHERE season = :season
    """)
    pbp_query = text("""
        SELECT
            season AS "Season", gamecode AS "Gamecode",
            period AS "PERIOD", playtype AS "PLAYTYPE",
            player_id AS "PLAYER_ID", player AS "PLAYER",
            codeteam AS "CODETEAM", markertime AS "MARKERTIME",
            numberofplay AS "NUMBEROFPLAY", comment AS "COMMENT"
        FROM play_by_play
        WHERE season = :season
        ORDER BY id ASC
    """)
    with engine.connect() as conn:
        all_shots = pd.read_sql(shots_query, conn, params={"season": season})
        all_pbp = pd.read_sql(pbp_query, conn, params={"season": season})

    if not all_pbp.empty and "NUMBEROFPLAY" in all_pbp.columns:
        all_pbp["TRUE_NUMBEROFPLAY"] = all_pbp["NUMBEROFPLAY"]
    # Reconstruct POINTS_A / POINTS_B from PBP COMMENT if present
    if not all_pbp.empty and "COMMENT" in all_pbp.columns:
        import re
        def _extract_score(comment, pos):
            if not isinstance(comment, str):
                return None
            m = re.search(r'\((\d+)-(\d+)\)', comment)
            return int(m.group(pos)) if m else None
        all_pbp["POINTS_A"] = all_pbp["COMMENT"].apply(lambda c: _extract_score(c, 1))
        all_pbp["POINTS_B"] = all_pbp["COMMENT"].apply(lambda c: _extract_score(c, 2))

    # Filter team games from the schedule
    team_gamecodes = adv_df["Gamecode"].unique()

    for gc in team_gamecodes:
        g_adv = adv_df[adv_df["Gamecode"] == gc].copy()
        g_pbp = all_pbp[all_pbp["Gamecode"] == gc] if not all_pbp.empty else pd.DataFrame()
        g_shots = all_shots[all_shots["Gamecode"] == gc] if not all_shots.empty else pd.DataFrame()
        if not g_pbp.empty and not g_shots.empty:
            try:
                assist_links = link_assists_to_shots(g_pbp, g_shots)
                g_adv = compute_total_points_created(g_adv, assist_links)
            except Exception:
                g_adv["pts_from_assists"] = 0
                g_adv["total_pts_created"] = g_adv["points"].fillna(0)
        else:
            g_adv["pts_from_assists"] = 0
            g_adv["total_pts_created"] = g_adv["points"].fillna(0)
        per_game_frames.append(g_adv)

    per_game_stats = pd.concat(per_game_frames, ignore_index=True) if per_game_frames else pd.DataFrame()

    # 3. Lineup stats from PBP + boxscores
    lineup_stats = pd.DataFrame()
    box_query = text("""
        SELECT
            b.season AS "Season", b.gamecode AS "Gamecode",
            b.player_id AS "Player_ID", b.player AS "Player",
            b.team AS "Team", b.home AS "Home",
            b.is_starter AS "IsStarter", b.is_playing AS "IsPlaying",
            b.dorsal AS "Dorsal", b.minutes AS "Minutes",
            b.points AS "Points",
            b.fgm2 AS "FieldGoalsMade2", b.fga2 AS "FieldGoalsAttempted2",
            b.fgm3 AS "FieldGoalsMade3", b.fga3 AS "FieldGoalsAttempted3",
            b.ftm AS "FreeThrowsMade", b.fta AS "FreeThrowsAttempted",
            b.off_rebounds AS "OffensiveRebounds",
            b.def_rebounds AS "DefensiveRebounds",
            b.total_rebounds AS "TotalRebounds",
            b.assists AS "Assistances", b.steals AS "Steals",
            b.turnovers AS "Turnovers",
            b.blocks_favour AS "BlocksFavour",
            b.blocks_against AS "BlocksAgainst",
            b.fouls_committed AS "FoulsCommited",
            b.fouls_received AS "FoulsReceived",
            b.valuation AS "Valuation",
            b.plus_minus AS "Plusminus",
            p.position AS "position"
        FROM boxscores b
        LEFT JOIN players p ON b.player_id = p.player_id
        WHERE b.season = :season
          AND b.gamecode = ANY(:gamecodes)
    """)
    with engine.connect() as conn:
        all_box = pd.read_sql(box_query, conn, params={
            "season": season,
            "gamecodes": list(int(gc) for gc in team_gamecodes),
        })

    if not all_box.empty and not all_pbp.empty:
        pbp_lu_list = []
        for gc in team_gamecodes:
            g_pbp = all_pbp[all_pbp["Gamecode"] == gc]
            g_box = all_box[all_box["Gamecode"] == gc]
            if not g_pbp.empty and not g_box.empty:
                try:
                    pbp_lu_list.append(track_lineups(g_pbp, g_box))
                except Exception:
                    pass
        if pbp_lu_list:
            pbp_lu = pd.concat(pbp_lu_list, ignore_index=True)
            lineup_stats = compute_lineup_stats(pbp_lu, all_box, min_events=20)

    return {
        "player_season_stats": season_player_stats,
        "lineup_season_stats": lineup_stats,
        "per_game_stats": per_game_stats,
        "boxscore": all_box,
    }


# ========================================================================
# DB MODE — SQL queries for pre-loaded data
# ========================================================================

@st.cache_resource
def _get_db_engine():
    """Get a pooled SQLAlchemy engine for Streamlit database queries.

    Decorated with ``@st.cache_resource`` so the engine is created
    exactly **once** per app lifecycle (survives reruns and sessions).
    The underlying ``get_engine()`` also has a module-level cache,
    making this a belt-and-suspenders guard against pool sprawl.
    """
    try:
        from data_pipeline.load_to_db import get_engine
        return get_engine(use_pooler=True)
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


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_season_on_off_splits(
    season: int,
    team_code: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch pre-computed season-level On/Off Net Rating splits from the DB.

    Returns a DataFrame with one row per player:
        player_id, player_name, team, games,
        on_ortg, on_drtg, on_net_rtg,
        off_ortg, off_drtg, off_net_rtg, on_off_diff
    """
    engine = _get_db_engine()
    if engine is None:
        return pd.DataFrame()

    from sqlalchemy import text

    base = """
        SELECT player_id, player_name, team, games,
               on_events, on_pts_for, on_pts_against, on_poss,
               on_ortg, on_drtg, on_net_rtg,
               off_events, off_pts_for, off_pts_against, off_poss,
               off_ortg, off_drtg, off_net_rtg, on_off_diff
        FROM season_on_off_splits
        WHERE season = :season
    """
    params: dict = {"season": season}

    if team_code:
        base += " AND team = :team"
        params["team"] = team_code

    base += " ORDER BY on_off_diff DESC"

    try:
        with engine.connect() as conn:
            return pd.read_sql(text(base), conn, params=params)
    except Exception as e:
        logger.warning(f"season_on_off_splits query failed (table may not exist yet): {e}")
        return pd.DataFrame()


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_season_game_metadata(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch per-game metadata (including referees) for a full season."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
                from sqlalchemy import text
                with engine.connect() as conn:
                    has_refs = conn.execute(text(
                        "SELECT 1 FROM games WHERE season = :season AND referee1 IS NOT NULL LIMIT 1"
                    ), {"season": season}).fetchone()
                if has_refs:
                    return fetch_season_schedule(season, competition)
                logger.info("fetch_season_game_metadata(%s): no referee data in DB, falling back to API.", season)
            except Exception as e:
                logger.warning("DB fetch failed for fetch_season_game_metadata(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

    from data_pipeline.extractors import get_season_game_metadata
    return get_season_game_metadata(season, competition)


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
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
            try:
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
            except Exception as e:
                logger.warning("DB fetch failed for fetch_referee_stats(%s, %s). Falling back to API. Reason: %s: %s", season, team_code, type(e).__name__, e)

    metadata_df = fetch_season_game_metadata(season, competition)
    if metadata_df.empty:
        return pd.DataFrame()
    from data_pipeline.transformers import compute_referee_stats
    return compute_referee_stats(metadata_df, team_code, min_games)


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
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


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_situational_scoring(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch team-level situational scoring profile for a season."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
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
            except Exception as e:
                logger.warning("DB fetch failed for fetch_situational_scoring(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

    from data_pipeline.extractors import get_situational_scoring
    return get_situational_scoring(season, competition)


# ========================================================================
# MATCHUP VULNERABILITY ENGINE
# ========================================================================

_ARCHETYPE_RULES = [
    ("Tall Playmaker",  "Guard",   195, None,  "Guard > 195 cm"),
    ("Standard Guard",  "Guard",   None, 195,  "Guard <= 195 cm"),
    ("Stretch Big",     "Center",  200, None,  "Center > 200 cm"),
    ("Undersized Big",  "Center",  None, 200,  "Center <= 200 cm"),
    ("Tall Forward",    "Forward", 205, None,  "Forward > 205 cm"),
    ("Standard Forward","Forward", None, 205,  "Forward <= 205 cm"),
]


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_matchup_vulnerabilities(
    season: int,
    team_code: str,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Compute the analyzed team's DRtg when specific opponent archetypes
    are on the floor vs when they are absent.

    Returns a DataFrame with columns:
        archetype, description, with_drtg, without_drtg, drtg_diff, with_events
    """
    if not _use_db():
        return pd.DataFrame()

    engine = _get_db_engine()
    if engine is None:
        return pd.DataFrame()

    try:
        return _compute_matchup_vulnerabilities_db(engine, season, team_code)
    except Exception as e:
        logger.warning("Matchup vulnerability query failed: %s: %s", type(e).__name__, e)
        return pd.DataFrame()


def _compute_matchup_vulnerabilities_db(engine, season: int, team_code: str) -> pd.DataFrame:
    from sqlalchemy import text
    from data_pipeline.transformers import track_lineups

    # 1. Load player metadata (height + position)
    meta_query = text("""
        SELECT player_id, height, position FROM players
        WHERE height IS NOT NULL AND position IS NOT NULL
    """)
    with engine.connect() as conn:
        meta_df = pd.read_sql(meta_query, conn)

    if meta_df.empty:
        return pd.DataFrame()

    meta_lookup = dict(zip(meta_df["player_id"], zip(meta_df["height"], meta_df["position"])))

    # 2. Load PBP + boxscores for the team's games
    games_query = text("""
        SELECT DISTINCT gamecode FROM boxscores
        WHERE season = :season
          AND (team = :team OR gamecode IN (
                SELECT gamecode FROM boxscores WHERE season = :season AND team = :team
          ))
    """)
    with engine.connect() as conn:
        gamecodes = [r[0] for r in conn.execute(games_query, {"season": season, "team": team_code}).fetchall()]

    if not gamecodes:
        return pd.DataFrame()

    pbp_query = text("""
        SELECT season AS "Season", gamecode AS "Gamecode",
               period AS "PERIOD", playtype AS "PLAYTYPE",
               player_id AS "PLAYER_ID", player AS "PLAYER",
               codeteam AS "CODETEAM", markertime AS "MARKERTIME",
               numberofplay AS "NUMBEROFPLAY", comment AS "COMMENT"
        FROM play_by_play
        WHERE season = :season AND gamecode = ANY(:gamecodes)
        ORDER BY id ASC
    """)
    box_query = text("""
        SELECT season AS "Season", gamecode AS "Gamecode",
               player_id AS "Player_ID", player AS "Player",
               team AS "Team", home AS "Home",
               is_starter AS "IsStarter", is_playing AS "IsPlaying",
               dorsal AS "Dorsal", minutes AS "Minutes",
               points AS "Points",
               fgm2 AS "FieldGoalsMade2", fga2 AS "FieldGoalsAttempted2",
               fgm3 AS "FieldGoalsMade3", fga3 AS "FieldGoalsAttempted3",
               ftm AS "FreeThrowsMade", fta AS "FreeThrowsAttempted",
               off_rebounds AS "OffensiveRebounds",
               def_rebounds AS "DefensiveRebounds",
               total_rebounds AS "TotalRebounds",
               assists AS "Assistances", steals AS "Steals",
               turnovers AS "Turnovers",
               blocks_favour AS "BlocksFavour",
               blocks_against AS "BlocksAgainst",
               fouls_committed AS "FoulsCommited",
               fouls_received AS "FoulsReceived",
               valuation AS "Valuation",
               plus_minus AS "Plusminus"
        FROM boxscores
        WHERE season = :season AND gamecode = ANY(:gamecodes)
    """)
    with engine.connect() as conn:
        all_pbp = pd.read_sql(pbp_query, conn, params={"season": season, "gamecodes": gamecodes})
        all_box = pd.read_sql(box_query, conn, params={"season": season, "gamecodes": gamecodes})

    if all_pbp.empty or all_box.empty:
        return pd.DataFrame()

    if "NUMBEROFPLAY" in all_pbp.columns:
        all_pbp["TRUE_NUMBEROFPLAY"] = all_pbp["NUMBEROFPLAY"]

    # 3. Track lineups per game
    pbp_lu_frames = []
    for gc in gamecodes:
        g_pbp = all_pbp[all_pbp["Gamecode"] == gc]
        g_box = all_box[all_box["Gamecode"] == gc]
        if not g_pbp.empty and not g_box.empty:
            try:
                pbp_lu_frames.append(track_lineups(g_pbp, g_box))
            except Exception:
                pass

    if not pbp_lu_frames:
        return pd.DataFrame()

    pbp_lu = pd.concat(pbp_lu_frames, ignore_index=True)

    # 4. Identify which team is the opponent on each row
    home_team_col = pbp_lu["home_team"]
    away_team_col = pbp_lu["away_team"]

    is_home = home_team_col == team_code
    pbp_lu["opp_lineup"] = pbp_lu.apply(
        lambda r: r["home_lineup"] if r["away_team"] == team_code else r["away_lineup"],
        axis=1,
    )
    pbp_lu["team_lineup"] = pbp_lu.apply(
        lambda r: r["home_lineup"] if r["home_team"] == team_code else r["away_lineup"],
        axis=1,
    )

    # 5. Score and possession tracking (from the analyzed team's perspective)
    score_map = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    poss_events = {"2FGM", "2FGA", "3FGM", "3FGA", "TO", "D"}

    pbp_lu["score_pts"] = pbp_lu["PLAYTYPE"].map(score_map).fillna(0).astype(int)

    # Points scored AGAINST the analyzed team (= opponent scoring)
    pbp_lu["opp_pts"] = pbp_lu.apply(
        lambda r: r["score_pts"] if r["CODETEAM"] != team_code and r["score_pts"] > 0 else 0,
        axis=1,
    )
    pbp_lu["team_pts"] = pbp_lu.apply(
        lambda r: r["score_pts"] if r["CODETEAM"] == team_code and r["score_pts"] > 0 else 0,
        axis=1,
    )
    is_poss = pbp_lu["PLAYTYPE"].isin(poss_events)
    pbp_lu["opp_poss_ev"] = (is_poss & (pbp_lu["CODETEAM"] != team_code)).astype(int)
    pbp_lu["team_poss_ev"] = (is_poss & (pbp_lu["CODETEAM"] == team_code)).astype(int)

    # 6. Classify opponent archetypes present in each row's opponent lineup
    def _classify_lineup(lineup_fs):
        archetypes = set()
        for pid in lineup_fs:
            meta = meta_lookup.get(pid)
            if meta is None:
                continue
            h, pos = meta
            for name, req_pos, min_h, max_h, _ in _ARCHETYPE_RULES:
                if pos != req_pos:
                    continue
                if min_h is not None and h <= min_h:
                    continue
                if max_h is not None and h > max_h:
                    continue
                archetypes.add(name)
        return frozenset(archetypes)

    pbp_lu["opp_archetypes"] = pbp_lu["opp_lineup"].apply(_classify_lineup)

    # 7. For each archetype, compute DRtg WITH vs WITHOUT
    results = []
    total_events = len(pbp_lu)

    for arch_name, _, _, _, description in _ARCHETYPE_RULES:
        mask_with = pbp_lu["opp_archetypes"].apply(lambda a: arch_name in a)
        with_df = pbp_lu[mask_with]
        without_df = pbp_lu[~mask_with]

        def _calc_drtg(df):
            if df.empty:
                return None, 0
            opp_pts = df["opp_pts"].sum()
            opp_poss = df["opp_poss_ev"].sum()
            events = len(df)
            if opp_poss < 10:
                return None, events
            return (opp_pts / opp_poss) * 100, events

        with_drtg, with_events = _calc_drtg(with_df)
        without_drtg, without_events = _calc_drtg(without_df)

        if with_drtg is not None and without_drtg is not None and with_events >= 30:
            results.append({
                "archetype": arch_name,
                "description": description,
                "with_drtg": round(with_drtg, 2),
                "without_drtg": round(without_drtg, 2),
                "drtg_diff": round(with_drtg - without_drtg, 2),
                "with_events": with_events,
            })

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results).sort_values("drtg_diff", ascending=False).reset_index(drop=True)


# ========================================================================
# FATIGUE & BIOMETRICS — Double Game Week Veteran Penalty
# ========================================================================

@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_double_week_fatigue(season: int) -> pd.DataFrame:
    """
    Identify double game weeks (two games within 2-4 days for the same
    team) and compute per-player efficiency drop-offs between Game 1 and
    Game 2, grouped by age bracket.

    Returns a DataFrame with columns:
        age_bracket, metric, game1_avg, game2_avg, drop, drop_pct, sample_size
    """
    engine = _get_db_engine()
    if engine is None:
        return pd.DataFrame()

    from sqlalchemy import text

    try:
        return _compute_double_week_fatigue(engine, season)
    except Exception as e:
        logger.warning("Double-week fatigue query failed: %s: %s", type(e).__name__, e)
        return pd.DataFrame()


def _compute_double_week_fatigue(engine, season: int) -> pd.DataFrame:
    from sqlalchemy import text
    import numpy as np

    query = text("""
        WITH parsed_games AS (
            SELECT
                season, gamecode, home_team, away_team,
                TO_DATE(game_date, 'Mon DD, YYYY') AS gdate
            FROM games
            WHERE season = :season
              AND played = TRUE
              AND game_date IS NOT NULL
              AND game_date ~ '^[A-Z][a-z]+ \\d'
        ),
        team_games AS (
            SELECT season, gamecode, gdate, home_team AS team_code FROM parsed_games
            UNION ALL
            SELECT season, gamecode, gdate, away_team AS team_code FROM parsed_games
        ),
        double_weeks AS (
            SELECT
                g1.team_code,
                g1.gamecode AS gc1, g1.gdate AS date1,
                g2.gamecode AS gc2, g2.gdate AS date2,
                (g2.gdate - g1.gdate) AS day_gap
            FROM team_games g1
            JOIN team_games g2
                ON g1.team_code = g2.team_code
                AND g1.season = g2.season
                AND g2.gdate > g1.gdate
                AND (g2.gdate - g1.gdate) BETWEEN 2 AND 4
            WHERE NOT EXISTS (
                SELECT 1 FROM team_games g_mid
                WHERE g_mid.team_code = g1.team_code
                  AND g_mid.season = g1.season
                  AND g_mid.gdate > g1.gdate
                  AND g_mid.gdate < g2.gdate
            )
        )
        SELECT
            dw.team_code, dw.gc1, dw.gc2, dw.day_gap,
            pas1.player_id,
            pas1.player_name,
            p.birthdate,
            EXTRACT(YEAR FROM AGE(p.birthdate))::int AS age,
            pas1.minutes AS g1_minutes,
            pas1.ts_pct AS g1_ts,
            pas1.plus_minus AS g1_pm,
            pas1.fga2 + pas1.fga3 AS g1_fga,
            pas1.fta AS g1_fta,
            pas1.turnovers AS g1_tov,
            pas1.assists AS g1_ast,
            pas1.fouls_received AS g1_fr,
            pas1.possessions AS g1_poss,
            pas2.minutes AS g2_minutes,
            pas2.ts_pct AS g2_ts,
            pas2.plus_minus AS g2_pm,
            pas2.fga2 + pas2.fga3 AS g2_fga,
            pas2.fta AS g2_fta,
            pas2.turnovers AS g2_tov,
            pas2.assists AS g2_ast,
            pas2.fouls_received AS g2_fr,
            pas2.possessions AS g2_poss
        FROM double_weeks dw
        JOIN player_advanced_stats pas1
            ON pas1.season = :season
            AND pas1.gamecode = dw.gc1
            AND pas1.team_code = dw.team_code
            AND pas1.minutes > 5
        JOIN player_advanced_stats pas2
            ON pas2.season = :season
            AND pas2.gamecode = dw.gc2
            AND pas2.player_id = pas1.player_id
            AND pas2.team_code = dw.team_code
            AND pas2.minutes > 5
        JOIN players p ON p.player_id = pas1.player_id
        WHERE p.birthdate IS NOT NULL
        ORDER BY dw.team_code, dw.gc1, pas1.player_name
    """)

    with engine.connect() as conn:
        raw = pd.read_sql(query, conn, params={"season": season})

    if raw.empty:
        return pd.DataFrame()

    # Compute per-row usage rate
    def _usg(fga, fta, tov, ast, fr, poss):
        if poss is None or poss == 0:
            return None
        return (fga + 0.44 * fta + tov + ast + fr) / poss

    raw["g1_usg"] = raw.apply(
        lambda r: _usg(r["g1_fga"], r["g1_fta"], r["g1_tov"], r["g1_ast"], r["g1_fr"], r["g1_poss"]), axis=1,
    )
    raw["g2_usg"] = raw.apply(
        lambda r: _usg(r["g2_fga"], r["g2_fta"], r["g2_tov"], r["g2_ast"], r["g2_fr"], r["g2_poss"]), axis=1,
    )

    # Age brackets
    def _bracket(age):
        if age is None:
            return None
        if age < 25:
            return "Under 25"
        if age <= 30:
            return "25-30"
        return "31+ (Veterans)"

    raw["age_bracket"] = raw["age"].apply(_bracket)
    raw = raw.dropna(subset=["age_bracket"])

    # Aggregate by age bracket
    results = []
    for bracket in ["Under 25", "25-30", "31+ (Veterans)"]:
        bdf = raw[raw["age_bracket"] == bracket]
        if bdf.empty:
            continue
        n = len(bdf)

        for metric, g1_col, g2_col in [
            ("TS%", "g1_ts", "g2_ts"),
            ("Usage Rate", "g1_usg", "g2_usg"),
            ("Plus/Minus", "g1_pm", "g2_pm"),
        ]:
            g1_vals = bdf[g1_col].dropna()
            g2_vals = bdf[g2_col].dropna()
            if g1_vals.empty or g2_vals.empty:
                continue
            g1_avg = g1_vals.mean()
            g2_avg = g2_vals.mean()
            drop = g2_avg - g1_avg
            drop_pct = (drop / g1_avg * 100) if g1_avg != 0 else 0

            results.append({
                "age_bracket": bracket,
                "metric": metric,
                "game1_avg": round(g1_avg, 4),
                "game2_avg": round(g2_avg, 4),
                "drop": round(drop, 4),
                "drop_pct": round(drop_pct, 2),
                "sample_size": int(min(len(g1_vals), len(g2_vals))),
            })

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results)


# ========================================================================
# SCOUT FINDER — Moneyball Target Search
# ========================================================================

@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_scout_targets(season: int) -> pd.DataFrame:
    """
    Join player_advanced_stats (season aggregates), season_on_off_splits,
    and players metadata to produce a single scouting DataFrame.

    Returns one row per player with:
        player_id, player_name, team_code, team_name, position, height,
        birthdate, age, games, minutes_pg, points_pg, ts_pct, true_usg_pct,
        off_rating, assist_ratio, stop_rate, on_off_diff, on_net_rtg
    """
    engine = _get_db_engine()
    if engine is None:
        return pd.DataFrame()

    from sqlalchemy import text

    query = text("""
        WITH season_agg AS (
            SELECT
                pas.player_id,
                MAX(pas.player_name) AS player_name,
                pas.team_code,
                MAX(t.team_name) AS team_name,
                COUNT(DISTINCT pas.gamecode) AS games,
                AVG(pas.minutes) AS minutes_pg,
                AVG(pas.points) AS points_pg,
                SUM(pas.minutes) AS total_minutes,
                AVG(pas.ts_pct) AS ts_pct,
                AVG(pas.off_rating) AS off_rating,
                CASE WHEN SUM(pas.possessions) > 0
                     THEN (SUM(pas.fga2 + pas.fga3) + 0.44 * SUM(pas.fta)
                           + SUM(pas.turnovers) + SUM(pas.assists)
                           + SUM(pas.fouls_received))::float
                          / SUM(pas.possessions)
                     ELSE 0 END AS true_usg_pct,
                CASE WHEN SUM(pas.possessions) > 0
                     THEN SUM(pas.assists)::float / SUM(pas.possessions)
                     ELSE 0 END AS assist_ratio,
                CASE WHEN SUM(pas.possessions) > 0
                     THEN (SUM(pas.steals) + SUM(pas.blocks_favour)
                           + SUM(pas.def_rebounds))::float
                          / SUM(pas.possessions)
                     ELSE 0 END AS stop_rate,
                CASE WHEN SUM(pas.fga2 + pas.fga3) > 0
                     THEN SUM(pas.fga3)::float / SUM(pas.fga2 + pas.fga3)
                     ELSE 0 END AS three_pt_rate,
                CASE WHEN SUM(pas.fga2 + pas.fga3) > 0
                     THEN SUM(pas.fta)::float / SUM(pas.fga2 + pas.fga3)
                     ELSE 0 END AS ft_rate,
                AVG(pas.total_rebounds) AS rebounds_pg,
                AVG(pas.assists) AS assists_pg,
                AVG(pas.steals) AS steals_pg
            FROM player_advanced_stats pas
            LEFT JOIN teams t ON pas.team_code = t.team_code
            WHERE pas.season = :season AND pas.minutes > 0
            GROUP BY pas.player_id, pas.team_code
        )
        SELECT
            sa.*,
            p.height,
            p.birthdate,
            p.position,
            p.country,
            CASE WHEN p.birthdate IS NOT NULL
                 THEN EXTRACT(YEAR FROM AGE(p.birthdate))
                 ELSE NULL END AS age,
            oo.on_net_rtg,
            oo.off_net_rtg,
            oo.on_off_diff
        FROM season_agg sa
        LEFT JOIN players p ON sa.player_id = p.player_id
        LEFT JOIN season_on_off_splits oo
            ON sa.player_id = oo.player_id
            AND oo.season = :season
            AND sa.team_code = oo.team
        ORDER BY sa.minutes_pg DESC
    """)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"season": season})
        return df
    except Exception as e:
        logger.warning("fetch_scout_targets failed: %s: %s", type(e).__name__, e)
        return pd.DataFrame()


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

@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_scouting_player_pool(
    season: int, competition: str = COMPETITION
) -> pd.DataFrame:
    """Fetch and cache league-wide player stats for the scouting engine."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
                from sqlalchemy import text
                from data_pipeline.transformers import format_player_name
                from data_pipeline.scouting_engine import infer_position
                query = text("""
                    SELECT
                        pas.player_id AS player_code,
                        pas.player_name AS player_name_raw,
                        pas.team_code,
                        t.team_name,
                        COUNT(DISTINCT pas.gamecode) AS games_played,
                        AVG(pas.minutes) AS minutes_pg,
                        AVG(pas.points) AS points_pg,
                        AVG(pas.ts_pct) AS ts_pct,
                        AVG(pas.off_rating) AS off_rating,
                        AVG(pas.steals) AS steals_pg,
                        AVG(pas.blocks_favour) AS blocks_pg,
                        AVG(pas.assists) AS assists_pg,
                        AVG(pas.total_rebounds) AS rebounds_pg,
                        CASE WHEN SUM(pas.fga2 + pas.fga3) > 0
                             THEN SUM(pas.fga3)::float / SUM(pas.fga2 + pas.fga3)
                             ELSE 0 END AS three_pt_rate,
                        CASE WHEN SUM(pas.fga2 + pas.fga3) > 0
                             THEN SUM(pas.fta)::float / SUM(pas.fga2 + pas.fga3)
                             ELSE 0 END AS ft_rate,
                        CASE WHEN SUM(pas.turnovers) > 0
                             THEN SUM(pas.assists)::float / SUM(pas.turnovers)
                             ELSE 0 END AS ast_tov_ratio,
                        CASE WHEN SUM(pas.possessions) > 0
                             THEN (SUM(pas.fga2 + pas.fga3) + 0.44 * SUM(pas.fta)
                                   + SUM(pas.turnovers) + SUM(pas.assists)
                                   + SUM(pas.fouls_received))::float
                                  / SUM(pas.possessions)
                             ELSE 0 END AS true_usg_pct,
                        CASE WHEN SUM(pas.possessions) > 0
                             THEN (SUM(pas.steals) + SUM(pas.blocks_favour)
                                   + SUM(pas.def_rebounds))::float
                                  / SUM(pas.possessions)
                             ELSE 0 END AS stop_rate,
                        CASE WHEN SUM(pas.possessions) > 0
                             THEN SUM(pas.assists)::float / SUM(pas.possessions)
                             ELSE 0 END AS assist_ratio,
                        CASE WHEN SUM(pas.total_rebounds) > 0
                             THEN SUM(pas.off_rebounds)::float / SUM(pas.total_rebounds)
                             ELSE 0 END AS orb_pct,
                        CASE WHEN SUM(pas.total_rebounds) > 0
                             THEN SUM(pas.def_rebounds)::float / SUM(pas.total_rebounds)
                             ELSE 0 END AS drb_pct
                    FROM player_advanced_stats pas
                    LEFT JOIN teams t ON pas.team_code = t.team_code
                    WHERE pas.season = :season AND pas.minutes > 0
                    GROUP BY pas.player_id, pas.player_name, pas.team_code, t.team_name
                    HAVING COUNT(DISTINCT pas.gamecode) >= 10
                """)
                with engine.connect() as conn:
                    df = pd.read_sql(query, conn, params={"season": season})
                if not df.empty:
                    df["player_name"] = df["player_name_raw"].apply(format_player_name)
                    df["image_url"] = ""
                    df["position"] = df.apply(infer_position, axis=1)
                    return df
            except Exception as e:
                logger.warning("DB fetch failed for fetch_scouting_player_pool(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

    from data_pipeline.scouting_engine import fetch_league_player_stats
    return fetch_league_player_stats(season, competition)


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
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
    from data_pipeline.extractors import apply_team_aliases
    from data_pipeline.transformers import format_player_name

    # ------------------------------------------------------------------
    # DB-first path
    # ------------------------------------------------------------------
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
                result = _fetch_league_leaders_from_db(engine, season, format_player_name, apply_team_aliases)
                if result and not result["per_game"].empty:
                    return result
            except Exception as e:
                logger.warning("DB fetch failed for fetch_league_leaders(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

    # ------------------------------------------------------------------
    # API fallback
    # ------------------------------------------------------------------
    from euroleague_api.player_stats import PlayerStats
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

        for pct_col in ["twoPointersPercentage", "threePointersPercentage", "freeThrowsPercentage"]:
            if pct_col in df.columns:
                df[pct_col] = (
                    df[pct_col].astype(str)
                    .str.replace("%", "", regex=False)
                    .pipe(pd.to_numeric, errors="coerce")
                )

        df = apply_team_aliases(df, ["team_code"])

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


def _fetch_league_leaders_from_db(engine, season, format_player_name, apply_team_aliases):
    """DB-mode implementation for fetch_league_leaders."""
    from sqlalchemy import text
    import numpy as np

    query = text("""
        SELECT
            pas.player_id AS player_code,
            pas.player_name AS player_name_raw,
            pas.team_code,
            tm.team_name,
            COUNT(DISTINCT pas.gamecode) AS games,
            SUM(pas.minutes) AS minutes,
            SUM(pas.points) AS points,
            SUM(pas.fgm2) AS fgm2, SUM(pas.fga2) AS fga2,
            SUM(pas.fgm3) AS fgm3, SUM(pas.fga3) AS fga3,
            SUM(pas.ftm) AS ftm, SUM(pas.fta) AS fta,
            SUM(pas.total_rebounds) AS rebounds,
            SUM(pas.assists) AS assists,
            SUM(pas.turnovers) AS turnovers,
            SUM(pas.steals) AS steals,
            SUM(pas.blocks_favour) AS blocks
        FROM player_advanced_stats pas
        LEFT JOIN teams tm ON pas.team_code = tm.team_code
        WHERE pas.season = :season AND pas.minutes > 0
        GROUP BY pas.player_id, pas.player_name, pas.team_code, tm.team_name
    """)
    with engine.connect() as conn:
        totals = pd.read_sql(query, conn, params={"season": season})

    if totals.empty:
        return None

    totals = apply_team_aliases(totals, ["team_code"])
    totals["player_name"] = totals["player_name_raw"].apply(format_player_name)

    totals["fg2_pct"] = np.where(totals["fga2"] > 0, totals["fgm2"] / totals["fga2"] * 100, 0)
    totals["fg3_pct"] = np.where(totals["fga3"] > 0, totals["fgm3"] / totals["fga3"] * 100, 0)
    totals["ft_pct"] = np.where(totals["fta"] > 0, totals["ftm"] / totals["fta"] * 100, 0)

    per_game = totals.copy()
    per_game_cols = ["minutes", "points", "fgm2", "fga2", "fgm3", "fga3",
                     "ftm", "fta", "rebounds", "assists", "turnovers", "steals", "blocks"]
    for col in per_game_cols:
        if col in per_game.columns:
            per_game[col] = np.where(per_game["games"] > 0, per_game[col] / per_game["games"], 0)

    return {"per_game": per_game, "totals": totals}


@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_home_away_splits(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch Home vs Away advanced splits (ORtg, DRtg, Net Rating) for all teams."""
    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
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
            except Exception as e:
                logger.warning("DB fetch failed for fetch_home_away_splits(%s). Falling back to API. Reason: %s: %s", season, type(e).__name__, e)

    from data_pipeline.extractors import get_home_away_splits
    return get_home_away_splits(season, competition)


# ========================================================================
# SPATIAL ANALYTICS — Season-wide shot data with coordinates
# ========================================================================

@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_season_shot_data(
    season: int,
    team_code: str,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch all shot data with X/Y coordinates for a team's full season.

    Returns a DataFrame with lowercase column names:
        gamecode, team, player, id_action, action, points,
        coord_x, coord_y, zone, minute
    """
    from data_pipeline.extractors import TEAM_ALIASES

    reverse_alias = {v: k for k, v in TEAM_ALIASES.items()}
    alt_code = reverse_alias.get(
        team_code, TEAM_ALIASES.get(team_code, team_code),
    )

    if _use_db():
        engine = _get_db_engine()
        if engine:
            try:
                from sqlalchemy import text

                query = text("""
                    SELECT gamecode, team, player, id_action, action,
                           points, coord_x, coord_y, zone, minute
                    FROM shots
                    WHERE season = :season
                      AND (team = :code1 OR team = :code2)
                    ORDER BY gamecode, minute
                """)
                with engine.connect() as conn:
                    df = pd.read_sql(
                        query, conn,
                        params={"season": season, "code1": team_code, "code2": alt_code},
                    )
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning("DB fetch failed for fetch_season_shot_data(%s, %s). Falling back to API. Reason: %s: %s", season, team_code, type(e).__name__, e)

    # API fallback — fetch only shot data per game (lighter than full extract)
    from data_pipeline.extractors import (
        get_season_schedule,
        get_shot_data,
        apply_team_aliases,
    )

    sched = get_season_schedule(season, competition)
    if sched.empty:
        return pd.DataFrame()

    team_games = sched[
        ((sched["home_code"] == team_code) | (sched["away_code"] == team_code))
        & (sched["played"] == True)
    ]

    frames = []
    for _, game in team_games.iterrows():
        sdf = get_shot_data(season, game["gamecode"], competition)
        if not sdf.empty:
            frames.append(sdf)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = apply_team_aliases(result, ["TEAM"])
    result = result[result["TEAM"] == team_code]

    col_map = {
        "Gamecode": "gamecode", "TEAM": "team", "PLAYER": "player",
        "ID_ACTION": "id_action", "ACTION": "action", "POINTS": "points",
        "COORD_X": "coord_x", "COORD_Y": "coord_y", "ZONE": "zone",
        "MINUTE": "minute",
    }
    return result.rename(
        columns={k: v for k, v in col_map.items() if k in result.columns},
    )


# ========================================================================
# ML PREDICTIONS — Win Probability Model
# ========================================================================

@st.cache_resource(show_spinner=False)
def fetch_prediction_model(
    training_seasons: tuple,
    competition: str = COMPETITION,
):
    """Train and cache the win probability model across seasons."""
    from data_pipeline.ml_pipeline import get_or_train_model
    return get_or_train_model(list(training_seasons), competition)


def predict_game_outcome(
    model,
    home_team: str,
    away_team: str,
    season: int,
    competition: str = COMPETITION,
) -> float:
    """Predict the home team's win probability for a matchup."""
    from data_pipeline.ml_pipeline import predict_matchup
    return predict_matchup(model, home_team, away_team, season, competition)


# ========================================================================
# SEASONAL FORM — Predictive Monthly Performance Curve
# ========================================================================

@st.cache_data(ttl=_CACHE_TTL, show_spinner=False)
def fetch_seasonal_form_data(
    seasons: tuple,
    team_code: str,
) -> Dict:
    """Aggregate monthly stats, train (or load) the seasonal form model,
    and return everything needed for the Predictive Form Dashboard.

    Returns a dict with keys:
        monthly_df: per-team per-month aggregated stats
        predicted_curve: predicted xNetRtg per month for the selected team
        model_trained: whether the model was successfully trained
        insight_text: auto-generated insight string
    """
    from data_pipeline.seasonal_trends import (
        aggregate_monthly_stats,
        train_seasonal_form_model,
        load_model,
        save_model,
        predict_team_form_curve,
        build_team_form_features,
        generate_insights,
    )

    seasons_list = list(seasons)
    monthly_df = aggregate_monthly_stats(seasons_list)

    if monthly_df.empty:
        return {
            "monthly_df": pd.DataFrame(),
            "predicted_curve": pd.DataFrame(),
            "model_trained": False,
            "insight_text": "",
        }

    model = load_model()
    if model is None:
        model = train_seasonal_form_model(seasons_list)
        if model is not None:
            try:
                save_model(model)
            except Exception as e:
                logger.warning("Could not save seasonal form model: %s", e)

    if model is None:
        return {
            "monthly_df": monthly_df,
            "predicted_curve": pd.DataFrame(),
            "model_trained": False,
            "insight_text": "",
        }

    features = build_team_form_features(monthly_df, team_code)
    if features.empty:
        return {
            "monthly_df": monthly_df,
            "predicted_curve": pd.DataFrame(),
            "model_trained": True,
            "insight_text": "",
        }

    predicted_curve = predict_team_form_curve(model, features)
    insight_text = generate_insights(monthly_df, team_code, predicted_curve)

    return {
        "monthly_df": monthly_df,
        "predicted_curve": predicted_curve,
        "model_trained": True,
        "insight_text": insight_text,
    }
