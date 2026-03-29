"""
load_to_db.py — Database Loading Layer
========================================
Uses SQLAlchemy to insert processed DataFrames into the PostgreSQL database.
Supports idempotent upserts via ON CONFLICT DO UPDATE so the pipeline can be
re-run safely without creating duplicate rows.

Requires a `.env` file at the project root with:
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from streamlit_app.utils.secrets_manager import get_secret, format_pooler_url


def get_engine(use_pooler: bool = True) -> Engine:
    """
    Build a SQLAlchemy engine from environment variables.

    Parameters
    ----------
    use_pooler : bool
        When *True* (default), the connection string is rewritten to use
        Supabase Supavisor (port 6543, ``pool_mode=transaction``).
        Set to *False* for direct connections (migrations, admin tasks).
    """
    db_url = get_secret("DATABASE_URL", "") or get_secret("POSTGRES_URL", "")

    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
        elif db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        url = db_url
    else:
        user = get_secret("POSTGRES_USER", "euroleague")
        password = get_secret("POSTGRES_PASSWORD", "")
        host = get_secret("POSTGRES_HOST", "localhost")
        port = get_secret("POSTGRES_PORT", "5432")
        db = get_secret("POSTGRES_DB", "euroleague_db")
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    if use_pooler:
        url = format_pooler_url(url)

    try:
        engine = create_engine(
            url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        safe_url = url.split("@")[-1] if "@" in url else "unknown_host"
        logger.info(f"Database engine created for {safe_url}")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise


# ========================================================================
# BULK INSERT HELPER  (psycopg2 execute_values — 10-50x faster)
# ========================================================================

def _bulk_execute(
    engine: Engine,
    sql_template: str,
    columns: list,
    records: list,
    chunksize: int = 2000,
) -> int:
    """
    Fast-path bulk insert/upsert using psycopg2.extras.execute_values.

    Parameters
    ----------
    engine : SQLAlchemy Engine
    sql_template : str
        A full INSERT ... VALUES %s ON CONFLICT ... string.
        Must contain a single ``%s`` placeholder for the VALUES block.
    columns : list[str]
        Column names matching record dict keys.
    records : list[dict]
        Row data.
    chunksize : int
        Rows per execute_values call (default 2000).

    Returns
    -------
    int  — total rows sent.
    """
    if not records:
        return 0

    tuples = [tuple(r.get(c) for c in columns) for r in records]
    total = 0

    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        for start in range(0, len(tuples), chunksize):
            chunk = tuples[start : start + chunksize]
            execute_values(cur, sql_template, chunk, page_size=chunksize)
            total += len(chunk)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    return total


def _delete_games_bulk(
    engine: Engine,
    table: str,
    season_gamecodes: list,
) -> None:
    """
    Delete rows for a list of (season, gamecode) pairs in one statement.
    Used before bulk re-insert for tables without ON CONFLICT keys.
    """
    if not season_gamecodes:
        return
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        cur.execute(
            f"DELETE FROM {table} WHERE (season, gamecode) IN %s",
            (tuple(season_gamecodes),),
        )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def _safe_int(val):
    if pd.isna(val):
        return None
    return int(val)


def _safe_float(val):
    if pd.isna(val):
        return None
    return float(val)


def _safe_str(val, max_len: int = None):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if max_len:
        s = s[:max_len]
    return s or None


# ========================================================================
# PER-TABLE LOAD FUNCTIONS  (refactored to use _bulk_execute)
# ========================================================================

def load_teams(engine: Engine, boxscore_df: pd.DataFrame) -> None:
    """Upsert unique teams from boxscore into the teams table."""
    if boxscore_df.empty:
        return

    teams = boxscore_df["Team"].dropna().unique()
    records = [{"team_code": t.strip(), "team_name": t.strip()} for t in teams if t.strip()]
    if not records:
        return

    n = _bulk_execute(
        engine,
        """INSERT INTO teams (team_code, team_name) VALUES %s
           ON CONFLICT (team_code) DO UPDATE SET team_name = EXCLUDED.team_name""",
        ["team_code", "team_name"],
        records,
    )
    logger.info(f"Upserted {n} teams")


def load_players(engine: Engine, boxscore_df: pd.DataFrame) -> None:
    """Upsert unique players from the boxscore into the players table."""
    if boxscore_df.empty:
        return

    id_col = "Player_ID" if "Player_ID" in boxscore_df.columns else None
    if id_col is None:
        boxscore_df = boxscore_df.copy()
        boxscore_df["_synth_id"] = (
            boxscore_df["Player"].str.replace(" ", "_").str.upper()
            + "_" + boxscore_df["Team"].str.strip()
        )
        id_col = "_synth_id"

    unique = boxscore_df[[id_col, "Player", "Team", "Dorsal"]].drop_duplicates(subset=[id_col])
    records = []
    for _, row in unique.iterrows():
        records.append({
            "player_id": _safe_str(row[id_col]),
            "player_name": _safe_str(row["Player"]),
            "team_code": _safe_str(row["Team"]),
            "dorsal": _safe_str(row.get("Dorsal")),
        })

    n = _bulk_execute(
        engine,
        """INSERT INTO players (player_id, player_name, team_code, dorsal) VALUES %s
           ON CONFLICT (player_id) DO UPDATE
           SET player_name = EXCLUDED.player_name,
               team_code   = EXCLUDED.team_code,
               dorsal      = EXCLUDED.dorsal""",
        ["player_id", "player_name", "team_code", "dorsal"],
        records,
    )
    logger.info(f"Upserted {n} players")


def load_game(engine: Engine, game_info_df: pd.DataFrame) -> None:
    """Upsert game metadata into the games table."""
    if game_info_df.empty:
        return

    records = game_info_df.to_dict("records")
    for req_key in ['game_date', 'round', 'played', 'referee1', 'referee2', 'referee3']:
        for rec in records:
            if req_key not in rec or pd.isna(rec[req_key]):
                rec[req_key] = None if req_key != 'played' else False

    cols = ["season", "gamecode", "home_team", "away_team", "home_score",
            "away_score", "game_date", "round", "played",
            "referee1", "referee2", "referee3"]

    n = _bulk_execute(
        engine,
        """INSERT INTO games (season, gamecode, home_team, away_team,
               home_score, away_score, game_date, round, played,
               referee1, referee2, referee3)
           VALUES %s
           ON CONFLICT (season, gamecode) DO UPDATE SET
               home_team  = EXCLUDED.home_team,
               away_team  = EXCLUDED.away_team,
               home_score = EXCLUDED.home_score,
               away_score = EXCLUDED.away_score,
               game_date  = EXCLUDED.game_date,
               round      = EXCLUDED.round,
               played     = EXCLUDED.played,
               referee1   = EXCLUDED.referee1,
               referee2   = EXCLUDED.referee2,
               referee3   = EXCLUDED.referee3""",
        cols,
        records,
    )
    logger.info(f"Upserted {n} game(s)")


def load_play_by_play(engine: Engine, pbp_df: pd.DataFrame) -> None:
    """
    Load play-by-play data into the play_by_play table.
    Uses delete-then-bulk-insert per game for idempotent re-runs.
    """
    if pbp_df.empty:
        return

    game_pairs = list(
        pbp_df[["Season", "Gamecode"]]
        .drop_duplicates()
        .apply(lambda r: (int(r["Season"]), int(r["Gamecode"])), axis=1)
    )
    _delete_games_bulk(engine, "play_by_play", game_pairs)

    records = []
    for _, row in pbp_df.iterrows():
        records.append({
            "season": int(row["Season"]),
            "gamecode": int(row["Gamecode"]),
            "period": _safe_int(row.get("PERIOD")),
            "playtype": _safe_str(row.get("PLAYTYPE")),
            "player_id": _safe_str(row.get("PLAYER_ID")),
            "player": _safe_str(row.get("PLAYER")),
            "codeteam": _safe_str(row.get("CODETEAM")),
            "markertime": _safe_str(row.get("MARKERTIME")),
            "numberofplay": _safe_int(row.get("NUMBEROFPLAY")),
            "comment": _safe_str(row.get("COMMENT"), max_len=500),
        })

    cols = ["season", "gamecode", "period", "playtype", "player_id",
            "player", "codeteam", "markertime", "numberofplay", "comment"]

    n = _bulk_execute(
        engine,
        """INSERT INTO play_by_play
               (season, gamecode, period, playtype, player_id, player,
                codeteam, markertime, numberofplay, comment)
           VALUES %s""",
        cols,
        records,
    )
    logger.info(f"Loaded {n} PBP rows")


def load_player_advanced_stats(
    engine: Engine,
    advanced_df: pd.DataFrame,
) -> None:
    """Upsert computed advanced stats into the player_advanced_stats table."""
    if advanced_df.empty:
        return

    records = []
    for _, row in advanced_df.iterrows():
        records.append({
            "season":           int(row["Season"]),
            "gamecode":         int(row["Gamecode"]),
            "player_id":        _safe_str(row.get("player_id")),
            "player_name":      _safe_str(row.get("player_name")),
            "team_code":        _safe_str(row.get("team_code")),
            "is_home":          bool(row.get("is_home", False)),
            "minutes":          _safe_float(row.get("minutes")),
            "points":           _safe_int(row.get("points")),
            "fgm2":             _safe_int(row.get("fgm2")),
            "fga2":             _safe_int(row.get("fga2")),
            "fgm3":             _safe_int(row.get("fgm3")),
            "fga3":             _safe_int(row.get("fga3")),
            "ftm":              _safe_int(row.get("ftm")),
            "fta":              _safe_int(row.get("fta")),
            "off_rebounds":     _safe_int(row.get("off_rebounds")),
            "def_rebounds":     _safe_int(row.get("def_rebounds")),
            "total_rebounds":   _safe_int(row.get("total_rebounds")),
            "assists":          _safe_int(row.get("assists")),
            "steals":           _safe_int(row.get("steals")),
            "turnovers":        _safe_int(row.get("turnovers")),
            "blocks_favour":    _safe_int(row.get("blocks_favour")),
            "blocks_against":   _safe_int(row.get("blocks_against")),
            "fouls_committed":  _safe_int(row.get("fouls_committed")),
            "fouls_received":   _safe_int(row.get("fouls_received")),
            "plus_minus":       _safe_float(row.get("plus_minus")),
            "possessions":      _safe_float(row.get("possessions")),
            "ts_pct":           _safe_float(row.get("ts_pct")),
            "off_rating":       _safe_float(row.get("off_rating")),
            "def_rating":       _safe_float(row.get("def_rating")),
        })

    cols = [
        "season", "gamecode", "player_id", "player_name", "team_code", "is_home",
        "minutes", "points", "fgm2", "fga2", "fgm3", "fga3", "ftm", "fta",
        "off_rebounds", "def_rebounds", "total_rebounds",
        "assists", "steals", "turnovers", "blocks_favour", "blocks_against",
        "fouls_committed", "fouls_received", "plus_minus",
        "possessions", "ts_pct", "off_rating", "def_rating",
    ]

    n = _bulk_execute(
        engine,
        """INSERT INTO player_advanced_stats (
               season, gamecode, player_id, player_name, team_code, is_home,
               minutes, points, fgm2, fga2, fgm3, fga3, ftm, fta,
               off_rebounds, def_rebounds, total_rebounds,
               assists, steals, turnovers, blocks_favour, blocks_against,
               fouls_committed, fouls_received, plus_minus,
               possessions, ts_pct, off_rating, def_rating
           ) VALUES %s
           ON CONFLICT (season, gamecode, player_id) DO UPDATE SET
               player_name     = EXCLUDED.player_name,
               team_code       = EXCLUDED.team_code,
               is_home         = EXCLUDED.is_home,
               minutes         = EXCLUDED.minutes,
               points          = EXCLUDED.points,
               fgm2            = EXCLUDED.fgm2,
               fga2            = EXCLUDED.fga2,
               fgm3            = EXCLUDED.fgm3,
               fga3            = EXCLUDED.fga3,
               ftm             = EXCLUDED.ftm,
               fta             = EXCLUDED.fta,
               off_rebounds    = EXCLUDED.off_rebounds,
               def_rebounds    = EXCLUDED.def_rebounds,
               total_rebounds  = EXCLUDED.total_rebounds,
               assists         = EXCLUDED.assists,
               steals          = EXCLUDED.steals,
               turnovers       = EXCLUDED.turnovers,
               blocks_favour   = EXCLUDED.blocks_favour,
               blocks_against  = EXCLUDED.blocks_against,
               fouls_committed = EXCLUDED.fouls_committed,
               fouls_received  = EXCLUDED.fouls_received,
               plus_minus      = EXCLUDED.plus_minus,
               possessions     = EXCLUDED.possessions,
               ts_pct          = EXCLUDED.ts_pct,
               off_rating      = EXCLUDED.off_rating,
               def_rating      = EXCLUDED.def_rating""",
        cols,
        records,
    )
    logger.info(f"Upserted {n} player advanced stat rows")


def load_boxscores(engine: Engine, boxscore_df: pd.DataFrame) -> None:
    """Store raw boxscore into the boxscores table (delete + bulk insert)."""
    if boxscore_df.empty:
        return

    id_col = "Player_ID" if "Player_ID" in boxscore_df.columns else None
    if id_col is None:
        boxscore_df = boxscore_df.copy()
        boxscore_df["Player_ID"] = (
            boxscore_df["Player"].str.replace(" ", "_").str.upper()
            + "_" + boxscore_df["Team"].str.strip()
        )
        id_col = "Player_ID"

    game_pairs = list(
        boxscore_df[["Season", "Gamecode"]]
        .drop_duplicates()
        .apply(lambda r: (int(r["Season"]), int(r["Gamecode"])), axis=1)
    )
    _delete_games_bulk(engine, "boxscores", game_pairs)

    records = []
    for _, row in boxscore_df.iterrows():
        records.append({
            "season": int(row["Season"]),
            "gamecode": int(row["Gamecode"]),
            "player_id": _safe_str(row[id_col]),
            "player": _safe_str(row.get("Player")),
            "team": _safe_str(row.get("Team")),
            "home": _safe_int(row.get("Home")),
            "is_starter": _safe_int(row.get("IsStarter")),
            "is_playing": _safe_int(row.get("IsPlaying")),
            "dorsal": _safe_str(row.get("Dorsal")),
            "minutes": _safe_str(row.get("Minutes")),
            "points": _safe_int(row.get("Points")),
            "fgm2": _safe_int(row.get("FieldGoalsMade2")),
            "fga2": _safe_int(row.get("FieldGoalsAttempted2")),
            "fgm3": _safe_int(row.get("FieldGoalsMade3")),
            "fga3": _safe_int(row.get("FieldGoalsAttempted3")),
            "ftm": _safe_int(row.get("FreeThrowsMade")),
            "fta": _safe_int(row.get("FreeThrowsAttempted")),
            "off_rebounds": _safe_int(row.get("OffensiveRebounds")),
            "def_rebounds": _safe_int(row.get("DefensiveRebounds")),
            "total_rebounds": _safe_int(row.get("TotalRebounds")),
            "assists": _safe_int(row.get("Assistances")),
            "steals": _safe_int(row.get("Steals")),
            "turnovers": _safe_int(row.get("Turnovers")),
            "blocks_favour": _safe_int(row.get("BlocksFavour")),
            "blocks_against": _safe_int(row.get("BlocksAgainst")),
            "fouls_committed": _safe_int(row.get("FoulsCommited")),
            "fouls_received": _safe_int(row.get("FoulsReceived")),
            "valuation": _safe_int(row.get("Valuation")),
            "plus_minus": _safe_float(row.get("Plusminus")),
        })

    cols = [
        "season", "gamecode", "player_id", "player", "team", "home",
        "is_starter", "is_playing", "dorsal", "minutes",
        "points", "fgm2", "fga2", "fgm3", "fga3", "ftm", "fta",
        "off_rebounds", "def_rebounds", "total_rebounds",
        "assists", "steals", "turnovers", "blocks_favour", "blocks_against",
        "fouls_committed", "fouls_received", "valuation", "plus_minus",
    ]

    n = _bulk_execute(
        engine,
        """INSERT INTO boxscores
               (season, gamecode, player_id, player, team, home,
                is_starter, is_playing, dorsal, minutes,
                points, fgm2, fga2, fgm3, fga3, ftm, fta,
                off_rebounds, def_rebounds, total_rebounds,
                assists, steals, turnovers, blocks_favour, blocks_against,
                fouls_committed, fouls_received, valuation, plus_minus)
           VALUES %s""",
        cols,
        records,
    )
    logger.info(f"Loaded {n} boxscore rows")


def load_shots(engine: Engine, shots_df: pd.DataFrame) -> None:
    """Store shot data into the shots table (delete + bulk insert)."""
    if shots_df.empty:
        return

    game_pairs = list(
        shots_df[["Season", "Gamecode"]]
        .drop_duplicates()
        .apply(lambda r: (int(r["Season"]), int(r["Gamecode"])), axis=1)
    )
    _delete_games_bulk(engine, "shots", game_pairs)

    records = []
    for _, row in shots_df.iterrows():
        records.append({
            "season": int(row["Season"]),
            "gamecode": int(row["Gamecode"]),
            "num_anot": _safe_int(row.get("NUM_ANOT")),
            "team": _safe_str(row.get("TEAM")),
            "id_player": _safe_str(row.get("ID_PLAYER")),
            "player": _safe_str(row.get("PLAYER")),
            "id_action": _safe_str(row.get("ID_ACTION")),
            "action": _safe_str(row.get("ACTION")),
            "points": _safe_int(row.get("POINTS")),
            "coord_x": _safe_float(row.get("COORD_X")),
            "coord_y": _safe_float(row.get("COORD_Y")),
            "zone": _safe_str(row.get("ZONE")),
            "fastbreak": _safe_int(row.get("FASTBREAK")),
            "second_chance": _safe_int(row.get("SECOND_CHANCE")),
            "pts_off_turnover": _safe_int(row.get("POINTS_OFF_TURNOVER")),
            "minute": _safe_int(row.get("MINUTE")),
            "console": _safe_str(row.get("CONSOLE")),
            "points_a": _safe_int(row.get("POINTS_A")),
            "points_b": _safe_int(row.get("POINTS_B")),
        })

    cols = [
        "season", "gamecode", "num_anot", "team", "id_player", "player",
        "id_action", "action", "points", "coord_x", "coord_y", "zone",
        "fastbreak", "second_chance", "pts_off_turnover",
        "minute", "console", "points_a", "points_b",
    ]

    n = _bulk_execute(
        engine,
        """INSERT INTO shots
               (season, gamecode, num_anot, team, id_player, player,
                id_action, action, points, coord_x, coord_y, zone,
                fastbreak, second_chance, pts_off_turnover,
                minute, console, points_a, points_b)
           VALUES %s""",
        cols,
        records,
    )
    logger.info(f"Loaded {n} shot rows")


def teardown_database(engine: Engine) -> None:
    """
    Drop ALL tables by cascading the public schema.

    This is a destructive operation intended for hard resets when the
    schema was corrupted by loose Pandas to_sql(if_exists='replace')
    calls or when a clean slate is needed.
    """
    logger.warning("TEARDOWN: Dropping all tables (DROP SCHEMA public CASCADE)")
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.execute(text("GRANT ALL ON SCHEMA public TO postgres"))
        conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    logger.info("TEARDOWN: Public schema recreated (empty)")


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL line comments (--) so SQLAlchemy doesn't parse :params in them."""
    return re.sub(r"--[^\n]*", "", sql)


def ensure_schema(engine: Engine) -> None:
    """Create tables and indexes if they don't already exist (idempotent)."""
    db_dir = Path(__file__).resolve().parent.parent / "database"

    for sql_file in ("schema.sql", "indexes.sql"):
        sql_path = db_dir / sql_file
        if not sql_path.exists():
            continue
        ddl = _strip_sql_comments(sql_path.read_text())
        for statement in ddl.split(";"):
            statement = statement.strip()
            if not statement:
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(text(statement))
            except Exception as e:
                logger.warning(
                    "ensure_schema [%s]: %s",
                    sql_file,
                    str(e).split("\n")[0],
                )


def load_on_off_splits(engine: Engine, on_off_df: pd.DataFrame) -> None:
    """Upsert season-level on/off splits into the season_on_off_splits table."""
    if on_off_df.empty:
        return

    records = []
    for _, row in on_off_df.iterrows():
        records.append({
            "season":          int(row["season"]),
            "player_id":       _safe_str(row.get("player_id")),
            "player_name":     _safe_str(row.get("player_name")),
            "team":            _safe_str(row.get("team")),
            "games":           _safe_int(row.get("games")),
            "on_events":       _safe_int(row.get("on_events")),
            "on_pts_for":      _safe_int(row.get("on_pts_for")),
            "on_pts_against":  _safe_int(row.get("on_pts_against")),
            "on_poss":         _safe_float(row.get("on_poss")),
            "on_ortg":         _safe_float(row.get("on_ortg")),
            "on_drtg":         _safe_float(row.get("on_drtg")),
            "on_net_rtg":      _safe_float(row.get("on_net_rtg")),
            "off_events":      _safe_int(row.get("off_events")),
            "off_pts_for":     _safe_int(row.get("off_pts_for")),
            "off_pts_against": _safe_int(row.get("off_pts_against")),
            "off_poss":        _safe_float(row.get("off_poss")),
            "off_ortg":        _safe_float(row.get("off_ortg")),
            "off_drtg":        _safe_float(row.get("off_drtg")),
            "off_net_rtg":     _safe_float(row.get("off_net_rtg")),
            "on_off_diff":     _safe_float(row.get("on_off_diff")),
        })

    cols = [
        "season", "player_id", "player_name", "team", "games",
        "on_events", "on_pts_for", "on_pts_against", "on_poss",
        "on_ortg", "on_drtg", "on_net_rtg",
        "off_events", "off_pts_for", "off_pts_against", "off_poss",
        "off_ortg", "off_drtg", "off_net_rtg", "on_off_diff",
    ]

    n = _bulk_execute(
        engine,
        """INSERT INTO season_on_off_splits (
               season, player_id, player_name, team, games,
               on_events, on_pts_for, on_pts_against, on_poss,
               on_ortg, on_drtg, on_net_rtg,
               off_events, off_pts_for, off_pts_against, off_poss,
               off_ortg, off_drtg, off_net_rtg, on_off_diff
           ) VALUES %s
           ON CONFLICT (season, player_id, team) DO UPDATE SET
               player_name    = EXCLUDED.player_name,
               games          = EXCLUDED.games,
               on_events      = EXCLUDED.on_events,
               on_pts_for     = EXCLUDED.on_pts_for,
               on_pts_against = EXCLUDED.on_pts_against,
               on_poss        = EXCLUDED.on_poss,
               on_ortg        = EXCLUDED.on_ortg,
               on_drtg        = EXCLUDED.on_drtg,
               on_net_rtg     = EXCLUDED.on_net_rtg,
               off_events     = EXCLUDED.off_events,
               off_pts_for    = EXCLUDED.off_pts_for,
               off_pts_against= EXCLUDED.off_pts_against,
               off_poss       = EXCLUDED.off_poss,
               off_ortg       = EXCLUDED.off_ortg,
               off_drtg       = EXCLUDED.off_drtg,
               off_net_rtg    = EXCLUDED.off_net_rtg,
               on_off_diff    = EXCLUDED.on_off_diff""",
        cols,
        records,
    )
    logger.info(f"Upserted {n} season on/off split rows")


# ========================================================================
# SEASON AGGREGATION  (On/Off splits computed from DB data)
# ========================================================================

def run_season_aggregations(season: int, engine: Optional[Engine] = None) -> None:
    """
    Post-load aggregation step: reads the full season's PBP and boxscores
    from the database, computes per-game on/off splits via the transformer
    pipeline, then aggregates across games and upserts into
    season_on_off_splits.

    Processes one game at a time to keep peak RAM low on CI runners.
    """
    from data_pipeline.transformers import track_lineups, compute_on_off_splits

    if engine is None:
        engine = get_engine()

    t0 = time.time()
    logger.info(f"=== Season aggregation start: season={season} ===")

    with engine.connect() as conn:
        gamecodes = [
            r[0] for r in conn.execute(
                text("SELECT DISTINCT gamecode FROM boxscores WHERE season = :s ORDER BY gamecode"),
                {"s": season},
            ).fetchall()
        ]

    if not gamecodes:
        logger.warning("No boxscore data found — skipping aggregation.")
        return

    logger.info(f"Computing on/off splits for {len(gamecodes)} games...")

    per_game_frames = []
    for gc in gamecodes:
        try:
            with engine.connect() as conn:
                box_df = pd.read_sql(
                    text("""
                        SELECT season AS "Season", gamecode AS "Gamecode",
                               player_id AS "Player_ID", player AS "Player",
                               team AS "Team", home AS "Home",
                               is_starter AS "IsStarter", is_playing AS "IsPlaying"
                        FROM boxscores WHERE season = :s AND gamecode = :g
                    """),
                    conn, params={"s": season, "g": gc},
                )
                pbp_df = pd.read_sql(
                    text("""
                        SELECT season AS "Season", gamecode AS "Gamecode",
                               period AS "PERIOD", playtype AS "PLAYTYPE",
                               player_id AS "PLAYER_ID", player AS "PLAYER",
                               codeteam AS "CODETEAM", markertime AS "MARKERTIME",
                               numberofplay AS "NUMBEROFPLAY",
                               numberofplay AS "TRUE_NUMBEROFPLAY",
                               comment AS "COMMENT"
                        FROM play_by_play WHERE season = :s AND gamecode = :g
                        ORDER BY id ASC
                    """),
                    conn, params={"s": season, "g": gc},
                )

            if box_df.empty or pbp_df.empty:
                continue

            pbp_lu = track_lineups(pbp_df, box_df)
            splits = compute_on_off_splits(pbp_lu, box_df)
            if not splits.empty:
                splits["gamecode"] = gc
                per_game_frames.append(splits)
        except Exception as e:
            logger.warning(f"On/off split failed for game {gc}: {e}")

    if not per_game_frames:
        logger.warning("No on/off split data produced — skipping upsert.")
        return

    all_splits = pd.concat(per_game_frames, ignore_index=True)

    # Group by (player_id, team) only — player_name can vary between games
    # due to formatting differences. Take the latest name via 'last'.
    agg = all_splits.groupby(["player_id", "team"], as_index=False).agg(
        player_name=("player_name", "last"),
        games=("gamecode", "nunique"),
        on_events=("on_events", "sum"),
        on_pts_for=("on_pts_for", "sum"),
        on_pts_against=("on_pts_against", "sum"),
        on_poss=("on_poss", "sum"),
        off_events=("off_events", "sum"),
        off_pts_for=("off_pts_for", "sum"),
        off_pts_against=("off_pts_against", "sum"),
        off_poss=("off_poss", "sum"),
    )

    agg["on_ortg"] = (agg["on_pts_for"] / agg["on_poss"].clip(lower=1) * 100).round(1)
    agg["on_drtg"] = (agg["on_pts_against"] / agg["on_poss"].clip(lower=1) * 100).round(1)
    agg["on_net_rtg"] = (agg["on_ortg"] - agg["on_drtg"]).round(1)
    agg["off_ortg"] = (agg["off_pts_for"] / agg["off_poss"].clip(lower=1) * 100).round(1)
    agg["off_drtg"] = (agg["off_pts_against"] / agg["off_poss"].clip(lower=1) * 100).round(1)
    agg["off_net_rtg"] = (agg["off_ortg"] - agg["off_drtg"]).round(1)
    agg["on_off_diff"] = (agg["on_net_rtg"] - agg["off_net_rtg"]).round(1)
    agg["season"] = season

    load_on_off_splits(engine, agg)
    logger.info(
        f"=== Season aggregation complete: {len(agg)} players, "
        f"{time.time()-t0:.1f}s ==="
    )


# ========================================================================
# HIGH-LEVEL PIPELINE FUNCTIONS
# ========================================================================

def run_pipeline(
    season: int,
    gamecode: int,
    competition: str = "E",
    engine: Optional[Engine] = None,
) -> None:
    """
    End-to-end pipeline for a single game:
      1. Extract boxscore + PBP + shots (concurrently via ThreadPool)
      2. Transform -> compute advanced stats
      3. Load everything into PostgreSQL (bulk inserts)
    """
    from data_pipeline.extractors import extract_game_data
    from data_pipeline.transformers import compute_advanced_stats

    logger.info(f"=== Pipeline start: season={season}, gamecode={gamecode} ===")

    data = extract_game_data(season, gamecode, competition)
    boxscore_df = data["boxscore"]
    pbp_df = data["pbp"]
    game_info_df = data["game_info"]

    advanced_df = compute_advanced_stats(boxscore_df)

    if engine is None:
        engine = get_engine()

    load_teams(engine, boxscore_df)
    load_players(engine, boxscore_df)
    load_game(engine, game_info_df)
    load_boxscores(engine, boxscore_df)
    load_play_by_play(engine, pbp_df)
    load_shots(engine, data.get("shots", pd.DataFrame()))
    load_player_advanced_stats(engine, advanced_df)

    logger.info(f"=== Pipeline complete: season={season}, gamecode={gamecode} ===")


def run_pipeline_batch(
    season: int,
    gamecodes: List[int],
    competition: str = "E",
    engine: Optional[Engine] = None,
    max_workers: int = 12,
    progress_callback: Optional[callable] = None,
) -> Dict[str, int]:
    """
    High-throughput batch pipeline: extract N games concurrently, then
    bulk-load all data into PostgreSQL in a single pass.

    Steps:
      1. Concurrent extraction (ThreadPool, semaphore-throttled)
      2. Per-game transform (compute_advanced_stats)
      3. Concatenate all DataFrames
      4. Bulk-insert via psycopg2 execute_values

    Returns dict with keys: total, loaded, failed.
    """
    from data_pipeline.extractors import extract_games_concurrent
    from data_pipeline.transformers import compute_advanced_stats

    if engine is None:
        engine = get_engine()

    total = len(gamecodes)
    t0 = time.time()
    logger.info(f"=== Batch pipeline: {total} games, {max_workers} workers ===")

    # --- Phase 1: Concurrent extraction ---
    game_results = extract_games_concurrent(
        season, gamecodes, competition,
        max_workers=max_workers,
        progress_callback=progress_callback,
    )

    extracted = len(game_results)
    failed = total - extracted
    logger.info(f"Extracted {extracted}/{total} games ({failed} failed) in {time.time()-t0:.1f}s")

    if not game_results:
        return {"total": total, "loaded": 0, "failed": failed}

    # --- Phase 2: Transform + concatenate ---
    t1 = time.time()
    all_box, all_pbp, all_shots, all_info, all_adv = [], [], [], [], []

    for data in game_results:
        box = data["boxscore"]
        if not box.empty:
            all_box.append(box)
            adv = compute_advanced_stats(box)
            if not adv.empty:
                all_adv.append(adv)
        if not data["pbp"].empty:
            all_pbp.append(data["pbp"])
        if not data["shots"].empty:
            all_shots.append(data["shots"])
        if not data["game_info"].empty:
            all_info.append(data["game_info"])

    box_all = pd.concat(all_box, ignore_index=True) if all_box else pd.DataFrame()
    pbp_all = pd.concat(all_pbp, ignore_index=True) if all_pbp else pd.DataFrame()
    shots_all = pd.concat(all_shots, ignore_index=True) if all_shots else pd.DataFrame()
    info_all = pd.concat(all_info, ignore_index=True) if all_info else pd.DataFrame()
    adv_all = pd.concat(all_adv, ignore_index=True) if all_adv else pd.DataFrame()

    logger.info(f"Transform phase: {time.time()-t1:.1f}s")

    # --- Phase 3: Bulk load ---
    t2 = time.time()
    load_teams(engine, box_all)
    load_players(engine, box_all)
    load_game(engine, info_all)
    load_boxscores(engine, box_all)
    load_play_by_play(engine, pbp_all)
    load_shots(engine, shots_all)
    load_player_advanced_stats(engine, adv_all)

    elapsed = time.time() - t0
    logger.info(
        f"=== Batch complete: {extracted} games loaded in {elapsed:.1f}s "
        f"(extract={t1-t0:.1f}s, transform={t2-t1:.1f}s, load={time.time()-t2:.1f}s) ==="
    )

    return {"total": total, "loaded": extracted, "failed": failed}


def load_season(
    season: int,
    competition: str = "E",
    reset: bool = False,
    limit: Optional[int] = None,
) -> None:
    """
    End-to-end pipeline for an entire season using concurrent extraction
    and bulk database loading.

    Parameters
    ----------
    limit : int or None
        When set, only the first *limit* played games are processed.
        Useful for local testing without hitting API rate limits.
    """
    from data_pipeline.extractors import get_season_schedule

    engine = get_engine()

    if reset:
        teardown_database(engine)
        logger.info("Recreating strict schema from database/schema.sql ...")

    ensure_schema(engine)

    logger.info(f"=== Starting full-season load for {competition}{season} ===")
    schedule = get_season_schedule(season, competition)
    if schedule.empty:
        logger.error("Failed to load schedule. Aborting.")
        return

    played_games = schedule[schedule["played"] == True]
    gamecodes = played_games["gamecode"].tolist()
    total_available = len(gamecodes)

    if limit is not None:
        gamecodes = gamecodes[:limit]
        logger.warning(
            f"TESTING MODE ACTIVE: Processing only the first "
            f"{len(gamecodes)} of {total_available} played games."
        )

    logger.info(f"Found {total_available} played games; loading {len(gamecodes)}.")

    result = run_pipeline_batch(season, gamecodes, competition, engine=engine)

    # Phase 4: Season-level aggregations (on/off splits, etc.)
    if result["loaded"] > 0:
        run_season_aggregations(season, engine=engine)

    logger.info(
        f"=== Season {competition}{season} complete: "
        f"{result['loaded']}/{result['total']} loaded, "
        f"{result['failed']} failed ==="
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Euroleague ETL Pipeline")
    parser.add_argument("--season", type=int, help="Season year (e.g. 2024)", required=True)
    parser.add_argument("--game", type=int, help="Specific gamecode to load")
    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="Hard reset: drop ALL tables, recreate strict schema, then load.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of games to process (for local testing).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

    if args.reset:
        logger.warning(
            "*** HARD RESET requested. All existing data will be destroyed. ***"
        )

    if args.game:
        if args.reset:
            engine = get_engine()
            teardown_database(engine)
            ensure_schema(engine)
            run_pipeline(args.season, args.game, engine=engine)
        else:
            run_pipeline(args.season, args.game)
    else:
        load_season(args.season, reset=args.reset, limit=args.limit)
