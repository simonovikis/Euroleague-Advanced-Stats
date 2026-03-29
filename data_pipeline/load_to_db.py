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
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
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


def load_teams(engine: Engine, boxscore_df: pd.DataFrame) -> None:
    """
    Extract unique teams from a boxscore DataFrame and upsert into the
    `teams` table.

    The boxscore has a "Team" column with 3-letter codes (e.g. "BER", "OLY").
    We store the code as the name initially — can be enriched later.

    Uses ON CONFLICT (team_code) DO UPDATE to handle re-runs gracefully.
    """
    if boxscore_df.empty:
        return

    teams = boxscore_df["Team"].dropna().unique()
    team_records = [{"team_code": t.strip(), "team_name": t.strip()} for t in teams if t.strip()]

    if not team_records:
        return

    sql = text("""
        INSERT INTO teams (team_code, team_name)
        VALUES (:team_code, :team_name)
        ON CONFLICT (team_code) DO UPDATE
            SET team_name = EXCLUDED.team_name
    """)

    with engine.begin() as conn:
        conn.execute(sql, team_records)

    logger.info(f"Upserted {len(team_records)} teams")


def load_players(engine: Engine, boxscore_df: pd.DataFrame) -> None:
    """
    Extract unique players from the boxscore and upsert into the
    `players` table.

    The API returns a `Player_ID` column with the euroleague player ID,
    along with `Player` (name), `Team` (code), `Dorsal` (jersey #).
    """
    if boxscore_df.empty:
        return

    # Determine player ID column (the installed version uses 'Player_ID')
    id_col = "Player_ID" if "Player_ID" in boxscore_df.columns else None
    if id_col is None:
        # Fallback: generate synthetic IDs
        boxscore_df = boxscore_df.copy()
        boxscore_df["_synth_id"] = (
            boxscore_df["Player"].str.replace(" ", "_").str.upper()
            + "_"
            + boxscore_df["Team"].str.strip()
        )
        id_col = "_synth_id"

    # Deduplicate by player ID
    unique_players = (
        boxscore_df[[id_col, "Player", "Team", "Dorsal"]]
        .drop_duplicates(subset=[id_col])
    )

    records = []
    for _, row in unique_players.iterrows():
        records.append({
            "player_id": str(row[id_col]).strip(),
            "player_name": str(row["Player"]).strip(),
            "team_code": str(row["Team"]).strip(),
            "dorsal": str(row.get("Dorsal", "")).strip() if pd.notna(row.get("Dorsal")) else None,
        })

    sql = text("""
        INSERT INTO players (player_id, player_name, team_code, dorsal)
        VALUES (:player_id, :player_name, :team_code, :dorsal)
        ON CONFLICT (player_id) DO UPDATE
            SET player_name = EXCLUDED.player_name,
                team_code = EXCLUDED.team_code,
                dorsal = EXCLUDED.dorsal
    """)

    with engine.begin() as conn:
        conn.execute(sql, records)

    logger.info(f"Upserted {len(records)} players")


def load_game(engine: Engine, game_info_df: pd.DataFrame) -> None:
    """
    Insert or update game metadata into the `games` table.

    game_info_df columns: season, gamecode, home_team, away_team,
                          home_score, away_score
    """
    if game_info_df.empty:
        return

    records = game_info_df.to_dict("records")

    sql = text("""
        INSERT INTO games (season, gamecode, home_team, away_team, home_score, away_score, game_date, round, played, referee1, referee2, referee3)
        VALUES (:season, :gamecode, :home_team, :away_team, :home_score, :away_score, :game_date, :round, :played, :referee1, :referee2, :referee3)
        ON CONFLICT (season, gamecode) DO UPDATE
            SET home_team  = EXCLUDED.home_team,
                away_team  = EXCLUDED.away_team,
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                game_date  = EXCLUDED.game_date,
                round      = EXCLUDED.round,
                played     = EXCLUDED.played,
                referee1   = EXCLUDED.referee1,
                referee2   = EXCLUDED.referee2,
                referee3   = EXCLUDED.referee3
    """)

    # We need to make sure the records dict has these keys, even if missing from the dataframe
    for req_key in ['game_date', 'round', 'played', 'referee1', 'referee2', 'referee3']:
        for rec in records:
            if req_key not in rec or pd.isna(rec[req_key]):
                rec[req_key] = None if req_key != 'played' else False


    with engine.begin() as conn:
        conn.execute(sql, records)

    logger.info(f"Upserted {len(records)} game(s)")


def load_play_by_play(engine: Engine, pbp_df: pd.DataFrame) -> None:
    """
    Load play-by-play data into the `play_by_play` table.

    Since PBP rows don't have a natural unique key, we first delete
    existing rows for the game, then insert fresh — ensuring idempotent
    re-runs.

    PBP DataFrame columns (from euroleague-api):
        Season, Gamecode, PERIOD, PLAYTYPE, PLAYER_ID, PLAYER,
        CODETEAM, MARKERTIME, NUMBEROFPLAY, COMMENT, ...
    """
    if pbp_df.empty:
        return

    # Determine unique games in this batch
    games = pbp_df[["Season", "Gamecode"]].drop_duplicates()

    with engine.begin() as conn:
        # Delete existing PBP for these games (idempotent re-run)
        for _, game in games.iterrows():
            conn.execute(
                text("DELETE FROM play_by_play WHERE season = :s AND gamecode = :g"),
                {"s": int(game["Season"]), "g": int(game["Gamecode"])},
            )

        # Prepare records
        records = []
        for _, row in pbp_df.iterrows():
            records.append({
                "season": int(row["Season"]),
                "gamecode": int(row["Gamecode"]),
                "period": int(row.get("PERIOD", 0)) if pd.notna(row.get("PERIOD")) else None,
                "playtype": str(row.get("PLAYTYPE", "")).strip() if pd.notna(row.get("PLAYTYPE")) else None,
                "player_id": str(row.get("PLAYER_ID", "")).strip() if pd.notna(row.get("PLAYER_ID")) else None,
                "player": str(row.get("PLAYER", "")).strip() if pd.notna(row.get("PLAYER")) else None,
                "codeteam": str(row.get("CODETEAM", "")).strip() if pd.notna(row.get("CODETEAM")) else None,
                "markertime": str(row.get("MARKERTIME", "")).strip() if pd.notna(row.get("MARKERTIME")) else None,
                "numberofplay": int(row.get("NUMBEROFPLAY", 0)) if pd.notna(row.get("NUMBEROFPLAY")) else None,
                "comment": str(row.get("COMMENT", ""))[:500] if pd.notna(row.get("COMMENT")) else None,
            })

        if records:
            sql = text("""
                INSERT INTO play_by_play
                    (season, gamecode, period, playtype, player_id, player,
                     codeteam, markertime, numberofplay, comment)
                VALUES
                    (:season, :gamecode, :period, :playtype, :player_id, :player,
                     :codeteam, :markertime, :numberofplay, :comment)
            """)
            conn.execute(sql, records)

    logger.info(f"Loaded {len(records)} PBP rows")


def load_player_advanced_stats(
    engine: Engine,
    advanced_df: pd.DataFrame,
) -> None:
    """
    Load computed advanced stats into the `player_advanced_stats` table.

    Uses ON CONFLICT (season, gamecode, player_id) DO UPDATE for
    idempotent upserts.

    advanced_df columns (from transformers.py):
        Season, Gamecode, player_id, player_name, team_code, is_home,
        minutes, points, fgm2, fga2, fgm3, fga3, ftm, fta,
        off_rebounds, def_rebounds, total_rebounds,
        assists, steals, turnovers, blocks_favour, blocks_against,
        fouls_committed, fouls_received, plus_minus,
        possessions, ts_pct, off_rating, def_rating
    """
    if advanced_df.empty:
        return

    def safe_int(val):
        if pd.isna(val):
            return None
        return int(val)

    def safe_float(val):
        if pd.isna(val):
            return None
        return float(val)

    records = []
    for _, row in advanced_df.iterrows():
        records.append({
            "season":           int(row["Season"]),
            "gamecode":         int(row["Gamecode"]),
            "player_id":        str(row.get("player_id", "")).strip(),
            "player_name":      str(row.get("player_name", "")).strip(),
            "team_code":        str(row.get("team_code", "")).strip(),
            "is_home":          bool(row.get("is_home", False)),
            "minutes":          safe_float(row.get("minutes")),
            "points":           safe_int(row.get("points")),
            "fgm2":             safe_int(row.get("fgm2")),
            "fga2":             safe_int(row.get("fga2")),
            "fgm3":             safe_int(row.get("fgm3")),
            "fga3":             safe_int(row.get("fga3")),
            "ftm":              safe_int(row.get("ftm")),
            "fta":              safe_int(row.get("fta")),
            "off_rebounds":     safe_int(row.get("off_rebounds")),
            "def_rebounds":     safe_int(row.get("def_rebounds")),
            "total_rebounds":   safe_int(row.get("total_rebounds")),
            "assists":          safe_int(row.get("assists")),
            "steals":           safe_int(row.get("steals")),
            "turnovers":        safe_int(row.get("turnovers")),
            "blocks_favour":    safe_int(row.get("blocks_favour")),
            "blocks_against":   safe_int(row.get("blocks_against")),
            "fouls_committed":  safe_int(row.get("fouls_committed")),
            "fouls_received":   safe_int(row.get("fouls_received")),
            "plus_minus":       safe_float(row.get("plus_minus")),
            "possessions":      safe_float(row.get("possessions")),
            "ts_pct":           safe_float(row.get("ts_pct")),
            "off_rating":       safe_float(row.get("off_rating")),
            "def_rating":       safe_float(row.get("def_rating")),
        })

    sql = text("""
        INSERT INTO player_advanced_stats (
            season, gamecode, player_id, player_name, team_code, is_home,
            minutes, points,
            fgm2, fga2, fgm3, fga3, ftm, fta,
            off_rebounds, def_rebounds, total_rebounds,
            assists, steals, turnovers,
            blocks_favour, blocks_against,
            fouls_committed, fouls_received, plus_minus,
            possessions, ts_pct, off_rating, def_rating
        ) VALUES (
            :season, :gamecode, :player_id, :player_name, :team_code, :is_home,
            :minutes, :points,
            :fgm2, :fga2, :fgm3, :fga3, :ftm, :fta,
            :off_rebounds, :def_rebounds, :total_rebounds,
            :assists, :steals, :turnovers,
            :blocks_favour, :blocks_against,
            :fouls_committed, :fouls_received, :plus_minus,
            :possessions, :ts_pct, :off_rating, :def_rating
        )
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
            def_rating      = EXCLUDED.def_rating
    """)

    with engine.begin() as conn:
        conn.execute(sql, records)

    logger.info(f"Upserted {len(records)} player advanced stat rows")


def load_boxscores(engine: Engine, boxscore_df: pd.DataFrame) -> None:
    """
    Store the raw boxscore DataFrame into the `boxscores` table.
    Uses delete-then-insert per game for idempotent re-runs.
    """
    if boxscore_df.empty:
        return

    id_col = "Player_ID" if "Player_ID" in boxscore_df.columns else None
    if id_col is None:
        boxscore_df = boxscore_df.copy()
        boxscore_df["Player_ID"] = (
            boxscore_df["Player"].str.replace(" ", "_").str.upper()
            + "_"
            + boxscore_df["Team"].str.strip()
        )
        id_col = "Player_ID"

    games = boxscore_df[["Season", "Gamecode"]].drop_duplicates()

    def safe_int(val):
        if pd.isna(val):
            return None
        return int(val)

    def safe_float(val):
        if pd.isna(val):
            return None
        return float(val)

    with engine.begin() as conn:
        for _, game in games.iterrows():
            conn.execute(
                text("DELETE FROM boxscores WHERE season = :s AND gamecode = :g"),
                {"s": int(game["Season"]), "g": int(game["Gamecode"])},
            )

        records = []
        for _, row in boxscore_df.iterrows():
            records.append({
                "season": int(row["Season"]),
                "gamecode": int(row["Gamecode"]),
                "player_id": str(row[id_col]).strip(),
                "player": str(row.get("Player", "")).strip(),
                "team": str(row.get("Team", "")).strip(),
                "home": safe_int(row.get("Home")),
                "is_starter": safe_int(row.get("IsStarter")),
                "is_playing": safe_int(row.get("IsPlaying")),
                "dorsal": str(row.get("Dorsal", "")).strip() if pd.notna(row.get("Dorsal")) else None,
                "minutes": str(row.get("Minutes", "")).strip() if pd.notna(row.get("Minutes")) else None,
                "points": safe_int(row.get("Points")),
                "fgm2": safe_int(row.get("FieldGoalsMade2")),
                "fga2": safe_int(row.get("FieldGoalsAttempted2")),
                "fgm3": safe_int(row.get("FieldGoalsMade3")),
                "fga3": safe_int(row.get("FieldGoalsAttempted3")),
                "ftm": safe_int(row.get("FreeThrowsMade")),
                "fta": safe_int(row.get("FreeThrowsAttempted")),
                "off_rebounds": safe_int(row.get("OffensiveRebounds")),
                "def_rebounds": safe_int(row.get("DefensiveRebounds")),
                "total_rebounds": safe_int(row.get("TotalRebounds")),
                "assists": safe_int(row.get("Assistances")),
                "steals": safe_int(row.get("Steals")),
                "turnovers": safe_int(row.get("Turnovers")),
                "blocks_favour": safe_int(row.get("BlocksFavour")),
                "blocks_against": safe_int(row.get("BlocksAgainst")),
                "fouls_committed": safe_int(row.get("FoulsCommited")),
                "fouls_received": safe_int(row.get("FoulsReceived")),
                "valuation": safe_int(row.get("Valuation")),
                "plus_minus": safe_float(row.get("Plusminus")),
            })

        if records:
            sql = text("""
                INSERT INTO boxscores
                    (season, gamecode, player_id, player, team, home,
                     is_starter, is_playing, dorsal, minutes,
                     points, fgm2, fga2, fgm3, fga3, ftm, fta,
                     off_rebounds, def_rebounds, total_rebounds,
                     assists, steals, turnovers, blocks_favour, blocks_against,
                     fouls_committed, fouls_received, valuation, plus_minus)
                VALUES
                    (:season, :gamecode, :player_id, :player, :team, :home,
                     :is_starter, :is_playing, :dorsal, :minutes,
                     :points, :fgm2, :fga2, :fgm3, :fga3, :ftm, :fta,
                     :off_rebounds, :def_rebounds, :total_rebounds,
                     :assists, :steals, :turnovers, :blocks_favour, :blocks_against,
                     :fouls_committed, :fouls_received, :valuation, :plus_minus)
            """)
            conn.execute(sql, records)

    logger.info(f"Loaded {len(records)} boxscore rows")


def load_shots(engine: Engine, shots_df: pd.DataFrame) -> None:
    """
    Store shot data into the `shots` table.
    Uses delete-then-insert per game for idempotent re-runs.
    """
    if shots_df.empty:
        return

    games = shots_df[["Season", "Gamecode"]].drop_duplicates()

    def safe_int(val):
        if pd.isna(val):
            return None
        return int(val)

    def safe_float(val):
        if pd.isna(val):
            return None
        return float(val)

    with engine.begin() as conn:
        for _, game in games.iterrows():
            conn.execute(
                text("DELETE FROM shots WHERE season = :s AND gamecode = :g"),
                {"s": int(game["Season"]), "g": int(game["Gamecode"])},
            )

        records = []
        for _, row in shots_df.iterrows():
            records.append({
                "season": int(row["Season"]),
                "gamecode": int(row["Gamecode"]),
                "num_anot": safe_int(row.get("NUM_ANOT")),
                "team": str(row.get("TEAM", "")).strip() if pd.notna(row.get("TEAM")) else None,
                "id_player": str(row.get("ID_PLAYER", "")).strip() if pd.notna(row.get("ID_PLAYER")) else None,
                "player": str(row.get("PLAYER", "")).strip() if pd.notna(row.get("PLAYER")) else None,
                "id_action": str(row.get("ID_ACTION", "")).strip() if pd.notna(row.get("ID_ACTION")) else None,
                "action": str(row.get("ACTION", "")).strip() if pd.notna(row.get("ACTION")) else None,
                "points": safe_int(row.get("POINTS")),
                "coord_x": safe_float(row.get("COORD_X")),
                "coord_y": safe_float(row.get("COORD_Y")),
                "zone": str(row.get("ZONE", "")).strip() if pd.notna(row.get("ZONE")) else None,
                "fastbreak": safe_int(row.get("FASTBREAK")),
                "second_chance": safe_int(row.get("SECOND_CHANCE")),
                "pts_off_turnover": safe_int(row.get("POINTS_OFF_TURNOVER")),
                "minute": safe_int(row.get("MINUTE")),
                "console": str(row.get("CONSOLE", "")).strip() if pd.notna(row.get("CONSOLE")) else None,
                "points_a": safe_int(row.get("POINTS_A")),
                "points_b": safe_int(row.get("POINTS_B")),
            })

        if records:
            sql = text("""
                INSERT INTO shots
                    (season, gamecode, num_anot, team, id_player, player,
                     id_action, action, points, coord_x, coord_y, zone,
                     fastbreak, second_chance, pts_off_turnover,
                     minute, console, points_a, points_b)
                VALUES
                    (:season, :gamecode, :num_anot, :team, :id_player, :player,
                     :id_action, :action, :points, :coord_x, :coord_y, :zone,
                     :fastbreak, :second_chance, :pts_off_turnover,
                     :minute, :console, :points_a, :points_b)
            """)
            conn.execute(sql, records)

    logger.info(f"Loaded {len(records)} shot rows")


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
    logger.info("TEARDOWN: Public schema recreated (empty)")


def ensure_schema(engine: Engine) -> None:
    """Create tables if they don't already exist (idempotent)."""
    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    if schema_path.exists():
        ddl = schema_path.read_text()
        with engine.begin() as conn:
            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    try:
                        conn.execute(text(statement))
                    except Exception:
                        pass


# ========================================================================
# HIGH-LEVEL PIPELINE FUNCTION
# ========================================================================

def run_pipeline(
    season: int,
    gamecode: int,
    competition: str = "E",
    engine: Optional[Engine] = None,
) -> None:
    """
    End-to-end pipeline for a single game:
      1. Extract boxscore + PBP data from the Euroleague API
      2. Transform → compute advanced stats (Possessions, TS%, ORtg, DRtg)
      3. Load everything into PostgreSQL

    Usage:
        from data_pipeline.load_to_db import run_pipeline
        run_pipeline(season=2024, gamecode=1)
    """
    from data_pipeline.extractors import extract_game_data
    from data_pipeline.transformers import compute_advanced_stats

    logger.info(f"=== Pipeline start: season={season}, gamecode={gamecode} ===")

    # 1. Extract
    data = extract_game_data(season, gamecode, competition)
    boxscore_df = data["boxscore"]
    pbp_df = data["pbp"]
    game_info_df = data["game_info"]

    # 2. Transform
    advanced_df = compute_advanced_stats(boxscore_df)

    # 3. Load
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


def load_season(
    season: int,
    competition: str = "E",
    reset: bool = False,
) -> None:
    """
    End-to-end pipeline for an entire season:
      1. (Optional) Hard-reset the database if ``reset=True``.
      2. Ensure the strict schema exists.
      3. Fetch the schedule.
      4. Loop over every played game and run ``run_pipeline``.

    Usage:
        from data_pipeline.load_to_db import load_season
        load_season(season=2024, reset=True)
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
    total = len(played_games)
    logger.info(f"Found {total} played games to load.")

    for i, row in played_games.iterrows():
        gc = row["gamecode"]
        logger.info(f"Loading game {gc} ({i+1}/{total})...")
        try:
            run_pipeline(season, gc, competition, engine=engine)
        except Exception as e:
            logger.error(f"Error loading {competition}{season} game {gc}: {e}")

    logger.info(f"=== Completed full-season load for {competition}{season} ===")


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
        load_season(args.season, reset=args.reset)
