"""
sync_schedule.py -- Full Season Schedule Seeder
=================================================
Seeds the ``games`` table with the entire regular-season calendar
(Rounds 1-34) for a given season, including future unplayed fixtures.

The existing ETL (``load_to_db.run_pipeline``, ``sync.py``) only inserts
games that have boxscores -- i.e. already played.  This script fills
the gap by ensuring every scheduled fixture exists in the DB with the
correct ``played`` flag *before* any boxscore data is loaded.

Safe to run repeatedly: uses INSERT ... ON CONFLICT with conditional
updates that never overwrite existing scores/referees with NULLs.

Usage:
    python -m data_pipeline.sync_schedule --season 2025
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from sqlalchemy import text
from sqlalchemy.engine import Engine

from data_pipeline.load_to_db import get_engine, ensure_schema, _bulk_execute

logger = logging.getLogger(__name__)


def fetch_regular_season_schedule(
    season: int,
    competition: str = "E",
) -> pd.DataFrame:
    """Fetch the full regular-season schedule from the Euroleague API.

    Delegates to ``monte_carlo.fetch_full_schedule`` which probes for
    extra rounds (35+) that contain postponed / rescheduled fixtures.
    """
    from data_pipeline.monte_carlo import fetch_full_schedule

    schedule = fetch_full_schedule(season, competition)
    if schedule.empty:
        logger.error("API returned empty schedule for season %d.", season)
        return pd.DataFrame()

    played = int((schedule["played"] == True).sum())
    unplayed = int((schedule["played"] == False).sum())
    logger.info(
        "Regular-season schedule: %d games (%d played, %d unplayed).",
        len(schedule), played, unplayed,
    )
    return schedule


def _ensure_teams_exist(engine: Engine, schedule: pd.DataFrame) -> None:
    """Insert any teams from the schedule that don't exist in the DB yet."""
    teams = {}
    for _, row in schedule.iterrows():
        hc = row.get("home_code", "")
        ac = row.get("away_code", "")
        if hc and hc not in teams:
            teams[hc] = {
                "team_code": hc,
                "team_name": row.get("home_name") or hc,
                "logo_url": row.get("home_logo") or None,
            }
        if ac and ac not in teams:
            teams[ac] = {
                "team_code": ac,
                "team_name": row.get("away_name") or ac,
                "logo_url": row.get("away_logo") or None,
            }

    if not teams:
        return

    records = list(teams.values())
    _bulk_execute(
        engine,
        """INSERT INTO teams (team_code, team_name, logo_url) VALUES %s
           ON CONFLICT (team_code) DO UPDATE SET
               team_name = COALESCE(NULLIF(EXCLUDED.team_name, EXCLUDED.team_code), teams.team_name),
               logo_url  = COALESCE(EXCLUDED.logo_url, teams.logo_url)""",
        ["team_code", "team_name", "logo_url"],
        records,
    )
    logger.info("Ensured %d team(s) exist in DB.", len(records))


def seed_schedule(
    season: int,
    competition: str = "E",
    engine: Optional[Engine] = None,
) -> dict:
    """Seed the ``games`` table with the full regular-season calendar.

    For **played** games: upserts scores and ``played=True``.
    For **unplayed** games: inserts the fixture with ``played=False``
    and NULL scores; never overwrites existing scores if the DB already
    has them (the ON CONFLICT clause uses COALESCE).

    Returns a dict: ``{total, inserted, updated}``.
    """
    schedule = fetch_regular_season_schedule(season, competition)
    if schedule.empty:
        return {"total": 0, "inserted": 0, "updated": 0}

    if engine is None:
        engine = get_engine()
    ensure_schema(engine)

    _ensure_teams_exist(engine, schedule)

    # Build records for all games
    date_col = "date" if "date" in schedule.columns else "game_date"
    records = []
    for _, row in schedule.iterrows():
        is_played = bool(row.get("played", False))
        home_score = row.get("home_score")
        away_score = row.get("away_score")
        if pd.isna(home_score):
            home_score = None
        else:
            home_score = int(home_score)
        if pd.isna(away_score):
            away_score = None
        else:
            away_score = int(away_score)

        records.append({
            "season": int(season),
            "gamecode": int(row["gamecode"]),
            "home_team": row["home_code"],
            "away_team": row["away_code"],
            "home_score": home_score,
            "away_score": away_score,
            "game_date": str(row.get(date_col, "")) or None,
            "round": int(row["round"]) if pd.notna(row.get("round")) else None,
            "played": is_played,
        })

    # Count existing rows before upsert
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT COUNT(*) FROM games WHERE season = :s"),
            {"s": season},
        ).scalar() or 0

    # Upsert: for played games update everything; for unplayed games
    # only insert the fixture -- never overwrite real scores with NULL.
    n = _bulk_execute(
        engine,
        """INSERT INTO games (season, gamecode, home_team, away_team,
               home_score, away_score, game_date, round, played)
           VALUES %s
           ON CONFLICT (season, gamecode) DO UPDATE SET
               home_team  = EXCLUDED.home_team,
               away_team  = EXCLUDED.away_team,
               home_score = COALESCE(EXCLUDED.home_score, games.home_score),
               away_score = COALESCE(EXCLUDED.away_score, games.away_score),
               game_date  = COALESCE(EXCLUDED.game_date, games.game_date),
               round      = COALESCE(EXCLUDED.round, games.round),
               played     = CASE WHEN EXCLUDED.played THEN TRUE
                                 ELSE games.played END""",
        ["season", "gamecode", "home_team", "away_team",
         "home_score", "away_score", "game_date", "round", "played"],
        records,
    )

    # Count after
    with engine.connect() as conn:
        after = conn.execute(
            text("SELECT COUNT(*) FROM games WHERE season = :s"),
            {"s": season},
        ).scalar() or 0

    inserted = after - existing
    updated = n - inserted

    played_db = 0
    unplayed_db = 0
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    SUM(CASE WHEN played THEN 1 ELSE 0 END) AS p,
                    SUM(CASE WHEN NOT played THEN 1 ELSE 0 END) AS u
                FROM games WHERE season = :s
            """),
            {"s": season},
        ).fetchone()
        if row:
            played_db = row[0] or 0
            unplayed_db = row[1] or 0

    logger.info(
        "Schedule seeded: %d total rows sent, %d new inserts, %d updates. "
        "DB now has %d played + %d unplayed = %d games for season %d.",
        n, inserted, updated, played_db, unplayed_db,
        played_db + unplayed_db, season,
    )

    return {"total": n, "inserted": inserted, "updated": updated}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed the games table with the full regular-season schedule.",
    )
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g. 2025)")
    parser.add_argument("--competition", default="E", help="Competition code (default: E)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    result = seed_schedule(args.season, args.competition)
    print(
        f"\nDone. {result['total']} games processed: "
        f"{result['inserted']} new, {result['updated']} updated."
    )
