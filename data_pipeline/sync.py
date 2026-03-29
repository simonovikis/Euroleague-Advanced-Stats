"""
sync.py — Smart Synchronization Script
========================================
Checks the live Euroleague schedule for played games and compares them
against the ones already stored in your PostgreSQL container.

Only downloads and runs the ETL pipeline for missing games.
Uses concurrent extraction + bulk inserts for speed (Phase 59.5).
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from sqlalchemy import text
from data_pipeline.load_to_db import run_pipeline_batch, run_season_aggregations, get_engine, ensure_schema
from data_pipeline.extractors import get_season_schedule

logger = logging.getLogger(__name__)


def sync_recent_games(season: int, competition: str = "E") -> None:
    """
    Finds games that are marked as 'played' on the official schedule
    but are missing from the local database, and loads them using the
    concurrent batch pipeline.
    """
    logger.info(f"=== Starting Smart Sync for {competition}{season} ===")

    schedule = get_season_schedule(season, competition)
    if schedule.empty:
        logger.error("Failed to fetch live schedule. Aborting sync.")
        return

    played_official = schedule[schedule["played"] == True]["gamecode"].tolist()

    engine = get_engine()
    ensure_schema(engine)

    with engine.connect() as conn:
        db_games = pd.read_sql(
            text("SELECT gamecode FROM games WHERE season = :season AND played = TRUE"),
            conn,
            params={"season": season},
        )

    played_db = set(db_games["gamecode"].tolist()) if not db_games.empty else set()
    missing_games = [gc for gc in played_official if gc not in played_db]

    if not missing_games:
        print("\nNo unsynced games found. Database is already up to date.")
        logger.info("Database is fully up to date. No new games to sync.")
        return

    logger.info(f"Found {len(missing_games)} missing game(s): {missing_games}")

    result = run_pipeline_batch(season, missing_games, competition, engine=engine)

    if result["loaded"] > 0:
        run_season_aggregations(season, engine=engine)
        print(
            f"\nFound {result['total']} missing games — "
            f"successfully synced {result['loaded']}, "
            f"{result['failed']} failed."
        )
    else:
        print("\nFound missing games but failed to sync them. Check logs for details.")

    logger.info("=== Smart Sync Completed Successfully ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Euroleague DB Sync")
    parser.add_argument("--season", type=int, help="Season year (e.g. 2024)", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
    sync_recent_games(args.season)
