"""
sync.py — Smart Synchronization Script
========================================
Checks the live Euroleague schedule for played games and compares them 
against the ones already stored in your PostgreSQL container.

Only downloads and runs the ETL pipeline for missing games.
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
from data_pipeline.load_to_db import run_pipeline, get_engine
from data_pipeline.extractors import get_season_schedule

logger = logging.getLogger(__name__)

def sync_recent_games(season: int, competition: str = "E") -> None:
    """
    Finds games that are marked as 'played' on the official schedule
    but are missing from the local database, and loads them.
    """
    logger.info(f"=== Starting Smart Sync for {competition}{season} ===")
    
    # 1. Fetch official live schedule
    schedule = get_season_schedule(season, competition)
    if schedule.empty:
        logger.error("Failed to fetch live schedule. Aborting sync.")
        return
    
    played_official = schedule[schedule["played"] == True]["gamecode"].tolist()
    
    # 2. Fetch existing games from database
    engine = get_engine()
    with engine.connect() as conn:
        db_games = pd.read_sql(
            text("SELECT gamecode FROM games WHERE season = :season AND played = TRUE"),
            conn,
            params={"season": season},
        )
    
    played_db = db_games["gamecode"].tolist() if not db_games.empty else []
    
    # 3. Identify missing games
    missing_games = [gc for gc in played_official if gc not in played_db]
    
    if not missing_games:
        print("\n✅ No unsynced games found. Database is already up to date.")
        logger.info("Database is fully up to date. No new games to sync.")
        return
        
    logger.info(f"Found {len(missing_games)} missing game(s): {missing_games}")
    
    # 4. Run ETL Pipeline only for missing games
    synced_count = 0
    for gc in missing_games:
        logger.info(f"Syncing game {gc}...")
        try:
            run_pipeline(season, gc, competition)
            synced_count += 1
        except Exception as e:
            logger.error(f"Error syncing {competition}{season} game {gc}: {e}")
            
    if synced_count > 0:
        print(f"\n🚀 Found {len(missing_games)} missing games and successfully synced {synced_count}.")
    else:
        print("\n⚠️ Found missing games but failed to sync them. Check logs for details.")

    logger.info("=== Smart Sync Completed Successfully ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Euroleague DB Sync")
    parser.add_argument("--season", type=int, help="Season year (e.g. 2024)", required=True)
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

    sync_recent_games(args.season)
