"""
live_extractor.py — Live Game Detection & Real-Time Data Fetching
=================================================================
Identifies games currently in progress by comparing the daily schedule
against game completion status, then fetches live PBP and boxscore data.

The euroleague-api schedule endpoint returns a `played` boolean per game.
A game is considered "live" when:
  - It is scheduled for today (by date)
  - Its `played` flag is False (not yet final)
  - The game's PBP data is non-empty (game has started)

This heuristic avoids false positives from games not yet tipped off.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from euroleague_api.boxscore_data import BoxScoreData
from euroleague_api.game_stats import GameStats
from euroleague_api.play_by_play_data import PlayByPlay

from data_pipeline.extractors import (
    COMPETITION,
    apply_team_aliases,
    get_boxscore,
    get_play_by_play,
    get_shot_data,
    _extract_game_info,
)

logger = logging.getLogger(__name__)


def get_todays_schedule(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch the full season schedule and filter to today's games.

    Returns a DataFrame with columns: gamecode, home_code, away_code,
    home_name, away_name, played, date, round.
    """
    try:
        gs = GameStats(competition)
        df = gs.get_gamecodes_season(season)

        if df.empty:
            return pd.DataFrame()

        today_str = datetime.now().strftime("%b %d, %Y").replace(" 0", " ")
        today_alt = datetime.now().strftime("%Y-%m-%d")

        date_col = "date" if "date" in df.columns else None
        if date_col is None:
            for col in df.columns:
                if "date" in col.lower():
                    date_col = col
                    break

        if date_col is None:
            logger.warning("No date column found in schedule")
            return pd.DataFrame()

        today_games = df[
            df[date_col].astype(str).str.contains(today_str, case=False, na=False)
            | df[date_col].astype(str).str.contains(today_alt, case=False, na=False)
        ].copy()

        if today_games.empty:
            return pd.DataFrame()

        schedule = pd.DataFrame()
        schedule["gamecode"] = today_games.get("gameCode", today_games.get("gamecode", 0))
        schedule["home_code"] = today_games.get("homecode", "")
        schedule["away_code"] = today_games.get("awaycode", "")
        schedule["home_name"] = today_games.get("hometeam", "")
        schedule["away_name"] = today_games.get("awayteam", "")
        schedule["played"] = today_games.get("played", False)
        schedule["date"] = today_games.get(date_col, "")
        schedule["round"] = today_games.get("Round", 0)
        schedule["home_score"] = pd.to_numeric(
            today_games.get("homescore", 0), errors="coerce"
        ).fillna(0).astype(int)
        schedule["away_score"] = pd.to_numeric(
            today_games.get("awayscore", 0), errors="coerce"
        ).fillna(0).astype(int)

        schedule = apply_team_aliases(schedule, ["home_code", "away_code"])
        return schedule.reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to fetch today's schedule: {e}")
        return pd.DataFrame()


def detect_live_games(
    season: int,
    competition: str = COMPETITION,
) -> List[Dict]:
    """
    Identify games that are currently in progress.

    A game is "live" if:
      1. It is scheduled for today
      2. Its `played` flag is False (game not yet finalized)
      3. PBP data exists (game has tipped off)

    Returns a list of dicts with keys: gamecode, home_code, away_code,
    home_name, away_name, home_score, away_score.
    """
    schedule = get_todays_schedule(season, competition)
    if schedule.empty:
        return []

    not_final = schedule[schedule["played"] == False]
    if not_final.empty:
        return []

    live_games = []
    for _, row in not_final.iterrows():
        gamecode = int(row["gamecode"])
        try:
            pbp_api = PlayByPlay(competition)
            pbp = pbp_api.get_game_play_by_play_data(season, gamecode)
            if not pbp.empty:
                live_games.append({
                    "gamecode": gamecode,
                    "home_code": row["home_code"],
                    "away_code": row["away_code"],
                    "home_name": row["home_name"],
                    "away_name": row["away_name"],
                    "home_score": row.get("home_score", 0),
                    "away_score": row.get("away_score", 0),
                    "round": row.get("round", 0),
                })
        except Exception as e:
            logger.debug(f"Game {gamecode} not started yet: {e}")
            continue

    return live_games


def fetch_live_game_data(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch the latest boxscore and PBP data for a live game.
    Unlike the cached version, this always hits the API fresh.

    Returns dict with: boxscore, pbp, shots, game_info.
    """
    boxscore_df = get_boxscore(season, gamecode, competition)
    pbp_df = get_play_by_play(season, gamecode, competition)

    try:
        shots_df = get_shot_data(season, gamecode, competition)
    except Exception:
        shots_df = pd.DataFrame()

    game_info = _extract_game_info(boxscore_df, pbp_df, season, gamecode)

    return {
        "boxscore": boxscore_df,
        "pbp": pbp_df,
        "shots": shots_df,
        "game_info": game_info,
    }


def get_live_score_and_time(pbp_df: pd.DataFrame) -> Dict:
    """
    Extract current score and game clock from the latest PBP event.

    Returns dict with: home_score, away_score, period, time_remaining,
    total_seconds_elapsed, total_seconds_remaining.
    """
    if pbp_df.empty:
        return {
            "home_score": 0, "away_score": 0,
            "period": 1, "time_remaining": "10:00",
            "total_seconds_elapsed": 0, "total_seconds_remaining": 2400,
        }

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce")
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce")

    valid = df.dropna(subset=["POINTS_A", "POINTS_B"])
    if valid.empty:
        last = df.iloc[-1]
        home_score, away_score = 0, 0
    else:
        last = valid.iloc[-1]
        home_score = int(last["POINTS_A"])
        away_score = int(last["POINTS_B"])

    last_row = df.iloc[-1]
    period = int(last_row.get("PERIOD", 1))
    time_remaining = str(last_row.get("MARKERTIME", "00:00"))

    try:
        parts = time_remaining.split(":")
        secs_in_period = int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        secs_in_period = 0

    period_length = 600  # 10 minutes per quarter
    if period <= 4:
        elapsed = (period - 1) * period_length + (period_length - secs_in_period)
        total_regulation = 4 * period_length
        remaining = total_regulation - elapsed
    else:
        ot_period = period - 4
        ot_length = 300  # 5 min OT
        elapsed = 4 * period_length + (ot_period - 1) * ot_length + (ot_length - secs_in_period)
        remaining = max(0, ot_length - (ot_length - secs_in_period))

    return {
        "home_score": home_score,
        "away_score": away_score,
        "period": period,
        "time_remaining": time_remaining,
        "total_seconds_elapsed": max(0, elapsed),
        "total_seconds_remaining": max(0, remaining),
    }
