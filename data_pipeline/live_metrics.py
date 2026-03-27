"""
live_metrics.py — Real-Time Game Metrics
=========================================
Computes live analytics from in-progress PBP and boxscore data:

  1. Current Lineup Net Rating — 5 players on floor + their game net rating
  2. Momentum Tracker — active scoring runs with timing
  3. Live Win Probability — logistic model based on score diff & time remaining
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from data_pipeline.transformers import (
    track_lineups,
    compute_lineup_stats,
    parse_minutes,
    _markertime_to_seconds,
    format_player_name,
)
from data_pipeline.live_extractor import get_live_score_and_time

logger = logging.getLogger(__name__)


# ========================================================================
# 1. CURRENT LINEUP NET RATING
# ========================================================================

def get_current_lineups(
    pbp_df: pd.DataFrame,
    boxscore_df: pd.DataFrame,
) -> Dict[str, Dict]:
    """
    Determine the exact 5 players currently on the floor for both teams,
    and compute their real-time net rating in this game.

    Returns dict with keys 'home' and 'away', each containing:
      - team: team code
      - players: list of player names currently on court
      - player_ids: frozenset of player IDs
      - pts_for: points scored while this 5-man unit is on court
      - pts_against: points allowed
      - ortg: offensive rating (per 100 possessions)
      - drtg: defensive rating (per 100 possessions)
      - net_rtg: net rating
    """
    if pbp_df.empty or boxscore_df.empty:
        return {"home": None, "away": None}

    pbp_lu = track_lineups(pbp_df, boxscore_df)
    if pbp_lu.empty or "home_lineup" not in pbp_lu.columns:
        return {"home": None, "away": None}

    last_row = pbp_lu.iloc[-1]
    home_lineup = last_row.get("home_lineup")
    away_lineup = last_row.get("away_lineup")
    home_team = last_row.get("home_team", "HOME")
    away_team = last_row.get("away_team", "AWAY")

    lineup_stats = compute_lineup_stats(pbp_lu, boxscore_df, min_events=0)

    id_to_name = {}
    if not boxscore_df.empty:
        for _, r in boxscore_df.iterrows():
            pid = str(r.get("Player_ID", "")).strip()
            pname = r.get("Player", pid)
            id_to_name[pid] = format_player_name(pname) if pname else pid

    result = {}
    for side, lineup, team in [
        ("home", home_lineup, home_team),
        ("away", away_lineup, away_team),
    ]:
        if lineup is None or not isinstance(lineup, frozenset):
            result[side] = None
            continue

        player_names = [id_to_name.get(pid, pid) for pid in sorted(lineup)]

        lu_match = lineup_stats[
            (lineup_stats["team"] == team)
            & (lineup_stats["lineup"] == lineup)
        ]

        if not lu_match.empty:
            row = lu_match.iloc[0]
            result[side] = {
                "team": team,
                "players": player_names,
                "player_ids": lineup,
                "events": int(row.get("events", 0)),
                "pts_for": int(row.get("pts_for", 0)),
                "pts_against": int(row.get("pts_against", 0)),
                "ortg": float(row.get("ortg", 0)),
                "drtg": float(row.get("drtg", 0)),
                "net_rtg": float(row.get("net_rtg", 0)),
            }
        else:
            result[side] = {
                "team": team,
                "players": player_names,
                "player_ids": lineup,
                "events": 0,
                "pts_for": 0,
                "pts_against": 0,
                "ortg": 0.0,
                "drtg": 0.0,
                "net_rtg": 0.0,
            }

    return result


# ========================================================================
# 2. MOMENTUM TRACKER — Scoring Runs
# ========================================================================

def detect_active_run(pbp_df: pd.DataFrame) -> Optional[Dict]:
    """
    Check if there is an active scoring run at the current moment in the game.
    A run is defined as consecutive scoring by one team with no answer from
    the opponent.

    Returns None if no active run (>= 4 unanswered points), or a dict with:
      - run_team: team code on the run
      - run_points: unanswered points
      - opponent_team: the team being run on
      - duration_str: how long the run has lasted (e.g. "2:15")
      - scoring_plays: list of play descriptions in the run
    """
    if pbp_df.empty:
        return None

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce")
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce")

    scoring_types = {"2FGM", "3FGM", "FTM"}

    scored = df[df["PLAYTYPE"].isin(scoring_types)].copy()
    if scored.empty:
        return None

    scored = scored.dropna(subset=["POINTS_A", "POINTS_B"])
    if scored.empty:
        return None

    # Determine home/away teams
    home_team = None
    away_team = None
    if "home_team" in df.columns:
        vals = df["home_team"].dropna().unique()
        if len(vals) > 0:
            home_team = vals[0]
    if "away_team" in df.columns:
        vals = df["away_team"].dropna().unique()
        if len(vals) > 0:
            away_team = vals[0]

    # Walk backwards from the most recent scoring play to find the run
    run_points = 0
    run_team_code = None
    run_plays = []
    run_start_time = None
    run_end_time = None

    for i in range(len(scored) - 1, -1, -1):
        row = scored.iloc[i]
        team = row.get("CODETEAM", "")

        if run_team_code is None:
            run_team_code = team
            run_end_time = row.get("MARKERTIME", "00:00")

        if team != run_team_code:
            break

        pts = 0
        if row["PLAYTYPE"] == "3FGM":
            pts = 3
        elif row["PLAYTYPE"] == "2FGM":
            pts = 2
        elif row["PLAYTYPE"] == "FTM":
            pts = 1

        run_points += pts
        run_start_time = row.get("MARKERTIME", "00:00")
        player = row.get("PLAYER", "Unknown")
        run_plays.insert(0, f"{player} ({row['PLAYTYPE']})")

    if run_points < 4:
        return None

    # Calculate duration
    try:
        start_secs = _markertime_to_seconds(str(run_start_time))
        end_secs = _markertime_to_seconds(str(run_end_time))
        duration_secs = abs(start_secs - end_secs)
        dur_min = int(duration_secs // 60)
        dur_sec = int(duration_secs % 60)
        duration_str = f"{dur_min}:{dur_sec:02d}"
    except Exception:
        duration_str = "N/A"

    # Determine opponent
    teams_in_game = df["CODETEAM"].dropna().unique()
    opponent = [t for t in teams_in_game if t != run_team_code and t != ""]
    opponent_team = opponent[0] if opponent else "OPP"

    return {
        "run_team": run_team_code,
        "run_points": run_points,
        "opponent_team": opponent_team,
        "duration_str": duration_str,
        "scoring_plays": run_plays,
    }


def get_momentum_timeline(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a scoring differential timeline for momentum visualization.

    Returns a DataFrame with columns: play_number, period, time, 
    home_score, away_score, score_diff (home perspective),
    scoring_team, player, play_type.
    """
    if pbp_df.empty:
        return pd.DataFrame()

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce")
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce")

    scoring_types = {"2FGM", "3FGM", "FTM"}
    scored = df[df["PLAYTYPE"].isin(scoring_types)].dropna(subset=["POINTS_A", "POINTS_B"]).copy()

    if scored.empty:
        return pd.DataFrame()

    result = pd.DataFrame({
        "play_number": scored["TRUE_NUMBEROFPLAY"].values,
        "period": scored["PERIOD"].values,
        "time": scored["MARKERTIME"].values,
        "home_score": scored["POINTS_A"].astype(int).values,
        "away_score": scored["POINTS_B"].astype(int).values,
        "scoring_team": scored["CODETEAM"].values,
        "player": scored["PLAYER"].values,
        "play_type": scored["PLAYTYPE"].values,
    })
    result["score_diff"] = result["home_score"] - result["away_score"]

    return result.reset_index(drop=True)


# ========================================================================
# 3. LIVE WIN PROBABILITY
# ========================================================================

def compute_live_win_probability(
    home_score: int,
    away_score: int,
    total_seconds_remaining: int,
    is_home: bool = True,
) -> float:
    """
    Estimate win probability using a logistic model calibrated for
    Euroleague basketball.

    The model uses score differential adjusted by time remaining.
    As the game progresses, the same point differential becomes more
    predictive (the coefficient increases).

    Parameters
    ----------
    home_score : int
    away_score : int
    total_seconds_remaining : int
        Seconds remaining in regulation (2400 = full game).
    is_home : bool
        If True, returns home team win probability.

    Returns
    -------
    float
        Win probability between 0.0 and 1.0.
    """
    diff = home_score - away_score
    total_game_seconds = 2400.0

    # Time factor: as game progresses, each point matters more
    time_fraction = max(total_seconds_remaining / total_game_seconds, 0.01)

    # Base home court advantage (~3 points)
    home_advantage = 3.0 * time_fraction

    # Adjusted differential: accounts for pace of resolution
    # As time decreases, the effective weight of the lead increases
    adjusted_diff = (diff + home_advantage) / max(np.sqrt(time_fraction), 0.1)

    # Logistic function with calibrated coefficient
    # k ~0.15 calibrated to match historical Euroleague close game outcomes
    k = 0.15
    prob = 1.0 / (1.0 + np.exp(-k * adjusted_diff))

    # Clamp
    prob = max(0.001, min(0.999, prob))

    if not is_home:
        prob = 1.0 - prob

    return prob


def compute_win_probability_timeline(
    pbp_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute win probability at every scoring event for timeline charting.

    Returns DataFrame with: play_number, period, time, home_score,
    away_score, home_wp, away_wp.
    """
    timeline = get_momentum_timeline(pbp_df)
    if timeline.empty:
        return pd.DataFrame()

    results = []
    for _, row in timeline.iterrows():
        period = int(row["period"])
        time_str = str(row["time"])

        try:
            parts = time_str.split(":")
            secs_in_period = int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError):
            secs_in_period = 0

        period_length = 600
        if period <= 4:
            elapsed = (period - 1) * period_length + (period_length - secs_in_period)
            remaining = max(0, 4 * period_length - elapsed)
        else:
            ot_length = 300
            elapsed = 4 * period_length + (period - 5) * ot_length + (ot_length - secs_in_period)
            remaining = max(0, ot_length - (ot_length - secs_in_period))

        home_wp = compute_live_win_probability(
            int(row["home_score"]), int(row["away_score"]), remaining
        )

        results.append({
            "play_number": row["play_number"],
            "period": period,
            "time": time_str,
            "home_score": int(row["home_score"]),
            "away_score": int(row["away_score"]),
            "home_wp": round(home_wp, 3),
            "away_wp": round(1.0 - home_wp, 3),
        })

    return pd.DataFrame(results)
