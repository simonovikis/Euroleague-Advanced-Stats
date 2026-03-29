"""
monte_carlo.py -- Monte Carlo Playoff Probability Engine
=========================================================
Simulates the remainder of a Euroleague regular season N times to
estimate each team's probability of reaching key standings milestones:
  - Top 10 (Play-In)
  - Top 6 (Direct Playoffs)
  - Top 4 (Home Court Advantage)
  - Win Regular Season (#1 seed)

Uses team Net Rating as baseline strength, translated into game-level
win probability via a logistic function with a home-court modifier.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

HOME_COURT_ADVANTAGE = 2.5  # Net Rating points added to home team
LOGISTIC_DIVISOR = 10.0     # Scales the sigmoid so a +5 NRtg diff ~ 68% win prob
MAX_REGULAR_SEASON_ROUND = 38  # 34 base + up to 4 extra for postponed/rescheduled
MAX_PROBE_ROUND = 40           # stop probing for additional rounds beyond this


def _win_probability(home_net_rtg: float, away_net_rtg: float) -> float:
    """Convert a Net Rating differential into home win probability.

    Uses a logistic (sigmoid) function:
        P(home_win) = 1 / (1 + exp(-(home_rtg - away_rtg + HCA) / DIVISOR))

    The divisor is calibrated so that realistic Euroleague Net Rating gaps
    (typically -15 to +15) produce plausible win probabilities:
        +0  diff -> ~56% (home-court edge only)
        +5  diff -> ~68%
        +10 diff -> ~78%
        +18 diff -> ~89%  (best vs worst, still ~11% upset chance)
    """
    diff = home_net_rtg - away_net_rtg + HOME_COURT_ADVANTAGE
    return 1.0 / (1.0 + np.exp(-diff / LOGISTIC_DIVISOR))


def _parse_game_date(date_str) -> Optional[datetime]:
    """Try to parse a game date string into a datetime.

    Handles the ``Mon DD, YYYY`` format used by the Euroleague API
    as well as ISO-like formats.
    """
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, TypeError):
            continue
    try:
        from dateutil import parser as dp
        return dp.parse(date_str)
    except Exception:
        return None


def _fetch_extra_rounds(
    season: int,
    start_round: int,
    competition: str = "E",
) -> pd.DataFrame:
    """Probe the per-round API for rounds beyond get_gamecodes_season.

    The ``get_gamecodes_season`` endpoint often omits postponed /
    rescheduled rounds (35-38). ``get_gamecodes_round`` does return
    them.  This helper probes successive rounds until it gets an empty
    response or reaches ``MAX_PROBE_ROUND``.
    """
    from euroleague_api.game_stats import GameStats
    from data_pipeline.extractors import apply_team_aliases

    gs = GameStats(competition)
    extra_rows = []

    for r in range(start_round, MAX_PROBE_ROUND + 1):
        try:
            rdf = gs.get_gamecodes_round(season, r)
        except Exception:
            break
        if rdf is None or rdf.empty:
            break

        phase = ""
        if "Phase" in rdf.columns:
            phase = str(rdf["Phase"].iloc[0]).upper() if not rdf["Phase"].isna().all() else ""
        if phase and phase != "RS":
            logger.info("Round %d has phase '%s' (not RS) — stopping probe.", r, phase)
            break

        for _, row in rdf.iterrows():
            home_score = pd.to_numeric(row.get("local.score"), errors="coerce")
            away_score = pd.to_numeric(row.get("road.score"), errors="coerce")
            extra_rows.append({
                "season": season,
                "round": int(r),
                "round_name": "RS",
                "gamecode": int(row.get("gameCode", 0)),
                "played": bool(row.get("played", False)),
                "home_code": row.get("local.club.code", ""),
                "home_name": row.get("local.club.name", ""),
                "home_logo": row.get("local.club.images.crest", ""),
                "home_score": home_score if pd.notna(home_score) else None,
                "away_code": row.get("road.club.code", ""),
                "away_name": row.get("road.club.name", ""),
                "away_logo": row.get("road.club.images.crest", ""),
                "away_score": away_score if pd.notna(away_score) else None,
                "date": row.get("date", ""),
            })

        logger.info("Discovered Round %d with %d games (played=%s).",
                     r, len(rdf), rdf["played"].all() if "played" in rdf.columns else "?")

    if not extra_rows:
        return pd.DataFrame()

    df = pd.DataFrame(extra_rows)
    df = apply_team_aliases(df, ["home_code", "away_code"])
    return df


def fetch_full_schedule(season: int, competition: str = "E") -> pd.DataFrame:
    """Fetch the complete season schedule including unplayed future games.

    Combines two API sources:
      1. ``get_gamecodes_season`` — returns the bulk of the schedule but
         may omit postponed / rescheduled rounds beyond Round 34.
      2. ``get_gamecodes_round(N)`` for rounds 35+ — probed one-by-one
         until an empty response is returned.

    Post-processing:
      - Filter to regular-season (phase 'RS') games only.
      - Fix the ``played`` flag: any game whose date is in the future is
        forced to ``played=False``.
    """
    from data_pipeline.extractors import get_season_schedule, apply_team_aliases

    schedule = get_season_schedule(season, competition)
    if schedule.empty:
        return schedule

    schedule = apply_team_aliases(schedule, ["home_code", "away_code"])

    # --- 1. Regular-season filter on base schedule ---
    if "round_name" in schedule.columns:
        rs_mask = schedule["round_name"].str.upper().eq("RS") | schedule["round_name"].isna()
        schedule = schedule[rs_mask].copy()

    # --- 2. Discover extra rounds (postponed / rescheduled) ---
    max_round_in_base = int(schedule["round"].max()) if not schedule.empty else 0
    extra = _fetch_extra_rounds(season, max_round_in_base + 1, competition)
    if not extra.empty:
        existing_gamecodes = set(schedule["gamecode"].tolist())
        extra = extra[~extra["gamecode"].isin(existing_gamecodes)]
        if not extra.empty:
            schedule = pd.concat([schedule, extra], ignore_index=True)
            logger.info("Added %d games from extended rounds (%d-%d).",
                        len(extra), extra["round"].min(), extra["round"].max())

    # --- 3. Cap at MAX_REGULAR_SEASON_ROUND ---
    schedule = schedule[schedule["round"] <= MAX_REGULAR_SEASON_ROUND].copy()

    # --- 4. Date-based played validation ---
    now = datetime.now()
    date_col = "date" if "date" in schedule.columns else "game_date"
    if date_col in schedule.columns:
        corrected = 0
        for idx, row in schedule.iterrows():
            gd = _parse_game_date(str(row.get(date_col, "")))
            if gd is not None and gd > now and row.get("played") is True:
                schedule.at[idx, "played"] = False
                schedule.at[idx, "home_score"] = None
                schedule.at[idx, "away_score"] = None
                corrected += 1
        if corrected:
            logger.info(
                "Corrected %d future-dated games from played=True to played=False.",
                corrected,
            )

    return schedule


def build_current_standings(
    schedule: pd.DataFrame,
) -> pd.DataFrame:
    """Derive current W-L standings from the schedule of played games.

    Returns a DataFrame with columns:
        team_code, wins, losses, games_played
    """
    played = schedule[
        (schedule["played"] == True)
        & schedule["home_score"].notna()
        & schedule["away_score"].notna()
    ].copy()
    if played.empty:
        return pd.DataFrame(columns=["team_code", "wins", "losses", "games_played"])

    home_wins = played[played["home_score"] > played["away_score"]]
    home_losses = played[played["home_score"] < played["away_score"]]

    records: Dict[str, Dict[str, int]] = {}

    all_teams = set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())
    for t in all_teams:
        records[t] = {"wins": 0, "losses": 0}

    for _, g in home_wins.iterrows():
        records[g["home_code"]]["wins"] += 1
        records[g["away_code"]]["losses"] += 1

    for _, g in home_losses.iterrows():
        records[g["away_code"]]["wins"] += 1
        records[g["home_code"]]["losses"] += 1

    rows = [
        {"team_code": tc, "wins": r["wins"], "losses": r["losses"],
         "games_played": r["wins"] + r["losses"]}
        for tc, r in records.items()
    ]
    return pd.DataFrame(rows)


def get_remaining_games(schedule: pd.DataFrame) -> pd.DataFrame:
    """Extract unplayed games from the schedule."""
    return schedule[schedule["played"] == False][
        ["gamecode", "home_code", "away_code", "round"]
    ].copy()


def get_remaining_regular_season_games(
    season: int,
    competition: str = "E",
) -> pd.DataFrame:
    """Fetch and return only the unplayed regular-season games.

    Convenience wrapper that calls ``fetch_full_schedule`` (with its RS
    filter and date-based validation) and extracts the ``played=False``
    rows.  Useful for inspecting what the simulator will actually run.
    """
    schedule = fetch_full_schedule(season, competition)
    if schedule.empty:
        return pd.DataFrame(columns=["gamecode", "home_code", "away_code", "round"])
    remaining = get_remaining_games(schedule)
    logger.info(
        "Found %d unplayed regular-season games for season %d.",
        len(remaining), season,
    )
    return remaining


def simulate_season(
    schedule: pd.DataFrame,
    net_rtg_map: Dict[str, float],
    runs: int = 10_000,
    seed: int = 42,
) -> pd.DataFrame:
    """Run a Monte Carlo simulation of the remaining season.

    Parameters
    ----------
    schedule : pd.DataFrame
        Full season schedule with played/unplayed games.
    net_rtg_map : dict
        Mapping of team_code -> season Net Rating.
    runs : int
        Number of simulation iterations.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    pd.DataFrame with columns:
        team_code, current_wins, current_losses, avg_wins, avg_losses,
        pos_1_pct .. pos_N_pct (probability of finishing in each position),
        make_top_10_pct, make_top_6_pct, make_top_4_pct, win_rs_pct,
        games_simulated (int — 0 when season is complete)
    """
    rng = np.random.default_rng(seed)

    standings = build_current_standings(schedule)
    remaining = get_remaining_games(schedule)

    total_games = len(schedule)
    played_count = int((schedule["played"] == True).sum())
    unplayed_count = len(remaining)
    logger.info(
        "Schedule: %d total games, %d played, %d unplayed to simulate.",
        total_games, played_count, unplayed_count,
    )

    if standings.empty:
        return pd.DataFrame()

    teams = sorted(standings["team_code"].tolist())
    team_idx = {t: i for i, t in enumerate(teams)}
    n_teams = len(teams)

    current_wins = np.zeros(n_teams, dtype=np.int32)
    current_losses = np.zeros(n_teams, dtype=np.int32)
    for _, row in standings.iterrows():
        idx = team_idx.get(row["team_code"])
        if idx is not None:
            current_wins[idx] = row["wins"]
            current_losses[idx] = row["losses"]

    if remaining.empty:
        logger.info("No remaining games to simulate — returning current standings.")
        results = []
        sorted_wins = np.argsort(-current_wins)
        for i, tc in enumerate(teams):
            rank = int(np.where(sorted_wins == team_idx[tc])[0][0]) + 1
            row = {
                "team_code": tc,
                "current_wins": int(current_wins[team_idx[tc]]),
                "current_losses": int(current_losses[team_idx[tc]]),
                "avg_wins": float(current_wins[team_idx[tc]]),
                "avg_losses": float(current_losses[team_idx[tc]]),
            }
            for p in range(1, n_teams + 1):
                row[f"pos_{p}_pct"] = 100.0 if p == rank else 0.0
            row.update({
                "make_top_10_pct": 100.0 if rank <= 10 else 0.0,
                "make_top_6_pct": 100.0 if rank <= 6 else 0.0,
                "make_top_4_pct": 100.0 if rank <= 4 else 0.0,
                "win_rs_pct": 100.0 if rank == 1 else 0.0,
                "games_simulated": 0,
            })
            results.append(row)
        df = pd.DataFrame(results).sort_values("avg_wins", ascending=False).reset_index(drop=True)
        df.index = df.index + 1
        df.index.name = "proj_rank"
        return df.reset_index()

    # Pre-compute win probabilities for remaining games
    home_idxs = []
    away_idxs = []
    win_probs = []

    for _, g in remaining.iterrows():
        h = g["home_code"]
        a = g["away_code"]
        if h not in team_idx or a not in team_idx:
            continue
        home_idxs.append(team_idx[h])
        away_idxs.append(team_idx[a])
        h_rtg = net_rtg_map.get(h, 0.0)
        a_rtg = net_rtg_map.get(a, 0.0)
        win_probs.append(_win_probability(h_rtg, a_rtg))

    home_idxs = np.array(home_idxs, dtype=np.int32)
    away_idxs = np.array(away_idxs, dtype=np.int32)
    win_probs = np.array(win_probs, dtype=np.float64)
    n_games = len(win_probs)

    # Accumulators
    total_wins = np.zeros(n_teams, dtype=np.float64)
    total_losses = np.zeros(n_teams, dtype=np.float64)
    position_counts = np.zeros((n_teams, n_teams), dtype=np.int32)  # [team_idx, position_0based]

    for _ in range(runs):
        sim_wins = current_wins.copy()
        sim_losses = current_losses.copy()

        # Simulate all remaining games at once
        outcomes = rng.random(n_games) < win_probs  # True = home win

        for g_idx in range(n_games):
            if outcomes[g_idx]:
                sim_wins[home_idxs[g_idx]] += 1
                sim_losses[away_idxs[g_idx]] += 1
            else:
                sim_wins[away_idxs[g_idx]] += 1
                sim_losses[home_idxs[g_idx]] += 1

        total_wins += sim_wins
        total_losses += sim_losses

        # Rank by wins (tiebreak: random for simplicity)
        ranking_key = sim_wins + rng.random(n_teams) * 0.001
        sorted_idxs = np.argsort(-ranking_key)

        for rank, tidx in enumerate(sorted_idxs):
            position_counts[tidx, rank] += 1

    logger.info(
        "Simulation complete: %d runs x %d games. Teams: %d.",
        runs, n_games, n_teams,
    )

    results = []
    for i, tc in enumerate(teams):
        row = {
            "team_code": tc,
            "current_wins": int(current_wins[i]),
            "current_losses": int(current_losses[i]),
            "avg_wins": round(total_wins[i] / runs, 1),
            "avg_losses": round(total_losses[i] / runs, 1),
        }
        for p in range(n_teams):
            row[f"pos_{p + 1}_pct"] = round(position_counts[i, p] / runs * 100, 1)
        row.update({
            "make_top_10_pct": round(position_counts[i, :10].sum() / runs * 100, 1),
            "make_top_6_pct": round(position_counts[i, :6].sum() / runs * 100, 1),
            "make_top_4_pct": round(position_counts[i, :4].sum() / runs * 100, 1),
            "win_rs_pct": round(position_counts[i, 0] / runs * 100, 1),
            "games_simulated": n_games,
        })
        results.append(row)

    df = pd.DataFrame(results).sort_values("avg_wins", ascending=False).reset_index(drop=True)
    df.index = df.index + 1  # 1-based projected rank
    df.index.name = "proj_rank"
    return df.reset_index()
