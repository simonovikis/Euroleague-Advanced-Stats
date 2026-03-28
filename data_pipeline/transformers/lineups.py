import logging
from itertools import combinations
from typing import Dict, List
import numpy as np
import pandas as pd
from data_pipeline.transformers.utils import format_player_name, _markertime_to_seconds

logger = logging.getLogger(__name__)


def track_lineups(
    pbp_df: pd.DataFrame,
    boxscore_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reconstruct the exact 5 players on court at every PBP action.

    Algorithm:
      1. Initialize lineups from boxscore starters (IsStarter == 1.0)
      2. Walk through PBP in TRUE_NUMBEROFPLAY order
      3. On "IN" events: add player to team's active set
      4. On "OUT" events: remove player from team's active set
      5. Tag each PBP row with the current home/away lineup (frozensets)

    The lineup is stored as a frozenset of Player_IDs for efficient
    comparison and grouping.

    Parameters
    ----------
    pbp_df : pd.DataFrame
        Play-by-play data from extractors.
    boxscore_df : pd.DataFrame
        Boxscore data with IsStarter column.

    Returns
    -------
    pd.DataFrame
        PBP DataFrame with added columns: home_lineup, away_lineup,
        home_team, away_team.
    """
    if pbp_df.empty or boxscore_df.empty:
        return pbp_df.copy()

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()

    # --- Identify home and away teams ---
    home_players = boxscore_df[boxscore_df["Home"] == 1]
    away_players = boxscore_df[boxscore_df["Home"] == 0]

    if home_players.empty or away_players.empty:
        logger.warning("Cannot determine home/away teams from boxscore")
        return df

    home_team = home_players["Team"].iloc[0]
    away_team = away_players["Team"].iloc[0]

    # --- Initialize with starters ---
    # IsStarter == 1.0 marks the 5 starters per team
    home_starters = set(
        home_players[home_players["IsStarter"] == 1.0]["Player_ID"]
        .astype(str).str.strip().tolist()
    )
    away_starters = set(
        away_players[away_players["IsStarter"] == 1.0]["Player_ID"]
        .astype(str).str.strip().tolist()
    )

    # Current on-court sets (mutable during iteration)
    home_on_court = set(home_starters)
    away_on_court = set(away_starters)

    # Storage for lineup tags
    home_lineups = []
    away_lineups = []

    # --- Walk through PBP and track substitutions ---
    for _, row in df.iterrows():
        playtype = str(row.get("PLAYTYPE", "")).strip()
        codeteam = str(row.get("CODETEAM", "")).strip()
        player_id = str(row.get("PLAYER_ID", "")).strip()

        # Process substitution BEFORE recording the lineup for this event
        if playtype == "IN" and player_id:
            if codeteam == home_team:
                home_on_court.add(player_id)
            elif codeteam == away_team:
                away_on_court.add(player_id)

        elif playtype == "OUT" and player_id:
            if codeteam == home_team:
                home_on_court.discard(player_id)
            elif codeteam == away_team:
                away_on_court.discard(player_id)

        # Record the current lineup snapshot
        home_lineups.append(frozenset(home_on_court))
        away_lineups.append(frozenset(away_on_court))

    df["home_lineup"] = home_lineups
    df["away_lineup"] = away_lineups
    df["home_team"] = home_team
    df["away_team"] = away_team

    return df


def compute_lineup_stats(
    pbp_with_lineups: pd.DataFrame, 
    boxscore_df: pd.DataFrame = None,
    min_events: int = 0
) -> pd.DataFrame:
    """
    Aggregate stats per 5-man lineup combination.

    For each unique lineup (frozenset of 5 Player_IDs), compute:
      - Points scored, points allowed
      - Estimated possessions (from scoring events)
      - Net Rating = (ORtg - DRtg) per 100 possessions
      - Number of PBP events (proxy for minutes)

    Scoring events are identified by PLAYTYPE in {2FGM, 3FGM, FTM}
    with point values derived from the play type.
    """
    if pbp_with_lineups.empty or "home_lineup" not in pbp_with_lineups.columns:
        return pd.DataFrame()

    df = pbp_with_lineups.copy()
    home_team = df["home_team"].iloc[0]
    away_team = df["away_team"].iloc[0]

    # Build player_id → formatted_name map
    name_map = {}
    if boxscore_df is not None and "Player_ID" in boxscore_df.columns:
        for _, r in boxscore_df.iterrows():
            pid = str(r["Player_ID"]).strip()
            raw_name = str(r.get("Player", pid))
            name_map[pid] = format_player_name(raw_name)

    # Identify scoring events and their point values
    score_map = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    df["score_pts"] = df["PLAYTYPE"].map(score_map).fillna(0).astype(int)

    # Possession-ending events: made shots (non-FT), turnovers, defensive rebounds
    # We use a simplified count of possessions per lineup
    poss_events = {"2FGM", "2FGA", "3FGM", "3FGA", "TO", "D"}

    results = []

    # Process home lineups
    for lineup, group in df.groupby("home_lineup"):
        if len(lineup) != 5:
            continue

        pts_for = group.loc[group["CODETEAM"] == home_team, "score_pts"].sum()
        pts_against = group.loc[group["CODETEAM"] == away_team, "score_pts"].sum()

        # Estimate possessions: count possession-ending events by home team
        home_poss = len(group[
            (group["CODETEAM"] == home_team) & (group["PLAYTYPE"].isin(poss_events))
        ])
        # Estimate opponent possessions likewise
        opp_poss = len(group[
            (group["CODETEAM"] == away_team) & (group["PLAYTYPE"].isin(poss_events))
        ])

        total_poss = max((home_poss + opp_poss) / 2, 1)  # average both sides
        ortg = (pts_for / total_poss) * 100 if total_poss > 0 else 0
        drtg = (pts_against / total_poss) * 100 if total_poss > 0 else 0

        formatted_names = [name_map.get(pid, pid) for pid in lineup]

        results.append({
            "team": home_team,
            "lineup": lineup,
            "lineup_str": ", ".join(sorted(formatted_names)),
            "events": len(group),
            "pts_for": pts_for,
            "pts_against": pts_against,
            "poss": total_poss,
            "ortg": round(ortg, 1),
            "drtg": round(drtg, 1),
            "net_rtg": round(ortg - drtg, 1),
        })

    # Process away lineups
    for lineup, group in df.groupby("away_lineup"):
        if len(lineup) != 5:
            continue

        pts_for = group.loc[group["CODETEAM"] == away_team, "score_pts"].sum()
        pts_against = group.loc[group["CODETEAM"] == home_team, "score_pts"].sum()

        home_poss = len(group[
            (group["CODETEAM"] == away_team) & (group["PLAYTYPE"].isin(poss_events))
        ])
        opp_poss = len(group[
            (group["CODETEAM"] == home_team) & (group["PLAYTYPE"].isin(poss_events))
        ])

        total_poss = max((home_poss + opp_poss) / 2, 1)
        ortg = (pts_for / total_poss) * 100 if total_poss > 0 else 0
        drtg = (pts_against / total_poss) * 100 if total_poss > 0 else 0

        formatted_names = [name_map.get(pid, pid) for pid in lineup]

        results.append({
            "team": away_team,
            "lineup": lineup,
            "lineup_str": ", ".join(sorted(formatted_names)),
            "events": len(group),
            "pts_for": pts_for,
            "pts_against": pts_against,
            "poss": total_poss,
            "ortg": round(ortg, 1),
            "drtg": round(drtg, 1),
            "net_rtg": round(ortg - drtg, 1),
        })

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        # Phase 4 filter: remove noisy lineups with few events (minutes proxy)
        result_df = result_df[result_df["events"] >= min_events]
        result_df = result_df.sort_values("net_rtg", ascending=False).reset_index(drop=True)
    return result_df


def compute_duo_trio_synergy(
    pbp_with_lineups: pd.DataFrame,
    boxscore_df: pd.DataFrame,
    combo_size: int = 2,
) -> pd.DataFrame:
    """
    Compute performance for every 2-player (duo) or 3-player (trio) combo.

    For each combination of `combo_size` players on the same team:
      - "Together" stats: Net Rating when ALL players in the combo are on court
      - "Apart" stats:   Net Rating when at least one is off court
      - Synergy = Together Net Rtg − Apart Net Rtg

    A positive synergy means these players perform better together than apart.

    Parameters
    ----------
    combo_size : int
        2 for duos, 3 for trios.
    """
    if pbp_with_lineups.empty or "home_lineup" not in pbp_with_lineups.columns:
        return pd.DataFrame()

    df = pbp_with_lineups.copy()
    home_team = df["home_team"].iloc[0]
    away_team = df["away_team"].iloc[0]
    score_map = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    df["score_pts"] = df["PLAYTYPE"].map(score_map).fillna(0).astype(int)

    # Build player_id → format_player_name map from boxscore
    name_map = {}
    if "Player_ID" in boxscore_df.columns:
        for _, r in boxscore_df.iterrows():
            pid = str(r["Player_ID"]).strip()
            name_map[pid] = format_player_name(str(r.get("Player", "")))

    results = []

    for team, lineup_col in [(home_team, "home_lineup"), (away_team, "away_lineup")]:
        opp_team = away_team if team == home_team else home_team

        # Get all player IDs who appeared in lineups for this team
        all_players = set()
        for lu in df[lineup_col].dropna():
            all_players.update(lu)

        # Generate all combos
        for combo in combinations(sorted(all_players), combo_size):
            combo_set = set(combo)

            # "Together": all combo members in the lineup
            together_mask = df[lineup_col].apply(lambda lu: combo_set.issubset(lu))
            apart_mask = ~together_mask

            tog_df = df[together_mask]
            apt_df = df[apart_mask]

            if len(tog_df) < 5:
                continue

            # Net rating when together
            tog_pts_for = tog_df.loc[tog_df["CODETEAM"] == team, "score_pts"].sum()
            tog_pts_against = tog_df.loc[tog_df["CODETEAM"] == opp_team, "score_pts"].sum()
            tog_poss = max(len(tog_df) / 10, 1)  # rough possession estimate
            tog_net = ((tog_pts_for - tog_pts_against) / tog_poss) * 100

            # Net rating when apart
            apt_pts_for = apt_df.loc[apt_df["CODETEAM"] == team, "score_pts"].sum() if len(apt_df) > 0 else 0
            apt_pts_against = apt_df.loc[apt_df["CODETEAM"] == opp_team, "score_pts"].sum() if len(apt_df) > 0 else 0
            apt_poss = max(len(apt_df) / 10, 1)
            apt_net = ((apt_pts_for - apt_pts_against) / apt_poss) * 100

            combo_names = [name_map.get(pid, pid) for pid in combo]

            results.append({
                "team": team,
                "combo": combo,
                "combo_names": " + ".join(combo_names),
                "events_together": len(tog_df),
                "net_rtg_together": round(tog_net, 1),
                "net_rtg_apart": round(apt_net, 1),
                "synergy": round(tog_net - apt_net, 1),
            })

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values("synergy", ascending=False).reset_index(drop=True)
    return result_df


def _period_marker_to_game_seconds(period: int, markertime_secs: float) -> float:
    """Convert period + countdown seconds remaining to elapsed game seconds."""
    period_length = 600.0  # 10 minutes per period in Euroleague
    period_start = (period - 1) * period_length
    elapsed_in_period = period_length - markertime_secs
    return period_start + elapsed_in_period


def compute_player_stints(
    pbp_df: pd.DataFrame,
    boxscore_df: pd.DataFrame,
    team_code: str,
) -> pd.DataFrame:
    """
    Parse substitution IN/OUT events from PBP to build player rotation stints.

    Each stint represents a continuous stretch on court for a single player.
    Edge cases handled:
      - Starters have no explicit IN event at period start
      - Players still on court at period end have no explicit OUT event
      - Overtime periods (5+) supported

    Stint +/- is computed from POINTS_A / POINTS_B deltas while the player
    was on court, oriented so positive = good for the player's team.

    Parameters
    ----------
    pbp_df : pd.DataFrame
        Raw play-by-play with PLAYTYPE, PLAYER_ID, PLAYER, CODETEAM,
        PERIOD, MARKERTIME, POINTS_A, POINTS_B, TRUE_NUMBEROFPLAY.
    boxscore_df : pd.DataFrame
        Boxscore with Player_ID, Player, Team, Home, IsStarter.
    team_code : str
        Team code to extract stints for.

    Returns
    -------
    pd.DataFrame
        Columns: player_name, start_sec, end_sec, duration_sec,
                 plus_minus, period_start, period_end.
    """
    if pbp_df.empty or boxscore_df.empty:
        return pd.DataFrame(columns=[
            "player_name", "start_sec", "end_sec", "duration_sec",
            "plus_minus", "period_start", "period_end",
        ])

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce").fillna(0).astype(int)
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce").fillna(0).astype(int)
    df["_marker_secs"] = df["MARKERTIME"].apply(_markertime_to_seconds)
    df["PERIOD"] = pd.to_numeric(df["PERIOD"], errors="coerce").fillna(1).astype(int)

    # Determine if team is home or away for +/- orientation
    home_players = boxscore_df[boxscore_df["Home"] == 1]
    away_players = boxscore_df[boxscore_df["Home"] == 0]
    home_team = home_players["Team"].iloc[0] if not home_players.empty else None
    is_home = (team_code == home_team)

    # Build player_id → formatted name map
    name_map = {}
    team_box = boxscore_df[boxscore_df["Team"] == team_code]
    for _, r in team_box.iterrows():
        pid = str(r["Player_ID"]).strip()
        name_map[pid] = format_player_name(str(r.get("Player", pid)))

    # Identify starters for the selected team
    starters = set(
        team_box[team_box["IsStarter"] == 1.0]["Player_ID"]
        .astype(str).str.strip().tolist()
    )

    # Walk through PBP, tracking on-court set and open stint info per player
    on_court = set()  # player IDs currently on court
    # open_stints[pid] = {start_sec, start_score_a, start_score_b, period_start}
    open_stints: Dict[str, dict] = {}
    stints: List[dict] = []

    periods = sorted(df["PERIOD"].unique())

    for period in periods:
        period_df = df[df["PERIOD"] == period].copy()
        if period_df.empty:
            continue

        period_length = 600.0  # 10 min per period (regular + OT in Euroleague)
        period_start_sec = (period - 1) * period_length
        period_end_sec = period * period_length

        # At period start, close all open stints from previous period
        for pid in list(open_stints.keys()):
            s = open_stints.pop(pid)
            prev_period_end = period_start_sec
            stints.append({
                "player_id": pid,
                "start_sec": s["start_sec"],
                "end_sec": prev_period_end,
                "start_score_a": s["start_score_a"],
                "end_score_a": s.get("last_score_a", s["start_score_a"]),
                "start_score_b": s["start_score_b"],
                "end_score_b": s.get("last_score_b", s["start_score_b"]),
                "period_start": s["period_start"],
                "period_end": period - 1,
            })
        on_court.clear()

        # Determine who starts this period
        # For period 1, use boxscore starters; for later periods, detect from
        # first actions before any IN/OUT for the team in this period
        if period == 1:
            period_starters = set(starters)
        else:
            period_starters = set()
            # Look at team actions before first sub event in this period
            for _, row in period_df.iterrows():
                pt = str(row.get("PLAYTYPE", "")).strip()
                ct = str(row.get("CODETEAM", "")).strip()
                pid = str(row.get("PLAYER_ID", "")).strip()
                if ct == team_code and pt in ("IN", "OUT"):
                    break
                if ct == team_code and pid and pid in name_map:
                    period_starters.add(pid)

            # If we couldn't detect 5, fall back: look at who was on court
            # at end of last period by checking all team actions in this period
            if len(period_starters) < 5:
                for _, row in period_df.iterrows():
                    pt = str(row.get("PLAYTYPE", "")).strip()
                    ct = str(row.get("CODETEAM", "")).strip()
                    pid = str(row.get("PLAYER_ID", "")).strip()
                    if ct == team_code and pt == "OUT" and pid:
                        period_starters.add(pid)
                    if ct == team_code and pt == "IN" and pid:
                        period_starters.discard(pid)

        # Get first score in this period for baseline
        first_valid = period_df[(period_df["POINTS_A"] > 0) | (period_df["POINTS_B"] > 0)]
        if not first_valid.empty:
            base_a = int(first_valid.iloc[0]["POINTS_A"])
            base_b = int(first_valid.iloc[0]["POINTS_B"])
            # Subtract the scoring event itself if it just happened
            first_pt = str(first_valid.iloc[0].get("PLAYTYPE", ""))
            if first_pt in ("2FGM", "3FGM", "FTM"):
                score_val = {"2FGM": 2, "3FGM": 3, "FTM": 1}.get(first_pt, 0)
                first_ct = str(first_valid.iloc[0].get("CODETEAM", ""))
                if first_ct == home_team:
                    base_a = max(0, base_a - score_val)
                else:
                    base_b = max(0, base_b - score_val)
        else:
            base_a, base_b = 0, 0

        # Open stints for period starters
        on_court = set(period_starters)
        for pid in period_starters:
            open_stints[pid] = {
                "start_sec": period_start_sec,
                "start_score_a": base_a,
                "start_score_b": base_b,
                "period_start": period,
            }

        # Track the running score for this period
        running_a, running_b = base_a, base_b

        # Walk through events
        for _, row in period_df.iterrows():
            pt = str(row.get("PLAYTYPE", "")).strip()
            ct = str(row.get("CODETEAM", "")).strip()
            pid = str(row.get("PLAYER_ID", "")).strip()
            marker_secs = row["_marker_secs"]
            game_sec = _period_marker_to_game_seconds(period, marker_secs)

            # Update running score
            pa = int(row["POINTS_A"])
            pb = int(row["POINTS_B"])
            if pa > 0 or pb > 0:
                running_a = pa
                running_b = pb

            # Update last known score for all open stints
            for open_pid in open_stints:
                open_stints[open_pid]["last_score_a"] = running_a
                open_stints[open_pid]["last_score_b"] = running_b

            if ct != team_code:
                continue

            if pt == "OUT" and pid:
                if pid in open_stints:
                    s = open_stints.pop(pid)
                    stints.append({
                        "player_id": pid,
                        "start_sec": s["start_sec"],
                        "end_sec": game_sec,
                        "start_score_a": s["start_score_a"],
                        "end_score_a": running_a,
                        "start_score_b": s["start_score_b"],
                        "end_score_b": running_b,
                        "period_start": s["period_start"],
                        "period_end": period,
                    })
                on_court.discard(pid)

            elif pt == "IN" and pid:
                on_court.add(pid)
                open_stints[pid] = {
                    "start_sec": game_sec,
                    "start_score_a": running_a,
                    "start_score_b": running_b,
                    "period_start": period,
                }

    # Close any remaining open stints at end of last period
    if periods:
        last_period = max(periods)
        final_sec = last_period * 600.0
        for pid in list(open_stints.keys()):
            s = open_stints.pop(pid)
            stints.append({
                "player_id": pid,
                "start_sec": s["start_sec"],
                "end_sec": final_sec,
                "start_score_a": s["start_score_a"],
                "end_score_a": s.get("last_score_a", s["start_score_a"]),
                "start_score_b": s["start_score_b"],
                "end_score_b": s.get("last_score_b", s["start_score_b"]),
                "period_start": s["period_start"],
                "period_end": last_period,
            })

    if not stints:
        return pd.DataFrame(columns=[
            "player_name", "start_sec", "end_sec", "duration_sec",
            "plus_minus", "period_start", "period_end",
        ])

    result = pd.DataFrame(stints)

    # Compute +/- oriented for the selected team
    if is_home:
        result["plus_minus"] = (
            (result["end_score_a"] - result["start_score_a"])
            - (result["end_score_b"] - result["start_score_b"])
        )
    else:
        result["plus_minus"] = (
            (result["end_score_b"] - result["start_score_b"])
            - (result["end_score_a"] - result["start_score_a"])
        )

    result["player_name"] = result["player_id"].map(name_map).fillna(result["player_id"])
    result["duration_sec"] = result["end_sec"] - result["start_sec"]

    # Drop zero-duration stints
    result = result[result["duration_sec"] > 0].copy()

    return result[[
        "player_name", "start_sec", "end_sec", "duration_sec",
        "plus_minus", "period_start", "period_end",
    ]].sort_values(["start_sec", "player_name"]).reset_index(drop=True)
