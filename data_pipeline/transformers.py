"""
transformers.py — Advanced Statistics & PBP Analytics Engine
=============================================================
Takes the raw DataFrames returned by euroleague-api and computes:

BASE STATS (Phase 1):
  1. Estimated Possessions
  2. True Shooting Percentage (TS%)
  3. Offensive / Defensive Rating (per 100 possessions)

ADVANCED PBP ANALYTICS (Phase 2):
  4. Lineup Tracking — 5-man on-court reconstruction via IN/OUT events
  5. Lineup Net Rating — per 5-man combination
  6. Duo/Trio Synergy — 2/3-player combo performance
  7. Clutch Factor — last 5 min, ≤5 pt differential stats
  8. Run-Stopping Ability — breaking opponent 8-0+ runs
  9. Foul Trouble Impact — ORtg/DRtg when star has 2+ fouls early
  10. Assist Network — passer→scorer relationship matrix

CUSTOM METRICS (Phase 2):
  11. True Usage Rate (tUSG%) — includes AST + fouls drawn
  12. Stop Rate — defensive possession ending %
  13. Shot Quality — expected points per zone

All formulas follow standard basketball analytics conventions.
"""

import logging
from itertools import combinations
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ========================================================================
# COLUMN MAPPING (boxscore PascalCase → internal snake_case)
# ========================================================================
COL_MAP = {
    "Player":               "player_name",
    "Player_ID":            "player_id",
    "Team":                 "team_code",
    "Home":                 "is_home",
    "Dorsal":               "dorsal",
    "Minutes":              "minutes_str",
    "Points":               "points",
    "FieldGoalsMade2":      "fgm2",
    "FieldGoalsAttempted2": "fga2",
    "FieldGoalsMade3":      "fgm3",
    "FieldGoalsAttempted3": "fga3",
    "FreeThrowsMade":       "ftm",
    "FreeThrowsAttempted":  "fta",
    "OffensiveRebounds":    "off_rebounds",
    "DefensiveRebounds":    "def_rebounds",
    "TotalRebounds":        "total_rebounds",
    "Assistances":          "assists",
    "Steals":               "steals",
    "Turnovers":            "turnovers",
    "BlocksFavour":         "blocks_favour",
    "BlocksAgainst":        "blocks_against",
    "FoulsCommited":        "fouls_committed",
    "FoulsReceived":        "fouls_received",
    "Plusminus":            "plus_minus",
}


# ========================================================================
# UTILITY HELPERS
# ========================================================================

def parse_minutes(minutes_str: pd.Series) -> pd.Series:
    """Convert 'MM:SS' strings to decimal minutes.  'DNP'/empty → 0.0."""
    def _parse(val):
        if pd.isna(val) or not isinstance(val, str) or ":" not in val:
            return 0.0
        try:
            m, s = val.strip().split(":")
            return int(m) + int(s) / 60.0
        except (ValueError, IndexError):
            return 0.0
    return minutes_str.apply(_parse)


def _markertime_to_seconds(mt: str) -> float:
    """Convert MARKERTIME 'MM:SS' countdown to seconds remaining in period."""
    if pd.isna(mt) or not isinstance(mt, str) or ":" not in mt:
        return 0.0
    try:
        m, s = mt.strip().split(":")
        return int(m) * 60 + int(s)
    except (ValueError, IndexError):
        return 0.0


# ========================================================================
# CLUTCH TIME ISOLATOR
# ========================================================================

def filter_clutch_time(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter play-by-play data to only clutch-time rows.

    Clutch time = Period >= 4 (Q4 or OT), last 5 minutes of the period
    (MARKERTIME <= 05:00), and score differential within 5 points.
    """
    if pbp_df.empty:
        return pbp_df

    df = pbp_df.copy()
    df["_marker_secs"] = df["MARKERTIME"].apply(_markertime_to_seconds)
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce").fillna(0)
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce").fillna(0)

    clutch_mask = (
        (df["PERIOD"] >= 4)
        & (df["_marker_secs"] <= 300)
        & (abs(df["POINTS_A"] - df["POINTS_B"]) <= 5)
    )

    result = df[clutch_mask].drop(columns=["_marker_secs"], errors="ignore")
    return result.reset_index(drop=True)


def filter_clutch_shots(shots_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter shot data to only clutch-time shots.

    Shot data uses MINUTE (game minute, 1-based ascending) and running
    scores POINTS_A / POINTS_B.  Clutch = last 5 min of Q4 (minute 36-40)
    or any OT (minute > 40), with score differential <= 5.
    """
    if shots_df.empty:
        return shots_df

    df = shots_df.copy()
    df["POINTS_A"] = pd.to_numeric(df.get("POINTS_A", 0), errors="coerce").fillna(0)
    df["POINTS_B"] = pd.to_numeric(df.get("POINTS_B", 0), errors="coerce").fillna(0)
    minute_col = "MINUTE" if "MINUTE" in df.columns else "minute"
    df["_minute"] = pd.to_numeric(df[minute_col], errors="coerce").fillna(0)

    clutch_mask = (
        (df["_minute"] >= 36)
        & (abs(df["POINTS_A"] - df["POINTS_B"]) <= 5)
    )

    result = df[clutch_mask].drop(columns=["_minute"], errors="ignore")
    return result.reset_index(drop=True)


def build_clutch_boxscore(
    clutch_pbp: pd.DataFrame,
    original_boxscore: pd.DataFrame,
) -> pd.DataFrame:
    """
    Synthesize a boxscore DataFrame from clutch-filtered PBP events.

    Aggregates PLAYTYPE counts per player into the same column format
    that compute_advanced_stats expects (PascalCase boxscore columns).
    Player metadata (Team, Home, Season, Gamecode) is carried over from
    the original boxscore.
    """
    if clutch_pbp.empty or original_boxscore.empty:
        return pd.DataFrame()

    SCORE_MAP = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    meta_cols = {}
    if "Player_ID" in original_boxscore.columns:
        for _, r in original_boxscore.iterrows():
            pid = str(r["Player_ID"]).strip()
            meta_cols[pid] = {
                "Team": r.get("Team", ""),
                "Home": r.get("Home", 0),
                "Season": r.get("Season", ""),
                "Gamecode": r.get("Gamecode", ""),
                "Player": r.get("Player", pid),
                "IsStarter": r.get("IsStarter", 0),
                "IsPlaying": r.get("IsPlaying", 0),
            }

    rows = []
    for player_id, grp in clutch_pbp.groupby("PLAYER_ID"):
        pid = str(player_id).strip()
        if not pid:
            continue
        pt_counts = grp["PLAYTYPE"].value_counts()

        fgm2 = pt_counts.get("2FGM", 0)
        fga2 = fgm2 + pt_counts.get("2FGA", 0)
        fgm3 = pt_counts.get("3FGM", 0)
        fga3 = fgm3 + pt_counts.get("3FGA", 0)
        ftm = pt_counts.get("FTM", 0)
        fta = ftm + pt_counts.get("FTA", 0)
        off_reb = pt_counts.get("O", 0)
        def_reb = pt_counts.get("D", 0)
        ast = pt_counts.get("AS", 0)
        stl = pt_counts.get("ST", 0)
        tov = pt_counts.get("TO", 0)
        blk = pt_counts.get("FV", 0)
        fouls = sum(pt_counts.get(f, 0) for f in ("CM", "CMU", "CMT"))
        fouls_drawn = pt_counts.get("RV", 0)
        pts = fgm2 * 2 + fgm3 * 3 + ftm

        meta = meta_cols.get(pid, {})
        team = meta.get("Team", grp["CODETEAM"].mode().iloc[0] if not grp["CODETEAM"].mode().empty else "")
        player_name = meta.get("Player", grp["PLAYER"].mode().iloc[0] if "PLAYER" in grp.columns and not grp["PLAYER"].mode().empty else pid)

        # Estimate clutch minutes from MARKERTIME span
        secs = grp["MARKERTIME"].apply(_markertime_to_seconds)
        clutch_mins = max((secs.max() - secs.min()) / 60.0, 0.5) if len(secs) > 1 else 0.5
        mins_str = f"{int(clutch_mins)}:{int((clutch_mins % 1) * 60):02d}"

        rows.append({
            "Player": player_name,
            "Player_ID": pid,
            "Team": team,
            "Home": meta.get("Home", 0),
            "IsStarter": meta.get("IsStarter", 0),
            "IsPlaying": meta.get("IsPlaying", 0),
            "Season": meta.get("Season", ""),
            "Gamecode": meta.get("Gamecode", ""),
            "Minutes": mins_str,
            "Points": pts,
            "FieldGoalsMade2": fgm2,
            "FieldGoalsAttempted2": fga2,
            "FieldGoalsMade3": fgm3,
            "FieldGoalsAttempted3": fga3,
            "FreeThrowsMade": ftm,
            "FreeThrowsAttempted": fta,
            "OffensiveRebounds": off_reb,
            "DefensiveRebounds": def_reb,
            "TotalRebounds": off_reb + def_reb,
            "Assistances": ast,
            "Steals": stl,
            "Turnovers": tov,
            "BlocksFavour": blk,
            "BlocksAgainst": 0,
            "FoulsCommited": fouls,
            "FoulsReceived": fouls_drawn,
            "Plusminus": 0,
        })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def format_player_name(raw_name: str) -> str:
    """
    Format raw API name ('LESSORT, MATHIAS') to 'Surname F.' format ('Lessort M.').
    """
    if pd.isna(raw_name) or not isinstance(raw_name, str):
        return str(raw_name) if raw_name else ""
    parts = raw_name.split(",")
    if len(parts) >= 2:
        last = parts[0].strip().title()
        first = parts[1].strip()
        first_initial = first[0].upper() + "." if first else ""
        return f"{last} {first_initial}".strip()
    return raw_name.strip().title()


# ========================================================================
# PHASE 1: BASE ADVANCED STATS
# ========================================================================

def compute_advanced_stats(boxscore_df: pd.DataFrame) -> pd.DataFrame:
    """
    Master function: takes a raw boxscore DataFrame and returns a clean
    DataFrame with advanced stats computed.

    Returns one row per player per game with raw + advanced stats.
    """
    if boxscore_df.empty:
        logger.warning("Empty boxscore — returning empty DataFrame")
        return pd.DataFrame()

    df = boxscore_df.copy()

    # 1. Rename columns
    rename_cols = {k: v for k, v in COL_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_cols)

    # 2. Parse minutes
    if "minutes_str" in df.columns:
        df["minutes"] = parse_minutes(df["minutes_str"])
    else:
        df["minutes"] = 0.0

    # 3. Cast numeric columns
    numeric_cols = [
        "points", "fgm2", "fga2", "fgm3", "fga3", "ftm", "fta",
        "off_rebounds", "def_rebounds", "total_rebounds",
        "assists", "steals", "turnovers", "blocks_favour", "blocks_against",
        "fouls_committed", "fouls_received", "plus_minus",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 4. Compute team possessions by aggregation
    df["fga"] = df["fga2"].fillna(0) + df["fga3"].fillna(0)
    team_poss = _compute_team_possessions(df)
    df = df.merge(team_poss, on=["Season", "Gamecode", "team_code"], how="left")

    # 5. Advanced stats
    df["possessions"] = _calc_possessions(df)
    df["ts_pct"] = _calc_true_shooting(df)
    df["off_rating"], df["def_rating"] = _calc_ratings(df)

    # --- Phase 2 custom metrics ---
    df["true_usg_pct"] = _calc_true_usage_rate(df)
    df["stop_rate"] = _calc_stop_rate(df)

    # Select output
    output_cols = [
        "Season", "Gamecode", "player_id", "player_name", "team_code", "is_home",
        "minutes", "points",
        "fgm2", "fga2", "fgm3", "fga3", "ftm", "fta",
        "off_rebounds", "def_rebounds", "total_rebounds",
        "assists", "steals", "turnovers",
        "blocks_favour", "blocks_against",
        "fouls_committed", "fouls_received", "plus_minus",
        "possessions", "ts_pct", "off_rating", "def_rating",
        "true_usg_pct", "stop_rate",
    ]
    output_cols = [c for c in output_cols if c in df.columns]
    return df[output_cols].copy()


def _compute_team_possessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Team Possessions = FGA + 0.44 * FTA − ORB + TOV

    0.44 multiplier accounts for "and-one" / tech FTs / 3-shot fouls that
    don't consume a full possession.
    """
    team_agg = (
        df.groupby(["Season", "Gamecode", "team_code"])
        .agg(
            team_fga=("fga", "sum"),
            team_fta=("fta", "sum"),
            team_orb=("off_rebounds", "sum"),
            team_tov=("turnovers", "sum"),
            team_points=("points", "sum"),
            team_minutes=("minutes", "sum"),
            team_assists=("assists", "sum"),
            team_fouls_received=("fouls_received", "sum"),
        )
        .reset_index()
    )
    team_agg["team_poss"] = (
        team_agg["team_fga"] + 0.44 * team_agg["team_fta"]
        - team_agg["team_orb"] + team_agg["team_tov"]
    )
    return team_agg[[
        "Season", "Gamecode", "team_code",
        "team_poss", "team_points", "team_minutes",
        "team_assists", "team_fouls_received",
    ]]


def _calc_possessions(df: pd.DataFrame) -> pd.Series:
    """Player Poss = Team Poss × (Player Minutes / Team Minutes)."""
    if "team_poss" in df.columns and "team_minutes" in df.columns:
        share = np.where(df["team_minutes"] > 0, df["minutes"] / df["team_minutes"], 0.0)
        return pd.Series(df["team_poss"].values * share, index=df.index)
    return df["fga"].fillna(0) + 0.44 * df["fta"].fillna(0) - df["off_rebounds"].fillna(0) + df["turnovers"].fillna(0)


def _calc_true_shooting(df: pd.DataFrame) -> pd.Series:
    """TS% = PTS / (2 × (FGA + 0.44 × FTA)).  Measures multi-shot-type efficiency."""
    tsa = df["fga"].fillna(0) + 0.44 * df["fta"].fillna(0)
    return pd.Series(np.where(tsa > 0, df["points"].fillna(0) / (2.0 * tsa), np.nan), index=df.index)


def _calc_ratings(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    ORtg = (Player Points / Player Possessions) × 100
    DRtg = (Opponent Points / Team Possessions) × 100  (team-level assignment)
    """
    poss = df.get("possessions", pd.Series(dtype=float))
    ortg = np.where(poss > 0, (df["points"].fillna(0) / poss) * 100, np.nan)
    drtg = pd.Series(np.nan, index=df.index)

    if "team_poss" in df.columns:
        for (season, gamecode), grp in df.groupby(["Season", "Gamecode"]):
            teams = grp["team_code"].unique()
            if len(teams) < 2:
                continue
            team_pts = {tc: grp.loc[grp["team_code"] == tc, "team_points"].iloc[0] for tc in teams}
            for tc in teams:
                opp = [t for t in teams if t != tc][0]
                tp = grp.loc[grp["team_code"] == tc, "team_poss"].iloc[0]
                if tp > 0:
                    mask = (df["Season"] == season) & (df["Gamecode"] == gamecode) & (df["team_code"] == tc)
                    drtg.loc[mask] = (team_pts[opp] / tp) * 100

    return pd.Series(ortg, index=df.index), drtg


# ========================================================================
# PHASE 2 — CUSTOM METRICS
# ========================================================================

def _calc_true_usage_rate(df: pd.DataFrame) -> pd.Series:
    """
    True Usage Rate (tUSG%) — custom extended formula.

    Traditional USG% = (FGA + 0.44*FTA + TOV) / TeamPoss * (TeamMin/5 / PlayerMin)
    This only captures "possessions used" (shots, turnovers, FT trips).

    Our tUSG% ADDS productive involvement:
      tUSG% = (FGA + 0.44*FTA + TOV + AST + Fouls_Drawn) /
              (Team_Poss × (Player_Min / Team_Min))

    Why the additions:
      - AST: An assist is direct possession creation for a teammate.
      - Fouls_Drawn (FoulsReceived): Drawing a foul creates a scoring
        opportunity — it's an undervalued form of usage.

    This gives a fuller picture of a player's involvement in the offence.
    """
    player_involvement = (
        df["fga"].fillna(0)
        + 0.44 * df["fta"].fillna(0)
        + df["turnovers"].fillna(0)
        + df["assists"].fillna(0)
        + df["fouls_received"].fillna(0)
    )

    # Denominator: team possessions weighted by player's time share
    if "team_poss" in df.columns and "team_minutes" in df.columns:
        team_poss_per_player = np.where(
            df["team_minutes"] > 0,
            df["team_poss"] * (df["minutes"] / df["team_minutes"]),
            0,
        )
        return pd.Series(
            np.where(team_poss_per_player > 0, player_involvement / team_poss_per_player, np.nan),
            index=df.index,
        )
    return pd.Series(np.nan, index=df.index)


def _calc_stop_rate(df: pd.DataFrame) -> pd.Series:
    """
    Stop Rate — percentage of defensive possessions a player actively ends.

    Formula:
        Stop% = (Steals + Blocks + Defensive Rebounds) / Defensive Possessions

    Defensive possessions ≈ team possessions (roughly equal for both teams).
    We use the player's share of team possessions as the denominator.

    A high Stop Rate indicates an impactful defender who forces turnovers,
    blocks shots, or secures defensive boards.
    """
    stops = (
        df["steals"].fillna(0)
        + df["blocks_favour"].fillna(0)
        + df["def_rebounds"].fillna(0)
    )

    if "team_poss" in df.columns and "team_minutes" in df.columns:
        # Use opponent's possessions ≈ team's possessions
        def_poss = np.where(
            df["team_minutes"] > 0,
            df["team_poss"] * (df["minutes"] / df["team_minutes"]),
            0,
        )
        return pd.Series(np.where(def_poss > 0, stops / def_poss, np.nan), index=df.index)
    return pd.Series(np.nan, index=df.index)


# ========================================================================
# PHASE 2 — PBP LINEUP TRACKING
# ========================================================================

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


# ========================================================================
# PHASE 4 — SEASON AGGREGATIONS
# ========================================================================

def compute_season_player_stats(advanced_df: pd.DataFrame, team_code: str) -> pd.DataFrame:
    """
    Aggregate per-game advanced stats into mathematically accurate season-long metrics.
    Sums raw attempts/possessions before applying efficiency algorithms.
    """
    if advanced_df.empty:
        return pd.DataFrame()

    team_df = advanced_df[advanced_df["team_code"] == team_code].copy()
    if team_df.empty:
        return pd.DataFrame()

    # Columns required for advanced formulas + basic tracking
    sum_cols = [
        "minutes", "points", "fgm2", "fga2", "fgm3", "fga3", "ftm", "fta",
        "off_rebounds", "def_rebounds", "total_rebounds",
        "assists", "steals", "turnovers", "blocks_favour", "blocks_against",
        "fouls_committed", "fouls_received", "possessions"
    ]

    # Fill NaNs with 0 before aggregating
    for col in sum_cols:
        if col in team_df.columns:
            team_df[col] = pd.to_numeric(team_df[col], errors="coerce").fillna(0)

    # Convert minutes appropriately if it's not already decimal
    agg_dict = {col: "sum" for col in sum_cols if col in team_df.columns}
    
    # Keep the first name found for the player ID
    agg_dict["player_name"] = "first"

    season_df = team_df.groupby("player_id").agg(agg_dict).reset_index()

    # Prevent extremely low-minute players from cluttering
    season_df = season_df[season_df["minutes"] > 0].copy()

    # Recalculate TS%
    tsa = (season_df["fga2"] + season_df["fga3"]) + 0.44 * season_df["fta"]
    season_df["ts_pct"] = np.where(tsa > 0, season_df["points"] / (2.0 * tsa), np.nan)

    # Recalculate tUSG%
    player_inv = (
        (season_df["fga2"] + season_df["fga3"]) + 0.44 * season_df["fta"] +
        season_df["turnovers"] + season_df["assists"] + season_df["fouls_received"]
    )
    season_df["true_usg_pct"] = np.where(season_df["possessions"] > 0, player_inv / season_df["possessions"], np.nan)

    # Recalculate Stop Rate
    stops = season_df["steals"] + season_df["blocks_favour"] + season_df["def_rebounds"]
    season_df["stop_rate"] = np.where(season_df["possessions"] > 0, stops / season_df["possessions"], np.nan)
    
    # Recalculate ORtg
    season_df["off_rating"] = np.where(season_df["possessions"] > 0, (season_df["points"] / season_df["possessions"]) * 100, np.nan)

    return season_df.sort_values("minutes", ascending=False).reset_index(drop=True)


# ========================================================================
# PHASE 2 — LINEUP STATS & SYNERGY
# ========================================================================

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


# ========================================================================
# PHASE 2 — CLUTCH FACTOR
# ========================================================================

def compute_clutch_stats(
    pbp_df: pd.DataFrame,
    boxscore_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute player stats in "clutch" situations.

    Definition of clutch:
      - Period >= 4 (4th quarter and overtime)
      - MARKERTIME <= 05:00 (last 5 minutes of the period)
      - |POINTS_A − POINTS_B| <= 5 (score differential within 5 points)

    For each player active in clutch situations, compute:
      - Clutch TS%
      - Clutch Turnovers
      - Clutch Usage Rate (standard FGA+0.44*FTA+TOV formula)
      - Total clutch actions
    """
    if pbp_df.empty:
        return pd.DataFrame()

    df = pbp_df.copy()

    # Parse MARKERTIME to seconds for filtering
    df["_marker_secs"] = df["MARKERTIME"].apply(_markertime_to_seconds)

    # Ensure numeric score columns
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce").fillna(0)
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce").fillna(0)

    # --- CLUTCH FILTER ---
    # Period >= 4 (4th quarter + OT), last 5 minutes (≤ 300 seconds on clock),
    # score differential ≤ 5 points
    clutch_mask = (
        (df["PERIOD"] >= 4)
        & (df["_marker_secs"] <= 300)  # 5:00 or less remaining
        & (abs(df["POINTS_A"] - df["POINTS_B"]) <= 5)
    )
    clutch_df = df[clutch_mask].copy()

    if clutch_df.empty:
        logger.info("No clutch situations found in this game")
        return pd.DataFrame()

    # Build player_id → name map
    name_map = {}
    if "Player_ID" in boxscore_df.columns:
        for _, r in boxscore_df.iterrows():
            name_map[str(r["Player_ID"]).strip()] = str(r["Player"]).strip()

    # Aggregate per player in clutch
    score_types = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    fga_types = {"2FGM", "2FGA", "3FGM", "3FGA"}
    fta_types = {"FTM", "FTA"}

    results = []
    for player_id, pgrp in clutch_df.groupby("PLAYER_ID"):
        pid = str(player_id).strip()
        if not pid:
            continue

        pts = sum(score_types.get(pt, 0) for pt in pgrp["PLAYTYPE"])
        fga = len(pgrp[pgrp["PLAYTYPE"].isin(fga_types)])
        fta = len(pgrp[pgrp["PLAYTYPE"].isin(fta_types)])
        tov = len(pgrp[pgrp["PLAYTYPE"] == "TO"])

        # Clutch TS%
        tsa = fga + 0.44 * fta
        c_ts = pts / (2 * tsa) if tsa > 0 else None

        # Clutch usage — proportion of clutch possessions used
        total_clutch_fga = len(clutch_df[clutch_df["PLAYTYPE"].isin(fga_types)])
        total_clutch_fta = len(clutch_df[clutch_df["PLAYTYPE"].isin(fta_types)])
        total_clutch_tov = len(clutch_df[clutch_df["PLAYTYPE"] == "TO"])
        total_clutch_poss = total_clutch_fga + 0.44 * total_clutch_fta + total_clutch_tov
        player_poss = fga + 0.44 * fta + tov
        c_usg = player_poss / total_clutch_poss if total_clutch_poss > 0 else None

        results.append({
            "player_id": pid,
            "player_name": name_map.get(pid, pid),
            "team": pgrp["CODETEAM"].mode().iloc[0] if len(pgrp["CODETEAM"].mode()) > 0 else "",
            "clutch_actions": len(pgrp),
            "clutch_points": pts,
            "clutch_fga": fga,
            "clutch_fta": fta,
            "clutch_turnovers": tov,
            "clutch_ts_pct": round(c_ts, 3) if c_ts is not None else None,
            "clutch_usage": round(c_usg, 3) if c_usg is not None else None,
        })

    return pd.DataFrame(results).sort_values("clutch_actions", ascending=False).reset_index(drop=True)


# ========================================================================
# PHASE 2 — RUN-STOPPING ABILITY (MOMENTUM)
# ========================================================================

def detect_runs_and_stoppers(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify scoring runs of 8+ unanswered points and the player/play that
    breaks each run.

    Algorithm:
      1. Walk through PBP in order, tracking POINTS_A and POINTS_B.
      2. When one team scores consecutively (the other team's score stays
         static), accumulate a "run" counter.
      3. When the run reaches ≥ 8 unanswered points and the other team
         finally scores, record the "run stopper" — the player and play type
         that broke the run.

    Returns a DataFrame of run-stopping events with:
      - run_team: the team that was on the scoring run
      - run_points: total unanswered points in the run
      - stopper_player, stopper_player_id, stopper_team
      - stopper_playtype: the play that broke it (2FGM, 3FGM, FTM)
      - period, markertime
    """
    if pbp_df.empty:
        return pd.DataFrame()

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce").fillna(0)
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce").fillna(0)

    scoring_types = {"2FGM", "3FGM", "FTM"}

    results = []
    # Track the score at the start of the current "drought" for each team
    prev_a = 0
    prev_b = 0
    run_start_a = 0  # Score of team A when team B last scored
    run_start_b = 0  # Score of team B when team A last scored
    running_team = None  # Which team is currently on a run

    for _, row in df.iterrows():
        cur_a = int(row["POINTS_A"])
        cur_b = int(row["POINTS_B"])

        if cur_a == prev_a and cur_b == prev_b:
            # No scoring change, skip
            continue

        # Team A scored
        if cur_a > prev_a and cur_b == prev_b:
            if running_team != "A":
                # Reset: team A starts a new run
                running_team = "A"
                run_start_a = prev_a
                run_start_b = cur_b

            prev_a = cur_a

        # Team B scored
        elif cur_b > prev_b and cur_a == prev_a:
            if running_team != "B":
                running_team = "B"
                run_start_b = prev_b
                run_start_a = cur_a

            prev_b = cur_b

        # Both changed (rare, e.g. correction) — reset
        else:
            running_team = None
            prev_a = cur_a
            prev_b = cur_b
            continue

        # Check if a run was just broken
        if running_team == "A" and cur_b > prev_b:
            run_pts = prev_a - run_start_a
            if run_pts >= 8 and row["PLAYTYPE"] in scoring_types:
                results.append({
                    "run_team": row.get("home_team", "Team A"),
                    "run_points": run_pts,
                    "stopper_player": row["PLAYER"],
                    "stopper_player_id": row["PLAYER_ID"],
                    "stopper_team": row["CODETEAM"],
                    "stopper_playtype": row["PLAYTYPE"],
                    "period": row["PERIOD"],
                    "markertime": row["MARKERTIME"],
                })
            running_team = "B"
            run_start_b = prev_b
            prev_b = cur_b

        elif running_team == "B" and cur_a > prev_a:
            run_pts = prev_b - run_start_b
            if run_pts >= 8 and row["PLAYTYPE"] in scoring_types:
                results.append({
                    "run_team": row.get("away_team", "Team B"),
                    "run_points": run_pts,
                    "stopper_player": row["PLAYER"],
                    "stopper_player_id": row["PLAYER_ID"],
                    "stopper_team": row["CODETEAM"],
                    "stopper_playtype": row["PLAYTYPE"],
                    "period": row["PERIOD"],
                    "markertime": row["MARKERTIME"],
                })
            running_team = "A"
            run_start_a = prev_a
            prev_a = cur_a

    return pd.DataFrame(results)


# ========================================================================
# PHASE 2 — FOUL TROUBLE IMPACT
# ========================================================================

def foul_trouble_impact(
    pbp_df: pd.DataFrame,
    boxscore_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Measure how a team's performance changes when its highest-usage player
    picks up 2+ fouls in the first half.

    Steps:
      1. Identify the highest-usage player per team (by FGA + FTA from boxscore)
      2. Find when that player's 2nd foul occurs in the PBP (Period ≤ 2)
      3. Split game into "before foul trouble" and "after foul trouble"
      4. Compare team ORtg/DRtg in both periods

    Returns one row per team with: team, star_player, foul_event_period,
    ortg_before, ortg_after, drtg_before, drtg_after, impact.
    """
    if pbp_df.empty or boxscore_df.empty:
        return pd.DataFrame()

    score_map = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    poss_events = {"2FGM", "2FGA", "3FGM", "3FGA", "TO", "D"}

    results = []

    for team in boxscore_df["Team"].unique():
        team_bx = boxscore_df[boxscore_df["Team"] == team].copy()

        # Highest-usage player: most FGA + FTA
        team_bx["_usage"] = (
            pd.to_numeric(team_bx.get("FieldGoalsAttempted2", 0), errors="coerce").fillna(0)
            + pd.to_numeric(team_bx.get("FieldGoalsAttempted3", 0), errors="coerce").fillna(0)
            + pd.to_numeric(team_bx.get("FreeThrowsAttempted", 0), errors="coerce").fillna(0)
        )
        star = team_bx.sort_values("_usage", ascending=False).iloc[0]
        star_pid = str(star["Player_ID"]).strip()
        star_name = str(star["Player"]).strip()

        # Find 2nd personal foul (CM = committed foul) by this player in 1st half
        player_fouls = pbp_df[
            (pbp_df["PLAYER_ID"].astype(str).str.strip() == star_pid)
            & (pbp_df["PLAYTYPE"].isin(["CM", "CMU", "CMT"]))  # foul types
            & (pbp_df["PERIOD"] <= 2)  # first half
        ].sort_values("TRUE_NUMBEROFPLAY")

        if len(player_fouls) < 2:
            continue  # No foul trouble in first half

        # The moment of the 2nd foul
        second_foul_play = player_fouls.iloc[1]["TRUE_NUMBEROFPLAY"]

        # Split PBP
        before = pbp_df[pbp_df["TRUE_NUMBEROFPLAY"] < second_foul_play]
        after = pbp_df[pbp_df["TRUE_NUMBEROFPLAY"] >= second_foul_play]

        opp_team = boxscore_df[boxscore_df["Team"] != team]["Team"].iloc[0]

        def _calc_team_rtg(subset, t, opp):
            pts_for = sum(score_map.get(pt, 0) for _, pt in
                          subset[subset["CODETEAM"] == t]["PLAYTYPE"].items())
            pts_against = sum(score_map.get(pt, 0) for _, pt in
                              subset[subset["CODETEAM"] == opp]["PLAYTYPE"].items())
            poss = max(len(subset[subset["CODETEAM"].isin([t]) & subset["PLAYTYPE"].isin(poss_events)]), 1)
            return (pts_for / poss) * 100, (pts_against / poss) * 100

        ortg_b, drtg_b = _calc_team_rtg(before, team, opp_team)
        ortg_a, drtg_a = _calc_team_rtg(after, team, opp_team)

        results.append({
            "team": team,
            "star_player": star_name,
            "star_player_id": star_pid,
            "second_foul_play_num": int(second_foul_play),
            "foul_period": int(player_fouls.iloc[1]["PERIOD"]),
            "ortg_before": round(ortg_b, 1),
            "ortg_after": round(ortg_a, 1),
            "drtg_before": round(drtg_b, 1),
            "drtg_after": round(drtg_a, 1),
            "ortg_impact": round(ortg_a - ortg_b, 1),
            "drtg_impact": round(drtg_a - drtg_b, 1),
        })

    return pd.DataFrame(results)


# ========================================================================
# PHASE 2 — ASSIST NETWORK
# ========================================================================

def build_assist_network(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse PBP to extract "Player X assisted Player Y" relationships.

    The euroleague-api PBP sequence for an assisted basket:
      TRUE_NUMBEROFPLAY N:   2FGM/3FGM by SCORER
      TRUE_NUMBEROFPLAY N+1: AS by ASSISTER (same CODETEAM)

    We walk through the PBP and link each AS event to the immediately
    preceding made shot by the same team.

    Returns a DataFrame with columns:
      assister_id, assister_name, scorer_id, scorer_name, team, count, play_types
    """
    if pbp_df.empty:
        return pd.DataFrame()

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    scoring_types = {"2FGM", "3FGM"}  # FTM assists are rare/not tracked

    # Build a list of (assister, scorer) pairs
    pairs = []
    prev_row = None

    for _, row in df.iterrows():
        playtype = str(row.get("PLAYTYPE", "")).strip()
        codeteam = str(row.get("CODETEAM", "")).strip()

        if playtype == "AS" and prev_row is not None:
            prev_pt = str(prev_row.get("PLAYTYPE", "")).strip()
            prev_team = str(prev_row.get("CODETEAM", "")).strip()

            # Link: the AS event's player is the assister,
            # the previous made shot's player is the scorer
            if prev_pt in scoring_types and prev_team == codeteam:
                pairs.append({
                    "assister_id": str(row["PLAYER_ID"]).strip(),
                    "assister_name": str(row["PLAYER"]).strip(),
                    "scorer_id": str(prev_row["PLAYER_ID"]).strip(),
                    "scorer_name": str(prev_row["PLAYER"]).strip(),
                    "team": codeteam,
                    "play_type": prev_pt,
                })

        # Track previous row for linking
        if playtype in scoring_types:
            prev_row = row
        elif playtype != "AS":
            # Reset if a non-scoring, non-assist event intervenes
            prev_row = None

    if not pairs:
        return pd.DataFrame()

    pairs_df = pd.DataFrame(pairs)

    # Aggregate: count assists per passer→scorer pair
    network = (
        pairs_df
        .groupby(["assister_id", "assister_name", "scorer_id", "scorer_name", "team"])
        .agg(
            count=("play_type", "size"),
            play_types=("play_type", lambda x: ", ".join(sorted(set(x)))),
        )
        .reset_index()
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    return network


# ========================================================================
# PHASE 2 — SHOT QUALITY
# ========================================================================

def compute_shot_quality(shot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute expected points per shot based on historical zone accuracy.

    The shot data includes a ZONE column (A-G) and POINTS (0 for miss,
    2 or 3 for makes).  We compute:
      - Zone FG%: historical accuracy per zone
      - Expected Points = Zone_FG% × Point_Value_of_Shot
      - Shot Quality: per-player average expected points

    This tells us if a player takes high-quality shots (easy layups in
    zone C/F) vs. low-quality shots (contested threes in zone A/B).

    Edge case: if COORD_X/COORD_Y are missing, we still use ZONE.
    """
    if shot_df.empty:
        return pd.DataFrame()

    df = shot_df.copy()

    # Determine the point value of each shot attempt
    # ACTION column: "Two Pointer", "Three Pointer", "Missed Two Pointer", etc.
    # ID_ACTION: "2FGM", "2FGA", "3FGM", "3FGA"
    df["shot_value"] = df["ID_ACTION"].apply(
        lambda x: 3 if "3FG" in str(x) else 2
    )
    df["made"] = df["POINTS"].apply(lambda p: 1 if p > 0 else 0)

    # Zone-level FG% (across all shots in this data)
    if "ZONE" not in df.columns or df["ZONE"].isna().all():
        logger.warning("No ZONE data available for shot quality")
        return pd.DataFrame()

    zone_stats = (
        df.groupby("ZONE")
        .agg(
            total_shots=("made", "size"),
            makes=("made", "sum"),
            avg_shot_value=("shot_value", "mean"),
        )
        .reset_index()
    )
    zone_stats["zone_fg_pct"] = zone_stats["makes"] / zone_stats["total_shots"]
    zone_stats["expected_pts"] = zone_stats["zone_fg_pct"] * zone_stats["avg_shot_value"]

    # Merge zone stats back onto individual shots
    df = df.merge(zone_stats[["ZONE", "zone_fg_pct", "expected_pts"]], on="ZONE", how="left")

    # Per-player shot quality summary
    player_quality = (
        df.groupby(["ID_PLAYER", "PLAYER", "TEAM"])
        .agg(
            total_shots=("made", "size"),
            makes=("made", "sum"),
            actual_pts=("POINTS", "sum"),
            avg_expected_pts=("expected_pts", "mean"),
            total_expected_pts=("expected_pts", "sum"),
        )
        .reset_index()
    )
    player_quality["fg_pct"] = player_quality["makes"] / player_quality["total_shots"]
    player_quality["shot_quality_diff"] = (
        player_quality["actual_pts"] - player_quality["total_expected_pts"]
    )
    player_quality = player_quality.sort_values("total_shots", ascending=False).reset_index(drop=True)

    return player_quality


def compute_referee_stats(
    metadata_df: pd.DataFrame,
    team_code: str,
    min_games: int = 3,
) -> pd.DataFrame:
    """
    Calculate a team's win/loss record under each referee.

    Parameters
    ----------
    metadata_df : pd.DataFrame
        Output of `get_season_game_metadata()` with columns:
        CodeTeamA, CodeTeamB, ScoreA, ScoreB, Referee1, Referee2, Referee3.
    team_code : str
        The team code to analyze (e.g. "OLY", "PAO").
    min_games : int
        Minimum games officiated to include a referee (avoids noise).

    Returns
    -------
    pd.DataFrame
        Per-referee stats: referee, games, wins, losses, win_pct.
    """
    if metadata_df.empty:
        return pd.DataFrame()

    ref_cols = ["Referee1", "Referee2", "Referee3"]
    missing = [c for c in ref_cols if c not in metadata_df.columns]
    if missing:
        logger.warning(f"Missing referee columns: {missing}")
        return pd.DataFrame()

    # Filter to games involving the selected team
    team_games = metadata_df[
        (metadata_df["CodeTeamA"] == team_code) | (metadata_df["CodeTeamB"] == team_code)
    ].copy()

    if team_games.empty:
        return pd.DataFrame()

    # Determine if team won each game
    team_games["team_won"] = np.where(
        team_games["CodeTeamA"] == team_code,
        team_games["ScoreA"] > team_games["ScoreB"],   # Team is "A"
        team_games["ScoreB"] > team_games["ScoreA"],   # Team is "B"
    )

    # Melt referee columns into long format (3 rows per game)
    melted_rows = []
    for _, row in team_games.iterrows():
        won = row["team_won"]
        for rc in ref_cols:
            ref_name = row.get(rc)
            if pd.notna(ref_name) and str(ref_name).strip():
                melted_rows.append({"referee": str(ref_name).strip(), "won": won})

    if not melted_rows:
        return pd.DataFrame()

    ref_df = pd.DataFrame(melted_rows)

    # Aggregate per referee
    stats = (
        ref_df.groupby("referee")
        .agg(games=("won", "size"), wins=("won", "sum"))
        .reset_index()
    )
    stats["losses"] = stats["games"] - stats["wins"]
    stats["win_pct"] = (stats["wins"] / stats["games"] * 100).round(1)

    # Apply minimum games filter
    stats = stats[stats["games"] >= min_games].sort_values("win_pct", ascending=False).reset_index(drop=True)

    return stats


# ────────────────────────────────────────────────────────────────────
# 15) Positional Scoring Distribution
# ────────────────────────────────────────────────────────────────────

def classify_player_positions(boxscore: pd.DataFrame) -> pd.DataFrame:
    """
    Infer player positions from box-score statistical profiles.

    Since the euroleague-api does not expose explicit positions,
    we use a heuristic classification within each team:

    - **Guard**: Players with the highest assist-to-rebound ratio
      (top 2 per team by AST / (TotalRebounds + 1)).
    - **Center**: Players with the highest rebound-to-assist ratio
      (top 1 per team by TotalRebounds / (Assistances + 1)).
    - **Forward**: Everyone else.

    The classification only considers players with > 0 minutes.

    Returns the input DataFrame with an added ``position`` column
    containing one of 'Guard', 'Forward', or 'Center'.
    """
    df = boxscore.copy()

    # Ensure numeric columns exist
    for col in ["Assistances", "TotalRebounds"]:
        if col not in df.columns:
            df[col] = 0

    # Parse minutes — handle both "MM:SS" strings and floats
    def _parse_min(val):
        if pd.isna(val):
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val)
        if ":" in s:
            parts = s.split(":")
            return float(parts[0]) + float(parts[1]) / 60
        try:
            return float(s)
        except ValueError:
            return 0.0

    if df["Minutes"].dtype == object:
        df["_mins"] = df["Minutes"].apply(_parse_min)
    else:
        df["_mins"] = df["Minutes"]

    # Only classify players who actually played
    played = df["_mins"] > 0

    # Default = Forward
    df["position"] = "Forward"

    # Compute ratios
    df["_ast_ratio"] = df["Assistances"] / (df["TotalRebounds"].clip(lower=0) + 1)
    df["_reb_ratio"] = df["TotalRebounds"] / (df["Assistances"].clip(lower=0) + 1)

    for team in df.loc[played, "Team"].unique():
        mask = played & (df["Team"] == team)
        team_df = df.loc[mask]

        if len(team_df) < 3:
            continue

        # Top 2 ast_ratio → Guards
        guard_idx = team_df.nlargest(2, "_ast_ratio").index
        df.loc[guard_idx, "position"] = "Guard"

        # Among remaining, top 1 reb_ratio → Center
        remaining = team_df.drop(guard_idx)
        if not remaining.empty:
            center_idx = remaining.nlargest(1, "_reb_ratio").index
            df.loc[center_idx, "position"] = "Center"

    # Cleanup temp columns
    df.drop(columns=["_mins", "_ast_ratio", "_reb_ratio"], inplace=True, errors="ignore")

    return df


# ========================================================================
# PHASE 9 — ADVANCED PLAYMAKING: AAQ, AxP & ASSIST NETWORK WITH xP
# ========================================================================

# Hoop center in Euroleague shot coordinate system (pixel space)
_HOOP_X = 0.0
_HOOP_Y = 72.0


def _euclidean_distance_from_hoop(coord_x: float, coord_y: float) -> float:
    """Euclidean distance from hoop center in the shot coordinate system."""
    return np.sqrt((coord_x - _HOOP_X) ** 2 + (coord_y - _HOOP_Y) ** 2)


def compute_baseline_xp(coord_x: pd.Series, coord_y: pd.Series, shot_value: pd.Series) -> pd.Series:
    """
    Distance-based Expected Points (xP) fallback when no ML model is loaded.

    Zones (approximate, based on Euroleague court pixel coordinates):
      - Restricted area (dist < 150):  high FG% ~65% for 2PT
      - Short mid-range (150-300):     ~42% for 2PT
      - Long mid-range (300-500):      ~38% for 2PT
      - Three-point range (dist > 500): ~36% for 3PT

    xP = zone_fg_pct * shot_value
    """
    dist = np.sqrt((coord_x - _HOOP_X) ** 2 + (coord_y - _HOOP_Y) ** 2)

    fg_pct = pd.Series(np.nan, index=coord_x.index)

    is_3pt = shot_value == 3
    is_2pt = shot_value == 2

    # 2PT zones
    fg_pct = np.where(is_2pt & (dist < 150), 0.65, fg_pct)
    fg_pct = np.where(is_2pt & (dist >= 150) & (dist < 300), 0.42, fg_pct)
    fg_pct = np.where(is_2pt & (dist >= 300), 0.38, fg_pct)

    # 3PT zones
    fg_pct = np.where(is_3pt & (dist < 680), 0.37, fg_pct)   # corner 3
    fg_pct = np.where(is_3pt & (dist >= 680), 0.35, fg_pct)   # above-the-break 3

    # Fallback for missing coords
    fg_pct = np.where(np.isnan(fg_pct.astype(float)), np.where(is_3pt, 0.35, 0.50), fg_pct)

    return pd.Series(fg_pct.astype(float) * shot_value, index=coord_x.index)


def link_assists_to_shots(
    pbp_df: pd.DataFrame,
    shot_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Link PBP assist events to their corresponding made shots, then merge
    with shot coordinate data to compute baseline xP.

    Algorithm:
      1. Walk PBP in TRUE_NUMBEROFPLAY order.
      2. When an 'AS' event follows a '2FGM'/'3FGM' by the same team,
         record the passer-shooter pair with shot metadata.
      3. Match each assisted made shot to the shot_df by player_id,
         action type, and period/minute for X/Y coordinates.
      4. Compute baseline xP from Euclidean distance.

    Returns a DataFrame with columns:
        passer_id, passer_name, shooter_id, shooter_name, team,
        shot_value, coord_x, coord_y, zone, xp, season, gamecode
    """
    if pbp_df.empty:
        return pd.DataFrame()

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    scoring_types = {"2FGM": 2, "3FGM": 3}

    pairs = []
    prev_row = None

    for _, row in df.iterrows():
        playtype = str(row.get("PLAYTYPE", "")).strip()
        codeteam = str(row.get("CODETEAM", "")).strip()

        if playtype == "AS" and prev_row is not None:
            prev_pt = str(prev_row.get("PLAYTYPE", "")).strip()
            prev_team = str(prev_row.get("CODETEAM", "")).strip()

            if prev_pt in scoring_types and prev_team == codeteam:
                pairs.append({
                    "passer_id": str(row["PLAYER_ID"]).strip(),
                    "passer_name": str(row["PLAYER"]).strip(),
                    "shooter_id": str(prev_row["PLAYER_ID"]).strip(),
                    "shooter_name": str(prev_row["PLAYER"]).strip(),
                    "team": codeteam,
                    "shot_value": scoring_types[prev_pt],
                    "play_type": prev_pt,
                    "period": prev_row.get("PERIOD"),
                    "markertime": prev_row.get("MARKERTIME"),
                    "numberofplay": prev_row.get("NUMBEROFPLAY"),
                    "season": prev_row.get("Season"),
                    "gamecode": prev_row.get("Gamecode"),
                })

        if playtype in scoring_types:
            prev_row = row
        elif playtype != "AS":
            prev_row = None

    if not pairs:
        return pd.DataFrame()

    assist_df = pd.DataFrame(pairs)

    # Merge with shot data for coordinates
    if not shot_df.empty and "COORD_X" in shot_df.columns:
        shot_merge = shot_df.copy()
        shot_merge["_player_id"] = shot_merge["ID_PLAYER"].astype(str).str.strip()
        shot_merge["_action"] = shot_merge["ID_ACTION"].astype(str).str.strip()

        # Only made shots
        shot_merge = shot_merge[shot_merge["POINTS"] > 0].copy()

        # Build a lookup keyed by (season, gamecode, player_id, action_type, minute)
        # to handle multiple shots by same player — best-effort match
        shot_merge["_minute"] = pd.to_numeric(shot_merge.get("MINUTE"), errors="coerce")

        # For assist_df, derive approximate minute from MARKERTIME and period
        def _marker_to_game_minute(period, markertime):
            if pd.isna(period) or pd.isna(markertime):
                return np.nan
            secs = _markertime_to_seconds(str(markertime))
            return (int(period) - 1) * 10 + (10 - secs / 60.0)

        assist_df["_minute"] = assist_df.apply(
            lambda r: _marker_to_game_minute(r["period"], r["markertime"]), axis=1
        )

        # Merge: match player, action, and closest minute
        coord_x_list = []
        coord_y_list = []
        zone_list = []

        for idx, arow in assist_df.iterrows():
            pid = arow["shooter_id"]
            action = arow["play_type"]
            minute = arow["_minute"]
            season = arow.get("season")
            gc = arow.get("gamecode")

            candidates = shot_merge[
                (shot_merge["_player_id"] == pid)
                & (shot_merge["_action"] == action)
            ]

            # Filter by season/gamecode if available
            if season is not None and "Season" in candidates.columns:
                candidates = candidates[candidates["Season"] == season]
            if gc is not None and "Gamecode" in candidates.columns:
                candidates = candidates[candidates["Gamecode"] == gc]

            if candidates.empty:
                coord_x_list.append(np.nan)
                coord_y_list.append(np.nan)
                zone_list.append(None)
                continue

            # Find closest by minute
            if not np.isnan(minute) and "_minute" in candidates.columns:
                diffs = (candidates["_minute"] - minute).abs()
                best_idx = diffs.idxmin()
            else:
                best_idx = candidates.index[0]

            coord_x_list.append(candidates.loc[best_idx, "COORD_X"])
            coord_y_list.append(candidates.loc[best_idx, "COORD_Y"])
            zone_list.append(candidates.loc[best_idx].get("ZONE"))

            # Remove used shot to avoid double-matching
            shot_merge = shot_merge.drop(best_idx)

        assist_df["coord_x"] = coord_x_list
        assist_df["coord_y"] = coord_y_list
        assist_df["zone"] = zone_list

    else:
        assist_df["coord_x"] = np.nan
        assist_df["coord_y"] = np.nan
        assist_df["zone"] = None

    # Compute baseline xP
    assist_df["coord_x"] = pd.to_numeric(assist_df["coord_x"], errors="coerce")
    assist_df["coord_y"] = pd.to_numeric(assist_df["coord_y"], errors="coerce")

    assist_df["xp"] = compute_baseline_xp(
        assist_df["coord_x"],
        assist_df["coord_y"],
        assist_df["shot_value"],
    )

    # Clean up temp columns
    assist_df.drop(columns=["_minute"], inplace=True, errors="ignore")

    return assist_df


def compute_playmaking_metrics(
    assist_shot_df: pd.DataFrame,
    min_assists: int = 10,
) -> Dict[str, pd.DataFrame]:
    """
    Compute AAQ, AxP, and Duo metrics from linked assist-shot data.

    1. AAQ (Adjusted Assist Quality): Mean xP of shots created by a
       player's assists. Filters out players with < min_assists.
    2. AxP (Assisted xPoints): Total and mean xP of assisted shots
       a player takes and makes, grouped by shooter.
    3. Duos: Total xP generated by each passer-shooter combination.

    Returns dict with:
        'aaq': DataFrame — top creators (passer_name, team, total_assists, aaq)
        'axp': DataFrame — top finishers (shooter_name, team, assisted_shots, axp_total, axp_avg)
        'duos': DataFrame — lethal duos (passer_name, shooter_name, team, assists, duo_xp)
    """
    empty_result = {
        "aaq": pd.DataFrame(),
        "axp": pd.DataFrame(),
        "duos": pd.DataFrame(),
    }

    if assist_shot_df.empty or "xp" not in assist_shot_df.columns:
        return empty_result

    df = assist_shot_df.copy()

    # --- AAQ: Group by passer ---
    aaq = (
        df.groupby(["passer_id", "passer_name", "team"])
        .agg(total_assists=("xp", "size"), aaq=("xp", "mean"))
        .reset_index()
    )
    aaq = aaq[aaq["total_assists"] >= min_assists].copy()
    aaq = aaq.sort_values("aaq", ascending=False).reset_index(drop=True)
    aaq["aaq"] = aaq["aaq"].round(3)

    # --- AxP: Group by shooter ---
    axp = (
        df.groupby(["shooter_id", "shooter_name", "team"])
        .agg(
            assisted_shots=("xp", "size"),
            axp_total=("xp", "sum"),
            axp_avg=("xp", "mean"),
        )
        .reset_index()
    )
    axp = axp.sort_values("axp_total", ascending=False).reset_index(drop=True)
    axp["axp_total"] = axp["axp_total"].round(2)
    axp["axp_avg"] = axp["axp_avg"].round(3)

    # --- Duos: Group by passer + shooter ---
    duos = (
        df.groupby(["passer_id", "passer_name", "shooter_id", "shooter_name", "team"])
        .agg(assists=("xp", "size"), duo_xp=("xp", "sum"))
        .reset_index()
    )
    duos = duos.sort_values("duo_xp", ascending=False).reset_index(drop=True)
    duos["duo_xp"] = duos["duo_xp"].round(2)

    return {"aaq": aaq, "axp": axp, "duos": duos}


def compute_close_game_stats(
    schedule_df: pd.DataFrame,
    close_threshold: int = 5,
) -> pd.DataFrame:
    """
    Compute close-game (clutch DNA) stats for all teams in a season.

    For every completed game, calculates margin and flags close games
    (margin <= threshold). Then aggregates per team:
      - Total Games, Overall Win %, Avg Point Differential
      - Close Games Played, Close Game Win %
      - League Average Close Game Win % as baseline
    """
    played = schedule_df[schedule_df["played"] == True].copy()
    if played.empty:
        return pd.DataFrame()

    # Build team name lookup from schedule
    name_map = {}
    for _, row in played.iterrows():
        name_map[row["home_code"]] = row.get("home_name", row["home_code"])
        name_map[row["away_code"]] = row.get("away_name", row["away_code"])

    records = []
    for _, row in played.iterrows():
        hs, aws = row["home_score"], row["away_score"]
        if pd.isna(hs) or pd.isna(aws):
            continue
        margin = abs(hs - aws)
        is_close = margin <= close_threshold

        records.append({
            "team_code": row["home_code"],
            "won": hs > aws,
            "point_diff": hs - aws,
            "is_close_game": is_close,
        })
        records.append({
            "team_code": row["away_code"],
            "won": aws > hs,
            "point_diff": aws - hs,
            "is_close_game": is_close,
        })

    if not records:
        return pd.DataFrame()

    game_df = pd.DataFrame(records)

    team_stats = game_df.groupby("team_code").agg(
        total_games=("won", "size"),
        total_wins=("won", "sum"),
        avg_point_diff=("point_diff", "mean"),
    ).reset_index()
    team_stats["overall_win_pct"] = team_stats["total_wins"] / team_stats["total_games"] * 100

    close_df = game_df[game_df["is_close_game"]]
    if not close_df.empty:
        close_agg = close_df.groupby("team_code").agg(
            close_games_played=("won", "size"),
            close_wins=("won", "sum"),
        ).reset_index()
        team_stats = team_stats.merge(close_agg, on="team_code", how="left")
    else:
        team_stats["close_games_played"] = 0
        team_stats["close_wins"] = 0

    team_stats["close_games_played"] = team_stats["close_games_played"].fillna(0).astype(int)
    team_stats["close_wins"] = team_stats["close_wins"].fillna(0).astype(int)
    team_stats["close_losses"] = team_stats["close_games_played"] - team_stats["close_wins"]
    team_stats["close_win_pct"] = np.where(
        team_stats["close_games_played"] > 0,
        team_stats["close_wins"] / team_stats["close_games_played"] * 100,
        np.nan,
    )

    total_close_wins = team_stats["close_wins"].sum()
    total_close_games = team_stats["close_games_played"].sum()
    team_stats["league_avg_close_win_pct"] = (
        (total_close_wins / total_close_games * 100) if total_close_games > 0 else 50.0
    )

    team_stats["team_name"] = team_stats["team_code"].map(name_map).fillna(team_stats["team_code"])

    return team_stats.sort_values("close_win_pct", ascending=False, na_position="last").reset_index(drop=True)


def compute_total_points_created(
    advanced_df: pd.DataFrame,
    assist_shot_links: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute Total Points Created (TPC) by merging exact assist point values
    from PBP-linked assist-shot data with standard boxscore stats.

    Steps:
      1. Group assist_shot_links by passer_id, summing shot_value to get
         the exact points generated from each player's assists.
      2. Merge with the advanced stats DataFrame on player_id.
      3. Calculate: Total_Points_Created = points + pts_from_assists.

    Returns the advanced_df with two new columns:
      - pts_from_assists: exact points yielded by the player's assists
      - total_pts_created: points scored + pts_from_assists
    """
    df = advanced_df.copy()

    if assist_shot_links.empty or "passer_id" not in assist_shot_links.columns:
        df["pts_from_assists"] = 0
        df["total_pts_created"] = df["points"].fillna(0)
        return df

    pfa = (
        assist_shot_links
        .groupby("passer_id")["shot_value"]
        .sum()
        .reset_index()
        .rename(columns={"passer_id": "player_id", "shot_value": "pts_from_assists"})
    )

    df["player_id"] = df["player_id"].astype(str).str.strip()
    pfa["player_id"] = pfa["player_id"].astype(str).str.strip()

    df = df.merge(pfa, on="player_id", how="left")
    df["pts_from_assists"] = df["pts_from_assists"].fillna(0).astype(int)
    df["total_pts_created"] = df["points"].fillna(0) + df["pts_from_assists"]

    return df


def compute_positional_scoring(
    boxscore: pd.DataFrame,
    team_code: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute the percentage of team points contributed by each position.

    Args:
        boxscore: Player boxscore DataFrame (single game or multi-game).
        team_code: If provided, filter to a single team. Otherwise, all teams.

    Returns:
        DataFrame with columns: position, points, pct
    """
    df = classify_player_positions(boxscore)

    if team_code:
        df = df[df["Team"] == team_code]

    if df.empty:
        return pd.DataFrame(columns=["position", "points", "pct"])

    grouped = df.groupby("position")["Points"].sum().reset_index()
    grouped.columns = ["position", "points"]
    total = grouped["points"].sum()
    grouped["pct"] = (grouped["points"] / total * 100).round(1) if total > 0 else 0

    # Ensure all 3 positions exist
    for pos in ["Guard", "Forward", "Center"]:
        if pos not in grouped["position"].values:
            grouped = pd.concat([grouped, pd.DataFrame({"position": [pos], "points": [0], "pct": [0.0]})], ignore_index=True)

    position_order = ["Guard", "Forward", "Center"]
    grouped["position"] = pd.Categorical(grouped["position"], categories=position_order, ordered=True)
    grouped = grouped.sort_values("position").reset_index(drop=True)

    return grouped


# ========================================================================
# PHASE 7 — PLAYER ROTATION STINTS
# ========================================================================

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
