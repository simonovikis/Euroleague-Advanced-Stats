import logging
from typing import Tuple
import numpy as np
import pandas as pd
from data_pipeline.transformers.utils import COL_MAP, parse_minutes

logger = logging.getLogger(__name__)


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
