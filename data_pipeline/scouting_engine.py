"""
scouting_engine.py — AI Player Similarity Engine
==================================================
Fetches league-wide per-game player stats from the Euroleague API,
engineers a rich feature vector for each player, then uses cosine
similarity to find the most statistically similar players.

Feature vector (11 dimensions):
  - TS%              (scoring efficiency)
  - tUSG%            (offensive involvement — custom metric)
  - Stop Rate        (defensive impact — custom metric)
  - Assist Ratio     (playmaking)
  - AST/TOV Ratio    (ball security)
  - ORB%             (offensive rebounding)
  - DRB%             (defensive rebounding)
  - 3PA Ratio        (perimeter orientation)
  - FT Rate          (ability to draw fouls)
  - Steals per game  (active hands)
  - Blocks per game  (rim protection)

All features are scaled with StandardScaler before computing similarity.
"""

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler

from data_pipeline.extractors import COMPETITION, apply_team_aliases
from data_pipeline.transformers import format_player_name

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = [
    "ts_pct",
    "true_usg_pct",
    "stop_rate",
    "assist_ratio",
    "ast_tov_ratio",
    "orb_pct",
    "drb_pct",
    "three_pt_rate",
    "ft_rate",
    "steals_pg",
    "blocks_pg",
]

FEATURE_LABELS = {
    "ts_pct": "TS%",
    "true_usg_pct": "tUSG%",
    "stop_rate": "Stop Rate",
    "assist_ratio": "AST Ratio",
    "ast_tov_ratio": "AST/TOV",
    "orb_pct": "ORB%",
    "drb_pct": "DRB%",
    "three_pt_rate": "3PA Rate",
    "ft_rate": "FT Rate",
    "steals_pg": "STL/G",
    "blocks_pg": "BLK/G",
}

POSITION_GROUPS = {"Guard": "Guard", "Forward": "Forward", "Center": "Center"}

MIN_GAMES = 5
MIN_MINUTES_PG = 8.0


def infer_position(row: pd.Series) -> str:
    """
    Infer a player's position group (Guard / Forward / Center) from their
    statistical profile.  Uses a simple heuristic based on rebounds, blocks,
    assists, and 3PA Rate.
    """
    orb = row.get("orb_pct", 0) or 0
    drb = row.get("drb_pct", 0) or 0
    blk = row.get("blocks_pg", 0) or 0
    ast = row.get("assist_ratio", 0) or 0
    thr = row.get("three_pt_rate", 0) or 0

    big_score = (orb + drb) * 50 + blk * 10
    guard_score = ast * 50 + thr * 50

    if big_score > 18 and big_score > guard_score * 1.3:
        return "Center"
    if guard_score > 14 and guard_score > big_score * 1.1:
        return "Guard"
    return "Forward"


def fetch_league_player_stats(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch traditional + advanced per-game stats for all players in a season
    from the Euroleague API, merge them, and compute custom metrics.

    Returns a DataFrame with one row per player and all feature columns.
    """
    from euroleague_api.player_stats import PlayerStats

    ps = PlayerStats(competition)

    try:
        trad = ps.get_player_stats_single_season(
            endpoint="traditional",
            season=season,
            phase_type_code="RS",
            statistic_mode="PerGame",
        )
        adv = ps.get_player_stats_single_season(
            endpoint="advanced",
            season=season,
            phase_type_code="RS",
            statistic_mode="PerGame",
        )
    except Exception as e:
        logger.error(f"Failed to fetch player stats for season {season}: {e}")
        return pd.DataFrame()

    if trad.empty or adv.empty:
        logger.warning(f"Empty player stats for season {season}")
        return pd.DataFrame()

    trad = trad.rename(columns={
        "player.code": "player_code",
        "player.name": "player_name_raw",
        "player.team.code": "team_code",
        "player.team.name": "team_name",
        "player.imageUrl": "image_url",
    })
    adv = adv.rename(columns={
        "player.code": "player_code",
        "player.name": "player_name_raw_adv",
        "player.team.code": "team_code_adv",
    })

    merged = pd.merge(trad, adv, on="player_code", how="inner", suffixes=("", "_adv"))

    merged = merged[merged["gamesPlayed"] >= MIN_GAMES].copy()

    mpg = pd.to_numeric(merged["minutesPlayed"], errors="coerce").fillna(0)
    merged = merged[mpg >= MIN_MINUTES_PG].copy()

    if merged.empty:
        return pd.DataFrame()

    df = _engineer_features(merged)
    df = apply_team_aliases(df, ["team_code"])
    return df


def _parse_pct_str(col: pd.Series) -> pd.Series:
    """Convert '54.2%' string to 0.542 float."""
    return (
        col.astype(str)
        .str.replace("%", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        / 100.0
    )


def _engineer_features(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Build the feature DataFrame from the merged traditional + advanced stats.
    """
    df = pd.DataFrame()

    df["player_code"] = merged["player_code"]
    df["player_name"] = merged["player_name_raw"].apply(format_player_name)
    df["team_code"] = merged["team_code"]
    df["team_name"] = merged["team_name"]
    df["image_url"] = merged.get("image_url", "")
    df["games_played"] = pd.to_numeric(merged["gamesPlayed"], errors="coerce").fillna(0)
    df["minutes_pg"] = pd.to_numeric(merged["minutesPlayed"], errors="coerce").fillna(0)
    df["points_pg"] = pd.to_numeric(merged["pointsScored"], errors="coerce").fillna(0)

    # --- From advanced endpoint ---
    df["ts_pct"] = _parse_pct_str(merged["trueShootingPercentage"])
    df["assist_ratio"] = _parse_pct_str(merged["assistsRatio"])
    df["orb_pct"] = _parse_pct_str(merged["offensiveReboundsPercentage"])
    df["drb_pct"] = _parse_pct_str(merged["defensiveReboundsPercentage"])
    df["three_pt_rate"] = _parse_pct_str(merged["threePointAttemptsRatio"])
    df["ft_rate"] = _parse_pct_str(merged["freeThrowsRate"])

    ast_tov = merged["assistsToTurnoversRatio"]
    df["ast_tov_ratio"] = pd.to_numeric(ast_tov, errors="coerce").fillna(0)

    # --- From traditional endpoint: per-game counting stats ---
    steals = pd.to_numeric(merged["steals"], errors="coerce").fillna(0)
    blocks = pd.to_numeric(merged["blocks"], errors="coerce").fillna(0)
    df["steals_pg"] = steals
    df["blocks_pg"] = blocks

    # --- Custom metrics computed from traditional data ---
    fga = (
        pd.to_numeric(merged["twoPointersAttempted"], errors="coerce").fillna(0)
        + pd.to_numeric(merged["threePointersAttempted"], errors="coerce").fillna(0)
    )
    fta = pd.to_numeric(merged["freeThrowsAttempted"], errors="coerce").fillna(0)
    tov = pd.to_numeric(merged["turnovers"], errors="coerce").fillna(0)
    ast = pd.to_numeric(merged["assists"], errors="coerce").fillna(0)
    fouls_drawn = pd.to_numeric(merged["foulsDrawn"], errors="coerce").fillna(0)
    poss = pd.to_numeric(merged["possesions"], errors="coerce").fillna(0)

    # tUSG% = (FGA + 0.44*FTA + TOV + AST + FoulsDrawn) / Possessions
    involvement = fga + 0.44 * fta + tov + ast + fouls_drawn
    df["true_usg_pct"] = np.where(poss > 0, involvement / poss, np.nan)

    # Stop Rate = (STL + BLK + DRB) / Possessions
    drb = pd.to_numeric(merged["defensiveRebounds"], errors="coerce").fillna(0)
    stops = steals + blocks + drb
    df["stop_rate"] = np.where(poss > 0, stops / poss, np.nan)

    # Rebounds per game (for display)
    df["rebounds_pg"] = pd.to_numeric(merged["totalRebounds"], errors="coerce").fillna(0)
    df["assists_pg"] = ast

    # Infer position group from statistical profile
    df["position"] = df.apply(infer_position, axis=1)

    return df.reset_index(drop=True)


def build_similarity_model(
    player_df: pd.DataFrame,
) -> Tuple[np.ndarray, StandardScaler, pd.DataFrame]:
    """
    Scale features and return the scaled matrix + scaler + clean player index.

    Returns
    -------
    scaled_matrix : np.ndarray of shape (n_players, n_features)
    scaler : fitted StandardScaler
    index_df : DataFrame with player_code, player_name, team_code (same row order)
    """
    df = player_df.dropna(subset=FEATURE_COLUMNS).copy()
    if df.empty:
        return np.array([]), StandardScaler(), pd.DataFrame()

    features = df[FEATURE_COLUMNS].values
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)

    keep_cols = ["player_code", "player_name", "team_code", "team_name",
                 "image_url", "games_played", "minutes_pg", "points_pg"]
    if "position" in df.columns:
        keep_cols.append("position")
    index_df = df[keep_cols].reset_index(drop=True)

    return scaled, scaler, index_df


def find_similar_players(
    target_name: str,
    player_df: pd.DataFrame,
    top_n: int = 5,
    position_filter: Optional[str] = None,
) -> pd.DataFrame:
    """
    Find the top N most similar players to the target using cosine similarity.

    Parameters
    ----------
    target_name : str
        The player name to search for (partial match supported).
    player_df : pd.DataFrame
        Full league player DataFrame from fetch_league_player_stats.
    top_n : int
        Number of similar players to return.
    position_filter : str or None
        If set to a position group ("Guard", "Forward", "Center"), only
        match within that group. None means all positions.

    Returns
    -------
    pd.DataFrame with columns:
        player_name, team_code, position, similarity, similarity_pct,
        and all feature columns.
    """
    clean_df = player_df.dropna(subset=FEATURE_COLUMNS).reset_index(drop=True)

    # Find target player index in clean_df
    name_lower = target_name.lower()
    mask = clean_df["player_name"].str.lower() == name_lower
    if mask.sum() == 0:
        mask = clean_df["player_name"].str.lower().str.contains(name_lower, na=False)
    if mask.sum() == 0:
        logger.warning(f"Player '{target_name}' not found")
        return pd.DataFrame()
    target_idx = mask.idxmax()

    # Apply position filter: keep target + players matching the position
    if position_filter and "position" in clean_df.columns:
        pos_mask = (clean_df["position"] == position_filter) | (clean_df.index == target_idx)
        subset_df = clean_df[pos_mask].reset_index(drop=True)
        # Recompute target index in the filtered subset
        t_mask = subset_df["player_name"].str.lower() == name_lower
        if t_mask.sum() == 0:
            t_mask = subset_df["player_name"].str.lower().str.contains(name_lower, na=False)
        if t_mask.sum() == 0:
            return pd.DataFrame()
        target_idx_sub = t_mask.idxmax()
    else:
        subset_df = clean_df
        target_idx_sub = target_idx

    # Scale features & compute cosine similarity
    features = subset_df[FEATURE_COLUMNS].values
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)

    sim_matrix = cosine_similarity(scaled[target_idx_sub:target_idx_sub + 1], scaled)
    sim_scores = sim_matrix[0]

    # Exclude the target player themselves
    sim_scores[target_idx_sub] = -1.0

    top_indices = np.argsort(sim_scores)[::-1][:top_n]

    results = []
    for idx in top_indices:
        row = subset_df.iloc[idx]
        raw_sim = float(sim_scores[idx])
        result = {
            "player_name": row["player_name"],
            "team_code": row["team_code"],
            "team_name": row.get("team_name", ""),
            "image_url": row.get("image_url", ""),
            "position": row.get("position", ""),
            "similarity": round(raw_sim, 4),
            "similarity_pct": round(raw_sim * 100, 1),
            "games_played": row.get("games_played", 0),
            "minutes_pg": row.get("minutes_pg", 0),
            "points_pg": row.get("points_pg", 0),
        }
        for feat in FEATURE_COLUMNS:
            result[feat] = row[feat]
        results.append(result)

    return pd.DataFrame(results)


def get_player_feature_vector(
    player_name: str,
    player_df: pd.DataFrame,
) -> Optional[Dict[str, float]]:
    """
    Get the raw feature values for a single player.
    Returns a dict mapping feature name -> value, or None if not found.
    """
    clean_df = player_df.dropna(subset=FEATURE_COLUMNS).reset_index(drop=True)
    name_lower = player_name.lower()
    mask = clean_df["player_name"].str.lower() == name_lower
    if mask.sum() == 0:
        mask = clean_df["player_name"].str.lower().str.contains(name_lower, na=False)
    if mask.sum() == 0:
        return None

    row = clean_df.loc[mask.idxmax()]
    return {feat: float(row[feat]) for feat in FEATURE_COLUMNS}


def build_radar_comparison(
    target_name: str,
    similar_name: str,
    player_df: pd.DataFrame,
) -> Optional[Dict]:
    """
    Build normalized radar chart data for two players.
    Kept for backwards-compat; delegates to build_multi_radar.
    """
    result = build_multi_radar(target_name, [similar_name], player_df)
    if result is None:
        return None
    # Flatten to legacy format for any old callers
    return {
        "labels": result["labels"],
        "target_values": result["target_values"],
        "similar_values": result["players"][similar_name]["norm"],
        "target_raw": result["target_raw"],
        "similar_raw": result["players"][similar_name]["raw"],
    }


def build_multi_radar(
    target_name: str,
    compare_names: List[str],
    player_df: pd.DataFrame,
) -> Optional[Dict]:
    """
    Build normalized radar chart data for the target player plus multiple
    comparison players.

    Returns dict with:
      - labels: list of feature display names
      - target_values: normalized [0-1] values for target
      - target_raw: raw feature dict for target
      - players: {name: {"norm": [...], "raw": {...}}} for each comparison
    """
    target_feats = get_player_feature_vector(target_name, player_df)
    if target_feats is None:
        return None

    clean_df = player_df.dropna(subset=FEATURE_COLUMNS)
    mins = clean_df[FEATURE_COLUMNS].min()
    maxs = clean_df[FEATURE_COLUMNS].max()
    ranges = (maxs - mins).replace(0, 1)

    labels = [FEATURE_LABELS[f] for f in FEATURE_COLUMNS]
    target_norm = [
        float((target_feats[f] - mins[f]) / ranges[f])
        for f in FEATURE_COLUMNS
    ]

    players_data = {}
    for name in compare_names:
        feats = get_player_feature_vector(name, player_df)
        if feats is None:
            continue
        norm = [float((feats[f] - mins[f]) / ranges[f]) for f in FEATURE_COLUMNS]
        players_data[name] = {"norm": norm, "raw": feats}

    if not players_data:
        return None

    return {
        "labels": labels,
        "target_values": target_norm,
        "target_raw": target_feats,
        "players": players_data,
    }
