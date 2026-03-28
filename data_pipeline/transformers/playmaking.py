import logging
from typing import Dict

import numpy as np
import pandas as pd

from data_pipeline.transformers.utils import _markertime_to_seconds

logger = logging.getLogger(__name__)


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
