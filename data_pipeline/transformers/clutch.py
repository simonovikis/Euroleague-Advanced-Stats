import logging
import numpy as np
import pandas as pd
from data_pipeline.transformers.utils import _markertime_to_seconds

logger = logging.getLogger(__name__)


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
