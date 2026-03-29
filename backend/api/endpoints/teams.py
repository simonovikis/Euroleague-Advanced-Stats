"""
Team DNA endpoints — clustering, percentile radar profiles.
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from data_pipeline.team_dna import (
    extract_team_four_factors,
    cluster_teams,
    compute_percentile_ranks,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/teams", tags=["teams"])

FEATURE_LABELS = {
    "efg_pct": "eFG%",
    "tov_pct": "TOV% (inv.)",
    "orb_pct": "ORB%",
    "ft_rate": "FT Rate",
    "pace": "Pace",
    "three_pt_rate": "3PA Rate",
}

CLUSTER_COLORS = {
    "Pace & Space": "#6366f1",
    "Defensive Grinders": "#ef4444",
    "Rebound Dominant": "#f59e0b",
    "Balanced / Elite": "#10b981",
}


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return df.replace({np.nan: None}).to_dict(orient="records")


@router.get("/dna")
def get_all_team_dna(season: int = Query(..., description="Season year")):
    """
    Return the Team DNA clustering + percentile profiles for every team
    in the given season.

    Response shape::

        {
          "teams": [
            {
              "team_code": "OLY",
              "cluster_name": "Balanced / Elite",
              "cluster_color": "#10b981",
              "percentiles": [89.2, 72.1, ...],
              "categories": ["eFG%", "TOV% (inv.)", ...],
              "raw": { "efg_pct": 0.54, ... }
            },
            ...
          ]
        }
    """
    try:
        df = extract_team_four_factors(season)
    except Exception as e:
        logger.error("Failed to extract four-factors for season %s: %s", season, e)
        raise HTTPException(status_code=500, detail="Failed to extract team data")

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No team data found for season {season}",
        )

    try:
        clustered, *_ = cluster_teams(df)
        result = compute_percentile_ranks(clustered)
    except Exception as e:
        logger.error("Clustering failed for season %s: %s", season, e)
        raise HTTPException(status_code=500, detail="Clustering computation failed")

    categories = list(FEATURE_LABELS.values())
    pctl_cols = [f"{c}_pctl" for c in FEATURE_LABELS]

    teams = []
    for _, row in result.iterrows():
        cluster_name = row.get("cluster_name", "Unknown")
        teams.append({
            "team_code": row["team_code"],
            "cluster_name": cluster_name,
            "cluster_color": CLUSTER_COLORS.get(cluster_name, "#6b7280"),
            "percentiles": [
                round(row[c], 1) if pd.notna(row.get(c)) else 0
                for c in pctl_cols
            ],
            "categories": categories,
            "raw": {
                k: round(row[k], 4) if pd.notna(row.get(k)) else None
                for k in FEATURE_LABELS
            },
        })

    return {"teams": teams}


@router.get("/{team_code}/dna")
def get_team_dna(team_code: str, season: int = Query(..., description="Season year")):
    """
    Return the DNA profile for a single team.
    """
    all_data = get_all_team_dna(season)
    tc = team_code.upper()
    for team in all_data["teams"]:
        if team["team_code"] == tc:
            return team

    raise HTTPException(status_code=404, detail=f"Team {tc} not found in season {season}")
