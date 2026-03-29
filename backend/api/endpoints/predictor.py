"""
Monte Carlo Playoff Predictor endpoint.
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/season", tags=["predictor"])


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return df.replace({np.nan: None}).to_dict(orient="records")


@router.get("/{season_year}/monte-carlo")
def run_monte_carlo(
    season_year: int,
    runs: int = Query(10_000, ge=100, le=100_000, description="Number of simulation runs"),
):
    """
    Run a Monte Carlo simulation for the given season and return
    projected standings with playoff probabilities.

    Each row contains: proj_rank, team_code, current_wins, current_losses,
    avg_wins, avg_losses, make_top_4_pct, make_top_6_pct, make_top_10_pct,
    win_rs_pct, games_simulated.
    """
    try:
        from data_pipeline.monte_carlo import fetch_full_schedule, simulate_season
        from streamlit_app.queries import fetch_league_efficiency_landscape
    except ImportError as e:
        logger.error("Import error: %s", e)
        raise HTTPException(status_code=500, detail="Server configuration error")

    # Fetch schedule
    try:
        schedule = fetch_full_schedule(season_year)
    except Exception as e:
        logger.error("Failed to fetch schedule for season %s: %s", season_year, e)
        raise HTTPException(status_code=500, detail="Failed to fetch season schedule")

    if schedule.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No schedule data found for season {season_year}",
        )

    # Fetch efficiency landscape for net ratings
    try:
        eff = fetch_league_efficiency_landscape(season_year)
    except Exception as e:
        logger.error("Failed to fetch efficiency data for season %s: %s", season_year, e)
        raise HTTPException(status_code=500, detail="Failed to fetch team ratings")

    if eff.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No efficiency data found for season {season_year}",
        )

    if "net_rtg" not in eff.columns:
        eff["net_rtg"] = eff["ortg"] - eff["drtg"]

    net_rtg_map = dict(zip(eff["team_code"], eff["net_rtg"]))

    # Run simulation
    try:
        result = simulate_season(schedule, net_rtg_map, runs=runs)
    except Exception as e:
        logger.error("Simulation failed for season %s: %s", season_year, e)
        raise HTTPException(status_code=500, detail="Monte Carlo simulation failed")

    # Select the columns the frontend needs
    keep_cols = [
        "proj_rank", "team_code",
        "current_wins", "current_losses",
        "avg_wins", "avg_losses",
        "make_top_4_pct", "make_top_6_pct", "make_top_10_pct",
        "win_rs_pct", "games_simulated",
    ]
    available = [c for c in keep_cols if c in result.columns]
    return _df_to_records(result[available])
