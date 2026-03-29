"""
Season endpoints — standings and efficiency landscape.
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.engine import Connection

from backend.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/season", tags=["season"])


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert a DataFrame to a JSON-safe list of dicts (NaN → None)."""
    return df.replace({np.nan: None}).to_dict(orient="records")


@router.get("/{season_year}/standings")
def get_standings(season_year: int, conn: Connection = Depends(get_db)):
    """
    Return the league standings for *season_year*.

    Each row contains: team_code, team_name, logo_url, games,
    possessions, pace, ortg, drtg, net_rtg — ordered by net_rtg DESC.
    """
    query = text("""
        WITH team_poss AS (
            SELECT pa.team_code,
                   SUM(pa.possessions) AS poss,
                   MAX(tm.team_name)   AS team_name,
                   MAX(tm.logo_url)    AS logo_url
            FROM player_advanced_stats pa
            JOIN teams tm ON pa.team_code = tm.team_code
            WHERE pa.season = :season
            GROUP BY pa.team_code
        ),
        team_pts AS (
            SELECT
                t.team_code,
                SUM(CASE WHEN g.home_team = t.team_code
                         THEN g.home_score ELSE g.away_score END) AS pts_scored,
                SUM(CASE WHEN g.home_team = t.team_code
                         THEN g.away_score ELSE g.home_score END) AS pts_allowed,
                COUNT(g.gamecode) AS games
            FROM teams t
            JOIN games g
              ON t.team_code = g.home_team OR t.team_code = g.away_team
            WHERE g.season = :season AND g.played = TRUE
            GROUP BY t.team_code
        )
        SELECT
            p.team_code,
            p.team_name,
            p.logo_url,
            t.games,
            p.poss        AS possessions,
            (p.poss / t.games)                                       AS pace,
            (t.pts_scored  / p.poss * 100)                           AS ortg,
            (t.pts_allowed / p.poss * 100)                           AS drtg,
            ((t.pts_scored / p.poss * 100) - (t.pts_allowed / p.poss * 100)) AS net_rtg
        FROM team_poss p
        JOIN team_pts  t ON p.team_code = t.team_code
        ORDER BY net_rtg DESC
    """)

    try:
        df = pd.read_sql(query, conn, params={"season": season_year})
    except Exception as e:
        logger.error("Failed to fetch standings for season %s: %s", season_year, e)
        raise HTTPException(status_code=500, detail="Database query failed")

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No standings data found for season {season_year}",
        )

    return _df_to_records(df)
