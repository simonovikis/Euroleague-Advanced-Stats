"""
seasonal_trends.py -- Predictive Seasonal Form Analyzer
========================================================
Aggregates game-level advanced stats (Net Rating, ORtg, DRtg,
True Shooting %, Pace) by Team and Month across multiple seasons,
then trains a GradientBoostingRegressor to predict Expected Net
Rating (xNetRtg) for a given team in a given month.

Features:
  - Team_ID (label-encoded)
  - Month_of_Season (1=Oct, 2=Nov, ..., 8=May)
  - Home_Game_Ratio (fraction of home games that month)
  - Opponent_Strength_Average (mean opponent Net Rating)
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parent.parent / "config"
_MODEL_PATH = _MODEL_DIR / "seasonal_form_model.joblib"

SEASON_MONTH_ORDER = [10, 11, 12, 1, 2, 3, 4, 5]
MONTH_LABELS = {
    10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan",
    2: "Feb", 3: "Mar", 4: "Apr", 5: "May",
}
MONTH_TO_INDEX = {m: i + 1 for i, m in enumerate(SEASON_MONTH_ORDER)}

FEATURE_COLS = [
    "month_index",
    "home_game_ratio",
    "opp_strength_avg",
]


def _get_db_engine():
    try:
        from data_pipeline.load_to_db import get_engine
        eng = get_engine(use_pooler=True)
        from sqlalchemy import text
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception:
        return None


def aggregate_monthly_stats(
    seasons: List[int],
) -> pd.DataFrame:
    """Aggregate per-team, per-month stats from the DB for the given seasons.

    Returns a DataFrame with columns:
        season, team_code, month, month_index,
        net_rtg, ortg, drtg, ts_pct, pace,
        home_game_ratio, opp_strength_avg, games
    """
    engine = _get_db_engine()
    if engine is None:
        logger.warning("Database not available for seasonal trends aggregation.")
        return pd.DataFrame()

    from sqlalchemy import text

    query = text("""
        WITH parsed AS (
            SELECT
                g.season,
                g.gamecode,
                g.home_team,
                g.away_team,
                g.home_score,
                g.away_score,
                CASE
                    WHEN g.game_date ~ '^[A-Z][a-z]+ \\d' THEN
                        EXTRACT(MONTH FROM TO_DATE(g.game_date, 'Mon DD, YYYY'))::INT
                    ELSE NULL
                END AS game_month
            FROM games g
            WHERE g.season = ANY(:seasons)
              AND g.played = TRUE
              AND g.game_date IS NOT NULL
        ),
        team_game AS (
            SELECT season, gamecode, home_team AS team_code, TRUE AS is_home,
                   away_team AS opponent, game_month,
                   home_score AS pts_for, away_score AS pts_against
            FROM parsed WHERE game_month IS NOT NULL
            UNION ALL
            SELECT season, gamecode, away_team AS team_code, FALSE AS is_home,
                   home_team AS opponent, game_month,
                   away_score AS pts_for, home_score AS pts_against
            FROM parsed WHERE game_month IS NOT NULL
        ),
        team_poss AS (
            SELECT
                pas.season, pas.team_code, g_month.game_month,
                SUM(pas.possessions) AS poss,
                AVG(pas.ts_pct) AS ts_pct
            FROM player_advanced_stats pas
            JOIN (
                SELECT DISTINCT season, gamecode, game_month
                FROM (
                    SELECT season, gamecode, home_team, away_team, game_month FROM parsed WHERE game_month IS NOT NULL
                ) sub
            ) g_month ON pas.season = g_month.season AND pas.gamecode = g_month.gamecode
            WHERE pas.season = ANY(:seasons) AND pas.minutes > 0
            GROUP BY pas.season, pas.team_code, g_month.game_month
        ),
        season_net AS (
            SELECT
                tg.season, tg.team_code, tg.game_month,
                COUNT(*) AS games,
                SUM(tg.pts_for) AS total_pts_for,
                SUM(tg.pts_against) AS total_pts_against,
                AVG(CASE WHEN tg.is_home THEN 1.0 ELSE 0.0 END) AS home_game_ratio
            FROM team_game tg
            GROUP BY tg.season, tg.team_code, tg.game_month
        ),
        season_opp_strength AS (
            SELECT
                tg.season, tg.team_code, tg.game_month,
                AVG(opp_net.net_rtg) AS opp_strength_avg
            FROM team_game tg
            LEFT JOIN LATERAL (
                SELECT
                    (SUM(sub.pts_for)::FLOAT / NULLIF(SUM(sub.poss), 0) * 100)
                    - (SUM(sub.pts_against)::FLOAT / NULLIF(SUM(sub.poss), 0) * 100) AS net_rtg
                FROM (
                    SELECT tg2.pts_for, tg2.pts_against, tp2.poss
                    FROM team_game tg2
                    JOIN team_poss tp2
                        ON tg2.season = tp2.season
                        AND tg2.team_code = tp2.team_code
                        AND tg2.game_month = tp2.game_month
                    WHERE tg2.team_code = tg.opponent
                      AND tg2.season = tg.season
                ) sub
            ) opp_net ON TRUE
            GROUP BY tg.season, tg.team_code, tg.game_month
        )
        SELECT
            sn.season, sn.team_code, sn.game_month AS month,
            sn.games,
            sn.home_game_ratio,
            COALESCE(sos.opp_strength_avg, 0) AS opp_strength_avg,
            CASE WHEN tp.poss > 0
                 THEN (sn.total_pts_for::FLOAT / tp.poss * 100)
                 ELSE 0 END AS ortg,
            CASE WHEN tp.poss > 0
                 THEN (sn.total_pts_against::FLOAT / tp.poss * 100)
                 ELSE 0 END AS drtg,
            CASE WHEN tp.poss > 0
                 THEN (sn.total_pts_for::FLOAT / tp.poss * 100)
                      - (sn.total_pts_against::FLOAT / tp.poss * 100)
                 ELSE 0 END AS net_rtg,
            COALESCE(tp.ts_pct, 0) AS ts_pct,
            CASE WHEN sn.games > 0
                 THEN tp.poss / sn.games
                 ELSE 0 END AS pace
        FROM season_net sn
        JOIN team_poss tp
            ON sn.season = tp.season
            AND sn.team_code = tp.team_code
            AND sn.game_month = tp.game_month
        LEFT JOIN season_opp_strength sos
            ON sn.season = sos.season
            AND sn.team_code = sos.team_code
            AND sn.game_month = sos.game_month
        ORDER BY sn.season, sn.team_code, sn.game_month
    """)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"seasons": list(seasons)})
    except Exception as e:
        logger.warning("Failed to aggregate monthly stats: %s: %s", type(e).__name__, e)
        return pd.DataFrame()

    if df.empty:
        return df

    df["month_index"] = df["month"].map(MONTH_TO_INDEX)
    df = df.dropna(subset=["month_index"])
    df["month_index"] = df["month_index"].astype(int)

    return df


def train_seasonal_form_model(
    seasons: List[int],
) -> Optional[Pipeline]:
    """Train a GradientBoostingRegressor to predict monthly Net Rating.

    Returns a fitted Pipeline or None if insufficient data.
    """
    df = aggregate_monthly_stats(seasons)
    if df.empty or len(df) < 20:
        logger.warning("Insufficient data for seasonal form model (%d rows).", len(df))
        return None

    X = df[FEATURE_COLS].fillna(0)
    y = df["net_rtg"]

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("regressor", GradientBoostingRegressor(
            n_estimators=150,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )),
    ])

    pipeline.fit(X, y)
    train_score = pipeline.score(X, y)
    logger.info(
        "Seasonal form model trained on %d samples -- R^2 = %.3f",
        len(X), train_score,
    )
    return pipeline


def save_model(model: Pipeline, path: Path = None) -> Path:
    import joblib
    path = path or _MODEL_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    logger.info("Seasonal form model saved to %s", path)
    return path


def load_model(path: Path = None) -> Optional[Pipeline]:
    import joblib
    path = path or _MODEL_PATH
    if not path.exists():
        return None
    return joblib.load(path)


def predict_team_form_curve(
    model: Pipeline,
    team_monthly_df: pd.DataFrame,
) -> pd.DataFrame:
    """Predict xNetRtg for each month in the season for one team.

    Args:
        model: Trained seasonal form pipeline.
        team_monthly_df: DataFrame with columns month_index,
            home_game_ratio, opp_strength_avg for each month.

    Returns:
        Input DataFrame with an added 'x_net_rtg' column.
    """
    X = team_monthly_df[FEATURE_COLS].fillna(0)
    team_monthly_df = team_monthly_df.copy()
    team_monthly_df["x_net_rtg"] = model.predict(X)
    return team_monthly_df


def build_team_form_features(
    monthly_df: pd.DataFrame,
    team_code: str,
) -> pd.DataFrame:
    """Build per-month feature rows for a single team across all seasons.

    If the team has data for a month, uses actual home_game_ratio and
    opp_strength_avg. For prediction, fills missing months with
    season-average values.
    """
    team_df = monthly_df[monthly_df["team_code"] == team_code].copy()
    if team_df.empty:
        return pd.DataFrame()

    avg_home_ratio = team_df["home_game_ratio"].mean()
    avg_opp_strength = team_df["opp_strength_avg"].mean()

    rows = []
    for month_idx in range(1, 9):
        month_data = team_df[team_df["month_index"] == month_idx]
        if not month_data.empty:
            rows.append({
                "month_index": month_idx,
                "home_game_ratio": month_data["home_game_ratio"].mean(),
                "opp_strength_avg": month_data["opp_strength_avg"].mean(),
            })
        else:
            rows.append({
                "month_index": month_idx,
                "home_game_ratio": avg_home_ratio,
                "opp_strength_avg": avg_opp_strength,
            })

    return pd.DataFrame(rows)


def generate_insights(
    monthly_df: pd.DataFrame,
    team_code: str,
    predicted_curve: pd.DataFrame,
) -> str:
    """Generate a textual insight string about the team's seasonal form."""
    team_df = monthly_df[monthly_df["team_code"] == team_code].copy()
    if team_df.empty or predicted_curve.empty:
        return ""

    season_avg = predicted_curve["x_net_rtg"].mean()

    feb_mar = predicted_curve[predicted_curve["month_index"].isin([5, 6])]
    apr_may = predicted_curve[predicted_curve["month_index"].isin([7, 8])]

    insights = []

    if not feb_mar.empty:
        feb_mar_avg = feb_mar["x_net_rtg"].mean()
        diff = feb_mar_avg - season_avg
        if abs(diff) > 1.0:
            direction = "slump" if diff < 0 else "surge"
            insights.append(
                f"the model detects a {direction} ({diff:+.1f} Net Rating) "
                f"during February and March"
            )

    if not apr_may.empty:
        apr_may_avg = apr_may["x_net_rtg"].mean()
        diff = apr_may_avg - season_avg
        if abs(diff) > 1.0:
            direction = "surge" if diff > 0 else "drop-off"
            insights.append(
                f"a {direction} ({diff:+.1f} Net Rating) starting in April"
            )

    num_seasons = team_df["season"].nunique()
    if insights:
        return (
            f"Based on {num_seasons} season(s), "
            + ", followed by ".join(insights)
            + "."
        )

    return (
        f"Based on {num_seasons} season(s), no statistically significant "
        f"monthly pattern was detected (season avg xNetRtg: {season_avg:+.1f})."
    )


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    seasons = [2025, 2024, 2023]
    if len(sys.argv) > 1:
        seasons = [int(s) for s in sys.argv[1:]]

    logger.info("Training seasonal form model on seasons: %s", seasons)
    model = train_seasonal_form_model(seasons)
    if model is None:
        logger.error("Training failed -- insufficient data.")
        sys.exit(1)

    path = save_model(model)
    logger.info("Done. Model saved to %s", path)
