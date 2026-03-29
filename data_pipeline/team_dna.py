"""
team_dna.py -- Team DNA & Stylistic Clustering Engine
======================================================
Extracts the "Four Factors" of basketball success (eFG%, TOV%, ORB%,
FT Rate), plus Pace and 3-Point Attempt Rate for all teams, then uses
K-Means clustering to group teams into distinct stylistic buckets.

Features:
  - eFG%          = (FGM2 + 1.5 * FGM3) / FGA
  - TOV%          = TOV / (FGA + 0.44 * FTA + TOV)
  - ORB%          = ORB / (ORB + Opp_DRB)
  - FT Rate       = FTA / FGA
  - Pace          = Possessions per game
  - 3PA Rate      = FGA3 / FGA
"""

import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

FEATURE_COLS = ["efg_pct", "tov_pct", "orb_pct", "ft_rate", "pace", "three_pt_rate"]

CLUSTER_TEMPLATES = {
    "Pace & Space": {"three_pt_rate": 1, "pace": 1, "efg_pct": 0.5},
    "Defensive Grinders": {"tov_pct": -1, "pace": -1, "orb_pct": 0.5},
    "Rebound Dominant": {"orb_pct": 1, "ft_rate": 0.5, "pace": -0.5},
    "Balanced / Elite": {"efg_pct": 1, "tov_pct": -1, "orb_pct": 0.5, "ft_rate": 0.5},
}

CLUSTER_DESCRIPTIONS = {
    "Pace & Space": (
        "characterized by high tempo, frequent three-point attempts, "
        "and efficient perimeter shooting. These teams stretch the floor "
        "and create open looks through ball movement and spacing."
    ),
    "Defensive Grinders": (
        "characterized by slow pace, disciplined ball security, "
        "and stifling defense. These teams win by limiting opponent "
        "possessions and forcing difficult shots."
    ),
    "Rebound Dominant": (
        "characterized by aggressive offensive rebounding and physical play. "
        "These teams create second-chance opportunities and get to the "
        "free-throw line at a high rate."
    ),
    "Balanced / Elite": (
        "characterized by efficient shooting, low turnovers, and strong "
        "rebounding across the board. These teams excel in multiple areas "
        "without relying on a single stylistic identity."
    ),
}


def _get_db_engine():
    try:
        from data_pipeline.load_to_db import get_engine
        from sqlalchemy import text
        eng = get_engine(use_pooler=True)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception:
        return None


def extract_team_four_factors(season: int) -> pd.DataFrame:
    """Extract Four Factors + Pace + 3PA Rate for all teams.

    Tries DB first, falls back to API.

    Returns DataFrame with columns:
        team_code, efg_pct, tov_pct, orb_pct, ft_rate, pace, three_pt_rate
    """
    engine = _get_db_engine()
    if engine is not None:
        df = _extract_from_db(season, engine)
        if not df.empty:
            return df
    return _extract_from_api(season)


def _extract_from_db(season: int, engine) -> pd.DataFrame:
    from sqlalchemy import text

    query = text("""
        WITH team_game AS (
            SELECT
                season, gamecode, team_code,
                SUM(fgm2) AS fgm2, SUM(fga2) AS fga2,
                SUM(fgm3) AS fgm3, SUM(fga3) AS fga3,
                SUM(ftm) AS ftm, SUM(fta) AS fta,
                SUM(off_rebounds) AS orb, SUM(def_rebounds) AS drb,
                SUM(turnovers) AS tov, SUM(points) AS pts,
                SUM(possessions) AS poss
            FROM player_advanced_stats
            WHERE season = :season AND minutes > 0
            GROUP BY season, gamecode, team_code
        ),
        team_with_opp AS (
            SELECT
                t.team_code,
                t.gamecode,
                t.fgm2, t.fga2, t.fgm3, t.fga3,
                t.ftm, t.fta, t.orb, t.drb, t.tov,
                t.pts, t.poss,
                o.drb AS opp_drb
            FROM team_game t
            JOIN games g
                ON t.season = g.season AND t.gamecode = g.gamecode
            JOIN team_game o
                ON t.season = o.season AND t.gamecode = o.gamecode
                AND o.team_code = CASE
                    WHEN t.team_code = g.home_team THEN g.away_team
                    ELSE g.home_team END
        ),
        team_season AS (
            SELECT
                team_code,
                SUM(fgm2) AS fgm2, SUM(fga2) AS fga2,
                SUM(fgm3) AS fgm3, SUM(fga3) AS fga3,
                SUM(ftm) AS ftm, SUM(fta) AS fta,
                SUM(orb) AS orb, SUM(drb) AS drb,
                SUM(tov) AS tov, SUM(pts) AS pts,
                SUM(poss) AS poss, SUM(opp_drb) AS opp_drb,
                COUNT(DISTINCT gamecode) AS games
            FROM team_with_opp
            GROUP BY team_code
        )
        SELECT
            team_code,
            CASE WHEN (fga2 + fga3) > 0
                THEN (fgm2 + 1.5 * fgm3) / (fga2 + fga3)
                ELSE 0 END AS efg_pct,
            CASE WHEN ((fga2 + fga3) + 0.44 * fta + tov) > 0
                THEN tov / ((fga2 + fga3) + 0.44 * fta + tov)
                ELSE 0 END AS tov_pct,
            CASE WHEN (orb + opp_drb) > 0
                THEN orb::FLOAT / (orb + opp_drb)
                ELSE 0 END AS orb_pct,
            CASE WHEN (fga2 + fga3) > 0
                THEN fta::FLOAT / (fga2 + fga3)
                ELSE 0 END AS ft_rate,
            CASE WHEN games > 0
                THEN poss / games
                ELSE 0 END AS pace,
            CASE WHEN (fga2 + fga3) > 0
                THEN fga3::FLOAT / (fga2 + fga3)
                ELSE 0 END AS three_pt_rate
        FROM team_season
        ORDER BY team_code
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"season": season})
        if not df.empty:
            logger.info("Extracted Four Factors from DB for %d teams.", len(df))
        return df
    except Exception as e:
        logger.warning("DB Four Factors extraction failed: %s", e)
        return pd.DataFrame()


def _extract_from_api(season: int) -> pd.DataFrame:
    """Compute Four Factors from the Euroleague API team stats."""
    try:
        from euroleague_api.team_stats import TeamStats
        from data_pipeline.extractors import apply_team_aliases

        ts = TeamStats("E")
        off_df = ts.get_team_stats(
            endpoint="traditional",
            params={"Season": season},
            phase_type_code="RS",
            statistic_mode="Accumulated",
        )
        def_df = ts.get_team_stats(
            endpoint="opponentsTraditional",
            params={"Season": season},
            phase_type_code="RS",
            statistic_mode="Accumulated",
        )

        if off_df is None or off_df.empty or def_df is None or def_df.empty:
            return pd.DataFrame()

        off_df = off_df.rename(columns={"team.code": "team_code"})
        def_df = def_df.rename(columns={"team.code": "team_code"})
        off_df = apply_team_aliases(off_df, ["team_code"])
        def_df = apply_team_aliases(def_df, ["team_code"])

        off_cols = [
            "team_code", "gamesPlayed",
            "twoPointersMade", "twoPointersAttempted",
            "threePointersMade", "threePointersAttempted",
            "freeThrowsAttempted", "offensiveRebounds", "turnovers",
        ]
        def_cols = ["team_code", "defensiveRebounds"]

        off_available = [c for c in off_cols if c in off_df.columns]
        def_available = [c for c in def_cols if c in def_df.columns]

        merged = pd.merge(
            off_df[off_available],
            def_df[def_available],
            on="team_code",
            suffixes=("_off", "_def"),
        )

        rows = []
        for _, r in merged.iterrows():
            code = r["team_code"]
            fga2 = float(r.get("twoPointersAttempted", 0) or 0)
            fga3 = float(r.get("threePointersAttempted", 0) or 0)
            fgm2 = float(r.get("twoPointersMade", 0) or 0)
            fgm3 = float(r.get("threePointersMade", 0) or 0)
            fta = float(r.get("freeThrowsAttempted", 0) or 0)
            orb = float(r.get("offensiveRebounds", 0) or 0)
            opp_drb = float(r.get("defensiveRebounds", 0) or 0)
            tov = float(r.get("turnovers", 0) or 0)
            games = float(r.get("gamesPlayed", 1) or 1)

            fga = fga2 + fga3
            poss = fga + 0.44 * fta - orb + tov

            rows.append({
                "team_code": code,
                "efg_pct": (fgm2 + 1.5 * fgm3) / fga if fga > 0 else 0,
                "tov_pct": tov / (fga + 0.44 * fta + tov) if (fga + 0.44 * fta + tov) > 0 else 0,
                "orb_pct": orb / (orb + opp_drb) if (orb + opp_drb) > 0 else 0,
                "ft_rate": fta / fga if fga > 0 else 0,
                "pace": poss / games if games > 0 else 0,
                "three_pt_rate": fga3 / fga if fga > 0 else 0,
            })

        df = pd.DataFrame(rows)
        logger.info("Extracted Four Factors from API for %d teams.", len(df))
        return df
    except Exception as e:
        logger.warning("API Four Factors extraction failed: %s", e)
        return pd.DataFrame()


def cluster_teams(
    df: pd.DataFrame,
    k: int = 4,
    seed: int = 42,
) -> Tuple[pd.DataFrame, KMeans, StandardScaler, PCA]:
    """Standardize features, run K-Means, and assign semantic cluster names.

    Returns (result_df, kmeans_model, scaler, pca).
    result_df has additional columns: cluster_id, cluster_name, pc1, pc2.
    """
    features = df[FEATURE_COLS].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    kmeans = KMeans(n_clusters=k, random_state=seed, n_init=10)
    df = df.copy()
    df["cluster_id"] = kmeans.fit_predict(X_scaled)

    pca = PCA(n_components=2, random_state=seed)
    pcs = pca.fit_transform(X_scaled)
    df["pc1"] = pcs[:, 0]
    df["pc2"] = pcs[:, 1]

    cluster_names = _assign_cluster_names(kmeans.cluster_centers_)
    df["cluster_name"] = df["cluster_id"].map(cluster_names)

    return df, kmeans, scaler, pca


def _assign_cluster_names(centroids: np.ndarray) -> Dict[int, str]:
    """Match each centroid to the best-fit semantic template via greedy scoring."""
    names = list(CLUSTER_TEMPLATES.keys())
    feature_names = FEATURE_COLS
    assigned: Dict[int, str] = {}
    used_names: set = set()

    scores = np.zeros((len(centroids), len(names)))
    for ci, centroid in enumerate(centroids):
        for ni, name in enumerate(names):
            template = CLUSTER_TEMPLATES[name]
            score = sum(
                centroid[fi] * template[feat]
                for fi, feat in enumerate(feature_names)
                if feat in template
            )
            scores[ci, ni] = score

    for _ in range(min(len(centroids), len(names))):
        best_score = -np.inf
        best_ci, best_ni = 0, 0
        for ci in range(len(centroids)):
            if ci in assigned:
                continue
            for ni in range(len(names)):
                if names[ni] in used_names:
                    continue
                if scores[ci, ni] > best_score:
                    best_score = scores[ci, ni]
                    best_ci, best_ni = ci, ni
        assigned[best_ci] = names[best_ni]
        used_names.add(names[best_ni])

    for ci in range(len(centroids)):
        if ci not in assigned:
            assigned[ci] = f"Cluster {ci + 1}"

    return assigned


def compute_percentile_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """Add percentile rank columns for each feature (0-100).

    TOV% is inverted so that lower turnover rate = higher percentile.
    """
    df = df.copy()
    for col in FEATURE_COLS:
        df[f"{col}_pctl"] = df[col].rank(pct=True) * 100
    df["tov_pct_pctl"] = 100 - df["tov_pct_pctl"]
    return df


def get_cluster_description(cluster_name: str) -> str:
    """Return the prose description for a cluster name."""
    return CLUSTER_DESCRIPTIONS.get(
        cluster_name,
        "with a unique stylistic profile that doesn't fit neatly into "
        "a standard archetype.",
    )
