"""
ml_train.py -- Lineup Expected Net Rating Model
=================================================
Feature engineering and model training for predicting the Expected
Net Rating of any 5-man lineup based on individual advanced metrics.

Features per player (aggregated across the lineup):
  - TS%            (scoring efficiency)
  - tUSG%          (offensive involvement)
  - Stop Rate      (defensive impact)
  - Off Rating     (points generated per 100 possessions)
  - Def Rating     (points allowed per 100 possessions)
  - Assist Ratio   (playmaking share)
  - ORB%           (offensive rebounding contribution)
  - DRB%           (defensive rebounding contribution)
  - 3PA Rate       (spacing / perimeter orientation)
  - FT Rate        (ability to draw fouls)

For a 5-man lineup, features are aggregated as:
  - Mean of each stat across the 5 players
  - Std (diversity) of TS% and tUSG% to capture balance vs. hero-ball
  - Max Stop Rate (anchor defender signal)
  - Sum of Assist Ratio (total playmaking bandwidth)

Target: Net Rating of the actual lineup from historical PBP data.

Model: GradientBoostingRegressor (robust to small datasets, handles
nonlinear interactions between player profiles).
"""

import logging
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

_MODEL_DIR = Path(__file__).resolve().parent.parent / "config"

PLAYER_FEATURES = [
    "ts_pct",
    "true_usg_pct",
    "stop_rate",
    "off_rating",
    "assist_ratio",
    "orb_pct",
    "drb_pct",
    "three_pt_rate",
    "ft_rate",
]

LINEUP_FEATURE_COLS = [
    "mean_ts_pct",
    "mean_true_usg_pct",
    "mean_stop_rate",
    "mean_off_rating",
    "mean_assist_ratio",
    "mean_orb_pct",
    "mean_drb_pct",
    "mean_three_pt_rate",
    "mean_ft_rate",
    "std_ts_pct",
    "std_true_usg_pct",
    "max_stop_rate",
    "sum_assist_ratio",
]

RADAR_CATEGORIES = {
    "Offense": ["mean_ts_pct", "mean_off_rating", "mean_true_usg_pct"],
    "Defense": ["mean_stop_rate", "max_stop_rate"],
    "Rebounding": ["mean_orb_pct", "mean_drb_pct"],
    "Spacing": ["mean_three_pt_rate", "mean_ft_rate"],
    "Playmaking": ["mean_assist_ratio", "sum_assist_ratio"],
}


def _get_db_engine():
    """Get DB engine if available, else None."""
    try:
        from data_pipeline.load_to_db import get_engine
        eng = get_engine(use_pooler=True)
        from sqlalchemy import text
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception:
        return None


def _load_adv_stats_from_db(engine, season: int) -> pd.DataFrame:
    """Load all player_advanced_stats rows for a season from DB."""
    from sqlalchemy import text
    query = text("""
        SELECT
            season    AS "Season",
            gamecode  AS "Gamecode",
            player_id, player_name, team_code, is_home,
            minutes, points,
            fgm2, fga2, fgm3, fga3, ftm, fta,
            off_rebounds, def_rebounds, total_rebounds,
            assists, steals, turnovers,
            blocks_favour, blocks_against,
            fouls_committed, fouls_received, plus_minus,
            possessions, ts_pct, off_rating, def_rating
        FROM player_advanced_stats
        WHERE season = :season AND minutes > 0
        ORDER BY gamecode, player_name
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"season": season})


def _load_boxscores_from_db(engine, season: int) -> pd.DataFrame:
    """Load raw boxscores for a full season from DB."""
    from sqlalchemy import text
    query = text("""
        SELECT
            season AS "Season", gamecode AS "Gamecode",
            player_id AS "Player_ID", player AS "Player",
            team AS "Team", home AS "Home",
            is_starter AS "IsStarter", is_playing AS "IsPlaying",
            dorsal AS "Dorsal", minutes AS "Minutes",
            points AS "Points",
            fgm2 AS "FieldGoalsMade2", fga2 AS "FieldGoalsAttempted2",
            fgm3 AS "FieldGoalsMade3", fga3 AS "FieldGoalsAttempted3",
            ftm AS "FreeThrowsMade", fta AS "FreeThrowsAttempted",
            off_rebounds AS "OffensiveRebounds",
            def_rebounds AS "DefensiveRebounds",
            total_rebounds AS "TotalRebounds",
            assists AS "Assistances", steals AS "Steals",
            turnovers AS "Turnovers",
            blocks_favour AS "BlocksFavour",
            blocks_against AS "BlocksAgainst",
            fouls_committed AS "FoulsCommited",
            fouls_received AS "FoulsReceived",
            valuation AS "Valuation",
            plus_minus AS "Plusminus"
        FROM boxscores
        WHERE season = :season
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"season": season})


def _load_pbp_from_db(engine, season: int) -> pd.DataFrame:
    """Load play-by-play for a full season from DB."""
    from sqlalchemy import text
    query = text("""
        SELECT
            season AS "Season", gamecode AS "Gamecode",
            period AS "PERIOD", playtype AS "PLAYTYPE",
            player_id AS "PLAYER_ID", player AS "PLAYER",
            codeteam AS "CODETEAM", markertime AS "MARKERTIME",
            numberofplay AS "NUMBEROFPLAY", comment AS "COMMENT"
        FROM play_by_play
        WHERE season = :season
        ORDER BY id ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"season": season})
    if not df.empty and "NUMBEROFPLAY" in df.columns:
        df["TRUE_NUMBEROFPLAY"] = df["NUMBEROFPLAY"]
    return df


def _compute_player_season_features(
    season: int,
    competition: str = "E",
) -> pd.DataFrame:
    """Compute per-player season-level feature vectors.

    DB-first: reads player_advanced_stats from PostgreSQL.
    Falls back to the Euroleague API if DB is unavailable.

    Returns one row per player with aggregated advanced stats.
    """
    from data_pipeline.transformers import compute_advanced_stats

    adv_df = pd.DataFrame()

    # --- DB-first path ---
    engine = _get_db_engine()
    if engine is not None:
        try:
            adv_df = _load_adv_stats_from_db(engine, season)
            if not adv_df.empty:
                logger.info("Loaded %d adv stat rows from DB for season %d", len(adv_df), season)
        except Exception as e:
            logger.warning("DB read failed for season %d, falling back to API: %s", season, e)
            adv_df = pd.DataFrame()

    # --- API fallback ---
    if adv_df.empty:
        from data_pipeline.extractors import (
            get_season_schedule,
            extract_team_season_data,
            apply_team_aliases,
        )

        schedule = get_season_schedule(season, competition)
        if schedule.empty:
            return pd.DataFrame()

        schedule = apply_team_aliases(schedule, ["home_code", "away_code"])
        teams = sorted(
            set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())
        )

        all_adv = []
        for team_code in teams:
            try:
                raw = extract_team_season_data(season, team_code, competition)
                if raw["boxscore"].empty:
                    continue
                adv = compute_advanced_stats(raw["boxscore"])
                if not adv.empty:
                    all_adv.append(adv)
            except Exception as e:
                logger.warning("Failed to extract data for %s/%s: %s", season, team_code, e)
                continue

        if not all_adv:
            return pd.DataFrame()

        adv_df = pd.concat(all_adv, ignore_index=True)
    adv_df = adv_df[adv_df["minutes"] > 0].copy()

    adv_df["fga"] = adv_df["fga2"].fillna(0) + adv_df["fga3"].fillna(0)
    adv_df["assist_ratio"] = np.where(
        adv_df["fga"] + 0.44 * adv_df["fta"] + adv_df["turnovers"] + adv_df["assists"] > 0,
        adv_df["assists"] / (adv_df["fga"] + 0.44 * adv_df["fta"] + adv_df["turnovers"] + adv_df["assists"]),
        0,
    )
    total_reb = adv_df["total_rebounds"].fillna(0)
    adv_df["orb_pct"] = np.where(
        total_reb > 0, adv_df["off_rebounds"].fillna(0) / total_reb, 0,
    )
    adv_df["drb_pct"] = np.where(
        total_reb > 0, adv_df["def_rebounds"].fillna(0) / total_reb, 0,
    )
    adv_df["three_pt_rate"] = np.where(
        adv_df["fga"] > 0, adv_df["fga3"].fillna(0) / adv_df["fga"], 0,
    )
    adv_df["ft_rate"] = np.where(
        adv_df["fga"] > 0, adv_df["fta"].fillna(0) / adv_df["fga"], 0,
    )

    agg_dict = {
        "minutes": "sum",
        "points": "sum",
        "fga2": "sum",
        "fga3": "sum",
        "fga": "sum",
        "fta": "sum",
        "ftm": "sum",
        "fgm2": "sum",
        "fgm3": "sum",
        "off_rebounds": "sum",
        "def_rebounds": "sum",
        "total_rebounds": "sum",
        "assists": "sum",
        "steals": "sum",
        "turnovers": "sum",
        "blocks_favour": "sum",
        "player_name": "first",
        "team_code": "first",
    }
    agg_dict = {k: v for k, v in agg_dict.items() if k in adv_df.columns}

    season_df = adv_df.groupby("player_id").agg(agg_dict).reset_index()
    season_df = season_df[season_df["minutes"] > 0].copy()

    games_played = adv_df.groupby("player_id")["Gamecode"].nunique().reset_index()
    games_played.columns = ["player_id", "games"]
    season_df = season_df.merge(games_played, on="player_id", how="left")
    season_df["minutes_pg"] = season_df["minutes"] / season_df["games"].clip(lower=1)

    fga_total = season_df["fga"].clip(lower=1)
    tsa = fga_total + 0.44 * season_df["fta"].fillna(0)
    season_df["ts_pct"] = np.where(tsa > 0, season_df["points"] / (2.0 * tsa), 0)

    involvement = (
        fga_total + 0.44 * season_df["fta"].fillna(0)
        + season_df["turnovers"].fillna(0)
        + season_df["assists"].fillna(0)
    )
    possessions_est = fga_total + 0.44 * season_df["fta"].fillna(0) - season_df["off_rebounds"].fillna(0) + season_df["turnovers"].fillna(0)
    possessions_est = possessions_est.clip(lower=1)
    season_df["true_usg_pct"] = involvement / possessions_est

    stops = season_df["steals"].fillna(0) + season_df["blocks_favour"].fillna(0) + season_df["def_rebounds"].fillna(0)
    season_df["stop_rate"] = stops / possessions_est

    season_df["off_rating"] = np.where(
        possessions_est > 0, (season_df["points"] / possessions_est) * 100, 0,
    )

    total_reb = season_df["total_rebounds"].clip(lower=1)
    season_df["assist_ratio"] = np.where(
        involvement > 0, season_df["assists"] / involvement, 0,
    )
    season_df["orb_pct"] = season_df["off_rebounds"].fillna(0) / total_reb
    season_df["drb_pct"] = season_df["def_rebounds"].fillna(0) / total_reb
    season_df["three_pt_rate"] = np.where(
        fga_total > 0, season_df["fga3"].fillna(0) / fga_total, 0,
    )
    season_df["ft_rate"] = np.where(
        fga_total > 0, season_df["fta"].fillna(0) / fga_total, 0,
    )

    return season_df


def _compute_lineup_features_from_players(
    player_features: pd.DataFrame,
    player_ids: List[str],
) -> Optional[pd.Series]:
    """Build the lineup feature vector from 5 individual player profiles."""
    rows = player_features[player_features["player_id"].isin(player_ids)]
    if len(rows) < 5:
        return None

    feats = {}
    for col in PLAYER_FEATURES:
        vals = rows[col].fillna(0).values
        feats[f"mean_{col}"] = vals.mean()

    feats["std_ts_pct"] = rows["ts_pct"].fillna(0).std()
    feats["std_true_usg_pct"] = rows["true_usg_pct"].fillna(0).std()
    feats["max_stop_rate"] = rows["stop_rate"].fillna(0).max()
    feats["sum_assist_ratio"] = rows["assist_ratio"].fillna(0).sum()

    return pd.Series(feats)


def _build_lineup_stats_for_season_db(
    engine, season: int, min_events: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load boxscores + PBP from DB, compute lineup stats for all teams.

    Returns (lineup_stats_df, boxscores_df).
    """
    from data_pipeline.transformers import track_lineups, compute_lineup_stats

    all_box = _load_boxscores_from_db(engine, season)
    all_pbp = _load_pbp_from_db(engine, season)

    if all_box.empty or all_pbp.empty:
        return pd.DataFrame(), all_box

    gamecodes = sorted(set(all_box["Gamecode"].unique()) & set(all_pbp["Gamecode"].unique()))

    pbp_lu_list = []
    for gc in gamecodes:
        g_pbp = all_pbp[all_pbp["Gamecode"] == gc]
        g_box = all_box[all_box["Gamecode"] == gc]
        if not g_pbp.empty and not g_box.empty:
            try:
                pbp_lu_list.append(track_lineups(g_pbp, g_box))
            except Exception:
                pass

    if not pbp_lu_list:
        return pd.DataFrame(), all_box

    pbp_lu = pd.concat(pbp_lu_list, ignore_index=True)
    lineup_stats = compute_lineup_stats(pbp_lu, all_box, min_events=min_events)
    return lineup_stats, all_box


def build_training_data(
    seasons: List[int],
    competition: str = "E",
    min_events: int = 30,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Build training data from historical lineup stats.

    DB-first: reads boxscores and PBP from PostgreSQL for all games at once.
    Falls back to the Euroleague API per-team if DB is unavailable.

    Args:
        seasons: List of season years to include.
        min_events: Minimum PBP events for a lineup to be included.

    Returns:
        (X, y) where X is the feature matrix and y is the target net_rtg.
    """
    all_X = []
    all_y = []

    engine = _get_db_engine()

    for season in seasons:
        logger.info("Building training data for season %d", season)
        player_feats = _compute_player_season_features(season, competition)
        if player_feats.empty:
            continue

        lineup_stats = pd.DataFrame()

        # --- DB-first: load entire season at once ---
        if engine is not None:
            try:
                lineup_stats, _ = _build_lineup_stats_for_season_db(engine, season, min_events)
                if not lineup_stats.empty:
                    logger.info("Loaded %d lineup rows from DB for season %d", len(lineup_stats), season)
            except Exception as e:
                logger.warning("DB lineup build failed for season %d: %s", season, e)
                lineup_stats = pd.DataFrame()

        # --- API fallback: per-team extraction ---
        if lineup_stats.empty:
            from data_pipeline.extractors import (
                get_season_schedule,
                extract_team_season_data,
                apply_team_aliases,
            )
            from data_pipeline.transformers import (
                track_lineups,
                compute_lineup_stats,
            )

            schedule = get_season_schedule(season, competition)
            if schedule.empty:
                continue
            schedule = apply_team_aliases(schedule, ["home_code", "away_code"])
            teams = sorted(
                set(schedule["home_code"].unique()) | set(schedule["away_code"].unique())
            )

            season_lineup_frames = []
            for team_code in teams:
                try:
                    raw = extract_team_season_data(season, team_code, competition)
                    if raw["boxscore"].empty or raw["pbp"].empty:
                        continue

                    pbp_lu_list = []
                    for gc in raw["pbp"]["Gamecode"].unique():
                        g_pbp = raw["pbp"][raw["pbp"]["Gamecode"] == gc]
                        g_box = raw["boxscore"][raw["boxscore"]["Gamecode"] == gc]
                        if not g_pbp.empty and not g_box.empty:
                            pbp_lu_list.append(track_lineups(g_pbp, g_box))

                    if not pbp_lu_list:
                        continue

                    pbp_lu = pd.concat(pbp_lu_list, ignore_index=True)
                    team_lu = compute_lineup_stats(pbp_lu, raw["boxscore"], min_events=min_events)
                    if not team_lu.empty:
                        season_lineup_frames.append(team_lu)
                except Exception as e:
                    logger.warning("Failed lineup extraction for %s/%d: %s", team_code, season, e)
                    continue

            if season_lineup_frames:
                lineup_stats = pd.concat(season_lineup_frames, ignore_index=True)

        if lineup_stats.empty:
            continue

        # Match lineup net ratings to player feature vectors
        for _, lu_row in lineup_stats.iterrows():
            lineup_pids = list(lu_row["lineup"])
            lf = _compute_lineup_features_from_players(player_feats, lineup_pids)
            if lf is not None:
                all_X.append(lf)
                all_y.append(lu_row["net_rtg"])

    if not all_X:
        return pd.DataFrame(), pd.Series(dtype=float)

    X = pd.DataFrame(all_X)[LINEUP_FEATURE_COLS].fillna(0)
    y = pd.Series(all_y, name="net_rtg")
    return X, y


def train_lineup_model(
    seasons: List[int],
    competition: str = "E",
    min_events: int = 30,
) -> Optional[Pipeline]:
    """Train the lineup net-rating regression model.

    Returns a fitted sklearn Pipeline (StandardScaler + GradientBoostingRegressor),
    or None if insufficient data.
    """
    X, y = build_training_data(seasons, competition, min_events)
    if X.empty or len(X) < 30:
        logger.warning("Insufficient training data (%d samples). Need >= 30.", len(X))
        return None

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("regressor", GradientBoostingRegressor(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )),
    ])

    pipeline.fit(X, y)
    train_score = pipeline.score(X, y)
    logger.info(
        "Lineup model trained on %d samples — R² = %.3f",
        len(X), train_score,
    )
    return pipeline


def save_model(model: Pipeline, path: Path = None) -> Path:
    """Serialize the trained model to disk."""
    import joblib
    path = path or (_MODEL_DIR / "lineup_model.joblib")
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    logger.info("Model saved to %s", path)
    return path


def load_model(path: Path = None) -> Optional[Pipeline]:
    """Load a previously trained model from disk."""
    import joblib
    path = path or (_MODEL_DIR / "lineup_model.joblib")
    if not path.exists():
        return None
    return joblib.load(path)


def predict_lineup_net_rating(
    model: Pipeline,
    player_features: pd.DataFrame,
    player_ids: List[str],
) -> Optional[float]:
    """Predict the expected net rating for a 5-man lineup."""
    lf = _compute_lineup_features_from_players(player_features, player_ids)
    if lf is None:
        return None
    X = pd.DataFrame([lf])[LINEUP_FEATURE_COLS].fillna(0)
    return float(model.predict(X)[0])


def compute_lineup_radar_scores(
    player_features: pd.DataFrame,
    player_ids: List[str],
    league_player_features: pd.DataFrame = None,
) -> Dict[str, float]:
    """Compute radar chart scores (0-100 percentile) for a lineup.

    Compares the lineup's aggregate stats against all lineups that could
    theoretically be formed from the league's player pool. Uses the
    league_player_features to define percentile context.
    """
    lf = _compute_lineup_features_from_players(player_features, player_ids)
    if lf is None:
        return {}

    if league_player_features is None:
        league_player_features = player_features

    scores = {}
    for category, cols in RADAR_CATEGORIES.items():
        cat_vals = []
        for col in cols:
            if col in lf.index:
                val = lf[col]
                league_col = col.replace("mean_", "").replace("max_", "").replace("sum_", "")
                if league_col in league_player_features.columns:
                    league_vals = league_player_features[league_col].dropna()
                    if len(league_vals) > 0:
                        pct = (league_vals < val).mean() * 100
                        cat_vals.append(pct)
                    else:
                        cat_vals.append(50.0)
                else:
                    cat_vals.append(50.0)
        scores[category] = np.mean(cat_vals) if cat_vals else 50.0

    return scores


def find_best_5th_player(
    model: Pipeline,
    player_features: pd.DataFrame,
    locked_player_ids: List[str],
    roster_player_ids: List[str],
) -> List[Dict]:
    """Find the best 5th player to maximize expected net rating.

    Args:
        model: Trained lineup model.
        player_features: Season player features DataFrame.
        locked_player_ids: 4 already-selected player IDs.
        roster_player_ids: All available player IDs on the roster.

    Returns:
        Sorted list of dicts with player_id, player_name, predicted_net_rtg.
    """
    candidates = [
        pid for pid in roster_player_ids
        if pid not in locked_player_ids
        and pid in player_features["player_id"].values
    ]

    results = []
    for pid in candidates:
        lineup_ids = locked_player_ids + [pid]
        pred = predict_lineup_net_rating(model, player_features, lineup_ids)
        if pred is not None:
            name_row = player_features[player_features["player_id"] == pid]
            name = name_row["player_name"].iloc[0] if not name_row.empty else pid
            results.append({
                "player_id": pid,
                "player_name": name,
                "predicted_net_rtg": round(pred, 1),
            })

    return sorted(results, key=lambda x: x["predicted_net_rtg"], reverse=True)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    seasons = [2024, 2023, 2022]
    if len(sys.argv) > 1:
        seasons = [int(s) for s in sys.argv[1:]]

    logger.info("Training lineup model on seasons: %s", seasons)
    model = train_lineup_model(seasons)
    if model is None:
        logger.error("Training failed — insufficient data.")
        sys.exit(1)

    path = save_model(model)
    logger.info("Done. Model saved to %s", path)
