"""
ml_pipeline.py -- Win Probability Prediction Engine
=====================================================
Feature engineering, model training, and inference for predicting
Euroleague game outcomes based on historical team performance.

Features:
  - Season Net Rating (Home vs Away)
  - Home Court Advantage flag
  - Recent Form (point differential over last 5 games)
  - Rest Days (days since each team's last game)

Model: LogisticRegression with StandardScaler pipeline.
"""

import logging
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "home_net_rtg",
    "away_net_rtg",
    "net_rtg_diff",
    "home_court_adv",
    "home_form_5",
    "away_form_5",
    "home_rest_days",
    "away_rest_days",
]


def _compute_recent_form(
    schedule: pd.DataFrame, team: str, before_round: int, n: int = 5,
) -> float:
    played = schedule[(schedule["played"] == True) & (schedule["round"] < before_round)]

    home = played[played["home_code"] == team].copy()
    home["pt_diff"] = home["home_score"] - home["away_score"]

    away = played[played["away_code"] == team].copy()
    away["pt_diff"] = away["away_score"] - away["home_score"]

    all_games = pd.concat([
        home[["round", "pt_diff"]],
        away[["round", "pt_diff"]],
    ]).sort_values("round", ascending=False)

    if all_games.empty:
        return 0.0
    return float(all_games.head(n)["pt_diff"].mean())


def _compute_rest_days(
    schedule: pd.DataFrame, team: str, game_date: str, game_round: int,
) -> int:
    try:
        current_date = pd.to_datetime(game_date, errors="coerce")
        if pd.isna(current_date):
            return 3
    except Exception:
        return 3

    played = schedule[(schedule["played"] == True) & (schedule["round"] < game_round)]
    team_games = played[
        (played["home_code"] == team) | (played["away_code"] == team)
    ].copy()

    if team_games.empty:
        return 7

    team_games["parsed_date"] = pd.to_datetime(team_games["date"], errors="coerce")
    team_games = team_games.dropna(subset=["parsed_date"])

    if team_games.empty:
        return 3

    rest = (current_date - team_games["parsed_date"].max()).days
    return max(rest, 0)


def _compute_rest_days_latest(schedule: pd.DataFrame, team: str) -> int:
    played = schedule[schedule["played"] == True]
    team_games = played[
        (played["home_code"] == team) | (played["away_code"] == team)
    ].copy()

    if team_games.empty:
        return 3

    team_games["parsed_date"] = pd.to_datetime(team_games["date"], errors="coerce")
    team_games = team_games.dropna(subset=["parsed_date"])

    if team_games.empty:
        return 3

    rest = (datetime.now() - team_games["parsed_date"].max()).days
    return max(rest, 0)


def extract_prediction_features(
    season: int,
    competition: str = "E",
) -> pd.DataFrame:
    """
    Extract ML features for all completed games in a season.

    Returns a DataFrame with FEATURE_COLS + target column ``home_win``.
    """
    from data_pipeline.extractors import (
        get_season_schedule,
        get_league_efficiency_landscape,
    )

    schedule = get_season_schedule(season, competition)
    efficiency = get_league_efficiency_landscape(season, competition)

    if schedule.empty or efficiency.empty:
        logger.warning(f"No data available for season {season}")
        return pd.DataFrame()

    net_rtg_map = dict(zip(efficiency["team_code"], efficiency["net_rtg"]))

    played_games = schedule[schedule["played"] == True].copy()
    played_games = played_games.dropna(subset=["home_score", "away_score"])

    records = []
    for _, game in played_games.iterrows():
        home = game["home_code"]
        away = game["away_code"]
        game_round = game["round"]
        game_date = game.get("date", "")

        home_net = net_rtg_map.get(home, 0.0)
        away_net = net_rtg_map.get(away, 0.0)

        records.append({
            "season": season,
            "gamecode": game["gamecode"],
            "round": game_round,
            "home_code": home,
            "away_code": away,
            "home_net_rtg": home_net,
            "away_net_rtg": away_net,
            "net_rtg_diff": home_net - away_net,
            "home_court_adv": 1,
            "home_form_5": _compute_recent_form(schedule, home, game_round),
            "away_form_5": _compute_recent_form(schedule, away, game_round),
            "home_rest_days": _compute_rest_days(
                schedule, home, game_date, game_round,
            ),
            "away_rest_days": _compute_rest_days(
                schedule, away, game_date, game_round,
            ),
            "home_win": int(game["home_score"] > game["away_score"]),
        })

    return pd.DataFrame(records)


def train_win_probability_model(features_df: pd.DataFrame) -> Pipeline:
    """Train a LogisticRegression pipeline on historical game features."""
    X = features_df[FEATURE_COLS].fillna(0)
    y = features_df["home_win"]

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=1000, random_state=42)),
    ])

    pipeline.fit(X, y)
    accuracy = pipeline.score(X, y)
    logger.info(f"Model trained on {len(X)} games — training accuracy: {accuracy:.3f}")
    return pipeline


def get_or_train_model(
    training_seasons: List[int],
    competition: str = "E",
) -> Optional[Pipeline]:
    """Train a model on data from multiple historical seasons."""
    all_features = []
    for season in training_seasons:
        features = extract_prediction_features(season, competition)
        if not features.empty:
            all_features.append(features)

    if not all_features:
        return None

    combined = pd.concat(all_features, ignore_index=True)
    if len(combined) < 20:
        return None

    return train_win_probability_model(combined)


def predict_matchup(
    model: Pipeline,
    home_team: str,
    away_team: str,
    season: int,
    competition: str = "E",
) -> float:
    """
    Predict the home team's win probability for a hypothetical matchup
    using current-season stats.
    """
    from data_pipeline.extractors import (
        get_season_schedule,
        get_league_efficiency_landscape,
    )

    schedule = get_season_schedule(season, competition)
    efficiency = get_league_efficiency_landscape(season, competition)

    if efficiency.empty:
        return 0.5

    net_rtg_map = dict(zip(efficiency["team_code"], efficiency["net_rtg"]))
    home_net = net_rtg_map.get(home_team, 0.0)
    away_net = net_rtg_map.get(away_team, 0.0)

    max_round = schedule["round"].max() if not schedule.empty else 1

    features = pd.DataFrame([{
        "home_net_rtg": home_net,
        "away_net_rtg": away_net,
        "net_rtg_diff": home_net - away_net,
        "home_court_adv": 1,
        "home_form_5": _compute_recent_form(schedule, home_team, max_round + 1),
        "away_form_5": _compute_recent_form(schedule, away_team, max_round + 1),
        "home_rest_days": _compute_rest_days_latest(schedule, home_team),
        "away_rest_days": _compute_rest_days_latest(schedule, away_team),
    }])

    proba = model.predict_proba(features)[0]
    return float(proba[1])
