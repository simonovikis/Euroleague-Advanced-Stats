"""
extractors.py — Data Extraction Layer
======================================
Uses the euroleague-api library to fetch real boxscore, play-by-play, and
shot data from the Euroleague live API.

Key classes from euroleague-api:
  - BoxScoreData("E")  → get_player_boxscore_stats_data(season, gamecode)
  - PlayByPlay("E")    → get_game_play_by_play_data(season, gamecode)
  - ShotData("E")      → get_game_shot_data(season, gamecode)

The API returns pandas DataFrames directly.  Column names come from the
Euroleague official API (PascalCase for boxscore, UPPERCASE for PBP/shots).
"""

import logging
from typing import Dict

import numpy as np
import pandas as pd
from euroleague_api.boxscore_data import BoxScoreData
from euroleague_api.game_stats import GameStats
from euroleague_api.play_by_play_data import PlayByPlay
from euroleague_api.shot_data import ShotData

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# COMPETITION CODE & TEAM ALIAS CONFIGURATION
# ---------------------------------------------------------------------------
# "E" = Euroleague,  "U" = EuroCup
COMPETITION = "E"

# Map Euroleague's historical internal sponsor acronyms to user-preferred acronyms
TEAM_ALIASES = {
    "ULK": "FEN",  # Fenerbahce (historically Ulker)
    "IST": "EFS",  # Anadolu Efes (historically Dynamo/Efes)
    "TEL": "MTA",  # Maccabi
    "BAS": "BKN",  # Baskonia
    "RED": "CZV",  # Red Star Belgrade
    "MCO": "ASM",  # AS Monaco
    "MAD": "RMB",  # Real Madrid
    "BAR": "FCB",  # Barcelona
    "PAN": "PAO",  # Panathinaikos
    "PAM": "VAL",  # Valencia Basket
}

def apply_team_aliases(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Helper to consistently rename internal Euroleague team acronyms."""
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].replace(TEAM_ALIASES)
    return df


def get_boxscore(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch player-level boxscore stats for a single game.

    The euroleague-api `get_player_boxscore_stats_data` method returns a
    DataFrame with one row per player (no Team/Total summary rows).

    Returned columns (28 total):
        Season, Gamecode, Home, Player_ID, IsStarter, IsPlaying,
        Team, Dorsal, Player, Minutes,
        Points, FieldGoalsMade2, FieldGoalsAttempted2,
        FieldGoalsMade3, FieldGoalsAttempted3,
        FreeThrowsMade, FreeThrowsAttempted,
        OffensiveRebounds, DefensiveRebounds, TotalRebounds,
        Assistances, Steals, Turnovers, BlocksFavour, BlocksAgainst,
        FoulsCommited, FoulsReceived, Valuation, Plusminus

    Parameters
    ----------
    season : int
        The start year of the season (e.g. 2024 for the 2024-25 season).
    gamecode : int
        The game code from the Euroleague website.
    competition : str
        "E" for Euroleague, "U" for EuroCup.

    Returns
    -------
    pd.DataFrame
        Player-level boxscore stats for the game.
    """
    logger.info(f"Fetching boxscore — season={season}, gamecode={gamecode}")

    boxscore_api = BoxScoreData(competition)
    df = boxscore_api.get_player_boxscore_stats_data(season, gamecode)

    logger.info(f"Boxscore returned {len(df)} rows (before filtering)")

    # ---------------------------------------------------------------
    # The API may return "Team" and "Total" summary rows alongside
    # real player rows.  These have Player_ID == "Team" or "Total".
    # We strip them here so downstream code only ever sees real players.
    # ---------------------------------------------------------------
    summary_mask = df["Player_ID"].astype(str).str.strip().isin(["Team", "Total"])
    df = df[~summary_mask].reset_index(drop=True)

    logger.info(f"Boxscore: {len(df)} player rows after removing summary rows")
    df = apply_team_aliases(df, ["Team"])
    return df


def get_play_by_play(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch play-by-play data for a single game.

    The euroleague-api `get_game_play_by_play_data` method returns a DataFrame
    with one row per action.  Columns include:
        Season, Gamecode, CODETEAM, PLAYER_ID, PLAYER,
        PLAYTYPE, PERIOD, MARKERTIME, NUMBEROFPLAY,
        PLAYINFO, COMMENT, TRUE_NUMBEROFPLAY, ...

    PLAYTYPE values include: "2FGM" (2pt made), "2FGA" (2pt attempt/miss),
    "3FGM", "3FGA", "FTM", "FTA", "D" (defensive rebound), "O" (offensive
    rebound), "TO" (turnover), "ST" (steal), "AS" (assist), "FV" (block),
    "IN" (sub in), "OUT" (sub out), etc.

    Parameters
    ----------
    season : int
        The start year of the season.
    gamecode : int
        The game code.
    competition : str
        "E" for Euroleague, "U" for EuroCup.

    Returns
    -------
    pd.DataFrame
        Play-by-play actions for the game.
    """
    logger.info(f"Fetching PBP — season={season}, gamecode={gamecode}")

    pbp_api = PlayByPlay(competition)
    df = pbp_api.get_game_play_by_play_data(season, gamecode)

    logger.info(f"PBP returned {len(df)} rows")
    df = apply_team_aliases(df, ["CODETEAM"])
    return df


def get_shot_data(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch shot-level data with X/Y coordinates for a single game.

    The euroleague-api `get_game_shot_data` method returns a DataFrame with:
        Season, Gamecode, NUM_ANOT, TEAM, ID_PLAYER, PLAYER,
        ID_ACTION, ACTION, POINTS, COORD_X, COORD_Y, ZONE,
        FASTBREAK, SECOND_CHANCE, POINTS_OFF_TURNOVER,
        MINUTE, CONSOLE, POINTS_A, POINTS_B, UTC

    ZONE values: A-G representing different court zones.
    COORD_X / COORD_Y: pixel coordinates on the court diagram.

    Parameters
    ----------
    season : int
    gamecode : int
    competition : str

    Returns
    -------
    pd.DataFrame
        Shot-level data with coordinates.
    """
    logger.info(f"Fetching shot data — season={season}, gamecode={gamecode}")

    shot_api = ShotData(competition)
    try:
        df = shot_api.get_game_shot_data(season, gamecode)
        logger.info(f"Shot data returned {len(df)} rows")
    except Exception as e:
        logger.warning(f"Shot data unavailable for game {gamecode}: {e}")
        df = pd.DataFrame()

    return df


def extract_game_data(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> Dict[str, pd.DataFrame]:
    """
    High-level extraction function: fetches all data for a single game.

    Returns a dict with:
      - "boxscore"  : player boxscore DataFrame
      - "pbp"       : play-by-play DataFrame
      - "shots"     : shot data with X/Y coordinates
      - "game_info" : single-row DataFrame with game metadata

    Parameters
    ----------
    season : int
    gamecode : int
    competition : str

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    boxscore_df = get_boxscore(season, gamecode, competition)
    pbp_df = get_play_by_play(season, gamecode, competition)
    shots_df = get_shot_data(season, gamecode, competition)

    # ---------------------------------------------------------------
    # Extract game metadata.  Use PBP final score (most reliable)
    # and boxscore for team codes / home-away designation.
    # ---------------------------------------------------------------
    game_info = _extract_game_info(boxscore_df, pbp_df, season, gamecode)

    return {
        "boxscore": boxscore_df,
        "pbp": pbp_df,
        "shots": shots_df,
        "game_info": game_info,
    }


def _extract_game_info(
    boxscore_df: pd.DataFrame,
    pbp_df: pd.DataFrame,
    season: int,
    gamecode: int,
) -> pd.DataFrame:
    """
    Extract game-level metadata (teams, final score).

    Uses PBP POINTS_A / POINTS_B as the source of truth for the final
    score — boxscore aggregation can be unreliable when summary rows
    are included.  Falls back to boxscore aggregation if PBP is empty.
    """
    if boxscore_df.empty:
        logger.warning("Empty boxscore — returning empty game_info")
        return pd.DataFrame()

    # Identify home and away teams from boxscore Home column
    home_players = boxscore_df[boxscore_df["Home"] == 1]
    away_players = boxscore_df[boxscore_df["Home"] == 0]

    home_team = home_players["Team"].iloc[0] if len(home_players) > 0 else None
    away_team = away_players["Team"].iloc[0] if len(away_players) > 0 else None

    # --- Get final score from PBP (most reliable) ---
    home_score = None
    away_score = None

    if not pbp_df.empty:
        pbp_scored = pbp_df.copy()
        pbp_scored["POINTS_A"] = pd.to_numeric(pbp_scored["POINTS_A"], errors="coerce")
        pbp_scored["POINTS_B"] = pd.to_numeric(pbp_scored["POINTS_B"], errors="coerce")
        valid_scores = pbp_scored.dropna(subset=["POINTS_A", "POINTS_B"])
        valid_scores = valid_scores[valid_scores["POINTS_A"] > 0]

        if not valid_scores.empty:
            last = valid_scores.sort_values("TRUE_NUMBEROFPLAY").iloc[-1]
            home_score = int(last["POINTS_A"])
            away_score = int(last["POINTS_B"])

    # Fallback: aggregate from boxscore
    if home_score is None:
        home_score = int(home_players["Points"].sum()) if len(home_players) > 0 else None
        away_score = int(away_players["Points"].sum()) if len(away_players) > 0 else None

    game_info = {
        "season": season,
        "gamecode": gamecode,
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
    }

    return pd.DataFrame([game_info])


def extract_multiple_games(
    season: int,
    gamecodes: list,
    competition: str = COMPETITION,
) -> Dict[str, pd.DataFrame]:
    """
    Extract data for multiple games in a season.

    Concatenates results into a single dict of DataFrames.
    Gracefully handles failures for individual games.
    """
    all_boxscores = []
    all_pbps = []
    all_shots = []
    all_game_infos = []

    for gc in gamecodes:
        try:
            data = extract_game_data(season, gc, competition)
            all_boxscores.append(data["boxscore"])
            all_pbps.append(data["pbp"])
            all_shots.append(data["shots"])
            all_game_infos.append(data["game_info"])
        except Exception as e:
            logger.error(f"Failed to extract game {gc}, season {season}: {e}")
            continue

    return {
        "boxscore": pd.concat(all_boxscores, ignore_index=True) if all_boxscores else pd.DataFrame(),
        "pbp": pd.concat(all_pbps, ignore_index=True) if all_pbps else pd.DataFrame(),
        "shots": pd.concat(all_shots, ignore_index=True) if all_shots else pd.DataFrame(),
        "game_info": pd.concat(all_game_infos, ignore_index=True) if all_game_infos else pd.DataFrame(),
    }


def get_season_schedule(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch the full schedule for a season to populate the UI dropdowns.
    Returns a DataFrame with round, gamecode, team names, logos, and scores.
    """
    try:
        gs = GameStats(competition)
        # Fetch all games for the season (fast, ~0.3s)
        df = gs.get_gamecodes_season(season)
        if df.empty:
            logger.warning(f"No schedule found for season {season}")
            return pd.DataFrame()

        # get_gamecodes_season drops logos, so we fetch Round 1 to build a logo dictionary
        logos = {}
        try:
            r1 = gs.get_gamecodes_round(season, 1)
            if not r1.empty:
                logos.update(dict(zip(r1["local.club.code"], r1["local.club.images.crest"])))
                logos.update(dict(zip(r1["road.club.code"], r1["road.club.images.crest"])))
        except Exception as e:
            logger.warning(f"Could not fetch Round 1 logos: {e}")

        # Clean and rename columns for the dashboard
        schedule = pd.DataFrame()
        schedule["season"] = season
        schedule["round"] = df.get("Round", 0).astype(int)
        schedule["round_name"] = df.get("Phase", "")
        schedule["gamecode"] = df.get("gameCode", 0).astype(int)
        schedule["played"] = df.get("played", False).astype(bool)

        schedule["home_code"] = df.get("homecode", "")
        schedule["home_name"] = df.get("hometeam", "")
        schedule["home_logo"] = schedule["home_code"].map(logos).fillna("")
        schedule["home_score"] = pd.to_numeric(df.get("homescore"), errors="coerce")

        schedule["away_code"] = df.get("awaycode", "")
        schedule["away_name"] = df.get("awayteam", "")
        schedule["away_logo"] = schedule["away_code"].map(logos).fillna("")
        schedule["away_score"] = pd.to_numeric(df.get("awayscore"), errors="coerce")

        schedule["date"] = df.get("date", "")
        
        schedule = apply_team_aliases(schedule, ["home_code", "away_code"])

        return schedule.sort_values(["round", "gamecode"]).reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to fetch schedule for season {season}: {e}")
        return pd.DataFrame()


def extract_team_season_data(season: int, team_code: str, competition: str = COMPETITION) -> Dict[str, pd.DataFrame]:
    """
    Fetch all games for a specific team in a given season, concatenating
    the boxscores, play-by-play, and shot data into season-long dataframes.
    """
    schedule = get_season_schedule(season, competition)
    if schedule.empty:
        return {"boxscore": pd.DataFrame(), "pbp": pd.DataFrame(), "shots": pd.DataFrame(), "game_info": pd.DataFrame()}

    # Filter for games where the team played AND the game is completed (played == True)
    team_games = schedule[
        ((schedule["home_code"] == team_code) | (schedule["away_code"] == team_code)) &
        (schedule["played"] == True)
    ]

    boxscores, pbps, shots_dfs, info_dfs = [], [], [], []

    for _, row in team_games.iterrows():
        gamecode = row["gamecode"]
        raw = extract_game_data(season, gamecode, competition)

        if not raw["boxscore"].empty:
            boxscores.append(raw["boxscore"])
        if not raw["pbp"].empty:
            pbps.append(raw["pbp"])
        if not raw["shots"].empty:
            shots_dfs.append(raw["shots"])
        if not raw["game_info"].empty:
            info_dfs.append(raw["game_info"])

    return {
        "boxscore": pd.concat(boxscores, ignore_index=True) if boxscores else pd.DataFrame(),
        "pbp": pd.concat(pbps, ignore_index=True) if pbps else pd.DataFrame(),
        "shots": pd.concat(shots_dfs, ignore_index=True) if shots_dfs else pd.DataFrame(),
        "game_info": pd.concat(info_dfs, ignore_index=True) if info_dfs else pd.DataFrame(),
    }


def get_league_efficiency_landscape(season: int, competition: str = COMPETITION) -> pd.DataFrame:
    """
    Fetch league-wide traditional statistics to compute ORtg and DRtg for each team.
    """
    try:
        from euroleague_api.team_stats import TeamStats
        ts = TeamStats(competition)

        # Fetch offensive stats
        off_df = ts.get_team_stats(endpoint='traditional', params={'Season': season}, phase_type_code='RS', statistic_mode='Accumulated')
        # Fetch defensive (opponents) stats
        def_df = ts.get_team_stats(endpoint='opponentsTraditional', params={'Season': season}, phase_type_code='RS', statistic_mode='Accumulated')

        if off_df.empty or def_df.empty:
            return pd.DataFrame()

        # Rename columns for consistency and merge
        off_df = off_df.rename(columns={"team.code": "team_code", "team.name": "team_name"})
        def_df = def_df.rename(columns={"team.code": "team_code"})

        # Apply team aliases right after extraction and renaming
        off_df = apply_team_aliases(off_df, ["team_code"])
        def_df = apply_team_aliases(def_df, ["team_code"])

        merged = pd.merge(
            off_df[['team_code', 'team_name', 'pointsScored', 'twoPointersAttempted', 'threePointersAttempted', 'freeThrowsAttempted', 'offensiveRebounds', 'turnovers']],
            def_df[['team_code', 'pointsScored', 'twoPointersAttempted', 'threePointersAttempted', 'freeThrowsAttempted', 'offensiveRebounds', 'turnovers']],
            on='team_code',
            suffixes=('_off', '_def')
        )

        # Team possessions: FGA + 0.44 * FTA - ORB + TOV
        # Note: Euroleague advanced possession formula varies, but we stick to our phase 1 base definition for consistency.
        merged['fga_off'] = merged['twoPointersAttempted_off'] + merged['threePointersAttempted_off']
        merged['poss_off'] = merged['fga_off'] + 0.44 * merged['freeThrowsAttempted_off'] - merged['offensiveRebounds_off'] + merged['turnovers_off']

        merged['fga_def'] = merged['twoPointersAttempted_def'] + merged['threePointersAttempted_def']
        merged['poss_def'] = merged['fga_def'] + 0.44 * merged['freeThrowsAttempted_def'] - merged['offensiveRebounds_def'] + merged['turnovers_def']

        merged['ortg'] = np.where(merged['poss_off'] > 0, (merged['pointsScored_off'] / merged['poss_off']) * 100, np.nan)
        merged['drtg'] = np.where(merged['poss_def'] > 0, (merged['pointsScored_def'] / merged['poss_def']) * 100, np.nan)

        return merged[['team_code', 'team_name', 'ortg', 'drtg', 'poss_off']].sort_values('ortg', ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to fetch league efficiency for {season}: {e}")
        return pd.DataFrame()
