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

Performance (Phase 59.5):
  - Single-game extraction parallelises boxscore/pbp/shots via ThreadPoolExecutor.
  - extract_games_concurrent() fetches N games in parallel with a Semaphore
    to cap concurrent HTTP connections and tenacity-powered retries for 429/5xx.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from requests.exceptions import ConnectionError, HTTPError, Timeout
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)
from euroleague_api.boxscore_data import BoxScoreData
from euroleague_api.game_stats import GameStats
from euroleague_api.play_by_play_data import PlayByPlay
from euroleague_api.shot_data import ShotData

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CONCURRENCY & RETRY CONFIGURATION
# ---------------------------------------------------------------------------
MAX_CONCURRENT_API_CALLS = 15
_api_semaphore = threading.Semaphore(MAX_CONCURRENT_API_CALLS)


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient HTTP errors worth retrying."""
    if isinstance(exc, HTTPError) and hasattr(exc, "response") and exc.response is not None:
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, (ConnectionError, Timeout, OSError))


_api_retry = retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


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


@_api_retry
def _fetch_boxscore_raw(season: int, gamecode: int, competition: str) -> pd.DataFrame:
    """Retry-wrapped raw API call for boxscore data."""
    with _api_semaphore:
        api = BoxScoreData(competition)
        return api.get_player_boxscore_stats_data(season, gamecode)


def get_boxscore(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch player-level boxscore stats for a single game.

    Returned columns (28 total):
        Season, Gamecode, Home, Player_ID, IsStarter, IsPlaying,
        Team, Dorsal, Player, Minutes,
        Points, FieldGoalsMade2, FieldGoalsAttempted2,
        FieldGoalsMade3, FieldGoalsAttempted3,
        FreeThrowsMade, FreeThrowsAttempted,
        OffensiveRebounds, DefensiveRebounds, TotalRebounds,
        Assistances, Steals, Turnovers, BlocksFavour, BlocksAgainst,
        FoulsCommited, FoulsReceived, Valuation, Plusminus
    """
    logger.info(f"Fetching boxscore — season={season}, gamecode={gamecode}")
    df = _fetch_boxscore_raw(season, gamecode, competition)
    logger.info(f"Boxscore returned {len(df)} rows (before filtering)")

    summary_mask = df["Player_ID"].astype(str).str.strip().isin(["Team", "Total"])
    df = df[~summary_mask].reset_index(drop=True)

    logger.info(f"Boxscore: {len(df)} player rows after removing summary rows")
    df = apply_team_aliases(df, ["Team"])
    return df


@_api_retry
def _fetch_pbp_raw(season: int, gamecode: int, competition: str) -> pd.DataFrame:
    """Retry-wrapped raw API call for play-by-play data."""
    with _api_semaphore:
        api = PlayByPlay(competition)
        return api.get_game_play_by_play_data(season, gamecode)


def get_play_by_play(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch play-by-play data for a single game.

    PLAYTYPE values include: "2FGM", "2FGA", "3FGM", "3FGA", "FTM", "FTA",
    "D", "O", "TO", "ST", "AS", "FV", "IN", "OUT", etc.
    """
    logger.info(f"Fetching PBP — season={season}, gamecode={gamecode}")
    df = _fetch_pbp_raw(season, gamecode, competition)
    logger.info(f"PBP returned {len(df)} rows")
    df = apply_team_aliases(df, ["CODETEAM"])
    return df


@_api_retry
def _fetch_shots_raw(season: int, gamecode: int, competition: str) -> pd.DataFrame:
    """Retry-wrapped raw API call for shot data."""
    with _api_semaphore:
        api = ShotData(competition)
        return api.get_game_shot_data(season, gamecode)


def get_shot_data(
    season: int,
    gamecode: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """Fetch shot-level data with X/Y coordinates for a single game."""
    logger.info(f"Fetching shot data — season={season}, gamecode={gamecode}")
    try:
        df = _fetch_shots_raw(season, gamecode, competition)
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
    Boxscore, PBP, and shots are fetched concurrently (3 threads).

    Returns a dict with keys: boxscore, pbp, shots, game_info.
    """
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_box = pool.submit(get_boxscore, season, gamecode, competition)
        f_pbp = pool.submit(get_play_by_play, season, gamecode, competition)
        f_shots = pool.submit(get_shot_data, season, gamecode, competition)

    boxscore_df = f_box.result()
    pbp_df = f_pbp.result()
    shots_df = f_shots.result()

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
        "played": True,
    }

    return pd.DataFrame([game_info])


def extract_multiple_games(
    season: int,
    gamecodes: list,
    competition: str = COMPETITION,
) -> Dict[str, pd.DataFrame]:
    """
    Extract data for multiple games in a season (legacy sequential).
    Kept for backward-compat; prefer extract_games_concurrent() for ETL.
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


def extract_games_concurrent(
    season: int,
    gamecodes: List[int],
    competition: str = COMPETITION,
    max_workers: int = 12,
    progress_callback: Optional[callable] = None,
) -> List[Dict[str, pd.DataFrame]]:
    """
    Extract data for many games concurrently using a thread pool.

    Each game internally spawns 3 sub-threads (box/pbp/shots) but the
    global _api_semaphore caps total in-flight HTTP requests to
    MAX_CONCURRENT_API_CALLS, preventing 429s.

    Parameters
    ----------
    season : int
    gamecodes : list[int]
    competition : str
    max_workers : int
        Number of games fetched in parallel (default 12).
    progress_callback : callable(current, total) or None

    Returns
    -------
    list[dict]
        One dict per successfully-extracted game (same shape as
        extract_game_data output). Failed games are logged and skipped.
    """
    results: List[Dict[str, pd.DataFrame]] = []
    total = len(gamecodes)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_gc = {
            pool.submit(extract_game_data, season, gc, competition): gc
            for gc in gamecodes
        }

        for i, future in enumerate(as_completed(future_to_gc), 1):
            gc = future_to_gc[future]
            try:
                data = future.result()
                results.append(data)
                logger.info(f"Extracted game {gc} ({i}/{total})")
            except Exception as e:
                logger.error(f"Failed to extract game {gc}: {e}")

            if progress_callback:
                progress_callback(i, total)

    return results


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
            off_df[['team_code', 'team_name', 'gamesPlayed', 'pointsScored', 'twoPointersAttempted', 'threePointersAttempted', 'freeThrowsAttempted', 'offensiveRebounds', 'turnovers']],
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
        merged['net_rtg'] = merged['ortg'] - merged['drtg']

        # Pace = average possessions per 40 minutes
        # Accumulated possessions / games played = possessions per game = pace per 40 min
        merged['pace'] = np.where(
            merged['gamesPlayed'] > 0,
            merged['poss_off'] / merged['gamesPlayed'],
            np.nan,
        )

        return merged[['team_code', 'team_name', 'ortg', 'drtg', 'net_rtg', 'pace', 'poss_off']].sort_values('ortg', ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to fetch league efficiency for {season}: {e}")
        return pd.DataFrame()


def get_season_game_metadata(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch per-game metadata for a full season from the GameMetadata endpoint.

    Returns a DataFrame with columns including:
        Season, Gamecode, Round, Date, Stadium,
        CodeTeamA, CodeTeamB, ScoreA, ScoreB,
        CoachA, CoachB, Referee1, Referee2, Referee3, Phase, ...

    Parameters
    ----------
    season : int
        The start year of the season.
    competition : str
        "E" for Euroleague, "U" for EuroCup.
    """
    try:
        from euroleague_api.game_metadata import GameMetadata
        gm = GameMetadata(competition)
        df = gm.get_game_metadata_single_season(season)

        if df.empty:
            logger.warning(f"No game metadata found for season {season}")
            return pd.DataFrame()

        logger.info(f"Game metadata: {len(df)} games for season {season}")

        # Apply team aliases
        df = apply_team_aliases(df, ["CodeTeamA", "CodeTeamB"])

        return df

    except Exception as e:
        logger.error(f"Failed to fetch game metadata for season {season}: {e}")
        return pd.DataFrame()


def get_situational_scoring(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Fetch team-level situational scoring profile for a full season.

    Merges advanced stats (scoring distribution percentages) with
    traditional stats (steals, turnovers, offensive rebounds, assists).

    Returns a DataFrame with columns:
        team_code, team_name, points_per_game,
        pts_from_2pt_pct, pts_from_3pt_pct, pts_from_ft_pct,
        steals_pg, turnovers_pg, off_reb_pg, assists_pg
    """
    try:
        from euroleague_api.team_stats import TeamStats
        ts = TeamStats(competition)

        adv = ts.get_team_stats(
            endpoint='advanced',
            params={'Season': season},
            phase_type_code='RS',
            statistic_mode='PerGame',
        )
        trad = ts.get_team_stats(
            endpoint='traditional',
            params={'Season': season},
            phase_type_code='RS',
            statistic_mode='PerGame',
        )

        if adv.empty or trad.empty:
            logger.warning(f"Empty stats for situational scoring, season {season}")
            return pd.DataFrame()

        # Rename team code columns
        adv = adv.rename(columns={"team.code": "team_code", "team.name": "team_name"})
        trad = trad.rename(columns={"team.code": "team_code"})

        # Apply team aliases
        adv = apply_team_aliases(adv, ["team_code"])
        trad = apply_team_aliases(trad, ["team_code"])

        # Parse percentage strings (e.g. "54.2%" -> 54.2)
        def parse_pct(col):
            return col.astype(str).str.replace('%', '', regex=False).astype(float)

        result = pd.DataFrame({
            "team_code": adv["team_code"],
            "team_name": adv["team_name"],
            "pts_from_2pt_pct": parse_pct(adv["pointsFromTwoPointersPercentage"]),
            "pts_from_3pt_pct": parse_pct(adv["pointsFromThreePointersPercentage"]),
            "pts_from_ft_pct": parse_pct(adv["pointsFromFreeThrowsPercentage"]),
            "efg_pct": parse_pct(adv["effectiveFieldGoalPercentage"]),
            "ts_pct": parse_pct(adv["trueShootingPercentage"]),
        })

        # Merge traditional stats
        trad_slim = trad[["team_code", "pointsScored", "steals", "turnovers",
                          "offensiveRebounds", "assists"]].rename(columns={
            "pointsScored": "points_per_game",
            "steals": "steals_pg",
            "turnovers": "turnovers_pg",
            "offensiveRebounds": "off_reb_pg",
            "assists": "assists_pg",
        })

        result = result.merge(trad_slim, on="team_code", how="left")
        logger.info(f"Situational scoring: {len(result)} teams for season {season}")
        return result.sort_values("points_per_game", ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to fetch situational scoring for season {season}: {e}")
        return pd.DataFrame()


def get_home_away_splits(
    season: int,
    competition: str = COMPETITION,
) -> pd.DataFrame:
    """
    Calculate Home vs. Away ORtg, DRtg, Net Rating, and Win Percentage for all teams.

    Instead of fetching 300+ individual game advanced stats (which takes ~1 min),
    this uses the team's season-long Pace (from get_league_efficiency_landscape)
    combined with the game scores from the season schedule to approximate
    per-game possessions and calculate accurate ratings instantly.
    """
    try:
        schedule = get_season_schedule(season, competition)
        eff = get_league_efficiency_landscape(season, competition)

        if schedule.empty or eff.empty:
            logger.warning(f"Empty schedule or efficiency data for splits, season {season}")
            return pd.DataFrame()

        # Only use completed games
        played = schedule[schedule["played"] == True].copy()
        
        # Merge with efficiency data to get season-long Pace per team
        pace_map = dict(zip(eff["team_code"], eff["pace"]))
        
        team_codes = eff["team_code"].unique()
        split_records = []

        for team in team_codes:
            team_pace = pace_map.get(team, 70.0) # default fallback pace
            
            # Home Games
            hm = played[played["home_code"] == team]
            hm_games = len(hm)
            hm_wins = len(hm[hm["home_score"] > hm["away_score"]])
            hm_pts_for = hm["home_score"].sum()
            hm_pts_against = hm["away_score"].sum()
            hm_win_pct = (hm_wins / hm_games * 100) if hm_games > 0 else 0.0
            hm_poss = team_pace * hm_games
            hm_ortg = (hm_pts_for / hm_poss * 100) if hm_poss > 0 else 0.0
            hm_drtg = (hm_pts_against / hm_poss * 100) if hm_poss > 0 else 0.0
            hm_net = hm_ortg - hm_drtg

            # Away Games
            aw = played[played["away_code"] == team]
            aw_games = len(aw)
            aw_wins = len(aw[aw["away_score"] > aw["home_score"]])
            aw_pts_for = aw["away_score"].sum()
            aw_pts_against = aw["home_score"].sum()
            aw_win_pct = (aw_wins / aw_games * 100) if aw_games > 0 else 0.0
            aw_poss = team_pace * aw_games
            aw_ortg = (aw_pts_for / aw_poss * 100) if aw_poss > 0 else 0.0
            aw_drtg = (aw_pts_against / aw_poss * 100) if aw_poss > 0 else 0.0
            aw_net = aw_ortg - aw_drtg

            # Advantage
            hm_adv = hm_net - aw_net

            # Get team name
            tm_name = eff[eff["team_code"] == team]["team_name"].iloc[0]

            split_records.append({
                "team_code": team,
                "team_name": tm_name,
                "home_games": hm_games,
                "home_win_pct": hm_win_pct,
                "home_ortg": hm_ortg,
                "home_drtg": hm_drtg,
                "home_net": hm_net,
                "away_games": aw_games,
                "away_win_pct": aw_win_pct,
                "away_ortg": aw_ortg,
                "away_drtg": aw_drtg,
                "away_net": aw_net,
                "home_adv_diff": hm_adv
            })

        df = pd.DataFrame(split_records)
        logger.info(f"Home/Away splits computed for {len(df)} teams")
        return df.sort_values("home_adv_diff", ascending=False).reset_index(drop=True)

    except Exception as e:
        logger.error(f"Failed to compute home/away splits for season {season}: {e}")
        return pd.DataFrame()
