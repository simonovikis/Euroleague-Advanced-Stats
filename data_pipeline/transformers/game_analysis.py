import logging
from typing import Optional
import numpy as np
import pandas as pd
from data_pipeline.transformers.utils import _markertime_to_seconds

logger = logging.getLogger(__name__)


def detect_runs_and_stoppers(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify scoring runs of 8+ unanswered points and the player/play that
    breaks each run.

    Algorithm:
      1. Walk through PBP in order, tracking POINTS_A and POINTS_B.
      2. When one team scores consecutively (the other team's score stays
         static), accumulate a "run" counter.
      3. When the run reaches ≥ 8 unanswered points and the other team
         finally scores, record the "run stopper" — the player and play type
         that broke the run.

    Returns a DataFrame of run-stopping events with:
      - run_team: the team that was on the scoring run
      - run_points: total unanswered points in the run
      - stopper_player, stopper_player_id, stopper_team
      - stopper_playtype: the play that broke it (2FGM, 3FGM, FTM)
      - period, markertime
    """
    if pbp_df.empty:
        return pd.DataFrame()

    df = pbp_df.sort_values("TRUE_NUMBEROFPLAY").copy()
    df["POINTS_A"] = pd.to_numeric(df["POINTS_A"], errors="coerce").fillna(0)
    df["POINTS_B"] = pd.to_numeric(df["POINTS_B"], errors="coerce").fillna(0)

    scoring_types = {"2FGM", "3FGM", "FTM"}

    results = []
    # Track the score at the start of the current "drought" for each team
    prev_a = 0
    prev_b = 0
    run_start_a = 0  # Score of team A when team B last scored
    run_start_b = 0  # Score of team B when team A last scored
    running_team = None  # Which team is currently on a run

    for _, row in df.iterrows():
        cur_a = int(row["POINTS_A"])
        cur_b = int(row["POINTS_B"])

        if cur_a == prev_a and cur_b == prev_b:
            # No scoring change, skip
            continue

        # Team A scored
        if cur_a > prev_a and cur_b == prev_b:
            if running_team != "A":
                # Reset: team A starts a new run
                running_team = "A"
                run_start_a = prev_a
                run_start_b = cur_b

            prev_a = cur_a

        # Team B scored
        elif cur_b > prev_b and cur_a == prev_a:
            if running_team != "B":
                running_team = "B"
                run_start_b = prev_b
                run_start_a = cur_a

            prev_b = cur_b

        # Both changed (rare, e.g. correction) — reset
        else:
            running_team = None
            prev_a = cur_a
            prev_b = cur_b
            continue

        # Check if a run was just broken
        if running_team == "A" and cur_b > prev_b:
            run_pts = prev_a - run_start_a
            if run_pts >= 8 and row["PLAYTYPE"] in scoring_types:
                results.append({
                    "run_team": row.get("home_team", "Team A"),
                    "run_points": run_pts,
                    "stopper_player": row["PLAYER"],
                    "stopper_player_id": row["PLAYER_ID"],
                    "stopper_team": row["CODETEAM"],
                    "stopper_playtype": row["PLAYTYPE"],
                    "period": row["PERIOD"],
                    "markertime": row["MARKERTIME"],
                })
            running_team = "B"
            run_start_b = prev_b
            prev_b = cur_b

        elif running_team == "B" and cur_a > prev_a:
            run_pts = prev_b - run_start_b
            if run_pts >= 8 and row["PLAYTYPE"] in scoring_types:
                results.append({
                    "run_team": row.get("away_team", "Team B"),
                    "run_points": run_pts,
                    "stopper_player": row["PLAYER"],
                    "stopper_player_id": row["PLAYER_ID"],
                    "stopper_team": row["CODETEAM"],
                    "stopper_playtype": row["PLAYTYPE"],
                    "period": row["PERIOD"],
                    "markertime": row["MARKERTIME"],
                })
            running_team = "A"
            run_start_a = prev_a
            prev_a = cur_a

    return pd.DataFrame(results)


def foul_trouble_impact(
    pbp_df: pd.DataFrame,
    boxscore_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Measure how a team's performance changes when its highest-usage player
    picks up 2+ fouls in the first half.

    Steps:
      1. Identify the highest-usage player per team (by FGA + FTA from boxscore)
      2. Find when that player's 2nd foul occurs in the PBP (Period ≤ 2)
      3. Split game into "before foul trouble" and "after foul trouble"
      4. Compare team ORtg/DRtg in both periods

    Returns one row per team with: team, star_player, foul_event_period,
    ortg_before, ortg_after, drtg_before, drtg_after, impact.
    """
    if pbp_df.empty or boxscore_df.empty:
        return pd.DataFrame()

    score_map = {"2FGM": 2, "3FGM": 3, "FTM": 1}
    poss_events = {"2FGM", "2FGA", "3FGM", "3FGA", "TO", "D"}

    results = []

    for team in boxscore_df["Team"].unique():
        team_bx = boxscore_df[boxscore_df["Team"] == team].copy()

        # Highest-usage player: most FGA + FTA
        team_bx["_usage"] = (
            pd.to_numeric(team_bx.get("FieldGoalsAttempted2", 0), errors="coerce").fillna(0)
            + pd.to_numeric(team_bx.get("FieldGoalsAttempted3", 0), errors="coerce").fillna(0)
            + pd.to_numeric(team_bx.get("FreeThrowsAttempted", 0), errors="coerce").fillna(0)
        )
        star = team_bx.sort_values("_usage", ascending=False).iloc[0]
        star_pid = str(star["Player_ID"]).strip()
        star_name = str(star["Player"]).strip()

        # Find 2nd personal foul (CM = committed foul) by this player in 1st half
        player_fouls = pbp_df[
            (pbp_df["PLAYER_ID"].astype(str).str.strip() == star_pid)
            & (pbp_df["PLAYTYPE"].isin(["CM", "CMU", "CMT"]))  # foul types
            & (pbp_df["PERIOD"] <= 2)  # first half
        ].sort_values("TRUE_NUMBEROFPLAY")

        if len(player_fouls) < 2:
            continue  # No foul trouble in first half

        # The moment of the 2nd foul
        second_foul_play = player_fouls.iloc[1]["TRUE_NUMBEROFPLAY"]

        # Split PBP
        before = pbp_df[pbp_df["TRUE_NUMBEROFPLAY"] < second_foul_play]
        after = pbp_df[pbp_df["TRUE_NUMBEROFPLAY"] >= second_foul_play]

        opp_team = boxscore_df[boxscore_df["Team"] != team]["Team"].iloc[0]

        def _calc_team_rtg(subset, t, opp):
            pts_for = sum(score_map.get(pt, 0) for _, pt in
                          subset[subset["CODETEAM"] == t]["PLAYTYPE"].items())
            pts_against = sum(score_map.get(pt, 0) for _, pt in
                              subset[subset["CODETEAM"] == opp]["PLAYTYPE"].items())
            poss = max(len(subset[subset["CODETEAM"].isin([t]) & subset["PLAYTYPE"].isin(poss_events)]), 1)
            return (pts_for / poss) * 100, (pts_against / poss) * 100

        ortg_b, drtg_b = _calc_team_rtg(before, team, opp_team)
        ortg_a, drtg_a = _calc_team_rtg(after, team, opp_team)

        results.append({
            "team": team,
            "star_player": star_name,
            "star_player_id": star_pid,
            "second_foul_play_num": int(second_foul_play),
            "foul_period": int(player_fouls.iloc[1]["PERIOD"]),
            "ortg_before": round(ortg_b, 1),
            "ortg_after": round(ortg_a, 1),
            "drtg_before": round(drtg_b, 1),
            "drtg_after": round(drtg_a, 1),
            "ortg_impact": round(ortg_a - ortg_b, 1),
            "drtg_impact": round(drtg_a - drtg_b, 1),
        })

    return pd.DataFrame(results)


def compute_referee_stats(
    metadata_df: pd.DataFrame,
    team_code: str,
    min_games: int = 3,
) -> pd.DataFrame:
    """
    Calculate a team's win/loss record under each referee.

    Parameters
    ----------
    metadata_df : pd.DataFrame
        Output of `get_season_game_metadata()` with columns:
        CodeTeamA, CodeTeamB, ScoreA, ScoreB, Referee1, Referee2, Referee3.
    team_code : str
        The team code to analyze (e.g. "OLY", "PAO").
    min_games : int
        Minimum games officiated to include a referee (avoids noise).

    Returns
    -------
    pd.DataFrame
        Per-referee stats: referee, games, wins, losses, win_pct.
    """
    if metadata_df.empty:
        return pd.DataFrame()

    ref_cols = ["Referee1", "Referee2", "Referee3"]
    missing = [c for c in ref_cols if c not in metadata_df.columns]
    if missing:
        logger.warning(f"Missing referee columns: {missing}")
        return pd.DataFrame()

    # Filter to games involving the selected team
    team_games = metadata_df[
        (metadata_df["CodeTeamA"] == team_code) | (metadata_df["CodeTeamB"] == team_code)
    ].copy()

    if team_games.empty:
        return pd.DataFrame()

    # Determine if team won each game
    team_games["team_won"] = np.where(
        team_games["CodeTeamA"] == team_code,
        team_games["ScoreA"] > team_games["ScoreB"],   # Team is "A"
        team_games["ScoreB"] > team_games["ScoreA"],   # Team is "B"
    )

    # Melt referee columns into long format (3 rows per game)
    melted_rows = []
    for _, row in team_games.iterrows():
        won = row["team_won"]
        for rc in ref_cols:
            ref_name = row.get(rc)
            if pd.notna(ref_name) and str(ref_name).strip():
                melted_rows.append({"referee": str(ref_name).strip(), "won": won})

    if not melted_rows:
        return pd.DataFrame()

    ref_df = pd.DataFrame(melted_rows)

    # Aggregate per referee
    stats = (
        ref_df.groupby("referee")
        .agg(games=("won", "size"), wins=("won", "sum"))
        .reset_index()
    )
    stats["losses"] = stats["games"] - stats["wins"]
    stats["win_pct"] = (stats["wins"] / stats["games"] * 100).round(1)

    # Apply minimum games filter
    stats = stats[stats["games"] >= min_games].sort_values("win_pct", ascending=False).reset_index(drop=True)

    return stats


def classify_player_positions(boxscore: pd.DataFrame) -> pd.DataFrame:
    """
    Infer player positions from box-score statistical profiles.

    Since the euroleague-api does not expose explicit positions,
    we use a heuristic classification within each team:

    - **Guard**: Players with the highest assist-to-rebound ratio
      (top 2 per team by AST / (TotalRebounds + 1)).
    - **Center**: Players with the highest rebound-to-assist ratio
      (top 1 per team by TotalRebounds / (Assistances + 1)).
    - **Forward**: Everyone else.

    The classification only considers players with > 0 minutes.

    Returns the input DataFrame with an added ``position`` column
    containing one of 'Guard', 'Forward', or 'Center'.
    """
    df = boxscore.copy()

    # Ensure numeric columns exist
    for col in ["Assistances", "TotalRebounds"]:
        if col not in df.columns:
            df[col] = 0

    # Parse minutes — handle both "MM:SS" strings and floats
    def _parse_min(val):
        if pd.isna(val):
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val)
        if ":" in s:
            parts = s.split(":")
            return float(parts[0]) + float(parts[1]) / 60
        try:
            return float(s)
        except ValueError:
            return 0.0

    if df["Minutes"].dtype == object:
        df["_mins"] = df["Minutes"].apply(_parse_min)
    else:
        df["_mins"] = df["Minutes"]

    # Only classify players who actually played
    played = df["_mins"] > 0

    # Default = Forward
    df["position"] = "Forward"

    # Compute ratios
    df["_ast_ratio"] = df["Assistances"] / (df["TotalRebounds"].clip(lower=0) + 1)
    df["_reb_ratio"] = df["TotalRebounds"] / (df["Assistances"].clip(lower=0) + 1)

    for team in df.loc[played, "Team"].unique():
        mask = played & (df["Team"] == team)
        team_df = df.loc[mask]

        if len(team_df) < 3:
            continue

        # Top 2 ast_ratio → Guards
        guard_idx = team_df.nlargest(2, "_ast_ratio").index
        df.loc[guard_idx, "position"] = "Guard"

        # Among remaining, top 1 reb_ratio → Center
        remaining = team_df.drop(guard_idx)
        if not remaining.empty:
            center_idx = remaining.nlargest(1, "_reb_ratio").index
            df.loc[center_idx, "position"] = "Center"

    # Cleanup temp columns
    df.drop(columns=["_mins", "_ast_ratio", "_reb_ratio"], inplace=True, errors="ignore")

    return df


def compute_close_game_stats(
    schedule_df: pd.DataFrame,
    close_threshold: int = 5,
) -> pd.DataFrame:
    """
    Compute close-game (clutch DNA) stats for all teams in a season.

    For every completed game, calculates margin and flags close games
    (margin <= threshold). Then aggregates per team:
      - Total Games, Overall Win %, Avg Point Differential
      - Close Games Played, Close Game Win %
      - League Average Close Game Win % as baseline
    """
    played = schedule_df[schedule_df["played"] == True].copy()
    if played.empty:
        return pd.DataFrame()

    # Build team name lookup from schedule
    name_map = {}
    for _, row in played.iterrows():
        name_map[row["home_code"]] = row.get("home_name", row["home_code"])
        name_map[row["away_code"]] = row.get("away_name", row["away_code"])

    records = []
    for _, row in played.iterrows():
        hs, aws = row["home_score"], row["away_score"]
        if pd.isna(hs) or pd.isna(aws):
            continue
        margin = abs(hs - aws)
        is_close = margin <= close_threshold

        records.append({
            "team_code": row["home_code"],
            "won": hs > aws,
            "point_diff": hs - aws,
            "is_close_game": is_close,
        })
        records.append({
            "team_code": row["away_code"],
            "won": aws > hs,
            "point_diff": aws - hs,
            "is_close_game": is_close,
        })

    if not records:
        return pd.DataFrame()

    game_df = pd.DataFrame(records)

    team_stats = game_df.groupby("team_code").agg(
        total_games=("won", "size"),
        total_wins=("won", "sum"),
        avg_point_diff=("point_diff", "mean"),
    ).reset_index()
    team_stats["overall_win_pct"] = team_stats["total_wins"] / team_stats["total_games"] * 100

    close_df = game_df[game_df["is_close_game"]]
    if not close_df.empty:
        close_agg = close_df.groupby("team_code").agg(
            close_games_played=("won", "size"),
            close_wins=("won", "sum"),
        ).reset_index()
        team_stats = team_stats.merge(close_agg, on="team_code", how="left")
    else:
        team_stats["close_games_played"] = 0
        team_stats["close_wins"] = 0

    team_stats["close_games_played"] = team_stats["close_games_played"].fillna(0).astype(int)
    team_stats["close_wins"] = team_stats["close_wins"].fillna(0).astype(int)
    team_stats["close_losses"] = team_stats["close_games_played"] - team_stats["close_wins"]
    team_stats["close_win_pct"] = np.where(
        team_stats["close_games_played"] > 0,
        team_stats["close_wins"] / team_stats["close_games_played"] * 100,
        np.nan,
    )

    total_close_wins = team_stats["close_wins"].sum()
    total_close_games = team_stats["close_games_played"].sum()
    team_stats["league_avg_close_win_pct"] = (
        (total_close_wins / total_close_games * 100) if total_close_games > 0 else 50.0
    )

    team_stats["team_name"] = team_stats["team_code"].map(name_map).fillna(team_stats["team_code"])

    return team_stats.sort_values("close_win_pct", ascending=False, na_position="last").reset_index(drop=True)


def compute_positional_scoring(
    boxscore: pd.DataFrame,
    team_code: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute the percentage of team points contributed by each position.

    Args:
        boxscore: Player boxscore DataFrame (single game or multi-game).
        team_code: If provided, filter to a single team. Otherwise, all teams.

    Returns:
        DataFrame with columns: position, points, pct
    """
    df = classify_player_positions(boxscore)

    if team_code:
        df = df[df["Team"] == team_code]

    if df.empty:
        return pd.DataFrame(columns=["position", "points", "pct"])

    grouped = df.groupby("position")["Points"].sum().reset_index()
    grouped.columns = ["position", "points"]
    total = grouped["points"].sum()
    grouped["pct"] = (grouped["points"] / total * 100).round(1) if total > 0 else 0

    # Ensure all 3 positions exist
    for pos in ["Guard", "Forward", "Center"]:
        if pos not in grouped["position"].values:
            grouped = pd.concat([grouped, pd.DataFrame({"position": [pos], "points": [0], "pct": [0.0]})], ignore_index=True)

    position_order = ["Guard", "Forward", "Center"]
    grouped["position"] = pd.Categorical(grouped["position"], categories=position_order, ordered=True)
    grouped = grouped.sort_values("position").reset_index(drop=True)

    return grouped
