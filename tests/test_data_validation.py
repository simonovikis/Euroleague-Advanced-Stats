"""
tests/test_data_validation.py — Data Validation Test Suite
============================================================
Validates the correctness and integrity of data extracted from the
Euroleague API and processed through the transformers.

Tests cover:
  1. Extraction — schema correctness, required columns, data types
  2. Transformation — stat ranges, formula consistency, no NaN explosions
  3. PBP Analytics — lineup tracking, assist network, clutch detection
  4. Shot Data — coordinate validity, zone coverage

Usage:
    pytest tests/ -v
    pytest tests/ -v -k "test_boxscore"      # run specific test
    pytest tests/ -v --tb=short               # short traceback
"""

import warnings
import pytest
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ========================================================================
# FIXTURES — Fetch real data once, reuse across all tests
# ========================================================================

# Default test game: Season 2025 (2025-26), Game 1
TEST_SEASON = 2025
TEST_GAMECODE = 1


@pytest.fixture(scope="session")
def game_data():
    """
    Fetch all game data once per test session.
    This makes a real API call — requires internet.
    """
    from data_pipeline.extractors import extract_game_data
    return extract_game_data(TEST_SEASON, TEST_GAMECODE)


@pytest.fixture(scope="session")
def boxscore(game_data):
    return game_data["boxscore"]


@pytest.fixture(scope="session")
def pbp(game_data):
    return game_data["pbp"]


@pytest.fixture(scope="session")
def shots(game_data):
    return game_data["shots"]


@pytest.fixture(scope="session")
def game_info(game_data):
    return game_data["game_info"]


@pytest.fixture(scope="session")
def advanced_stats(boxscore):
    from data_pipeline.transformers import compute_advanced_stats
    return compute_advanced_stats(boxscore)


@pytest.fixture(scope="session")
def pbp_with_lineups(pbp, boxscore):
    from data_pipeline.transformers import track_lineups
    return track_lineups(pbp, boxscore)


# ========================================================================
# 1. EXTRACTION TESTS — Schema & Data Integrity
# ========================================================================

class TestBoxscoreExtraction:
    """Validate the structure and content of the boxscore DataFrame."""

    REQUIRED_COLUMNS = [
        "Season", "Gamecode", "Home", "Player_ID", "Team",
        "Player", "Minutes", "Points",
        "FieldGoalsMade2", "FieldGoalsAttempted2",
        "FieldGoalsMade3", "FieldGoalsAttempted3",
        "FreeThrowsMade", "FreeThrowsAttempted",
        "OffensiveRebounds", "DefensiveRebounds", "TotalRebounds",
        "Assistances", "Steals", "Turnovers",
        "BlocksFavour", "BlocksAgainst",
        "FoulsCommited", "FoulsReceived", "Plusminus",
    ]

    def test_not_empty(self, boxscore):
        """Boxscore must contain player rows."""
        assert not boxscore.empty, "Boxscore DataFrame is empty"

    def test_required_columns_present(self, boxscore):
        """All expected columns must be present in the boxscore."""
        missing = set(self.REQUIRED_COLUMNS) - set(boxscore.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_exactly_two_teams(self, boxscore):
        """A game must have exactly 2 teams."""
        teams = boxscore["Team"].dropna().unique()
        assert len(teams) == 2, f"Expected 2 teams, got {len(teams)}: {teams}"

    def test_home_away_flags(self, boxscore):
        """Home column must contain only 0 and 1."""
        valid_vals = {0, 1, 0.0, 1.0}
        actual_vals = set(boxscore["Home"].dropna().unique())
        assert actual_vals.issubset(valid_vals), f"Unexpected Home values: {actual_vals}"

    def test_player_ids_not_null(self, boxscore):
        """Every player row must have a Player_ID."""
        null_count = boxscore["Player_ID"].isna().sum()
        assert null_count == 0, f"{null_count} rows have null Player_ID"

    def test_player_ids_mostly_unique(self, boxscore):
        """Player_IDs should be mostly unique. Rare duplicates can occur
        (e.g. player transferred mid-season and appears in both team rosters)."""
        dupes = boxscore["Player_ID"].duplicated().sum()
        dupe_ratio = dupes / len(boxscore)
        assert dupe_ratio < 0.1, f"{dupes} duplicate Player_IDs ({dupe_ratio:.0%} of rows)"

    def test_points_non_negative(self, boxscore):
        """Points must be ≥ 0."""
        min_pts = boxscore["Points"].min()
        assert min_pts >= 0, f"Negative points found: {min_pts}"

    def test_starters_per_team(self, boxscore):
        """Each team should have exactly 5 starters (IsStarter == 1)."""
        if "IsStarter" not in boxscore.columns:
            pytest.skip("IsStarter column not found")
        for team in boxscore["Team"].unique():
            team_df = boxscore[boxscore["Team"] == team]
            starters = team_df[team_df["IsStarter"] == 1.0]
            assert len(starters) == 5, (
                f"Team {team} has {len(starters)} starters, expected 5"
            )

    def test_minutes_format(self, boxscore):
        """Minutes should be 'MM:SS' strings or 'DNP'."""
        for val in boxscore["Minutes"].dropna():
            assert isinstance(val, str), f"Minutes value is not string: {val}"
            assert ":" in val or val.strip() == "DNP", (
                f"Invalid Minutes format: '{val}'"
            )

    def test_stat_consistency(self, boxscore):
        """
        Field goals made cannot exceed field goals attempted.
        FGM2 ≤ FGA2 and FGM3 ≤ FGA3 and FTM ≤ FTA.
        """
        for made_col, att_col in [
            ("FieldGoalsMade2", "FieldGoalsAttempted2"),
            ("FieldGoalsMade3", "FieldGoalsAttempted3"),
            ("FreeThrowsMade", "FreeThrowsAttempted"),
        ]:
            violations = boxscore[boxscore[made_col] > boxscore[att_col]]
            assert violations.empty, (
                f"{len(violations)} rows have {made_col} > {att_col}: "
                f"{violations[['Player', made_col, att_col]].to_string()}"
            )

    def test_total_rebounds_sum(self, boxscore):
        """TotalRebounds should equal OffensiveRebounds + DefensiveRebounds."""
        computed = boxscore["OffensiveRebounds"] + boxscore["DefensiveRebounds"]
        mismatches = boxscore[boxscore["TotalRebounds"] != computed]
        assert mismatches.empty, (
            f"{len(mismatches)} rows have TotalRebounds ≠ ORB + DRB"
        )


class TestPBPExtraction:
    """Validate play-by-play data structure and content."""

    REQUIRED_COLUMNS = [
        "Season", "Gamecode", "PLAYTYPE", "PLAYER_ID",
        "PLAYER", "CODETEAM", "PERIOD", "MARKERTIME",
    ]

    def test_not_empty(self, pbp):
        """PBP must contain action rows."""
        assert not pbp.empty, "PBP DataFrame is empty"

    def test_required_columns_present(self, pbp):
        missing = set(self.REQUIRED_COLUMNS) - set(pbp.columns)
        assert not missing, f"Missing PBP columns: {missing}"

    def test_at_least_4_periods(self, pbp):
        """A completed game has at least 4 periods (quarters)."""
        max_period = pbp["PERIOD"].max()
        assert max_period >= 4, f"Only {max_period} periods found"

    def test_valid_playtypes(self, pbp):
        """All PLAYTYPEs should be known basketball events."""
        known_types = {
            "2FGM", "2FGA", "3FGM", "3FGA", "FTM", "FTA",
            "D", "O", "TO", "ST", "AS", "FV", "AG",
            "IN", "OUT", "CM", "CMU", "CMT", "CMF", "CMP",
            "RV", "JB", "BP", "EG", "EP", "CCH",
            "TOUT", "TOUT_TV", "OF",
        }
        actual = set(pbp["PLAYTYPE"].dropna().str.strip().unique())
        unknown = actual - known_types
        # Warn rather than fail — API may add new play types
        if unknown:
            warnings.warn(f"Unknown PLAYTYPEs found: {unknown}")

    def test_markertime_format(self, pbp):
        """MARKERTIME should mostly follow 'MM:SS' format.
        Some PBP events (period start/end, timeouts) may have empty MARKERTIME."""
        non_empty = pbp["MARKERTIME"].dropna()
        non_empty = non_empty[non_empty.astype(str).str.strip() != ""]
        valid = non_empty.apply(lambda v: ":" in str(v))
        valid_pct = valid.mean()
        assert valid_pct > 0.9, f"Only {valid_pct:.0%} of MARKERTIME values have MM:SS format"

    def test_true_numberofplay_sequential(self, pbp):
        """TRUE_NUMBEROFPLAY should be strictly increasing."""
        if "TRUE_NUMBEROFPLAY" not in pbp.columns:
            pytest.skip("TRUE_NUMBEROFPLAY column not found")
        tnop = pbp["TRUE_NUMBEROFPLAY"].values
        assert np.all(tnop[1:] >= tnop[:-1]), "TRUE_NUMBEROFPLAY is not sequential"

    def test_sub_events_paired(self, pbp):
        """IN and OUT events should appear in roughly equal numbers."""
        ins = len(pbp[pbp["PLAYTYPE"] == "IN"])
        outs = len(pbp[pbp["PLAYTYPE"] == "OUT"])
        assert abs(ins - outs) <= 2, (
            f"Sub events unbalanced: {ins} INs vs {outs} OUTs"
        )


class TestShotExtraction:
    """Validate shot data with coordinates."""

    def test_not_empty(self, shots):
        assert not shots.empty, "Shot data DataFrame is empty"

    def test_has_coordinates(self, shots):
        """Shot data must have COORD_X and COORD_Y columns."""
        assert "COORD_X" in shots.columns, "Missing COORD_X"
        assert "COORD_Y" in shots.columns, "Missing COORD_Y"

    def test_coordinates_reasonable(self, shots):
        """X/Y coordinates should be within court boundaries."""
        # Court diagram coordinates are typically in range [-800, 800] x [-100, 1500]
        assert shots["COORD_X"].between(-1000, 1000).all(), "COORD_X out of range"
        assert shots["COORD_Y"].between(-200, 1600).all(), "COORD_Y out of range"

    def test_has_zone(self, shots):
        assert "ZONE" in shots.columns, "Missing ZONE column"

    def test_points_values(self, shots):
        """POINTS should be 0 (miss), 1 (FT), 2, or 3."""
        valid_points = {0, 1, 2, 3}
        actual = set(int(x) for x in shots["POINTS"].dropna().unique())
        assert actual.issubset(valid_points), f"Unexpected POINTS values: {actual - valid_points}"

    def test_action_types(self, shots):
        """ID_ACTION should be 2FGM, 2FGA, 3FGM, 3FGA, or FTM/FTA."""
        valid = {"2FGM", "2FGA", "3FGM", "3FGA", "FTM", "FTA"}
        actual = set(shots["ID_ACTION"].dropna().unique())
        assert actual.issubset(valid), f"Unexpected ID_ACTION: {actual - valid}"


class TestGameInfo:
    """Validate extracted game metadata."""

    def test_not_empty(self, game_info):
        assert not game_info.empty

    def test_has_two_teams(self, game_info):
        gi = game_info.iloc[0]
        assert pd.notna(gi["home_team"]), "Missing home_team"
        assert pd.notna(gi["away_team"]), "Missing away_team"
        assert gi["home_team"] != gi["away_team"], "Home and away teams are the same"

    def test_scores_positive(self, game_info):
        gi = game_info.iloc[0]
        assert gi["home_score"] > 0, "Home score should be positive"
        assert gi["away_score"] > 0, "Away score should be positive"


# ========================================================================
# 2. TRANSFORMATION TESTS — Advanced Stat Validity
# ========================================================================

class TestAdvancedStats:
    """Validate computed advanced statistics."""

    def test_not_empty(self, advanced_stats):
        assert not advanced_stats.empty

    def test_all_expected_columns(self, advanced_stats):
        expected = [
            "player_name", "team_code", "minutes", "points",
            "possessions", "ts_pct", "off_rating", "def_rating",
            "true_usg_pct", "stop_rate",
        ]
        missing = set(expected) - set(advanced_stats.columns)
        assert not missing, f"Missing advanced stat columns: {missing}"

    def test_minutes_non_negative(self, advanced_stats):
        assert (advanced_stats["minutes"] >= 0).all(), "Negative minutes found"

    def test_ts_pct_range(self, advanced_stats):
        """
        TS% should be between 0 and ~1.5.
        Theoretical max: a player who only makes 3s scores 3 pts on 1 FGA
         → TS% = 3 / (2 × 1) = 1.5.
        Players with 0 attempts get NaN (which is fine).
        """
        valid = advanced_stats["ts_pct"].dropna()
        if not valid.empty:
            assert valid.min() >= 0, f"TS% below 0: {valid.min()}"
            assert valid.max() <= 1.5, f"TS% above 1.5: {valid.max()}"

    def test_possessions_non_negative(self, advanced_stats):
        valid = advanced_stats["possessions"].dropna()
        assert (valid >= 0).all(), f"Negative possessions found"

    def test_ortg_positive_when_has_possessions(self, advanced_stats):
        """Players with possessions and points should have positive ORtg."""
        with_poss = advanced_stats[
            (advanced_stats["possessions"] > 0) & (advanced_stats["points"] > 0)
        ]
        if not with_poss.empty:
            assert (with_poss["off_rating"].dropna() > 0).all(), "ORtg <= 0 for scoring players"

    def test_drtg_consistent_within_team(self, advanced_stats):
        """
        DRtg is team-level — all players on the same team should have
        the same DRtg value (within floating-point tolerance).
        """
        for _, grp in advanced_stats.groupby(["Season", "Gamecode", "team_code"]):
            valid_drtg = grp["def_rating"].dropna()
            if len(valid_drtg) > 1:
                assert valid_drtg.std() < 0.01, (
                    f"DRtg varies within team: std={valid_drtg.std():.4f}"
                )

    def test_stop_rate_range(self, advanced_stats):
        """Stop Rate should be between 0 and some reasonable max."""
        valid = advanced_stats["stop_rate"].dropna()
        if not valid.empty:
            assert valid.min() >= 0, f"Stop Rate below 0"

    def test_points_formula_consistency(self, advanced_stats):
        """
        Points should approximately equal: 2*FGM2 + 3*FGM3 + FTM.
        This validates that the raw stats are internally consistent.
        """
        df = advanced_stats.copy()
        computed = 2 * df["fgm2"] + 3 * df["fgm3"] + df["ftm"]
        diff = (df["points"] - computed).abs()
        # Allow small tolerance (rounding in rare cases)
        violations = diff[diff > 0.5]
        assert violations.empty, (
            f"{len(violations)} players have PTS ≠ 2×FGM2 + 3×FGM3 + FTM"
        )


# ========================================================================
# 3. PBP ANALYTICS TESTS — Lineup & Assist Validation
# ========================================================================

class TestLineupTracking:
    """Validate lineup reconstruction from PBP substitution events."""

    def test_lineups_populated(self, pbp_with_lineups):
        assert "home_lineup" in pbp_with_lineups.columns
        assert "away_lineup" in pbp_with_lineups.columns

    def test_starting_lineups_have_five(self, pbp_with_lineups):
        """First recorded lineup should have exactly 5 players per team."""
        first = pbp_with_lineups.iloc[0]
        assert len(first["home_lineup"]) == 5, (
            f"Home starting lineup has {len(first['home_lineup'])} players"
        )
        assert len(first["away_lineup"]) == 5, (
            f"Away starting lineup has {len(first['away_lineup'])} players"
        )

    def test_lineups_never_empty(self, pbp_with_lineups):
        """Lineups should never drop to 0 players."""
        for _, row in pbp_with_lineups.iterrows():
            assert len(row["home_lineup"]) >= 4, (
                f"Home lineup has {len(row['home_lineup'])} players at play {row.get('TRUE_NUMBEROFPLAY')}"
            )
            assert len(row["away_lineup"]) >= 4, (
                f"Away lineup has {len(row['away_lineup'])} players at play {row.get('TRUE_NUMBEROFPLAY')}"
            )


class TestLineupStats:
    """Validate lineup-level aggregated stats."""

    def test_lineup_stats_computed(self, pbp_with_lineups):
        from data_pipeline.transformers import compute_lineup_stats
        stats = compute_lineup_stats(pbp_with_lineups)
        assert not stats.empty, "Lineup stats DataFrame is empty"
        assert "net_rtg" in stats.columns

    def test_both_teams_represented(self, pbp_with_lineups):
        from data_pipeline.transformers import compute_lineup_stats
        stats = compute_lineup_stats(pbp_with_lineups)
        teams = stats["team"].unique()
        assert len(teams) == 2, f"Expected 2 teams in lineup stats, got {len(teams)}"


class TestAssistNetwork:
    """Validate assist network extraction."""

    def test_assist_network_computed(self, pbp):
        from data_pipeline.transformers import build_assist_network
        network = build_assist_network(pbp)
        assert not network.empty, "Assist network is empty"

    def test_assist_columns(self, pbp):
        from data_pipeline.transformers import build_assist_network
        network = build_assist_network(pbp)
        required = ["assister_name", "scorer_name", "team", "count"]
        missing = set(required) - set(network.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_assister_not_scorer(self, pbp):
        """A player should not assist themselves."""
        from data_pipeline.transformers import build_assist_network
        network = build_assist_network(pbp)
        self_assists = network[network["assister_id"] == network["scorer_id"]]
        assert self_assists.empty, (
            f"{len(self_assists)} self-assists found"
        )

    def test_assist_counts_positive(self, pbp):
        from data_pipeline.transformers import build_assist_network
        network = build_assist_network(pbp)
        assert (network["count"] > 0).all(), "Assist counts must be positive"


class TestClutchStats:
    """Validate clutch situation detection."""

    def test_clutch_computation_runs(self, pbp, boxscore):
        """Clutch function should run without error (may return empty)."""
        from data_pipeline.transformers import compute_clutch_stats
        clutch = compute_clutch_stats(pbp, boxscore)
        assert isinstance(clutch, pd.DataFrame)

    def test_clutch_ts_range(self, pbp, boxscore):
        from data_pipeline.transformers import compute_clutch_stats
        clutch = compute_clutch_stats(pbp, boxscore)
        if not clutch.empty and "clutch_ts_pct" in clutch.columns:
            valid = clutch["clutch_ts_pct"].dropna()
            if not valid.empty:
                assert valid.min() >= 0, "Clutch TS% below 0"
                assert valid.max() <= 1.5, "Clutch TS% above 1.5"


class TestShotQuality:
    """Validate shot quality computations."""

    def test_shot_quality_computed(self, shots):
        from data_pipeline.transformers import compute_shot_quality
        sq = compute_shot_quality(shots)
        assert not sq.empty, "Shot quality DataFrame is empty"

    def test_fg_pct_range(self, shots):
        from data_pipeline.transformers import compute_shot_quality
        sq = compute_shot_quality(shots)
        assert (sq["fg_pct"] >= 0).all(), "FG% below 0"
        assert (sq["fg_pct"] <= 1).all(), "FG% above 1"

    def test_expected_pts_positive(self, shots):
        from data_pipeline.transformers import compute_shot_quality
        sq = compute_shot_quality(shots)
        assert (sq["avg_expected_pts"] >= 0).all(), "Expected pts below 0"


# ========================================================================
# 4. INTEGRATION TEST — Full Pipeline Smoke Test
# ========================================================================

class TestFullPipeline:
    """End-to-end integration test: extract → transform → validate."""

    def test_queries_live_mode(self):
        """The queries.py live mode should return all expected keys."""
        from streamlit_app.queries import fetch_game_data_live
        result = fetch_game_data_live(TEST_SEASON, TEST_GAMECODE)

        expected_keys = {
            "boxscore", "pbp", "shots", "game_info",
            "advanced_stats", "pbp_with_lineups",
            "lineup_stats", "assist_network",
            "clutch_stats", "run_stoppers", "foul_trouble",
            "duo_synergy", "trio_synergy", "shot_quality",
        }
        missing = expected_keys - set(result.keys())
        assert not missing, f"Missing keys in live data: {missing}"

    def test_all_dataframes_valid_types(self):
        """Every value returned should be a pandas DataFrame."""
        from streamlit_app.queries import fetch_game_data_live
        result = fetch_game_data_live(TEST_SEASON, TEST_GAMECODE)

        for key, val in result.items():
            assert isinstance(val, pd.DataFrame), (
                f"'{key}' is {type(val).__name__}, expected DataFrame"
            )
