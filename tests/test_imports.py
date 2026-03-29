"""
test_imports.py — Import Guard Tests
=====================================
Ensures that all public functions across the data pipeline and query layers
are importable. This catches missing functions, circular imports, and syntax
errors early — before Streamlit's runtime hits them.
"""

import sys
from pathlib import Path

import pytest

# Add project root to path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class TestExtractorImports:
    """Verify every public extractor function is importable."""

    def test_import_extract_game_data(self):
        from data_pipeline.extractors import extract_game_data

    def test_import_extract_multiple_games(self):
        from data_pipeline.extractors import extract_multiple_games

    def test_import_extract_team_season_data(self):
        from data_pipeline.extractors import extract_team_season_data

    def test_import_get_boxscore(self):
        from data_pipeline.extractors import get_boxscore

    def test_import_get_play_by_play(self):
        from data_pipeline.extractors import get_play_by_play

    def test_import_get_shot_data(self):
        from data_pipeline.extractors import get_shot_data

    def test_import_get_season_schedule(self):
        from data_pipeline.extractors import get_season_schedule

    def test_import_get_league_efficiency_landscape(self):
        from data_pipeline.extractors import get_league_efficiency_landscape

    def test_import_get_season_game_metadata(self):
        from data_pipeline.extractors import get_season_game_metadata

    def test_import_get_situational_scoring(self):
        from data_pipeline.extractors import get_situational_scoring

    def test_import_apply_team_aliases(self):
        from data_pipeline.extractors import apply_team_aliases


class TestTransformerImports:
    """Verify every public transformer function is importable."""

    def test_import_compute_advanced_stats(self):
        from data_pipeline.transformers import compute_advanced_stats

    def test_import_compute_lineup_stats(self):
        from data_pipeline.transformers import compute_lineup_stats

    def test_import_build_assist_network(self):
        from data_pipeline.transformers import build_assist_network

    def test_import_compute_duo_trio_synergy(self):
        from data_pipeline.transformers import compute_duo_trio_synergy

    def test_import_compute_referee_stats(self):
        from data_pipeline.transformers import compute_referee_stats

    def test_import_compute_clutch_stats(self):
        from data_pipeline.transformers import compute_clutch_stats

    def test_import_compute_shot_quality(self):
        from data_pipeline.transformers import compute_shot_quality

    def test_import_compute_season_player_stats(self):
        from data_pipeline.transformers import compute_season_player_stats

    def test_import_track_lineups(self):
        from data_pipeline.transformers import track_lineups

    def test_import_detect_runs_and_stoppers(self):
        from data_pipeline.transformers import detect_runs_and_stoppers

    def test_import_foul_trouble_impact(self):
        from data_pipeline.transformers import foul_trouble_impact

    def test_import_link_assists_to_shots(self):
        from data_pipeline.transformers import link_assists_to_shots

    def test_import_compute_baseline_xp(self):
        from data_pipeline.transformers import compute_baseline_xp

    def test_import_compute_playmaking_metrics(self):
        from data_pipeline.transformers import compute_playmaking_metrics


class TestQueryImports:
    """Verify every public query/wrapper function is importable."""

    def test_import_fetch_game_data_live(self):
        from streamlit_app.queries import fetch_game_data_live

    def test_import_fetch_season_schedule(self):
        from streamlit_app.queries import fetch_season_schedule

    def test_import_fetch_league_efficiency_landscape(self):
        from streamlit_app.queries import fetch_league_efficiency_landscape

    def test_import_fetch_team_season_data(self):
        from streamlit_app.queries import fetch_team_season_data

    def test_import_fetch_season_game_metadata(self):
        from streamlit_app.queries import fetch_season_game_metadata

    def test_import_fetch_referee_stats(self):
        from streamlit_app.queries import fetch_referee_stats

    def test_import_fetch_situational_scoring(self):
        from streamlit_app.queries import fetch_situational_scoring


class TestLoaderImports:
    """Verify every public loader function is importable."""

    def test_import_get_engine(self):
        from data_pipeline.load_to_db import get_engine

    def test_import_teardown_database(self):
        from data_pipeline.load_to_db import teardown_database

    def test_import_ensure_schema(self):
        from data_pipeline.load_to_db import ensure_schema

    def test_import_load_teams(self):
        from data_pipeline.load_to_db import load_teams

    def test_import_load_players(self):
        from data_pipeline.load_to_db import load_players

    def test_import_load_game(self):
        from data_pipeline.load_to_db import load_game

    def test_import_load_boxscores(self):
        from data_pipeline.load_to_db import load_boxscores

    def test_import_load_play_by_play(self):
        from data_pipeline.load_to_db import load_play_by_play

    def test_import_load_shots(self):
        from data_pipeline.load_to_db import load_shots

    def test_import_load_player_advanced_stats(self):
        from data_pipeline.load_to_db import load_player_advanced_stats

    def test_import_run_pipeline(self):
        from data_pipeline.load_to_db import run_pipeline

    def test_import_load_season(self):
        from data_pipeline.load_to_db import load_season


class TestMonteCarloImports:
    """Verify Monte Carlo simulation engine is importable."""

    def test_import_simulate_season(self):
        from data_pipeline.monte_carlo import simulate_season

    def test_import_build_current_standings(self):
        from data_pipeline.monte_carlo import build_current_standings

    def test_import_get_remaining_games(self):
        from data_pipeline.monte_carlo import get_remaining_games

    def test_import_win_probability(self):
        from data_pipeline.monte_carlo import _win_probability

    def test_import_fetch_full_schedule(self):
        from data_pipeline.monte_carlo import fetch_full_schedule

    def test_import_get_remaining_regular_season_games(self):
        from data_pipeline.monte_carlo import get_remaining_regular_season_games


class TestSyncScheduleImports:
    """Verify schedule seeder is importable."""

    def test_import_seed_schedule(self):
        from data_pipeline.sync_schedule import seed_schedule

    def test_import_fetch_regular_season_schedule(self):
        from data_pipeline.sync_schedule import fetch_regular_season_schedule


class TestSeasonalTrendsImports:
    """Verify seasonal trends ML pipeline is importable."""

    def test_import_aggregate_monthly_stats(self):
        from data_pipeline.seasonal_trends import aggregate_monthly_stats

    def test_import_train_seasonal_form_model(self):
        from data_pipeline.seasonal_trends import train_seasonal_form_model

    def test_import_predict_team_form_curve(self):
        from data_pipeline.seasonal_trends import predict_team_form_curve

    def test_import_build_team_form_features(self):
        from data_pipeline.seasonal_trends import build_team_form_features

    def test_import_generate_insights(self):
        from data_pipeline.seasonal_trends import generate_insights

    def test_import_save_load_model(self):
        from data_pipeline.seasonal_trends import save_model, load_model


class TestDataRepositoryImports:
    """Verify DataRepository is importable."""

    def test_import_data_repository(self):
        from data_pipeline.data_repository import DataRepository
