"""
transformers — Advanced Statistics & PBP Analytics Engine (Package)
===================================================================
Split into submodules for maintainability. All public symbols are
re-exported here for backward compatibility.
"""

from data_pipeline.transformers.utils import (
    COL_MAP,
    parse_minutes,
    format_player_name,
    _markertime_to_seconds,
)

from data_pipeline.transformers.base_stats import (
    compute_advanced_stats,
    compute_season_player_stats,
)

from data_pipeline.transformers.clutch import (
    filter_clutch_time,
    filter_clutch_shots,
    build_clutch_boxscore,
    compute_clutch_stats,
)

from data_pipeline.transformers.lineups import (
    track_lineups,
    compute_lineup_stats,
    compute_duo_trio_synergy,
    compute_player_stints,
)

from data_pipeline.transformers.playmaking import (
    build_assist_network,
    compute_shot_quality,
    compute_baseline_xp,
    link_assists_to_shots,
    compute_playmaking_metrics,
    compute_total_points_created,
)

from data_pipeline.transformers.game_analysis import (
    detect_runs_and_stoppers,
    foul_trouble_impact,
    compute_referee_stats,
    classify_player_positions,
    compute_close_game_stats,
    compute_positional_scoring,
)
