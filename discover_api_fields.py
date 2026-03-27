#!/usr/bin/env python3
"""
discover_api_fields.py — Euroleague API Data Discovery & Introspection
=======================================================================
Explores the euroleague-api Python library, calls every major endpoint
with sample parameters, and generates a comprehensive Markdown data
dictionary (api_data_dictionary.md) documenting all available fields.

Usage:
    python discover_api_fields.py
"""

import inspect
import importlib
import traceback
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COMPETITION = "E"
SAMPLE_SEASON = 2023
SAMPLE_GAMECODE = 1
SAMPLE_ROUND = 1
OUTPUT_FILE = "api_data_dictionary.md"

# ---------------------------------------------------------------------------
# Discovery Registry
# Each entry maps a human-readable name to:
#   (module_path, class_name, method_name, kwargs)
# ---------------------------------------------------------------------------
ENDPOINTS: List[Dict[str, Any]] = [
    # --- Game-Level Endpoints ---
    {
        "name": "Player Boxscore (Single Game)",
        "module": "euroleague_api.boxscore_data",
        "class": "BoxScoreData",
        "method": "get_player_boxscore_stats_data",
        "kwargs": {"season": SAMPLE_SEASON, "gamecode": SAMPLE_GAMECODE},
    },
    {
        "name": "Play-by-Play (Single Game)",
        "module": "euroleague_api.play_by_play_data",
        "class": "PlayByPlay",
        "method": "get_game_play_by_play_data",
        "kwargs": {"season": SAMPLE_SEASON, "gamecode": SAMPLE_GAMECODE},
    },
    {
        "name": "Play-by-Play with Lineups (Single Game)",
        "module": "euroleague_api.play_by_play_data",
        "class": "PlayByPlay",
        "method": "get_pbp_data_with_lineups",
        "kwargs": {"season": SAMPLE_SEASON, "gamecode": SAMPLE_GAMECODE},
    },
    {
        "name": "Shot Data (Single Game)",
        "module": "euroleague_api.shot_data",
        "class": "ShotData",
        "method": "get_game_shot_data",
        "kwargs": {"season": SAMPLE_SEASON, "gamecode": SAMPLE_GAMECODE},
    },
    {
        "name": "Team Advanced Stats (Single Game)",
        "module": "euroleague_api.team_stats",
        "class": "TeamStats",
        "method": "get_team_advanced_stats_single_game",
        "kwargs": {"season": SAMPLE_SEASON, "gamecode": SAMPLE_GAMECODE},
    },
    # --- Season Aggregate Endpoints ---
    {
        "name": "Player Stats Leaders (Season — Scoring)",
        "module": "euroleague_api.player_stats",
        "class": "PlayerStats",
        "method": "get_player_stats_leaders_single_season",
        "kwargs": {"season": SAMPLE_SEASON, "stat_category": "Score", "top_n": 10},
    },
    {
        "name": "Player Stats Leaders (Season — Rebounds)",
        "module": "euroleague_api.player_stats",
        "class": "PlayerStats",
        "method": "get_player_stats_leaders_single_season",
        "kwargs": {"season": SAMPLE_SEASON, "stat_category": "TotalRebounds", "top_n": 10},
    },
    {
        "name": "Player Stats Leaders (Season — Assists)",
        "module": "euroleague_api.player_stats",
        "class": "PlayerStats",
        "method": "get_player_stats_leaders_single_season",
        "kwargs": {"season": SAMPLE_SEASON, "stat_category": "Assistances", "top_n": 10},
    },
    {
        "name": "Player Stats (Season — Traditional, PerGame)",
        "module": "euroleague_api.player_stats",
        "class": "PlayerStats",
        "method": "get_player_stats_single_season",
        "kwargs": {"endpoint": "traditional", "season": SAMPLE_SEASON, "statistic_mode": "PerGame"},
    },
    {
        "name": "Player Stats (Season — Advanced, PerGame)",
        "module": "euroleague_api.player_stats",
        "class": "PlayerStats",
        "method": "get_player_stats_single_season",
        "kwargs": {"endpoint": "advanced", "season": SAMPLE_SEASON, "statistic_mode": "PerGame"},
    },
    {
        "name": "Player Stats (Season — Traditional, Accumulated)",
        "module": "euroleague_api.player_stats",
        "class": "PlayerStats",
        "method": "get_player_stats_single_season",
        "kwargs": {"endpoint": "traditional", "season": SAMPLE_SEASON, "statistic_mode": "Accumulated"},
    },
    {
        "name": "Team Stats (Season — Traditional, PerGame)",
        "module": "euroleague_api.team_stats",
        "class": "TeamStats",
        "method": "get_team_stats_single_season",
        "kwargs": {"endpoint": "traditional", "season": SAMPLE_SEASON, "statistic_mode": "PerGame"},
    },
    {
        "name": "Team Stats (Season — Advanced, PerGame)",
        "module": "euroleague_api.team_stats",
        "class": "TeamStats",
        "method": "get_team_stats_single_season",
        "kwargs": {"endpoint": "advanced", "season": SAMPLE_SEASON, "statistic_mode": "PerGame"},
    },
    {
        "name": "Team Stats (Season — Opponents Advanced, PerGame)",
        "module": "euroleague_api.team_stats",
        "class": "TeamStats",
        "method": "get_team_stats_single_season",
        "kwargs": {"endpoint": "opponentsAdvanced", "season": SAMPLE_SEASON, "statistic_mode": "PerGame"},
    },
    {
        "name": "Team Stats (Season — Opponents Traditional, PerGame)",
        "module": "euroleague_api.team_stats",
        "class": "TeamStats",
        "method": "get_team_stats_single_season",
        "kwargs": {"endpoint": "opponentsTraditional", "season": SAMPLE_SEASON, "statistic_mode": "PerGame"},
    },
    # --- Standings ---
    {
        "name": "Standings (Season, Round 1)",
        "module": "euroleague_api.standings",
        "class": "Standings",
        "method": "get_standings",
        "kwargs": {"season": SAMPLE_SEASON, "round_number": SAMPLE_ROUND},
    },
    # --- Gamecodes / Schedule ---
    {
        "name": "Gamecodes for Season",
        "module": "euroleague_api.play_by_play_data",
        "class": "PlayByPlay",
        "method": "get_gamecodes_season",
        "kwargs": {"season": SAMPLE_SEASON},
    },
    {
        "name": "Gamecodes for Round",
        "module": "euroleague_api.play_by_play_data",
        "class": "PlayByPlay",
        "method": "get_gamecodes_round",
        "kwargs": {"season": SAMPLE_SEASON, "round_number": SAMPLE_ROUND},
    },
]


def call_endpoint(entry: dict) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Dynamically instantiate the class, call the method, return (df, error)."""
    try:
        mod = importlib.import_module(entry["module"])
        cls = getattr(mod, entry["class"])
        instance = cls(COMPETITION)
        method = getattr(instance, entry["method"])
        result = method(**entry["kwargs"])
        if isinstance(result, pd.DataFrame):
            return result, None
        elif isinstance(result, dict):
            # Some endpoints return dicts of DataFrames
            return pd.DataFrame([result]) if result else pd.DataFrame(), None
        else:
            return pd.DataFrame(), f"Unexpected return type: {type(result).__name__}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def format_sample_value(val: Any) -> str:
    """Format a single value for display in the Markdown table."""
    s = str(val)
    if len(s) > 60:
        return s[:57] + "..."
    return s


def build_field_table(df: pd.DataFrame) -> str:
    """Generate a Markdown table of columns, dtypes, and sample values."""
    lines = []
    lines.append("| # | Column Name | Dtype | Sample Value |")
    lines.append("|---|-------------|-------|--------------|")
    sample_row = df.iloc[0] if len(df) > 0 else pd.Series(dtype=object)
    for i, col in enumerate(df.columns, 1):
        dtype = str(df[col].dtype)
        sample = format_sample_value(sample_row.get(col, "N/A"))
        # Escape pipe characters
        sample = sample.replace("|", "\\|")
        lines.append(f"| {i} | `{col}` | `{dtype}` | {sample} |")
    return "\n".join(lines)


def main():
    print(f"🏀 Euroleague API Data Discovery")
    print(f"   Season={SAMPLE_SEASON}, Gamecode={SAMPLE_GAMECODE}")
    print(f"   Exploring {len(ENDPOINTS)} endpoints...\n")

    md_lines = []
    md_lines.append("# 🏀 Euroleague API — Data Dictionary")
    md_lines.append("")
    md_lines.append(f"> Auto-generated on **{datetime.now().strftime('%Y-%m-%d %H:%M')}** "
                    f"by `discover_api_fields.py`")
    md_lines.append(f"> Sample parameters: `season={SAMPLE_SEASON}`, `gamecode={SAMPLE_GAMECODE}`")
    md_lines.append(f"> Library: [`euroleague-api`](https://pypi.org/project/euroleague-api/)")
    md_lines.append("")
    md_lines.append("---")
    md_lines.append("")

    # Table of Contents
    md_lines.append("## Table of Contents")
    md_lines.append("")
    for i, entry in enumerate(ENDPOINTS, 1):
        anchor = entry["name"].lower().replace(" ", "-").replace("(", "").replace(")", "").replace(",", "").replace("—", "").replace("/", "")
        md_lines.append(f"{i}. [{entry['name']}](#{anchor})")
    md_lines.append("")
    md_lines.append("---")
    md_lines.append("")

    success_count = 0
    error_count = 0

    for i, entry in enumerate(ENDPOINTS, 1):
        name = entry["name"]
        call_sig = f"{entry['class']}('{COMPETITION}').{entry['method']}({entry['kwargs']})"
        print(f"  [{i}/{len(ENDPOINTS)}] {name} ... ", end="", flush=True)

        df, error = call_endpoint(entry)

        md_lines.append(f"## {name}")
        md_lines.append("")
        md_lines.append(f"**API Call:** `{call_sig}`")
        md_lines.append("")

        if error:
            error_count += 1
            print(f"❌ {error}")
            md_lines.append(f"> ⚠️ **Error:** `{error}`")
            md_lines.append("")
        elif df is None or df.empty:
            error_count += 1
            print("⚠️ Empty DataFrame")
            md_lines.append("> ⚠️ **Result:** Empty DataFrame (no data returned)")
            md_lines.append("")
        else:
            success_count += 1
            print(f"✅ {len(df)} rows × {len(df.columns)} columns")
            md_lines.append(f"**Shape:** {len(df)} rows × {len(df.columns)} columns")
            md_lines.append("")
            md_lines.append(build_field_table(df))
            md_lines.append("")

        md_lines.append("---")
        md_lines.append("")

    # Summary
    md_lines.append("## Summary")
    md_lines.append("")
    md_lines.append(f"- **Endpoints explored:** {len(ENDPOINTS)}")
    md_lines.append(f"- **Successful:** {success_count}")
    md_lines.append(f"- **Failed / Empty:** {error_count}")
    md_lines.append("")

    # Write file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    print(f"\n✅ Data dictionary written to {OUTPUT_FILE}")
    print(f"   {success_count} successful, {error_count} failed/empty")


if __name__ == "__main__":
    main()
