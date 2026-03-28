"""
transformers.py — Advanced Statistics & PBP Analytics Engine
=============================================================
Takes the raw DataFrames returned by euroleague-api and computes:

BASE STATS (Phase 1):
  1. Estimated Possessions
  2. True Shooting Percentage (TS%)
  3. Offensive / Defensive Rating (per 100 possessions)

ADVANCED PBP ANALYTICS (Phase 2):
  4. Lineup Tracking — 5-man on-court reconstruction via IN/OUT events
  5. Lineup Net Rating — per 5-man combination
  6. Duo/Trio Synergy — 2/3-player combo performance
  7. Clutch Factor — last 5 min, ≤5 pt differential stats
  8. Run-Stopping Ability — breaking opponent 8-0+ runs
  9. Foul Trouble Impact — ORtg/DRtg when star has 2+ fouls early
  10. Assist Network — passer→scorer relationship matrix

CUSTOM METRICS (Phase 2):
  11. True Usage Rate (tUSG%) — includes AST + fouls drawn
  12. Stop Rate — defensive possession ending %
  13. Shot Quality — expected points per zone

All formulas follow standard basketball analytics conventions.
"""

import logging
from itertools import combinations
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ========================================================================
# COLUMN MAPPING (boxscore PascalCase → internal snake_case)
# ========================================================================
COL_MAP = {
    "Player":               "player_name",
    "Player_ID":            "player_id",
    "Team":                 "team_code",
    "Home":                 "is_home",
    "Dorsal":               "dorsal",
    "Minutes":              "minutes_str",
    "Points":               "points",
    "FieldGoalsMade2":      "fgm2",
    "FieldGoalsAttempted2": "fga2",
    "FieldGoalsMade3":      "fgm3",
    "FieldGoalsAttempted3": "fga3",
    "FreeThrowsMade":       "ftm",
    "FreeThrowsAttempted":  "fta",
    "OffensiveRebounds":    "off_rebounds",
    "DefensiveRebounds":    "def_rebounds",
    "TotalRebounds":        "total_rebounds",
    "Assistances":          "assists",
    "Steals":               "steals",
    "Turnovers":            "turnovers",
    "BlocksFavour":         "blocks_favour",
    "BlocksAgainst":        "blocks_against",
    "FoulsCommited":        "fouls_committed",
    "FoulsReceived":        "fouls_received",
    "Plusminus":            "plus_minus",
}


# ========================================================================
# UTILITY HELPERS
# ========================================================================

def parse_minutes(minutes_str: pd.Series) -> pd.Series:
    """Convert 'MM:SS' strings to decimal minutes.  'DNP'/empty → 0.0."""
    def _parse(val):
        if pd.isna(val) or not isinstance(val, str) or ":" not in val:
            return 0.0
        try:
            m, s = val.strip().split(":")
            return int(m) + int(s) / 60.0
        except (ValueError, IndexError):
            return 0.0
    return minutes_str.apply(_parse)


def _markertime_to_seconds(mt: str) -> float:
    """Convert MARKERTIME 'MM:SS' countdown to seconds remaining in period."""
    if pd.isna(mt) or not isinstance(mt, str) or ":" not in mt:
        return 0.0
    try:
        m, s = mt.strip().split(":")
        return int(m) * 60 + int(s)
    except (ValueError, IndexError):
        return 0.0


def format_player_name(raw_name: str) -> str:
    """
    Format raw API name ('LESSORT, MATHIAS') to 'Surname F.' style ('Lessort M.').
    """
    if pd.isna(raw_name) or not isinstance(raw_name, str):
        return str(raw_name) if raw_name else ""
    parts = raw_name.split(",")
    if len(parts) >= 2:
        last = parts[0].strip().title()
        first = parts[1].strip()
        first_initial = first[0].upper() + "." if first else ""
        return f"{last} {first_initial}".strip()
    return raw_name.strip().title()
