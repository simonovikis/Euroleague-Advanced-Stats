"""
config_loader.py — Centralised YAML configuration reader
=========================================================
Loads ``config/config.yaml`` once per Streamlit session via
``@st.cache_resource`` and exposes convenience accessors so the
rest of the codebase never touches raw YAML parsing.

Usage::

    from streamlit_app.utils.config_loader import get_config

    cfg = get_config()
    seasons = cfg["data"]["supported_seasons"]
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml
import streamlit as st

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "config" / "config.yaml"


@st.cache_resource(show_spinner=False)
def get_config() -> Dict[str, Any]:
    """Load and cache the application configuration from *config.yaml*.

    The file is read exactly once per Streamlit process thanks to
    ``@st.cache_resource``.  Returns the full nested dict.
    """
    if not _CONFIG_PATH.exists():
        logger.warning("config.yaml not found at %s — using built-in defaults", _CONFIG_PATH)
        return _build_fallback_config()

    with open(_CONFIG_PATH, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg, dict):
        logger.error("config.yaml did not parse to a dict — using fallback")
        return _build_fallback_config()

    return cfg


# ------------------------------------------------------------------
# Convenience helpers (thin wrappers around the dict)
# ------------------------------------------------------------------

def get_supported_seasons() -> list[int]:
    return get_config()["data"]["supported_seasons"]


def get_default_season() -> int:
    return get_config()["data"]["default_season"]


def get_default_competition() -> str:
    return get_config()["app"]["default_competition"]


def get_default_language() -> str:
    return get_config()["app"]["default_language"]


def get_language_map() -> Dict[str, str]:
    return get_config()["app"]["languages"]


def get_cache_ttl() -> int:
    return get_config()["data"]["cache_ttl_seconds"]


def get_leaders_defaults() -> Dict[str, int]:
    return get_config()["data"]["leaders_defaults"]


def get_team_colors() -> Dict[str, Tuple[str, str]]:
    """Return ``{team_code: (primary_hex, secondary_hex)}`` compat dict."""
    raw = get_config().get("ui", {}).get("team_colors", {})
    return {code: (info["primary"], info["secondary"]) for code, info in raw.items()}


def get_team_name_map() -> Dict[str, str]:
    """Return ``{team_code: display_name}`` from the config team_colors section."""
    raw = get_config().get("ui", {}).get("team_colors", {})
    return {code: info.get("name", code) for code, info in raw.items()}


def get_default_accent() -> Tuple[str, str]:
    accent = get_config().get("ui", {}).get("default_accent", {})
    return (accent.get("primary", "#6366f1"), accent.get("secondary", "#8b5cf6"))


# ------------------------------------------------------------------
# Fallback (keeps the app running even without the YAML file)
# ------------------------------------------------------------------

def _build_fallback_config() -> Dict[str, Any]:
    return {
        "app": {
            "page_title": "Euroleague Advanced Analytics",
            "page_icon": "\U0001f3c0",
            "layout": "wide",
            "default_language": "en",
            "default_competition": "E",
            "languages": {
                "\U0001f1ec\U0001f1e7 English": "en",
                "\U0001f1ec\U0001f1f7 Ελληνικά": "el",
                "\U0001f1e9\U0001f1ea Deutsch": "de",
                "\U0001f1ea\U0001f1f8 Español": "es",
            },
        },
        "data": {
            "supported_seasons": list(range(2025, 2010, -1)),
            "default_season": 2025,
            "cache_ttl_seconds": 3600,
            "leaders_defaults": {
                "min_games": 10,
                "min_fga2": 40,
                "min_fga3": 30,
                "min_fta": 30,
            },
        },
        "ui": {
            "default_accent": {"primary": "#6366f1", "secondary": "#8b5cf6"},
            "team_colors": {},
        },
    }
