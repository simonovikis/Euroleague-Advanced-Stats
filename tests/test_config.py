"""Tests for the YAML configuration system."""

import yaml
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def test_config_file_exists():
    assert CONFIG_PATH.exists(), f"config.yaml not found at {CONFIG_PATH}"


def test_config_valid_yaml():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    assert isinstance(cfg, dict)


def test_config_has_required_sections():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    for section in ("app", "data", "ui"):
        assert section in cfg, f"Missing top-level section: {section}"


def test_app_section_keys():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    app = cfg["app"]
    for key in ("page_title", "page_icon", "layout", "default_language",
                "default_competition", "languages"):
        assert key in app, f"Missing app.{key}"
    assert app["default_competition"] == "E"
    assert app["default_language"] in app["languages"].values()


def test_data_section_keys():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    data = cfg["data"]
    assert isinstance(data["supported_seasons"], list)
    assert len(data["supported_seasons"]) > 0
    assert data["default_season"] in data["supported_seasons"]
    assert isinstance(data["cache_ttl_seconds"], int)
    assert data["cache_ttl_seconds"] > 0
    for key in ("min_games", "min_fga2", "min_fga3", "min_fta"):
        assert key in data["leaders_defaults"]


def test_ui_team_colors_structure():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    colors = cfg["ui"]["team_colors"]
    assert len(colors) > 0
    for code, info in colors.items():
        assert "primary" in info, f"{code} missing 'primary'"
        assert "secondary" in info, f"{code} missing 'secondary'"
        assert info["primary"].startswith("#"), f"{code} primary is not a hex color"
        assert info["secondary"].startswith("#"), f"{code} secondary is not a hex color"


def test_config_loader_convenience_helpers():
    """Test the Python loader returns correct types."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from streamlit_app.utils.config_loader import (
        get_supported_seasons,
        get_default_season,
        get_default_competition,
        get_default_language,
        get_language_map,
        get_cache_ttl,
        get_leaders_defaults,
        get_team_colors,
        get_default_accent,
    )

    assert isinstance(get_supported_seasons(), list)
    assert isinstance(get_default_season(), int)
    assert get_default_competition() == "E"
    assert get_default_language() == "en"
    assert isinstance(get_language_map(), dict)
    assert isinstance(get_cache_ttl(), int)
    assert isinstance(get_leaders_defaults(), dict)

    tc = get_team_colors()
    assert isinstance(tc, dict)
    assert all(isinstance(v, tuple) and len(v) == 2 for v in tc.values())

    accent = get_default_accent()
    assert isinstance(accent, tuple) and len(accent) == 2
