"""Tests for the feature flags system."""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from streamlit_app.utils.feature_flags import (
    is_feature_enabled,
    FEATURE_FLAGS,
    FEATURE_MAINTENANCE_MESSAGES,
)


class TestIsFeatureEnabled:
    """Tests for the is_feature_enabled() function."""

    def test_env_var_true_values(self):
        for val in ("True", "true", "1", "yes", "on", "TRUE", "YES"):
            with patch.dict(os.environ, {"ENABLE_LLM_CHAT": val}):
                assert is_feature_enabled("ENABLE_LLM_CHAT") is True

    def test_env_var_false_values(self):
        for val in ("False", "false", "0", "no", "off", ""):
            with patch.dict(os.environ, {"ENABLE_LLM_CHAT": val}):
                assert is_feature_enabled("ENABLE_LLM_CHAT") is False

    def test_falls_back_to_default_when_no_env(self):
        with patch.dict(os.environ, {}, clear=True):
            for flag, default in FEATURE_FLAGS.items():
                env_clean = {k: v for k, v in os.environ.items() if k != flag}
                with patch.dict(os.environ, env_clean, clear=True):
                    assert is_feature_enabled(flag) is default

    def test_unknown_flag_returns_false(self):
        with patch.dict(os.environ, {}, clear=True):
            assert is_feature_enabled("ENABLE_NONEXISTENT_FEATURE") is False

    def test_env_var_takes_precedence_over_default(self):
        with patch.dict(os.environ, {"ENABLE_LLM_CHAT": "false"}):
            assert is_feature_enabled("ENABLE_LLM_CHAT") is False
        with patch.dict(os.environ, {"ENABLE_LLM_CHAT": "true"}):
            assert is_feature_enabled("ENABLE_LLM_CHAT") is True

    def test_whitespace_handling(self):
        with patch.dict(os.environ, {"ENABLE_LLM_CHAT": "  True  "}):
            assert is_feature_enabled("ENABLE_LLM_CHAT") is True
        with patch.dict(os.environ, {"ENABLE_LLM_CHAT": "  false  "}):
            assert is_feature_enabled("ENABLE_LLM_CHAT") is False


class TestFeatureFlagsRegistry:
    """Verify the flags registry is well-formed."""

    def test_all_flags_are_boolean(self):
        for flag, default in FEATURE_FLAGS.items():
            assert isinstance(default, bool), f"{flag} default is not bool"

    def test_all_flags_have_maintenance_messages(self):
        for flag in FEATURE_FLAGS:
            assert flag in FEATURE_MAINTENANCE_MESSAGES, (
                f"{flag} missing from FEATURE_MAINTENANCE_MESSAGES"
            )

    def test_maintenance_messages_are_non_empty_strings(self):
        for flag, msg in FEATURE_MAINTENANCE_MESSAGES.items():
            assert isinstance(msg, str) and len(msg) > 0, (
                f"{flag} has an empty/invalid maintenance message"
            )
