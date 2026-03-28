"""
feature_flags.py — Enterprise Feature Flags System
====================================================
Reads boolean feature flags from environment variables (via .env)
or Streamlit secrets, with safe defaults (False).

Usage::

    from streamlit_app.utils.feature_flags import is_feature_enabled

    if is_feature_enabled("ENABLE_LLM_CHAT"):
        render_chat_page()
"""

from __future__ import annotations

import logging
from typing import Dict

import streamlit as st

from streamlit_app.utils.secrets_manager import get_secret

logger = logging.getLogger(__name__)

_TRUTHY = {"true", "1", "yes", "on"}

FEATURE_FLAGS: Dict[str, bool] = {
    "ENABLE_LLM_CHAT": True,
    "ENABLE_ML_PREDICTIONS": True,
    "ENABLE_LIVE_MATCH": True,
    "ENABLE_SCOUTING": False,
}

FEATURE_MAINTENANCE_MESSAGES: Dict[str, str] = {
    "ENABLE_LLM_CHAT": "The AI Chatbot feature is currently undergoing maintenance. Please check back later.",
    "ENABLE_ML_PREDICTIONS": "ML-based predictions are currently undergoing maintenance. Please check back later.",
    "ENABLE_LIVE_MATCH": "The Live Match Center is currently undergoing maintenance. Please check back later.",
    "ENABLE_SCOUTING": "The AI Scouting Engine is currently undergoing maintenance. Please check back later.",
}


def is_feature_enabled(feature_name: str) -> bool:
    """Check whether a feature flag is enabled.

    Resolution order (via ``get_secret``):
      1. Environment variable (loaded from .env via ``python-dotenv``)
      2. ``st.secrets`` (Streamlit Cloud deployment)
      3. Built-in default from ``FEATURE_FLAGS`` dict
      4. ``False`` if the flag is completely unknown
    """
    val = get_secret(feature_name)
    if val is not None:
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in _TRUTHY

    default = FEATURE_FLAGS.get(feature_name)
    if default is not None:
        return default

    logger.warning("Unknown feature flag '%s' — defaulting to False", feature_name)
    return False


def show_disabled_message(feature_name: str) -> None:
    """Display a polite st.info message for a disabled feature."""
    msg = FEATURE_MAINTENANCE_MESSAGES.get(
        feature_name,
        "This feature is currently undergoing maintenance. Please check back later.",
    )
    st.info(msg)
