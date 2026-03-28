"""
secrets_manager.py — Centralised Secrets & Environment Manager
===============================================================
Single source of truth for every secret / env-var the application
needs.  Works transparently in both environments:

  * **Local dev** — reads from ``.env`` via ``python-dotenv``
  * **Streamlit Cloud** — reads from ``st.secrets`` (secrets.toml)

Resolution order for ``get_secret(key)``:
  1. ``os.environ``  (covers .env + real env vars)
  2. ``st.secrets``  (Streamlit Cloud)
  3. caller-supplied *default*

Usage::

    from streamlit_app.utils.secrets_manager import (
        get_secret,
        SUPABASE_URL,
        SUPABASE_KEY,
        OPENAI_API_KEY,
        ADMIN_EMAILS,
    )
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, List, Optional

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _st_secrets_get(key: str) -> Any | None:
    """Safely read a value from ``st.secrets`` without crashing outside
    of a running Streamlit context (e.g. CLI scripts, tests)."""
    try:
        import streamlit as st
        return st.secrets.get(key)
    except Exception:
        return None


def get_secret(key_name: str, default: Optional[str] = None) -> Optional[str]:
    """Return the value for *key_name* using the standard fallback chain.

    1. ``os.environ``
    2. ``st.secrets``
    3. *default*
    """
    value = os.environ.get(key_name)
    if value is not None:
        return value

    secret_value = _st_secrets_get(key_name)
    if secret_value is not None:
        return str(secret_value)

    return default


def get_secret_as_list(key_name: str) -> List[str]:
    """Return a secret that is stored as a comma-separated string,
    parsed into a clean Python list with whitespace stripped.

    If the underlying value is already a list/tuple (possible when
    defined in ``secrets.toml`` as a TOML array), it is returned
    as-is after stripping each element.
    """
    try:
        import streamlit as st
        raw = st.secrets.get(key_name)
        if isinstance(raw, (list, tuple)):
            return [e.strip().lower() for e in raw if str(e).strip()]
    except Exception:
        pass

    raw_str = get_secret(key_name, "")
    if not raw_str:
        return []
    return [e.strip().lower() for e in raw_str.split(",") if e.strip()]


# ------------------------------------------------------------------
# Application-wide constants
# ------------------------------------------------------------------
SUPABASE_URL: str = get_secret("SUPABASE_URL", "")
SUPABASE_KEY: str = get_secret("SUPABASE_KEY", "")
OPENAI_API_KEY: str = get_secret("OPENAI_API_KEY", "")
ADMIN_EMAILS: List[str] = get_secret_as_list("ADMIN_EMAILS")

# Database
DATABASE_URL: str = get_secret("DATABASE_URL", "") or get_secret("POSTGRES_URL", "")
POSTGRES_USER: str = get_secret("POSTGRES_USER", "euroleague")
POSTGRES_PASSWORD: str = get_secret("POSTGRES_PASSWORD", "")
POSTGRES_HOST: str = get_secret("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = get_secret("POSTGRES_PORT", "5432")
POSTGRES_DB: str = get_secret("POSTGRES_DB", "euroleague_db")

# Feature / mode flags
USE_DB: bool = get_secret("USE_DB", "false").lower() == "true"
REQUIRE_LOGIN: bool = get_secret("REQUIRE_LOGIN", "true").lower() == "true"
