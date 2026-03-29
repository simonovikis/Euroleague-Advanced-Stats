"""
Database dependency for FastAPI.

Resolution order for the database connection:
1. If DATABASE_URL is set in the environment, build a standalone engine
   (production on Render — no dependency on Streamlit secrets).
2. Otherwise, reuse the existing get_engine() singleton from
   data_pipeline.load_to_db (local dev, shares pool with Streamlit).
"""

import logging
import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

# ── Module-level engine (lazy init) ──────────────────────────────────

_engine: Engine | None = None


def _get_engine() -> Engine:
    global _engine
    if _engine is not None:
        return _engine

    # Production: use DATABASE_URL directly
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        # Render / Heroku provide postgres:// — normalise for SQLAlchemy 2.x
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
        elif db_url.startswith("postgresql://") and "+psycopg2" not in db_url:
            db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

        _engine = create_engine(
            db_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        logger.info("Database engine created from DATABASE_URL")
        return _engine

    # Local dev: reuse the shared singleton from data_pipeline
    from data_pipeline.load_to_db import get_engine
    _engine = get_engine(use_pooler=True)
    return _engine


def get_db() -> Generator[Connection, None, None]:
    """
    FastAPI dependency that yields a SQLAlchemy **Connection**.

    Usage in an endpoint::

        @router.get("/example")
        def example(conn: Connection = Depends(get_db)):
            df = pd.read_sql(query, conn)
            ...

    The connection is returned to the pool when the request ends.
    """
    engine = _get_engine()
    with engine.connect() as conn:
        yield conn
