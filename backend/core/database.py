"""
Database dependency for FastAPI.
Reuses the existing get_engine() singleton from data_pipeline.load_to_db
so both the Streamlit app and the API share the same connection-pool logic.
"""

import logging
from typing import Generator

from sqlalchemy.engine import Connection, Engine

from data_pipeline.load_to_db import get_engine

logger = logging.getLogger(__name__)

# ── Module-level engine (lazy init) ──────────────────────────────────

_engine: Engine | None = None


def _get_engine() -> Engine:
    global _engine
    if _engine is None:
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
