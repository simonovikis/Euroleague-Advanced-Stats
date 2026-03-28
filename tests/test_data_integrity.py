"""
test_data_integrity.py — Data Quality & Integrity Tests
=========================================================
Automated checks to verify that the Euroleague ETL pipeline has loaded
structurally sound and complete data into the PostgreSQL database.

Run with:
    pytest tests/test_data_integrity.py -v
"""

import sys
from pathlib import Path

import pytest
from sqlalchemy import inspect, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.load_to_db import get_engine

EXPECTED_TABLES = [
    "teams",
    "players",
    "games",
    "play_by_play",
    "player_advanced_stats",
    "boxscores",
    "shots",
]

REQUIRED_PBP_COLUMNS = [
    "season",
    "gamecode",
    "player_id",
    "playtype",
    "period",
    "codeteam",
]

REQUIRED_BOXSCORE_COLUMNS = [
    "season",
    "gamecode",
    "player_id",
    "player",
    "team",
    "points",
]


@pytest.fixture(scope="module")
def engine():
    return get_engine()


@pytest.fixture(scope="module")
def inspector(engine):
    return inspect(engine)


# ------------------------------------------------------------------
# 1. Table Existence
# ------------------------------------------------------------------

class TestTableExistence:
    """Verify that all expected tables exist in the database."""

    def test_all_expected_tables_exist(self, inspector):
        existing = inspector.get_table_names()
        for table in EXPECTED_TABLES:
            assert table in existing, (
                f"Table '{table}' is missing from the database. "
                f"Please run: python -m data_pipeline.load_to_db --season <YEAR> "
                f"or execute database/schema.sql to create the schema."
            )


# ------------------------------------------------------------------
# 2. Row Count (Not Empty)
# ------------------------------------------------------------------

class TestRowCounts:
    """Ensure each required table has at least one row of data."""

    @pytest.mark.parametrize("table", EXPECTED_TABLES)
    def test_table_is_not_empty(self, engine, table):
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
        assert count > 0, (
            f"Table '{table}' has 0 rows. A silent ETL failure may have "
            f"occurred. Please drop the tables and re-run the pipeline: "
            f"python -m data_pipeline.load_to_db --season <YEAR>"
        )


# ------------------------------------------------------------------
# 3. Schema / Column Validation
# ------------------------------------------------------------------

class TestSchemaValidation:
    """Check that critical columns exist in key tables."""

    def test_play_by_play_columns(self, inspector):
        columns = [c["name"] for c in inspector.get_columns("play_by_play")]
        for col in REQUIRED_PBP_COLUMNS:
            assert col in columns, (
                f"Column '{col}' is missing from 'play_by_play'. "
                f"The Euroleague API may have changed its response format. "
                f"Please review the schema in database/schema.sql and update "
                f"both the schema and load_to_db.py accordingly."
            )

    def test_boxscores_columns(self, inspector):
        columns = [c["name"] for c in inspector.get_columns("boxscores")]
        for col in REQUIRED_BOXSCORE_COLUMNS:
            assert col in columns, (
                f"Column '{col}' is missing from 'boxscores'. "
                f"The Euroleague API may have changed its response format. "
                f"Please review the schema in database/schema.sql and update "
                f"both the schema and load_to_db.py accordingly."
            )


# ------------------------------------------------------------------
# 4. Relational Integrity (No Orphans)
# ------------------------------------------------------------------

class TestRelationalIntegrity:
    """Ensure foreign-key relationships hold across tables."""

    def test_no_orphan_play_by_play_gamecodes(self, engine):
        """Every gamecode in play_by_play must exist in games."""
        query = text("""
            SELECT COUNT(DISTINCT pbp.gamecode) AS orphan_count
            FROM play_by_play pbp
            LEFT JOIN games g
                ON pbp.season = g.season AND pbp.gamecode = g.gamecode
            WHERE g.gamecode IS NULL
        """)
        with engine.connect() as conn:
            orphan_count = conn.execute(query).scalar()
        assert orphan_count == 0, (
            f"Found {orphan_count} gamecode(s) in 'play_by_play' with no "
            f"matching row in 'games'. This indicates the schedule was not "
            f"loaded or some games were deleted. Re-run the full ETL pipeline "
            f"to restore consistency."
        )

    def test_no_orphan_boxscore_gamecodes(self, engine):
        """Every gamecode in boxscores must exist in games."""
        query = text("""
            SELECT COUNT(DISTINCT b.gamecode) AS orphan_count
            FROM boxscores b
            LEFT JOIN games g
                ON b.season = g.season AND b.gamecode = g.gamecode
            WHERE g.gamecode IS NULL
        """)
        with engine.connect() as conn:
            orphan_count = conn.execute(query).scalar()
        assert orphan_count == 0, (
            f"Found {orphan_count} gamecode(s) in 'boxscores' with no "
            f"matching row in 'games'. Re-run the full ETL pipeline to "
            f"restore consistency."
        )

    def test_no_orphan_shots_gamecodes(self, engine):
        """Every gamecode in shots must exist in games."""
        query = text("""
            SELECT COUNT(DISTINCT s.gamecode) AS orphan_count
            FROM shots s
            LEFT JOIN games g
                ON s.season = g.season AND s.gamecode = g.gamecode
            WHERE g.gamecode IS NULL
        """)
        with engine.connect() as conn:
            orphan_count = conn.execute(query).scalar()
        assert orphan_count == 0, (
            f"Found {orphan_count} gamecode(s) in 'shots' with no matching "
            f"row in 'games'. Re-run the full ETL pipeline to restore "
            f"consistency."
        )

    def test_no_orphan_advanced_stats_gamecodes(self, engine):
        """Every gamecode in player_advanced_stats must exist in games."""
        query = text("""
            SELECT COUNT(DISTINCT a.gamecode) AS orphan_count
            FROM player_advanced_stats a
            LEFT JOIN games g
                ON a.season = g.season AND a.gamecode = g.gamecode
            WHERE g.gamecode IS NULL
        """)
        with engine.connect() as conn:
            orphan_count = conn.execute(query).scalar()
        assert orphan_count == 0, (
            f"Found {orphan_count} gamecode(s) in 'player_advanced_stats' "
            f"with no matching row in 'games'. Re-run the full ETL pipeline "
            f"to restore consistency."
        )


# ------------------------------------------------------------------
# 5. Null Value Checks on Primary Identifiers
# ------------------------------------------------------------------

class TestNullIdentifiers:
    """Primary identifiers must never be NULL."""

    @pytest.mark.parametrize("table,column", [
        ("games", "season"),
        ("games", "gamecode"),
        ("play_by_play", "season"),
        ("play_by_play", "gamecode"),
        ("boxscores", "season"),
        ("boxscores", "gamecode"),
        ("boxscores", "player_id"),
        ("player_advanced_stats", "season"),
        ("player_advanced_stats", "gamecode"),
        ("player_advanced_stats", "player_id"),
        ("shots", "season"),
        ("shots", "gamecode"),
    ])
    def test_no_null_identifiers(self, engine, table, column):
        query = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
        with engine.connect() as conn:
            null_count = conn.execute(query).scalar()
        assert null_count == 0, (
            f"Found {null_count} NULL value(s) in '{table}.{column}'. "
            f"This primary identifier must never be NULL. The ETL pipeline "
            f"may have ingested malformed data. Inspect the source data, "
            f"then drop and re-load the affected table."
        )
