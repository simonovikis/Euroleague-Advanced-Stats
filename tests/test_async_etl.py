"""
tests/test_async_etl.py — Concurrent ETL Pipeline Test Suite
===============================================================
Validates the Phase 59.5 refactored ETL pipeline:
  1. Concurrent API extraction (ThreadPoolExecutor + Semaphore)
  2. Retry logic (tenacity for 429/5xx)
  3. Bulk database inserts (psycopg2 execute_values)
  4. Batch pipeline orchestration (run_pipeline_batch)
  5. Semaphore / concurrency-limit enforcement

All tests are fully offline — the Euroleague API and Supabase database
are never contacted. We mock at the lowest possible layer.

Usage:
    pytest tests/test_async_etl.py -v
    pytest tests/test_async_etl.py -v -k "test_concurrent"
"""

import threading
import time
from unittest.mock import MagicMock, patch, call

import numpy as np
import pandas as pd
import pytest
from requests.exceptions import HTTPError


# ========================================================================
# FIXTURE FACTORIES — realistic dummy DataFrames
# ========================================================================

def _make_boxscore(season: int, gamecode: int) -> pd.DataFrame:
    """Two-team boxscore with 5 home + 5 away players."""
    rows = []
    for i in range(5):
        rows.append({
            "Season": season, "Gamecode": gamecode, "Home": 1,
            "Player_ID": f"P{gamecode}H{i}", "IsStarter": 1, "IsPlaying": 1,
            "Team": "OLY", "Dorsal": str(i), "Player": f"HomePlayer {i}",
            "Minutes": "25:00", "Points": 10 + i,
            "FieldGoalsMade2": 2, "FieldGoalsAttempted2": 5,
            "FieldGoalsMade3": 1, "FieldGoalsAttempted3": 3,
            "FreeThrowsMade": 2, "FreeThrowsAttempted": 2,
            "OffensiveRebounds": 1, "DefensiveRebounds": 3,
            "TotalRebounds": 4, "Assistances": 3, "Steals": 1,
            "Turnovers": 1, "BlocksFavour": 0, "BlocksAgainst": 0,
            "FoulsCommited": 2, "FoulsReceived": 2,
            "Valuation": 15, "Plusminus": 5.0,
        })
        rows.append({
            "Season": season, "Gamecode": gamecode, "Home": 0,
            "Player_ID": f"P{gamecode}A{i}", "IsStarter": 1, "IsPlaying": 1,
            "Team": "PAO", "Dorsal": str(10 + i), "Player": f"AwayPlayer {i}",
            "Minutes": "24:00", "Points": 8 + i,
            "FieldGoalsMade2": 2, "FieldGoalsAttempted2": 6,
            "FieldGoalsMade3": 0, "FieldGoalsAttempted3": 2,
            "FreeThrowsMade": 2, "FreeThrowsAttempted": 3,
            "OffensiveRebounds": 1, "DefensiveRebounds": 2,
            "TotalRebounds": 3, "Assistances": 2, "Steals": 1,
            "Turnovers": 2, "BlocksFavour": 1, "BlocksAgainst": 0,
            "FoulsCommited": 3, "FoulsReceived": 1,
            "Valuation": 10, "Plusminus": -5.0,
        })
    return pd.DataFrame(rows)


def _make_pbp(season: int, gamecode: int) -> pd.DataFrame:
    """Minimal play-by-play with scoring actions and subs."""
    actions = [
        ("2FGM", "P{gc}H0", "HomePlayer 0", "OLY", 1, "10:00", 0, 2, 0, ""),
        ("3FGM", "P{gc}A0", "AwayPlayer 0", "PAO", 1, "09:30", 1, 2, 3, ""),
        ("FTM",  "P{gc}H1", "HomePlayer 1", "OLY", 1, "09:00", 2, 3, 3, ""),
        ("TO",   "P{gc}A1", "AwayPlayer 1", "PAO", 2, "09:50", 3, 3, 3, ""),
        ("2FGM", "P{gc}H2", "HomePlayer 2", "OLY", 3, "08:00", 4, 5, 3, ""),
        ("AS",   "P{gc}H0", "HomePlayer 0", "OLY", 4, "04:00", 5, 70, 65, ""),
    ]
    rows = []
    for i, (pt, pid, pname, team, period, mt, nop, pa, pb, comment) in enumerate(actions):
        rows.append({
            "Season": season, "Gamecode": gamecode,
            "PLAYTYPE": pt,
            "PLAYER_ID": pid.format(gc=gamecode),
            "PLAYER": pname, "CODETEAM": team,
            "PERIOD": period, "MARKERTIME": mt,
            "NUMBEROFPLAY": nop,
            "TRUE_NUMBEROFPLAY": i,
            "POINTS_A": pa, "POINTS_B": pb,
            "PLAYINFO": "", "COMMENT": comment,
        })
    return pd.DataFrame(rows)


def _make_shots(season: int, gamecode: int) -> pd.DataFrame:
    """A handful of shot records with coordinates."""
    rows = []
    for i in range(4):
        rows.append({
            "Season": season, "Gamecode": gamecode,
            "NUM_ANOT": i + 1,
            "TEAM": "OLY" if i % 2 == 0 else "PAO",
            "ID_PLAYER": f"P{gamecode}H{i}" if i % 2 == 0 else f"P{gamecode}A{i}",
            "PLAYER": f"Player {i}",
            "ID_ACTION": "2FGM" if i < 2 else "3FGA",
            "ACTION": "Two Pointer" if i < 2 else "Three Pointer",
            "POINTS": 2 if i < 2 else 0,
            "COORD_X": 100 + i * 10, "COORD_Y": 200 + i * 5,
            "ZONE": chr(65 + i),
            "FASTBREAK": 0, "SECOND_CHANCE": 0, "POINTS_OFF_TURNOVER": 0,
            "MINUTE": 5 + i, "CONSOLE": f"Q{i+1}",
            "POINTS_A": 50 + i, "POINTS_B": 48 + i,
        })
    return pd.DataFrame(rows)


def _make_game_data(season: int, gamecode: int) -> dict:
    """Full game data dict as returned by extract_game_data."""
    box = _make_boxscore(season, gamecode)
    pbp = _make_pbp(season, gamecode)
    shots = _make_shots(season, gamecode)
    game_info = pd.DataFrame([{
        "season": season, "gamecode": gamecode,
        "home_team": "OLY", "away_team": "PAO",
        "home_score": 70, "away_score": 65,
    }])
    return {"boxscore": box, "pbp": pbp, "shots": shots, "game_info": game_info}


# ========================================================================
# 1. EXTRACTION LAYER TESTS
# ========================================================================

class TestConcurrentExtraction:
    """Verify that extract_game_data fetches box/pbp/shots in parallel."""

    @patch("data_pipeline.extractors.get_shot_data")
    @patch("data_pipeline.extractors.get_play_by_play")
    @patch("data_pipeline.extractors.get_boxscore")
    def test_extract_game_data_calls_all_three(self, mock_box, mock_pbp, mock_shots):
        """extract_game_data must invoke boxscore, pbp, and shots fetchers."""
        mock_box.return_value = _make_boxscore(2025, 1)
        mock_pbp.return_value = _make_pbp(2025, 1)
        mock_shots.return_value = _make_shots(2025, 1)

        from data_pipeline.extractors import extract_game_data
        result = extract_game_data(2025, 1, "E")

        mock_box.assert_called_once_with(2025, 1, "E")
        mock_pbp.assert_called_once_with(2025, 1, "E")
        mock_shots.assert_called_once_with(2025, 1, "E")

        assert not result["boxscore"].empty
        assert not result["pbp"].empty
        assert not result["shots"].empty
        assert not result["game_info"].empty

    @patch("data_pipeline.extractors.get_shot_data")
    @patch("data_pipeline.extractors.get_play_by_play")
    @patch("data_pipeline.extractors.get_boxscore")
    def test_extract_game_data_runs_concurrently(self, mock_box, mock_pbp, mock_shots):
        """The three fetchers must overlap in time (not sequential)."""
        barrier = threading.Barrier(3, timeout=5)

        def slow_box(*a, **kw):
            barrier.wait()
            return _make_boxscore(2025, 1)

        def slow_pbp(*a, **kw):
            barrier.wait()
            return _make_pbp(2025, 1)

        def slow_shots(*a, **kw):
            barrier.wait()
            return _make_shots(2025, 1)

        mock_box.side_effect = slow_box
        mock_pbp.side_effect = slow_pbp
        mock_shots.side_effect = slow_shots

        from data_pipeline.extractors import extract_game_data
        result = extract_game_data(2025, 1, "E")

        assert not result["boxscore"].empty

    @patch("data_pipeline.extractors.get_shot_data")
    @patch("data_pipeline.extractors.get_play_by_play")
    @patch("data_pipeline.extractors.get_boxscore")
    def test_extract_game_data_produces_correct_game_info(self, mock_box, mock_pbp, mock_shots):
        """game_info must derive home/away teams and scores from boxscore/pbp."""
        mock_box.return_value = _make_boxscore(2025, 5)
        mock_pbp.return_value = _make_pbp(2025, 5)
        mock_shots.return_value = _make_shots(2025, 5)

        from data_pipeline.extractors import extract_game_data
        result = extract_game_data(2025, 5, "E")
        gi = result["game_info"]

        assert gi.iloc[0]["season"] == 2025
        assert gi.iloc[0]["gamecode"] == 5
        assert gi.iloc[0]["home_team"] == "OLY"
        assert gi.iloc[0]["away_team"] == "PAO"
        assert gi.iloc[0]["home_score"] is not None
        assert gi.iloc[0]["away_score"] is not None


class TestBatchExtraction:
    """Verify extract_games_concurrent fetches multiple games in parallel."""

    @patch("data_pipeline.extractors.extract_game_data")
    def test_extracts_all_games(self, mock_extract):
        """All gamecodes should produce results when API succeeds."""
        mock_extract.side_effect = lambda s, gc, c: _make_game_data(s, gc)

        from data_pipeline.extractors import extract_games_concurrent
        results = extract_games_concurrent(2025, [1, 2, 3], "E", max_workers=3)

        assert len(results) == 3
        assert mock_extract.call_count == 3

    @patch("data_pipeline.extractors.extract_game_data")
    def test_handles_partial_failures(self, mock_extract):
        """Failed games are skipped; successful ones are returned."""
        def side_effect(s, gc, c):
            if gc == 2:
                raise RuntimeError("API exploded")
            return _make_game_data(s, gc)

        mock_extract.side_effect = side_effect

        from data_pipeline.extractors import extract_games_concurrent
        results = extract_games_concurrent(2025, [1, 2, 3], "E", max_workers=3)

        assert len(results) == 2

    @patch("data_pipeline.extractors.extract_game_data")
    def test_progress_callback_called(self, mock_extract):
        """progress_callback must be invoked once per game."""
        mock_extract.side_effect = lambda s, gc, c: _make_game_data(s, gc)
        cb = MagicMock()

        from data_pipeline.extractors import extract_games_concurrent
        extract_games_concurrent(2025, [1, 2, 3], "E", max_workers=3, progress_callback=cb)

        assert cb.call_count == 3
        totals = [c.args[1] for c in cb.call_args_list]
        assert all(t == 3 for t in totals)

    @patch("data_pipeline.extractors.extract_game_data")
    def test_returns_empty_on_total_failure(self, mock_extract):
        """If every game fails, return an empty list."""
        mock_extract.side_effect = RuntimeError("everything is broken")

        from data_pipeline.extractors import extract_games_concurrent
        results = extract_games_concurrent(2025, [1, 2], "E", max_workers=2)

        assert results == []

    @patch("data_pipeline.extractors.extract_game_data")
    def test_max_workers_respected(self, mock_extract):
        """With max_workers=2 and 4 games, at most 2 run concurrently."""
        concurrency_counter = {"current": 0, "peak": 0}
        lock = threading.Lock()

        def tracked_extract(s, gc, c):
            with lock:
                concurrency_counter["current"] += 1
                concurrency_counter["peak"] = max(
                    concurrency_counter["peak"], concurrency_counter["current"]
                )
            time.sleep(0.05)
            result = _make_game_data(s, gc)
            with lock:
                concurrency_counter["current"] -= 1
            return result

        mock_extract.side_effect = tracked_extract

        from data_pipeline.extractors import extract_games_concurrent
        extract_games_concurrent(2025, [1, 2, 3, 4], "E", max_workers=2)

        assert concurrency_counter["peak"] <= 2


# ========================================================================
# 2. RETRY LOGIC TESTS
# ========================================================================

class TestRetryLogic:
    """Verify tenacity retry configuration on the raw fetch functions."""

    @patch("data_pipeline.extractors._api_semaphore", threading.Semaphore(15))
    @patch("data_pipeline.extractors.BoxScoreData")
    def test_retries_on_429(self, mock_cls):
        """A 429 response should trigger retries then succeed."""
        mock_resp = MagicMock()
        mock_resp.status_code = 429

        err_429 = HTTPError(response=mock_resp)

        api_instance = MagicMock()
        call_count = {"n": 0}

        def flaky_call(season, gamecode):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise err_429
            return _make_boxscore(2025, 1)

        api_instance.get_player_boxscore_stats_data.side_effect = flaky_call
        mock_cls.return_value = api_instance

        from data_pipeline.extractors import _fetch_boxscore_raw
        result = _fetch_boxscore_raw(2025, 1, "E")

        assert not result.empty
        assert call_count["n"] == 3

    @patch("data_pipeline.extractors._api_semaphore", threading.Semaphore(15))
    @patch("data_pipeline.extractors.BoxScoreData")
    def test_does_not_retry_on_404(self, mock_cls):
        """A 404 is not retryable — should raise immediately."""
        mock_resp = MagicMock()
        mock_resp.status_code = 404

        err_404 = HTTPError(response=mock_resp)
        api_instance = MagicMock()
        api_instance.get_player_boxscore_stats_data.side_effect = err_404
        mock_cls.return_value = api_instance

        from data_pipeline.extractors import _fetch_boxscore_raw
        with pytest.raises(HTTPError):
            _fetch_boxscore_raw(2025, 1, "E")

        assert api_instance.get_player_boxscore_stats_data.call_count == 1

    @patch("data_pipeline.extractors._api_semaphore", threading.Semaphore(15))
    @patch("data_pipeline.extractors.BoxScoreData")
    def test_retries_on_500(self, mock_cls):
        """A 500 server error should trigger retries."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        err_500 = HTTPError(response=mock_resp)

        api_instance = MagicMock()
        call_count = {"n": 0}

        def flaky(s, g):
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise err_500
            return _make_boxscore(2025, 1)

        api_instance.get_player_boxscore_stats_data.side_effect = flaky
        mock_cls.return_value = api_instance

        from data_pipeline.extractors import _fetch_boxscore_raw
        result = _fetch_boxscore_raw(2025, 1, "E")
        assert not result.empty
        assert call_count["n"] == 2

    def test_is_retryable_identifies_transient_errors(self):
        """_is_retryable should return True for 429, 5xx, connection errors."""
        from data_pipeline.extractors import _is_retryable
        from requests.exceptions import ConnectionError, Timeout

        resp_429 = MagicMock()
        resp_429.status_code = 429
        assert _is_retryable(HTTPError(response=resp_429)) is True

        resp_502 = MagicMock()
        resp_502.status_code = 502
        assert _is_retryable(HTTPError(response=resp_502)) is True

        resp_400 = MagicMock()
        resp_400.status_code = 400
        assert _is_retryable(HTTPError(response=resp_400)) is False

        assert _is_retryable(ConnectionError("refused")) is True
        assert _is_retryable(Timeout("timed out")) is True
        assert _is_retryable(ValueError("bad value")) is False


# ========================================================================
# 3. SEMAPHORE / CONCURRENCY LIMIT TESTS
# ========================================================================

class TestSemaphoreEnforcement:
    """Verify that the global _api_semaphore caps concurrent API calls."""

    def test_semaphore_initial_value(self):
        """Semaphore should start at MAX_CONCURRENT_API_CALLS."""
        from data_pipeline.extractors import _api_semaphore, MAX_CONCURRENT_API_CALLS
        assert _api_semaphore._value == MAX_CONCURRENT_API_CALLS

    @patch("data_pipeline.extractors.BoxScoreData")
    @patch("data_pipeline.extractors.PlayByPlay")
    @patch("data_pipeline.extractors.ShotData")
    def test_semaphore_limits_concurrent_calls(self, mock_shot_cls, mock_pbp_cls, mock_box_cls):
        """
        With a small semaphore, concurrent in-flight API calls must not
        exceed the semaphore value. We patch it to 3 and fire 6 games
        (each making 3 API calls = 18 total tasks) and verify the peak.
        """
        test_sem = threading.Semaphore(3)
        concurrency = {"current": 0, "peak": 0}
        lock = threading.Lock()

        def tracked_api_call(*args, **kwargs):
            with lock:
                concurrency["current"] += 1
                concurrency["peak"] = max(concurrency["peak"], concurrency["current"])
            time.sleep(0.02)
            with lock:
                concurrency["current"] -= 1
            return _make_boxscore(2025, 1)

        for mock_cls in [mock_box_cls, mock_pbp_cls, mock_shot_cls]:
            inst = MagicMock()
            inst.get_player_boxscore_stats_data.side_effect = tracked_api_call
            inst.get_game_play_by_play_data.side_effect = lambda s, g: _make_pbp(s, g)
            inst.get_game_shot_data.side_effect = lambda s, g: _make_shots(s, g)
            mock_cls.return_value = inst

        with patch("data_pipeline.extractors._api_semaphore", test_sem):
            from data_pipeline.extractors import extract_games_concurrent
            extract_games_concurrent(2025, [1, 2, 3, 4, 5, 6], "E", max_workers=6)

        assert concurrency["peak"] <= 3


# ========================================================================
# 4. BULK DATABASE INSERT TESTS
# ========================================================================

class TestBulkExecute:
    """Verify _bulk_execute uses execute_values and respects chunksize."""

    def _make_mock_engine(self):
        """Build a mock SQLAlchemy engine with a fake raw_connection."""
        engine = MagicMock()
        raw_conn = MagicMock()
        cursor = MagicMock()
        raw_conn.cursor.return_value = cursor
        engine.raw_connection.return_value = raw_conn
        return engine, raw_conn, cursor

    def test_calls_execute_values_with_correct_data(self):
        """Records should be converted to tuples and sent via execute_values."""
        engine, raw_conn, cursor = self._make_mock_engine()
        records = [
            {"team_code": "OLY", "team_name": "Olympiacos"},
            {"team_code": "PAO", "team_name": "Panathinaikos"},
        ]

        with patch("data_pipeline.load_to_db.execute_values") as mock_ev:
            from data_pipeline.load_to_db import _bulk_execute
            n = _bulk_execute(
                engine,
                "INSERT INTO teams (team_code, team_name) VALUES %s",
                ["team_code", "team_name"],
                records,
            )

        assert n == 2
        mock_ev.assert_called_once()
        args = mock_ev.call_args
        sent_tuples = args[0][2]
        assert ("OLY", "Olympiacos") in sent_tuples
        assert ("PAO", "Panathinaikos") in sent_tuples
        raw_conn.commit.assert_called_once()
        raw_conn.close.assert_called_once()

    def test_chunking_splits_large_batches(self):
        """With chunksize=3 and 7 records, execute_values is called 3 times."""
        engine, raw_conn, cursor = self._make_mock_engine()
        records = [{"a": i, "b": i * 10} for i in range(7)]

        with patch("data_pipeline.load_to_db.execute_values") as mock_ev:
            from data_pipeline.load_to_db import _bulk_execute
            n = _bulk_execute(
                engine,
                "INSERT INTO t (a, b) VALUES %s",
                ["a", "b"],
                records,
                chunksize=3,
            )

        assert n == 7
        assert mock_ev.call_count == 3
        chunk_sizes = [len(c[0][2]) for c in mock_ev.call_args_list]
        assert chunk_sizes == [3, 3, 1]

    def test_empty_records_returns_zero(self):
        """No records = no DB calls, return 0."""
        engine = MagicMock()

        from data_pipeline.load_to_db import _bulk_execute
        n = _bulk_execute(engine, "INSERT INTO t VALUES %s", ["a"], [])

        assert n == 0
        engine.raw_connection.assert_not_called()

    def test_rollback_on_error(self):
        """On DB exception, rollback and close must be called."""
        engine, raw_conn, cursor = self._make_mock_engine()

        with patch("data_pipeline.load_to_db.execute_values", side_effect=Exception("DB boom")):
            from data_pipeline.load_to_db import _bulk_execute
            with pytest.raises(Exception, match="DB boom"):
                _bulk_execute(engine, "INSERT INTO t VALUES %s", ["a"], [{"a": 1}])

        raw_conn.rollback.assert_called_once()
        raw_conn.close.assert_called_once()


class TestDeleteGamesBulk:
    """Verify _delete_games_bulk sends the correct DELETE statement."""

    def test_deletes_with_in_clause(self):
        engine = MagicMock()
        raw_conn = MagicMock()
        cursor = MagicMock()
        raw_conn.cursor.return_value = cursor
        engine.raw_connection.return_value = raw_conn

        from data_pipeline.load_to_db import _delete_games_bulk
        _delete_games_bulk(engine, "play_by_play", [(2025, 1), (2025, 2)])

        cursor.execute.assert_called_once()
        sql_arg = cursor.execute.call_args[0][0]
        assert "DELETE FROM play_by_play" in sql_arg
        assert "(season, gamecode) IN" in sql_arg
        raw_conn.commit.assert_called_once()

    def test_noop_on_empty_list(self):
        engine = MagicMock()

        from data_pipeline.load_to_db import _delete_games_bulk
        _delete_games_bulk(engine, "shots", [])

        engine.raw_connection.assert_not_called()


# ========================================================================
# 5. PER-TABLE LOAD FUNCTION TESTS
# ========================================================================

class TestLoadTeams:
    """Verify load_teams calls _bulk_execute with correct ON CONFLICT."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=2)
    def test_upserts_unique_teams(self, mock_be):
        box = _make_boxscore(2025, 1)
        engine = MagicMock()

        from data_pipeline.load_to_db import load_teams
        load_teams(engine, box)

        mock_be.assert_called_once()
        args = mock_be.call_args
        sql = args[0][1]
        assert "ON CONFLICT" in sql
        assert "team_code" in sql
        records = args[0][3]
        codes = {r["team_code"] for r in records}
        assert codes == {"OLY", "PAO"}

    @patch("data_pipeline.load_to_db._bulk_execute")
    def test_empty_boxscore_skipped(self, mock_be):
        from data_pipeline.load_to_db import load_teams
        load_teams(MagicMock(), pd.DataFrame())
        mock_be.assert_not_called()


class TestLoadPlayers:
    """Verify load_players deduplicates and upserts."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=10)
    def test_upserts_unique_players(self, mock_be):
        box = _make_boxscore(2025, 1)
        engine = MagicMock()

        from data_pipeline.load_to_db import load_players
        load_players(engine, box)

        mock_be.assert_called_once()
        records = mock_be.call_args[0][3]
        ids = [r["player_id"] for r in records]
        assert len(ids) == len(set(ids))


class TestLoadGame:
    """Verify load_game handles missing metadata columns gracefully."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=1)
    def test_fills_missing_optional_fields(self, mock_be):
        gi = pd.DataFrame([{
            "season": 2025, "gamecode": 1,
            "home_team": "OLY", "away_team": "PAO",
            "home_score": 70, "away_score": 65,
        }])
        from data_pipeline.load_to_db import load_game
        load_game(MagicMock(), gi)

        records = mock_be.call_args[0][3]
        rec = records[0]
        assert rec["played"] is False
        assert rec["referee1"] is None
        assert rec["game_date"] is None


class TestLoadBoxscores:
    """Verify load_boxscores deletes old rows then bulk-inserts."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=10)
    @patch("data_pipeline.load_to_db._delete_games_bulk")
    def test_delete_then_insert(self, mock_del, mock_be):
        box = _make_boxscore(2025, 1)
        engine = MagicMock()

        from data_pipeline.load_to_db import load_boxscores
        load_boxscores(engine, box)

        mock_del.assert_called_once()
        del_args = mock_del.call_args[0]
        assert del_args[1] == "boxscores"
        assert (2025, 1) in del_args[2]

        mock_be.assert_called_once()
        records = mock_be.call_args[0][3]
        assert len(records) == 10


class TestLoadPlayByPlay:
    """Verify load_play_by_play deletes and re-inserts."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=6)
    @patch("data_pipeline.load_to_db._delete_games_bulk")
    def test_delete_then_insert(self, mock_del, mock_be):
        pbp = _make_pbp(2025, 1)
        engine = MagicMock()

        from data_pipeline.load_to_db import load_play_by_play
        load_play_by_play(engine, pbp)

        mock_del.assert_called_once_with(engine, "play_by_play", [(2025, 1)])
        mock_be.assert_called_once()
        records = mock_be.call_args[0][3]
        for r in records:
            assert r["season"] == 2025
            assert r["gamecode"] == 1
            assert r["playtype"] is not None


class TestLoadShots:
    """Verify load_shots deletes and re-inserts."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=4)
    @patch("data_pipeline.load_to_db._delete_games_bulk")
    def test_delete_then_insert(self, mock_del, mock_be):
        shots = _make_shots(2025, 1)
        engine = MagicMock()

        from data_pipeline.load_to_db import load_shots
        load_shots(engine, shots)

        mock_del.assert_called_once_with(engine, "shots", [(2025, 1)])
        mock_be.assert_called_once()
        records = mock_be.call_args[0][3]
        assert len(records) == 4
        assert all(r["coord_x"] is not None for r in records)


class TestLoadPlayerAdvancedStats:
    """Verify load_player_advanced_stats upserts with ON CONFLICT."""

    @patch("data_pipeline.load_to_db._bulk_execute", return_value=10)
    def test_upsert_advanced_stats(self, mock_be):
        from data_pipeline.transformers import compute_advanced_stats
        box = _make_boxscore(2025, 1)
        adv = compute_advanced_stats(box)
        engine = MagicMock()

        from data_pipeline.load_to_db import load_player_advanced_stats
        load_player_advanced_stats(engine, adv)

        mock_be.assert_called_once()
        sql = mock_be.call_args[0][1]
        assert "ON CONFLICT (season, gamecode, player_id)" in sql
        records = mock_be.call_args[0][3]
        assert len(records) == len(adv)
        for r in records:
            assert r["season"] == 2025
            assert r["player_id"] is not None


# ========================================================================
# 6. BATCH PIPELINE (run_pipeline_batch) INTEGRATION TEST
# ========================================================================

class TestRunPipelineBatch:
    """End-to-end test of run_pipeline_batch with all layers mocked."""

    @patch("data_pipeline.load_to_db.load_player_advanced_stats")
    @patch("data_pipeline.load_to_db.load_shots")
    @patch("data_pipeline.load_to_db.load_play_by_play")
    @patch("data_pipeline.load_to_db.load_boxscores")
    @patch("data_pipeline.load_to_db.load_game")
    @patch("data_pipeline.load_to_db.load_players")
    @patch("data_pipeline.load_to_db.load_teams")
    @patch("data_pipeline.extractors.extract_games_concurrent")
    def test_batch_pipeline_calls_all_loaders(
        self, mock_extract, mock_teams, mock_players, mock_game,
        mock_box, mock_pbp, mock_shots, mock_adv,
    ):
        """All 7 load functions must be called exactly once per batch."""
        mock_extract.return_value = [
            _make_game_data(2025, 1),
            _make_game_data(2025, 2),
        ]
        engine = MagicMock()

        from data_pipeline.load_to_db import run_pipeline_batch
        result = run_pipeline_batch(2025, [1, 2], "E", engine=engine)

        assert result == {"total": 2, "loaded": 2, "failed": 0}

        mock_teams.assert_called_once()
        mock_players.assert_called_once()
        mock_game.assert_called_once()
        mock_box.assert_called_once()
        mock_pbp.assert_called_once()
        mock_shots.assert_called_once()
        mock_adv.assert_called_once()

    @patch("data_pipeline.load_to_db.load_player_advanced_stats")
    @patch("data_pipeline.load_to_db.load_shots")
    @patch("data_pipeline.load_to_db.load_play_by_play")
    @patch("data_pipeline.load_to_db.load_boxscores")
    @patch("data_pipeline.load_to_db.load_game")
    @patch("data_pipeline.load_to_db.load_players")
    @patch("data_pipeline.load_to_db.load_teams")
    @patch("data_pipeline.extractors.extract_games_concurrent")
    def test_batch_pipeline_concatenates_dataframes(
        self, mock_extract, mock_teams, mock_players, mock_game,
        mock_box, mock_pbp, mock_shots, mock_adv,
    ):
        """Boxscores from multiple games must be concatenated before loading."""
        mock_extract.return_value = [
            _make_game_data(2025, 1),
            _make_game_data(2025, 2),
            _make_game_data(2025, 3),
        ]
        engine = MagicMock()

        from data_pipeline.load_to_db import run_pipeline_batch
        run_pipeline_batch(2025, [1, 2, 3], "E", engine=engine)

        box_arg = mock_box.call_args[0][1]
        assert len(box_arg) == 30  # 10 players * 3 games

        pbp_arg = mock_pbp.call_args[0][1]
        assert len(pbp_arg) == 18  # 6 actions * 3 games

    @patch("data_pipeline.extractors.extract_games_concurrent")
    def test_batch_pipeline_handles_zero_extractions(self, mock_extract):
        """If all games fail extraction, no loaders should be called."""
        mock_extract.return_value = []
        engine = MagicMock()

        from data_pipeline.load_to_db import run_pipeline_batch
        result = run_pipeline_batch(2025, [1, 2], "E", engine=engine)

        assert result == {"total": 2, "loaded": 0, "failed": 2}

    @patch("data_pipeline.load_to_db.load_player_advanced_stats")
    @patch("data_pipeline.load_to_db.load_shots")
    @patch("data_pipeline.load_to_db.load_play_by_play")
    @patch("data_pipeline.load_to_db.load_boxscores")
    @patch("data_pipeline.load_to_db.load_game")
    @patch("data_pipeline.load_to_db.load_players")
    @patch("data_pipeline.load_to_db.load_teams")
    @patch("data_pipeline.extractors.extract_games_concurrent")
    def test_batch_pipeline_reports_partial_failures(
        self, mock_extract, mock_teams, mock_players, mock_game,
        mock_box, mock_pbp, mock_shots, mock_adv,
    ):
        """If 1 of 3 games fails extraction, report loaded=2, failed=1."""
        mock_extract.return_value = [
            _make_game_data(2025, 1),
            _make_game_data(2025, 3),
        ]
        engine = MagicMock()

        from data_pipeline.load_to_db import run_pipeline_batch
        result = run_pipeline_batch(2025, [1, 2, 3], "E", engine=engine)

        assert result["total"] == 3
        assert result["loaded"] == 2
        assert result["failed"] == 1


# ========================================================================
# 7. HELPER FUNCTION TESTS
# ========================================================================

class TestSafeConversions:
    """Verify _safe_int, _safe_float, _safe_str handle edge cases."""

    def test_safe_int_with_nan(self):
        from data_pipeline.load_to_db import _safe_int
        assert _safe_int(float("nan")) is None
        assert _safe_int(None) is None
        assert _safe_int(42) == 42
        assert _safe_int(3.9) == 3

    def test_safe_float_with_nan(self):
        from data_pipeline.load_to_db import _safe_float
        assert _safe_float(float("nan")) is None
        assert _safe_float(None) is None
        assert _safe_float(3.14) == 3.14
        assert _safe_float(42) == 42.0

    def test_safe_str_with_nan(self):
        from data_pipeline.load_to_db import _safe_str
        assert _safe_str(float("nan")) is None
        assert _safe_str(None) is None
        assert _safe_str("  hello  ") == "hello"
        assert _safe_str("") is None

    def test_safe_str_max_len(self):
        from data_pipeline.load_to_db import _safe_str
        assert _safe_str("abcdefgh", max_len=5) == "abcde"
        assert _safe_str("abc", max_len=10) == "abc"


# ========================================================================
# 8. SINGLE-GAME PIPELINE TEST
# ========================================================================

class TestRunPipeline:
    """Verify run_pipeline works end-to-end with all layers mocked."""

    @patch("data_pipeline.load_to_db.load_player_advanced_stats")
    @patch("data_pipeline.load_to_db.load_shots")
    @patch("data_pipeline.load_to_db.load_play_by_play")
    @patch("data_pipeline.load_to_db.load_boxscores")
    @patch("data_pipeline.load_to_db.load_game")
    @patch("data_pipeline.load_to_db.load_players")
    @patch("data_pipeline.load_to_db.load_teams")
    @patch("data_pipeline.extractors.extract_game_data")
    def test_single_game_pipeline(
        self, mock_extract, mock_teams, mock_players, mock_game,
        mock_box, mock_pbp, mock_shots, mock_adv,
    ):
        mock_extract.return_value = _make_game_data(2025, 1)
        engine = MagicMock()

        from data_pipeline.load_to_db import run_pipeline
        run_pipeline(2025, 1, "E", engine=engine)

        mock_extract.assert_called_once_with(2025, 1, "E")
        mock_teams.assert_called_once()
        mock_players.assert_called_once()
        mock_game.assert_called_once()
        mock_box.assert_called_once()
        mock_pbp.assert_called_once()
        mock_shots.assert_called_once()
        mock_adv.assert_called_once()
