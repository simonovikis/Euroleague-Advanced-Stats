"""
data_repository.py — DB-First Cache-Aside Data Access Layer
=============================================================
Single source of truth for the Streamlit app. Always reads from
PostgreSQL for instant load times. On cache miss, fetches from the
Euroleague API, persists to DB, then returns the data.

Usage:
    repo = DataRepository()
    game_data = repo.get_game_data(season=2024, gamecode=1)
"""

import asyncio
import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataRepository:
    """
    Cache-aside wrapper: DB first, API fallback, then persist.

    The repository stores *raw* API data (boxscore, pbp, shots, game_info)
    in PostgreSQL and re-derives all advanced analytics in-memory on each
    load. This keeps the cache simple while transforms stay fast (~50ms).
    """

    def __init__(self):
        self._engine = None
        self._db_ok: Optional[bool] = None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------
    @property
    def engine(self):
        if self._engine is None:
            from data_pipeline.load_to_db import get_engine
            self._engine = get_engine()
        return self._engine

    def db_available(self) -> bool:
        """Return True if the database is reachable. Cached after first check."""
        if self._db_ok is not None:
            return self._db_ok
        try:
            from data_pipeline.load_to_db import get_engine, ensure_schema
            eng = get_engine()
            with eng.connect() as conn:
                conn.execute(__import__("sqlalchemy").text("SELECT 1"))
            ensure_schema(eng)
            self._engine = eng
            self._db_ok = True
        except Exception as e:
            logger.warning(f"Database not available, falling back to API-only mode: {e}")
            self._db_ok = False
        return self._db_ok

    # ------------------------------------------------------------------
    # Cache check
    # ------------------------------------------------------------------
    def is_game_cached(self, season: int, gamecode: int) -> bool:
        """Check whether raw data for a game exists in the DB."""
        if not self.db_available():
            return False
        try:
            from sqlalchemy import text
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT EXISTS("
                        "  SELECT 1 FROM boxscores WHERE season = :s AND gamecode = :g"
                        ")"
                    ),
                    {"s": season, "g": gamecode},
                ).scalar()
            return bool(row)
        except Exception as e:
            logger.warning(f"Cache check failed: {e}")
            return False

    # ------------------------------------------------------------------
    # DB readers (raw API-format DataFrames)
    # ------------------------------------------------------------------
    def _load_boxscore_from_db(self, season: int, gamecode: int) -> pd.DataFrame:
        from sqlalchemy import text
        query = text("""
            SELECT
                season AS "Season", gamecode AS "Gamecode",
                player_id AS "Player_ID", player AS "Player",
                team AS "Team", home AS "Home",
                is_starter AS "IsStarter", is_playing AS "IsPlaying",
                dorsal AS "Dorsal", minutes AS "Minutes",
                points AS "Points",
                fgm2 AS "FieldGoalsMade2", fga2 AS "FieldGoalsAttempted2",
                fgm3 AS "FieldGoalsMade3", fga3 AS "FieldGoalsAttempted3",
                ftm AS "FreeThrowsMade", fta AS "FreeThrowsAttempted",
                off_rebounds AS "OffensiveRebounds",
                def_rebounds AS "DefensiveRebounds",
                total_rebounds AS "TotalRebounds",
                assists AS "Assistances", steals AS "Steals",
                turnovers AS "Turnovers",
                blocks_favour AS "BlocksFavour",
                blocks_against AS "BlocksAgainst",
                fouls_committed AS "FoulsCommited",
                fouls_received AS "FoulsReceived",
                valuation AS "Valuation",
                plus_minus AS "Plusminus"
            FROM boxscores
            WHERE season = :s AND gamecode = :g
        """)
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"s": season, "g": gamecode})

    def _load_pbp_from_db(self, season: int, gamecode: int) -> pd.DataFrame:
        from sqlalchemy import text
        query = text("""
            SELECT
                season AS "Season", gamecode AS "Gamecode",
                period AS "PERIOD", playtype AS "PLAYTYPE",
                player_id AS "PLAYER_ID", player AS "PLAYER",
                codeteam AS "CODETEAM", markertime AS "MARKERTIME",
                numberofplay AS "NUMBEROFPLAY",
                comment AS "COMMENT"
            FROM play_by_play
            WHERE season = :s AND gamecode = :g
            ORDER BY id ASC
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"s": season, "g": gamecode})

        # Reconstruct columns expected by transformers
        if not df.empty:
            if "NUMBEROFPLAY" in df.columns:
                df["TRUE_NUMBEROFPLAY"] = df["NUMBEROFPLAY"]
            # Reconstruct running score from PBP comments or set to NaN
            for col in ["POINTS_A", "POINTS_B", "PLAYINFO"]:
                if col not in df.columns:
                    df[col] = None
        return df

    def _load_shots_from_db(self, season: int, gamecode: int) -> pd.DataFrame:
        from sqlalchemy import text
        query = text("""
            SELECT
                season AS "Season", gamecode AS "Gamecode",
                num_anot AS "NUM_ANOT", team AS "TEAM",
                id_player AS "ID_PLAYER", player AS "PLAYER",
                id_action AS "ID_ACTION", action AS "ACTION",
                points AS "POINTS",
                coord_x AS "COORD_X", coord_y AS "COORD_Y",
                zone AS "ZONE",
                fastbreak AS "FASTBREAK",
                second_chance AS "SECOND_CHANCE",
                pts_off_turnover AS "POINTS_OFF_TURNOVER",
                minute AS "MINUTE", console AS "CONSOLE",
                points_a AS "POINTS_A", points_b AS "POINTS_B"
            FROM shots
            WHERE season = :s AND gamecode = :g
            ORDER BY id ASC
        """)
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"s": season, "g": gamecode})

    def _load_game_info_from_db(self, season: int, gamecode: int) -> pd.DataFrame:
        from sqlalchemy import text
        query = text("""
            SELECT
                season, gamecode,
                home_team, away_team,
                home_score, away_score
            FROM games
            WHERE season = :s AND gamecode = :g
        """)
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"s": season, "g": gamecode})

    # ------------------------------------------------------------------
    # DB writer — persist raw API data
    # ------------------------------------------------------------------
    def _save_raw_to_db(
        self,
        raw: Dict[str, pd.DataFrame],
        season: int,
        gamecode: int,
    ) -> None:
        """Persist raw API data to all relevant tables."""
        try:
            from data_pipeline.load_to_db import (
                load_teams, load_players, load_game,
                load_boxscores, load_play_by_play, load_shots,
            )
            eng = self.engine
            boxscore_df = raw.get("boxscore", pd.DataFrame())
            if not boxscore_df.empty:
                load_teams(eng, boxscore_df)
                load_players(eng, boxscore_df)
            load_game(eng, raw.get("game_info", pd.DataFrame()))
            load_boxscores(eng, boxscore_df)
            load_play_by_play(eng, raw.get("pbp", pd.DataFrame()))
            load_shots(eng, raw.get("shots", pd.DataFrame()))
            logger.info(f"Cached game {season}/{gamecode} to database")
        except Exception as e:
            logger.error(f"Failed to cache game {season}/{gamecode}: {e}")

    # ------------------------------------------------------------------
    # Concurrent DB loading (parallel I/O via asyncio)
    # ------------------------------------------------------------------
    async def _load_game_data_from_db_async(
        self, season: int, gamecode: int
    ) -> Dict[str, pd.DataFrame]:
        """Fire all four raw-data queries in parallel using thread workers."""
        boxscore, pbp, shots, game_info = await asyncio.gather(
            asyncio.to_thread(self._load_boxscore_from_db, season, gamecode),
            asyncio.to_thread(self._load_pbp_from_db, season, gamecode),
            asyncio.to_thread(self._load_shots_from_db, season, gamecode),
            asyncio.to_thread(self._load_game_info_from_db, season, gamecode),
        )
        return {
            "boxscore": boxscore,
            "pbp": pbp,
            "shots": shots,
            "game_info": game_info,
        }

    def load_game_data_concurrent(
        self, season: int, gamecode: int
    ) -> Dict[str, pd.DataFrame]:
        """Synchronous wrapper for Streamlit — runs parallel DB fetches."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    "boxscore": pool.submit(self._load_boxscore_from_db, season, gamecode),
                    "pbp": pool.submit(self._load_pbp_from_db, season, gamecode),
                    "shots": pool.submit(self._load_shots_from_db, season, gamecode),
                    "game_info": pool.submit(self._load_game_info_from_db, season, gamecode),
                }
                return {k: f.result() for k, f in futures.items()}

        return asyncio.run(self._load_game_data_from_db_async(season, gamecode))

    # ------------------------------------------------------------------
    # Transformer pipeline (same logic as fetch_game_data_live)
    # ------------------------------------------------------------------
    @staticmethod
    def _transform_raw(raw: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Run the full transformer pipeline on raw DataFrames."""
        from data_pipeline.transformers import (
            compute_advanced_stats,
            track_lineups,
            compute_lineup_stats,
            compute_duo_trio_synergy,
            compute_clutch_stats,
            detect_runs_and_stoppers,
            foul_trouble_impact,
            build_assist_network,
            compute_shot_quality,
            link_assists_to_shots,
            compute_playmaking_metrics,
            compute_total_points_created,
            compute_on_off_splits,
        )

        boxscore_df = raw["boxscore"]
        pbp_df = raw["pbp"]
        shots_df = raw["shots"]
        game_info_df = raw["game_info"]

        advanced_df = compute_advanced_stats(boxscore_df)
        pbp_lu = track_lineups(pbp_df, boxscore_df)
        lineup_stats = compute_lineup_stats(pbp_lu, boxscore_df)
        duo_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=2)
        trio_synergy = compute_duo_trio_synergy(pbp_lu, boxscore_df, combo_size=3)
        on_off_splits = compute_on_off_splits(pbp_lu, boxscore_df)
        clutch = compute_clutch_stats(pbp_df, boxscore_df)
        stoppers = detect_runs_and_stoppers(pbp_lu)
        foul_impact = foul_trouble_impact(pbp_df, boxscore_df)
        assists = build_assist_network(pbp_df)
        shot_quality = compute_shot_quality(shots_df)
        assist_shot_links = link_assists_to_shots(pbp_df, shots_df)
        playmaking = compute_playmaking_metrics(assist_shot_links, min_assists=1)
        advanced_df = compute_total_points_created(advanced_df, assist_shot_links)

        return {
            "boxscore": boxscore_df,
            "pbp": pbp_df,
            "shots": shots_df,
            "game_info": game_info_df,
            "advanced_stats": advanced_df,
            "pbp_with_lineups": pbp_lu,
            "lineup_stats": lineup_stats,
            "assist_network": assists,
            "clutch_stats": clutch,
            "run_stoppers": stoppers,
            "foul_trouble": foul_impact,
            "duo_synergy": duo_synergy,
            "trio_synergy": trio_synergy,
            "on_off_splits": on_off_splits,
            "shot_quality": shot_quality,
            "assist_shot_links": assist_shot_links,
            "playmaking_aaq": playmaking["aaq"],
            "playmaking_axp": playmaking["axp"],
            "playmaking_duos": playmaking["duos"],
        }

    # ------------------------------------------------------------------
    # PUBLIC API — the smart wrapper
    # ------------------------------------------------------------------
    def get_game_data(self, season: int, gamecode: int, competition: str = "E") -> Dict[str, pd.DataFrame]:
        """
        Cache-aside: DB first, API fallback, persist, transform, return.

        The caller (Streamlit) only pays the API latency once per game.
        Subsequent loads are instant from PostgreSQL.
        """
        import streamlit as st

        # --- 1. Try loading from DB (concurrent) ---
        if self.is_game_cached(season, gamecode):
            logger.info(f"DB HIT: loading game {season}/{gamecode} from cache (parallel)")
            raw = self.load_game_data_concurrent(season, gamecode)
            return self._transform_raw(raw)

        # --- 2. Cache miss → fetch from API ---
        logger.info(f"DB MISS: fetching game {season}/{gamecode} from API")
        st.toast(
            "First time loading this game. Fetching from Euroleague API and caching to database...",
            icon="🔄",
        )

        from data_pipeline.extractors import extract_game_data
        raw = extract_game_data(season, gamecode, competition)

        # --- 3. Persist raw data to DB (silent, best-effort) ---
        if self.db_available():
            self._save_raw_to_db(raw, season, gamecode)

        # --- 4. Transform and return ---
        return self._transform_raw(raw)

    # ------------------------------------------------------------------
    # Schedule helpers
    # ------------------------------------------------------------------
    def get_cached_gamecodes(self, season: int) -> List[int]:
        """Return list of gamecodes already stored in the DB for a season."""
        if not self.db_available():
            return []
        try:
            from sqlalchemy import text
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT DISTINCT gamecode FROM boxscores WHERE season = :s ORDER BY gamecode"),
                    {"s": season},
                ).fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    def get_missing_gamecodes(self, season: int, competition: str = "E") -> List[int]:
        """Compare API schedule against DB and return missing played gamecodes."""
        from data_pipeline.extractors import get_season_schedule
        schedule = get_season_schedule(season, competition)
        if schedule.empty:
            return []

        played_api = schedule[schedule["played"] == True]["gamecode"].tolist()
        cached = set(self.get_cached_gamecodes(season))
        return [gc for gc in played_api if gc not in cached]

    def sync_missing_games(
        self,
        season: int,
        competition: str = "E",
        progress_callback=None,
    ) -> Dict[str, int]:
        """
        Bulk-sync all missing played games for a season using the
        concurrent batch pipeline.

        Parameters
        ----------
        progress_callback : callable(current, total) or None
            Called after each game extraction completes.

        Returns
        -------
        dict with keys: total, synced, failed
        """
        missing = self.get_missing_gamecodes(season, competition)
        if not missing:
            return {"total": 0, "synced": 0, "failed": 0}

        if self.db_available():
            from data_pipeline.load_to_db import run_pipeline_batch, ensure_schema
            ensure_schema(self.engine)
            result = run_pipeline_batch(
                season, missing, competition,
                engine=self.engine,
                progress_callback=progress_callback,
            )
            return {"total": result["total"], "synced": result["loaded"], "failed": result["failed"]}

        # Fallback: no DB, extract only (for UI display)
        from data_pipeline.extractors import extract_game_data
        total = len(missing)
        synced = 0
        failed = 0
        for i, gc in enumerate(missing):
            try:
                extract_game_data(season, gc, competition)
                synced += 1
            except Exception as e:
                logger.error(f"Failed to sync game {season}/{gc}: {e}")
                failed += 1
            if progress_callback:
                progress_callback(i + 1, total)

        return {"total": total, "synced": synced, "failed": failed}
