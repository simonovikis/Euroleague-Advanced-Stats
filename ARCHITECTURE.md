# Architectural Snapshot — EuroleagueStats

## 1. Frontend Routing & State

**Navigation:** Custom router using `streamlit-option-menu` (`option_menu` with `orientation="horizontal"`). There is **no** `st.navigation` / native multi-page app. `app.py` is a monolithic entry point that conditionally imports and calls `render()` from per-view modules under `streamlit_app/views/` via an `if/elif` chain based on the selected nav label.

**Deep Linking:** URL query params (`page`, `season`, `round`, `gamecode`, `team`) are read on first load into `session_state` and synced back to `st.query_params` after routing.

**Key `st.session_state` variables:**

| Key | Purpose |
|---|---|
| `selected_season` | Currently chosen season (int) |
| `selected_round` | Round number within season |
| `selected_team` | Team code for season/referee views |
| `game_data` | **Entire game analysis dict** (boxscore, pbp, advanced stats, lineups, on/off, etc.) — cached per gamecode |
| `game_info_cache` | Metadata of the currently selected matchup (scores, logos, codes) |
| `clutch_mode` | Boolean toggle to re-derive all stats for clutch-time only |
| `lang` | Active i18n locale key (en/el/de/es) |
| `authenticated`, `user_email`, `is_admin` | Auth state (Supabase-backed, behind `REQUIRE_LOGIN` flag) |
| `openai_api_key` | LLM chat key (chat view only) |
| `data_repository` | Singleton `DataRepository` instance |
| `live_cache`, `live_last_refresh` | Transient live-game polling cache |
| `_deep_link_applied` | One-shot guard for URL param hydration |
| `_active_gamecode`, `_active_home_team` | Current game context for URL sync & team branding |

## 2. Caching & Data Fetching

The UI requests data through `streamlit_app/queries.py`, which implements a **DB-first, API-fallback** pattern: if `USE_DB=True` and Postgres is reachable, SQL is executed; otherwise the Euroleague REST API is hit directly.

**All `@st.cache_data` functions and their TTL** (TTL is globally set from `config.yaml` → `data.cache_ttl_seconds`, default **3600 s**):

| Function | Location | TTL |
|---|---|---|
| `fetch_season_schedule()` | `queries.py:46` | `_CACHE_TTL` (3600s) |
| `fetch_league_efficiency_landscape()` | `queries.py:172` | `_CACHE_TTL` |
| `fetch_team_season_data()` | `queries.py:219` | `_CACHE_TTL` |
| `fetch_season_game_metadata()` | `queries.py:371` | `_CACHE_TTL` |
| `fetch_referee_stats()` | `queries.py:383` | `_CACHE_TTL` |
| `fetch_close_game_stats()` | `queries.py:426` | `_CACHE_TTL` |
| `fetch_situational_scoring()` | `queries.py:440` | `_CACHE_TTL` |
| `fetch_scouting_player_pool()` | `queries.py:503` | `_CACHE_TTL` |
| `fetch_league_leaders()` | `queries.py:512` | `_CACHE_TTL` |
| `fetch_home_away_splits()` | `queries.py:610` | `_CACHE_TTL` |
| `fetch_season_shot_data()` | `queries.py:664` | `_CACHE_TTL` |
| `_load_chat_dataframes()` | `views/chat.py:56` | `_CACHE_TTL` |

Additionally, `fetch_prediction_model()` uses `@st.cache_resource` (no TTL — survives for the lifetime of the process).

**Note:** Per-game data (boxscore, PBP, advanced) is **not** wrapped in `@st.cache_data`. It is instead cached manually in `st.session_state["game_data"]` inside `shared.ensure_game_data()`, keyed by `(season, gamecode)`.

## 3. Data Processing Heavy-Lifting

**All advanced stats and On/Off splits are computed on-the-fly via Pandas.** There is no nightly ETL for derived metrics.

The computation pipeline lives in `data_pipeline/transformers/` (5 sub-modules):

| Module | Key Functions |
|---|---|
| `base_stats.py` | `compute_advanced_stats()` — TS%, ORtg, DRtg, possessions |
| `lineups.py` | `track_lineups()`, `compute_lineup_stats()`, `compute_duo_trio_synergy()`, `compute_on_off_splits()` |
| `clutch.py` | `filter_clutch_time()`, `build_clutch_boxscore()`, `compute_clutch_stats()` |
| `playmaking.py` | `build_assist_network()`, `compute_shot_quality()`, `link_assists_to_shots()`, `compute_playmaking_metrics()`, `compute_total_points_created()` |
| `game_analysis.py` | `detect_runs_and_stoppers()`, `foul_trouble_impact()`, `compute_referee_stats()`, `compute_close_game_stats()` |

**Trigger path:** When a user selects a game, `shared.ensure_game_data()` downloads raw boxscore + PBP (from DB or API), then runs the full transformer chain in a `st.status()` block. The `clutch_mode` toggle re-runs the entire pipeline on clutch-filtered subsets. Season-level aggregation in `fetch_team_season_data()` iterates per-game and concats.

The only pre-computed data persisted to the DB is `player_advanced_stats` (TS%, ORtg, DRtg, possessions) — inserted by the ETL in `load_to_db.run_pipeline()`. Lineup stats, On/Off splits, synergy, assist networks, shot quality, and playmaking metrics are **never persisted** and are always recomputed on demand.

## 4. Database Connection & Schema

**Engine configuration** (`data_pipeline/load_to_db.get_engine()`):

```python
SQLAlchemy create_engine(
    url,                      # postgresql+psycopg2:// (Supavisor pooler on port 6543)
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,       # stale connection detection
    pool_recycle=300,          # recycle connections every 5 min
)
```

Connection pooling is **two-tier**: SQLAlchemy's QueuePool locally (10+20 connections) on top of Supabase Supavisor (`pool_mode=transaction`) externally. The `use_pooler=True` flag rewrites the connection URL to route through the Supavisor endpoint (port 6543).

**Schema — 7 tables:**

| Table | PK | Description |
|---|---|---|
| `teams` | `team_code` | Reference lookup |
| `players` | `player_id` | Reference, FK → teams |
| `games` | `(season, gamecode)` | Game metadata, scores, referees |
| `play_by_play` | `id` (SERIAL) | Raw PBP events, FK → games |
| `boxscores` | `(season, gamecode, player_id)` | Raw boxscore rows, FK → games |
| `player_advanced_stats` | `id` (SERIAL), UNIQUE `(season, gamecode, player_id)` | Computed per-player-per-game stats |
| `shots` | `id` (SERIAL) | Shot coordinates and context, FK → games |

**B-Tree Indexes** (from `database/indexes.sql`):

| Index | Table | Columns |
|---|---|---|
| `idx_games_season_played` | games | `(season, played)` |
| `idx_games_home_team` | games | `(home_team)` |
| `idx_games_away_team` | games | `(away_team)` |
| `idx_games_season_played_home` | games | `(season, played, home_team)` |
| `idx_games_season_played_away` | games | `(season, played, away_team)` |
| `idx_adv_stats_season` | player_advanced_stats | `(season)` |
| `idx_adv_stats_season_team` | player_advanced_stats | `(season, team_code)` |
| `idx_adv_stats_season_team_home` | player_advanced_stats | `(season, team_code, is_home)` |
| `idx_adv_stats_player_name` | player_advanced_stats | `(player_name)` |
| `idx_pbp_game` | play_by_play | `(season, gamecode)` |
| `idx_pbp_game_team` | play_by_play | `(season, gamecode, codeteam)` |
| `idx_pbp_playtype` | play_by_play | `(playtype)` |
| `idx_pbp_player_id` | play_by_play | `(player_id)` |
| `idx_shots_season_team` | shots | `(season, team)` |
| `idx_shots_season_player` | shots | `(season, id_player)` |
| `idx_shots_season_team_zone` | shots | `(season, team, zone)` |
| `idx_boxscores_team` | boxscores | `(team)` |
| `idx_boxscores_season_team` | boxscores | `(season, team)` |
| `idx_adv_stats_player` | player_advanced_stats | `(player_id)` |
| `idx_adv_stats_team` | player_advanced_stats | `(team_code)` |
| `idx_boxscores_game` | boxscores | `(season, gamecode)` |
| `idx_shots_game` | shots | `(season, gamecode)` |

## 5. Directory Structure

```
EuroleagueStats/
├── config/
│   └── config.yaml                  # YAML: seasons, TTL, team colors, feature flags
├── database/
│   ├── schema.sql                   # DDL for 7 tables (teams → shots)
│   └── indexes.sql                  # 22 B-Tree indexes targeting query patterns
├── data_pipeline/
│   ├── extractors.py                # Euroleague API client (REST)
│   ├── live_extractor.py            # Live game polling
│   ├── load_to_db.py                # ETL: extract → transform → upsert to Postgres
│   ├── data_repository.py           # DB-first cache-aside read layer
│   ├── sync.py                      # Incremental game sync
│   ├── ml_pipeline.py               # Win probability model (sklearn)
│   ├── scouting_engine.py           # Cosine-similarity player comparison
│   └── transformers/
│       ├── __init__.py              # Re-exports all public symbols
│       ├── base_stats.py            # TS%, ORtg, DRtg, possessions
│       ├── lineups.py               # Lineup tracking, On/Off splits, duo/trio synergy
│       ├── clutch.py                # Clutch-time filtering & boxscore rebuild
│       ├── playmaking.py            # Assist network, shot quality, TPC
│       ├── game_analysis.py         # Runs, stoppers, fouls, referee stats
│       └── utils.py                 # Column maps, minute parsing
├── streamlit_app/
│   ├── app.py                       # Router + page config + CSS + auth gate
│   ├── shared.py                    # Session helpers, ensure_game_data(), sidebar filters
│   ├── queries.py                   # Data access layer (DB-first, API-fallback)
│   ├── chat_agent.py                # LangChain agent for natural-language Q&A
│   ├── translations.json            # i18n strings (en/el/de/es)
│   ├── utils/
│   │   ├── config_loader.py         # YAML reader + convenience accessors
│   │   ├── feature_flags.py         # Runtime feature toggles
│   │   ├── secrets_manager.py       # Env/secret loading, DB URL formatting
│   │   ├── auth.py                  # Supabase auth (login/logout/admin check)
│   │   ├── court.py                 # Basketball court Plotly drawing helpers
│   │   └── pdf_report.py           # PDF export utilities
│   └── views/
│       ├── home.py                  # Landing page with feature cards
│       ├── single_game.py           # Single-game deep dive
│       ├── advanced_analytics.py    # Advanced per-game analytics
│       ├── season_overview.py       # Team season dashboard
│       ├── live_match.py            # Real-time game tracker
│       ├── leaders.py               # League leaderboards
│       ├── scouting.py              # Player similarity engine UI
│       ├── oracle.py                # ML win-probability predictions
│       ├── referee.py               # Referee analysis
│       ├── chat.py                  # LLM chat interface
│       └── glossary.py              # Stats glossary
├── tests/
│   ├── test_transformers.py
│   ├── test_config.py
│   ├── test_db_connection.py
│   ├── test_i18n.py
│   ├── test_data_integrity.py
│   ├── test_feature_flags.py
│   ├── test_imports.py
│   └── test_data_validation.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```
