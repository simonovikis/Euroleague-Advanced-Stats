-- ============================================================
-- Euroleague Advanced Analytics — Performance Indexes
-- ============================================================
-- Targets the WHERE / JOIN / GROUP BY patterns in queries.py
-- and data_repository.py. All statements are idempotent.
--
-- Run against the Supabase PostgreSQL database:
--   psql "$DATABASE_URL" -f database/indexes.sql
-- ============================================================


-- -----------------------------------------------------------
-- GAMES
-- -----------------------------------------------------------
-- fetch_season_schedule:       WHERE season = :season
-- fetch_league_efficiency:     WHERE g.season = :season AND g.played = TRUE
-- fetch_referee_stats:         WHERE season = :season AND played = TRUE AND (home_team = :team OR away_team = :team)
-- fetch_home_away_splits:      GROUP BY home_team / away_team with season + played filter

-- Covers all "season + played" filters and partial scans by season alone
CREATE INDEX IF NOT EXISTS idx_games_season_played
    ON games (season, played);

-- JOIN conditions: team_pts CTE joins games on home_team / away_team
CREATE INDEX IF NOT EXISTS idx_games_home_team
    ON games (home_team);

CREATE INDEX IF NOT EXISTS idx_games_away_team
    ON games (away_team);

-- Composite for referee queries filtering by season + played + team involvement
CREATE INDEX IF NOT EXISTS idx_games_season_played_home
    ON games (season, played, home_team);

CREATE INDEX IF NOT EXISTS idx_games_season_played_away
    ON games (season, played, away_team);


-- -----------------------------------------------------------
-- PLAYER_ADVANCED_STATS
-- -----------------------------------------------------------
-- fetch_league_efficiency:     GROUP BY team_code WHERE season = :season
-- fetch_situational_scoring:   WHERE season = :season AND points > 0 GROUP BY team_code
-- fetch_home_away_splits:      GROUP BY team_code, is_home WHERE season = :season
-- query_player_stats_db:       WHERE season / team_code / player_name filters
-- SQL_TEAM_STATS:              WHERE minutes > 0 GROUP BY team_code
--
-- NOTE: The UNIQUE(season, gamecode, player_id) constraint already provides
-- an index with leading columns (season, gamecode), but queries filtering
-- by season alone (without gamecode) cannot use it efficiently.

-- Primary workhorse: almost every dashboard query filters by season
CREATE INDEX IF NOT EXISTS idx_adv_stats_season
    ON player_advanced_stats (season);

-- Composite for team-level aggregations per season (efficiency, situational)
CREATE INDEX IF NOT EXISTS idx_adv_stats_season_team
    ON player_advanced_stats (season, team_code);

-- Composite for home/away splits (GROUP BY team_code, is_home WHERE season)
CREATE INDEX IF NOT EXISTS idx_adv_stats_season_team_home
    ON player_advanced_stats (season, team_code, is_home);

-- Covering index for player name search (query_player_stats_db ILIKE filter)
CREATE INDEX IF NOT EXISTS idx_adv_stats_player_name
    ON player_advanced_stats (player_name);


-- -----------------------------------------------------------
-- PLAY_BY_PLAY
-- -----------------------------------------------------------
-- data_repository._load_pbp_from_db: WHERE season = :s AND gamecode = :g
--   (already covered by idx_pbp_game)
-- Transformer pipelines filter by codeteam and playtype after loading

-- Composite for team-specific PBP lookups within a game
CREATE INDEX IF NOT EXISTS idx_pbp_game_team
    ON play_by_play (season, gamecode, codeteam);

-- Playtype lookups (used in assist network, clutch, run detection)
CREATE INDEX IF NOT EXISTS idx_pbp_playtype
    ON play_by_play (playtype);

-- Player-level PBP filtering
CREATE INDEX IF NOT EXISTS idx_pbp_player_id
    ON play_by_play (player_id);


-- -----------------------------------------------------------
-- SHOTS
-- -----------------------------------------------------------
-- fetch_season_shot_data: WHERE season = :season AND (team = :code1 OR team = :code2)
--   The existing idx_shots_game (season, gamecode) does NOT help here
--   because this query filters by season + team across all games.

-- Composite for season-wide team shot queries (spatial analytics)
CREATE INDEX IF NOT EXISTS idx_shots_season_team
    ON shots (season, team);

-- Player-level shot filtering
CREATE INDEX IF NOT EXISTS idx_shots_season_player
    ON shots (season, id_player);

-- Zone-based shot analysis
CREATE INDEX IF NOT EXISTS idx_shots_season_team_zone
    ON shots (season, team, zone);


-- -----------------------------------------------------------
-- BOXSCORES
-- -----------------------------------------------------------
-- PK (season, gamecode, player_id) covers game-level lookups.
-- Add team-level index for potential season-wide team queries.

CREATE INDEX IF NOT EXISTS idx_boxscores_team
    ON boxscores (team);

CREATE INDEX IF NOT EXISTS idx_boxscores_season_team
    ON boxscores (season, team);


-- -----------------------------------------------------------
-- SEASON_ON_OFF_SPLITS
-- -----------------------------------------------------------
-- Season overview queries: WHERE season = :season AND team = :team
CREATE INDEX IF NOT EXISTS idx_on_off_season_team
    ON season_on_off_splits (season, team);

CREATE INDEX IF NOT EXISTS idx_on_off_season_diff
    ON season_on_off_splits (season, on_off_diff DESC);
