-- ============================================================
-- Euroleague Advanced Analytics Platform — Database Schema
-- ============================================================
-- Five normalized tables:
--   teams, players, games, play_by_play, player_advanced_stats
-- ============================================================

-- -----------------------------------------------------------
-- 1. TEAMS — Reference table for team codes
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS teams (
    team_code   VARCHAR(10) PRIMARY KEY,      -- e.g. "PAO", "BAR", "OLY"
    team_name   VARCHAR(120) NOT NULL,
    logo_url    TEXT                           -- team crest / logo URL
);

-- -----------------------------------------------------------
-- 2. PLAYERS — Reference table for players
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS players (
    player_id   VARCHAR(20) PRIMARY KEY,      -- euroleague player ID
    player_name VARCHAR(150) NOT NULL,
    team_code   VARCHAR(10) REFERENCES teams(team_code)
        ON UPDATE CASCADE ON DELETE SET NULL,
    dorsal      VARCHAR(10)                   -- jersey number
);

-- -----------------------------------------------------------
-- 3. GAMES — One row per game
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS games (
    season      INT          NOT NULL,        -- start year of season (e.g. 2024)
    gamecode    INT          NOT NULL,        -- euroleague game code
    home_team   VARCHAR(10)  REFERENCES teams(team_code) ON UPDATE CASCADE,
    away_team   VARCHAR(10)  REFERENCES teams(team_code) ON UPDATE CASCADE,
    home_score  INT,
    away_score  INT,
    game_date   VARCHAR(30),                  -- stored as string; API doesn't always give clean dates
    round       INT,
    played      BOOLEAN      DEFAULT FALSE,
    referee1    VARCHAR(150),
    referee2    VARCHAR(150),
    referee3    VARCHAR(150),

    PRIMARY KEY (season, gamecode)
);

-- -----------------------------------------------------------
-- 4. PLAY_BY_PLAY — Raw play-by-play actions
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS play_by_play (
    id              SERIAL PRIMARY KEY,
    season          INT          NOT NULL,
    gamecode        INT          NOT NULL,
    period          INT,                      -- 1-4 for quarters, 5+ for OT
    playtype        VARCHAR(30),              -- e.g. "2FGM", "3FGA", "TO", "RV"
    player_id       VARCHAR(20),
    player          VARCHAR(150),
    codeteam        VARCHAR(10),
    markertime      VARCHAR(10),              -- "MM:SS" countdown timer
    numberofplay    INT,
    comment         TEXT,

    FOREIGN KEY (season, gamecode) REFERENCES games(season, gamecode)
        ON UPDATE CASCADE ON DELETE CASCADE
);

-- Index for fast lookups by game
CREATE INDEX IF NOT EXISTS idx_pbp_game
    ON play_by_play (season, gamecode);

-- -----------------------------------------------------------
-- 5. PLAYER_ADVANCED_STATS — Computed per-player-per-game
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS player_advanced_stats (
    id              SERIAL PRIMARY KEY,
    season          INT          NOT NULL,
    gamecode        INT          NOT NULL,
    player_id       VARCHAR(20) REFERENCES players(player_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    player_name     VARCHAR(150),
    team_code       VARCHAR(10) REFERENCES teams(team_code)
        ON UPDATE CASCADE,
    is_home         BOOLEAN,

    -- Raw stats (from boxscore)
    minutes         REAL,
    points          INT,
    fgm2            INT,       -- 2-point field goals made
    fga2            INT,       -- 2-point field goals attempted
    fgm3            INT,       -- 3-point field goals made
    fga3            INT,       -- 3-point field goals attempted
    ftm             INT,       -- free throws made
    fta             INT,       -- free throws attempted
    off_rebounds     INT,
    def_rebounds     INT,
    total_rebounds   INT,
    assists         INT,
    steals          INT,
    turnovers       INT,
    blocks_favour   INT,
    blocks_against  INT,
    fouls_committed INT,
    fouls_received  INT,
    plus_minus      REAL,

    -- Advanced stats (computed)
    possessions     REAL,       -- estimated possessions
    ts_pct          REAL,       -- True Shooting Percentage
    off_rating      REAL,       -- Offensive Rating (per 100 poss)
    def_rating      REAL,       -- Defensive Rating (per 100 poss)

    FOREIGN KEY (season, gamecode) REFERENCES games(season, gamecode)
        ON UPDATE CASCADE ON DELETE CASCADE,

    -- Prevent duplicate rows for the same player in the same game
    UNIQUE (season, gamecode, player_id)
);

-- Index for dashboard queries
CREATE INDEX IF NOT EXISTS idx_adv_stats_player
    ON player_advanced_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_adv_stats_team
    ON player_advanced_stats (team_code);


-- -----------------------------------------------------------
-- 6. BOXSCORES — Raw player boxscore data (API format)
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS boxscores (
    season          INT          NOT NULL,
    gamecode        INT          NOT NULL,
    player_id       VARCHAR(20)  NOT NULL,
    player          VARCHAR(150),
    team            VARCHAR(10),
    home            INT,
    is_starter      INT,
    is_playing      INT,
    dorsal          VARCHAR(10),
    minutes         VARCHAR(10),
    points          INT,
    fgm2            INT,
    fga2            INT,
    fgm3            INT,
    fga3            INT,
    ftm             INT,
    fta             INT,
    off_rebounds     INT,
    def_rebounds     INT,
    total_rebounds   INT,
    assists         INT,
    steals          INT,
    turnovers       INT,
    blocks_favour   INT,
    blocks_against  INT,
    fouls_committed INT,
    fouls_received  INT,
    valuation       INT,
    plus_minus      REAL,

    FOREIGN KEY (season, gamecode) REFERENCES games(season, gamecode)
        ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (season, gamecode, player_id)
);

CREATE INDEX IF NOT EXISTS idx_boxscores_game
    ON boxscores (season, gamecode);


-- -----------------------------------------------------------
-- 7. SHOTS — Shot data with X/Y coordinates
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS shots (
    id              SERIAL PRIMARY KEY,
    season          INT          NOT NULL,
    gamecode        INT          NOT NULL,
    num_anot        INT,
    team            VARCHAR(10),
    id_player       VARCHAR(20),
    player          VARCHAR(150),
    id_action       VARCHAR(10),
    action          VARCHAR(80),
    points          INT,
    coord_x         REAL,
    coord_y         REAL,
    zone            VARCHAR(5),
    fastbreak       INT,
    second_chance   INT,
    pts_off_turnover INT,
    minute          INT,
    console         VARCHAR(10),
    points_a        INT,
    points_b        INT,

    FOREIGN KEY (season, gamecode) REFERENCES games(season, gamecode)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_shots_game
    ON shots (season, gamecode);


-- -----------------------------------------------------------
-- 8. SEASON_ON_OFF_SPLITS — Pre-computed per-player On/Off
--    Net Rating aggregated across all games in a season.
--    Populated by run_season_aggregations() in the nightly ETL.
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS season_on_off_splits (
    season          INT          NOT NULL,
    player_id       VARCHAR(20)  NOT NULL,
    player_name     VARCHAR(150),
    team            VARCHAR(10),
    games           INT,          -- number of games the player appeared in
    on_events       INT,
    on_pts_for      INT,
    on_pts_against  INT,
    on_poss         REAL,
    on_ortg         REAL,
    on_drtg         REAL,
    on_net_rtg      REAL,
    off_events      INT,
    off_pts_for     INT,
    off_pts_against INT,
    off_poss        REAL,
    off_ortg        REAL,
    off_drtg        REAL,
    off_net_rtg     REAL,
    on_off_diff     REAL,

    PRIMARY KEY (season, player_id, team)
);
