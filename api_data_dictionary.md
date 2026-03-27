# 🏀 Euroleague API — Data Dictionary

> Auto-generated on **2026-03-26 18:16** by `discover_api_fields.py`
> Sample parameters: `season=2023`, `gamecode=1`
> Library: [`euroleague-api`](https://pypi.org/project/euroleague-api/)

---

## Table of Contents

1. [Player Boxscore (Single Game)](#player-boxscore-single-game)
2. [Play-by-Play (Single Game)](#play-by-play-single-game)
3. [Play-by-Play with Lineups (Single Game)](#play-by-play-with-lineups-single-game)
4. [Shot Data (Single Game)](#shot-data-single-game)
5. [Team Advanced Stats (Single Game)](#team-advanced-stats-single-game)
6. [Player Stats Leaders (Season — Scoring)](#player-stats-leaders-season--scoring)
7. [Player Stats Leaders (Season — Rebounds)](#player-stats-leaders-season--rebounds)
8. [Player Stats Leaders (Season — Assists)](#player-stats-leaders-season--assists)
9. [Player Stats (Season — Traditional, PerGame)](#player-stats-season--traditional-pergame)
10. [Player Stats (Season — Advanced, PerGame)](#player-stats-season--advanced-pergame)
11. [Player Stats (Season — Traditional, Accumulated)](#player-stats-season--traditional-accumulated)
12. [Team Stats (Season — Traditional, PerGame)](#team-stats-season--traditional-pergame)
13. [Team Stats (Season — Advanced, PerGame)](#team-stats-season--advanced-pergame)
14. [Team Stats (Season — Opponents Advanced, PerGame)](#team-stats-season--opponents-advanced-pergame)
15. [Team Stats (Season — Opponents Traditional, PerGame)](#team-stats-season--opponents-traditional-pergame)
16. [Standings (Season, Round 1)](#standings-season-round-1)
17. [Gamecodes for Season](#gamecodes-for-season)
18. [Gamecodes for Round](#gamecodes-for-round)

---

## Player Boxscore (Single Game)

**API Call:** `BoxScoreData('E').get_player_boxscore_stats_data({'season': 2023, 'gamecode': 1})`

**Shape:** 28 rows × 29 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `Season` | `int64` | 2023 |
| 2 | `Gamecode` | `int64` | 1 |
| 3 | `Home` | `int64` | 1 |
| 4 | `Player_ID` | `object` | P004720    |
| 5 | `IsStarter` | `float64` | 0.0 |
| 6 | `IsPlaying` | `float64` | 1.0 |
| 7 | `Team` | `object` | RED |
| 8 | `Dorsal` | `object` | 2 |
| 9 | `Player` | `object` | LAZAREVIC, STEFAN |
| 10 | `Minutes` | `object` | 05:04 |
| 11 | `Points` | `int64` | 0 |
| 12 | `FieldGoalsMade2` | `int64` | 0 |
| 13 | `FieldGoalsAttempted2` | `int64` | 0 |
| 14 | `FieldGoalsMade3` | `int64` | 0 |
| 15 | `FieldGoalsAttempted3` | `int64` | 0 |
| 16 | `FreeThrowsMade` | `int64` | 0 |
| 17 | `FreeThrowsAttempted` | `int64` | 0 |
| 18 | `OffensiveRebounds` | `int64` | 0 |
| 19 | `DefensiveRebounds` | `int64` | 1 |
| 20 | `TotalRebounds` | `int64` | 1 |
| 21 | `Assistances` | `int64` | 0 |
| 22 | `Steals` | `int64` | 0 |
| 23 | `Turnovers` | `int64` | 0 |
| 24 | `BlocksFavour` | `int64` | 0 |
| 25 | `BlocksAgainst` | `int64` | 0 |
| 26 | `FoulsCommited` | `int64` | 0 |
| 27 | `FoulsReceived` | `int64` | 0 |
| 28 | `Valuation` | `int64` | 1 |
| 29 | `Plusminus` | `float64` | -1.0 |

---

## Play-by-Play (Single Game)

**API Call:** `PlayByPlay('E').get_game_play_by_play_data({'season': 2023, 'gamecode': 1})`

**Shape:** 501 rows × 18 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `Season` | `int64` | 2023 |
| 2 | `Gamecode` | `int64` | 1 |
| 3 | `TYPE` | `int64` | 0 |
| 4 | `NUMBEROFPLAY` | `int64` | 2 |
| 5 | `CODETEAM` | `object` |  |
| 6 | `PLAYER_ID` | `object` |  |
| 7 | `PLAYTYPE` | `object` | BP |
| 8 | `PLAYER` | `object` | None |
| 9 | `TEAM` | `object` | None |
| 10 | `DORSAL` | `object` | None |
| 11 | `MINUTE` | `int64` | 1 |
| 12 | `MARKERTIME` | `object` |  |
| 13 | `POINTS_A` | `float64` | nan |
| 14 | `POINTS_B` | `float64` | nan |
| 15 | `COMMENT` | `object` |  |
| 16 | `PLAYINFO` | `object` | Begin Period |
| 17 | `PERIOD` | `int64` | 1 |
| 18 | `TRUE_NUMBEROFPLAY` | `int64` | 0 |

---

## Play-by-Play with Lineups (Single Game)

**API Call:** `PlayByPlay('E').get_pbp_data_with_lineups({'season': 2023, 'gamecode': 1})`

**Shape:** 501 rows × 22 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `Season` | `int64` | 2023 |
| 2 | `Gamecode` | `int64` | 1 |
| 3 | `TYPE` | `int64` | 0 |
| 4 | `NUMBEROFPLAY` | `int64` | 2 |
| 5 | `CODETEAM` | `object` |  |
| 6 | `PLAYER_ID` | `object` |  |
| 7 | `PLAYTYPE` | `object` | BP |
| 8 | `PLAYER` | `object` | None |
| 9 | `TEAM` | `object` | None |
| 10 | `DORSAL` | `object` | None |
| 11 | `MINUTE` | `int64` | 1 |
| 12 | `MARKERTIME` | `object` |  |
| 13 | `POINTS_A` | `float64` | nan |
| 14 | `POINTS_B` | `float64` | nan |
| 15 | `COMMENT` | `object` |  |
| 16 | `PLAYINFO` | `object` | Begin Period |
| 17 | `PERIOD` | `int64` | 1 |
| 18 | `IsHomeTeam` | `object` | None |
| 19 | `TRUE_NUMBEROFPLAY` | `int64` | 0 |
| 20 | `Lineup_A` | `object` | ['LAZIC, BRANKO', 'SIMONOVIC, MARKO', 'NAPIER, SHABAZZ', ... |
| 21 | `Lineup_B` | `object` | ['LEE, PARIS', 'LAUVERGNE, JOFFREY', 'LUWAWU-CABARROT, TI... |
| 22 | `validate_on_court_player` | `bool` | True |

---

## Shot Data (Single Game)

**API Call:** `ShotData('E').get_game_shot_data({'season': 2023, 'gamecode': 1})`

**Shape:** 148 rows × 20 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `Season` | `int64` | 2023 |
| 2 | `Gamecode` | `int64` | 1 |
| 3 | `NUM_ANOT` | `int64` | 4 |
| 4 | `TEAM` | `object` | ASV |
| 5 | `ID_PLAYER` | `object` | P004194 |
| 6 | `PLAYER` | `object` | LAUVERGNE, JOFFREY |
| 7 | `ID_ACTION` | `object` | 3FGA |
| 8 | `ACTION` | `object` | Missed Three Pointer |
| 9 | `POINTS` | `int64` | 0 |
| 10 | `COORD_X` | `int64` | 589 |
| 11 | `COORD_Y` | `int64` | 589 |
| 12 | `ZONE` | `object` | I |
| 13 | `FASTBREAK` | `object` | 0 |
| 14 | `SECOND_CHANCE` | `object` | 0 |
| 15 | `POINTS_OFF_TURNOVER` | `object` | 0 |
| 16 | `MINUTE` | `int64` | 1 |
| 17 | `CONSOLE` | `object` | 09:44 |
| 18 | `POINTS_A` | `int64` | 0 |
| 19 | `POINTS_B` | `int64` | 0 |
| 20 | `UTC` | `object` | 20231005170038 |

---

## Team Advanced Stats (Single Game)

**API Call:** `TeamStats('E').get_team_advanced_stats_single_game({'season': 2023, 'gamecode': 1})`

**Shape:** 3 rows × 7 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `Season` | `int64` | 2023 |
| 2 | `Gamecode` | `int64` | 1 |
| 3 | `Home` | `float64` | 1.0 |
| 4 | `Possessions (simple)` | `object` | 70.24000000000001 |
| 5 | `Possessions` | `object` | 66.98363636363636 |
| 6 | `Pace (simple)` | `float64` | nan |
| 7 | `Pace` | `float64` | nan |

---

## Player Stats Leaders (Season — Scoring)

**API Call:** `PlayerStats('E').get_player_stats_leaders_single_season({'season': 2023, 'stat_category': 'Score', 'top_n': 10})`

**Shape:** 10 rows × 20 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `rank` | `int64` | 1 |
| 2 | `playerCode` | `object` | 011948 |
| 3 | `playerName` | `object` | HOWARD, MARKUS |
| 4 | `playerAbbreviatedName` | `object` |  |
| 5 | `imageUrl` | `object` | https://media-cdn.cortextech.io/527ed926-2d87-4ba9-bed3-9... |
| 6 | `teamImageUrl` | `object` | https://media-cdn.cortextech.io/cbc49cb0-99ce-4462-bdb7-5... |
| 7 | `timePlayed` | `object` | 898:24 |
| 8 | `averageTimePlayed` | `object` | 23:02 |
| 9 | `secondsPlayed` | `int64` | 53904 |
| 10 | `gamesPlayed` | `int64` | 39 |
| 11 | `possessions` | `float64` | 779.1200000000001 |
| 12 | `total` | `float64` | 759.0 |
| 13 | `averagePerGame` | `float64` | 19.46153846153846 |
| 14 | `averagePerMinute` | `float64` | 33.793410507569014 |
| 15 | `averagePer100Possessions` | `float64` | 97.41759934284833 |
| 16 | `clubCodes` | `object` | BAS |
| 17 | `clubNames` | `object` | Baskonia Vitoria-Gasteiz |
| 18 | `abbreviatedClubNames` | `object` | Baskonia |
| 19 | `tvCodes` | `object` | BKN |
| 20 | `clubSeasonCodes` | `object` | E2023 |

---

## Player Stats Leaders (Season — Rebounds)

**API Call:** `PlayerStats('E').get_player_stats_leaders_single_season({'season': 2023, 'stat_category': 'TotalRebounds', 'top_n': 10})`

**Shape:** 10 rows × 20 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `rank` | `int64` | 1 |
| 2 | `playerCode` | `object` | 011199 |
| 3 | `playerName` | `object` | NEBO, JOSH |
| 4 | `playerAbbreviatedName` | `object` | Nebo, J. |
| 5 | `imageUrl` | `object` | https://media-cdn.cortextech.io/2d5f84ef-ad28-4415-96b2-6... |
| 6 | `teamImageUrl` | `object` | https://media-cdn.cortextech.io/1b533342-78f5-4932-b714-a... |
| 7 | `timePlayed` | `object` | 925:22 |
| 8 | `averageTimePlayed` | `object` | 23:44 |
| 9 | `secondsPlayed` | `int64` | 55522 |
| 10 | `gamesPlayed` | `int64` | 39 |
| 11 | `possessions` | `float64` | 373.51999999999987 |
| 12 | `total` | `float64` | 276.0 |
| 13 | `averagePerGame` | `float64` | 7.076923076923077 |
| 14 | `averagePerMinute` | `float64` | 11.930405965202983 |
| 15 | `averagePer100Possessions` | `float64` | 73.89162561576357 |
| 16 | `clubCodes` | `object` | TEL |
| 17 | `clubNames` | `object` | Maccabi Rapyd Tel Aviv |
| 18 | `abbreviatedClubNames` | `object` | Maccabi |
| 19 | `tvCodes` | `object` | MTA |
| 20 | `clubSeasonCodes` | `object` | E2023 |

---

## Player Stats Leaders (Season — Assists)

**API Call:** `PlayerStats('E').get_player_stats_leaders_single_season({'season': 2023, 'stat_category': 'Assistances', 'top_n': 10})`

**Shape:** 10 rows × 20 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `rank` | `int64` | 1 |
| 2 | `playerCode` | `object` | 009048 |
| 3 | `playerName` | `object` | MILLER-MCINTYRE, CODI |
| 4 | `playerAbbreviatedName` | `object` | Miller-McIntyre, C. |
| 5 | `imageUrl` | `object` | https://media-cdn.cortextech.io/be4091ef-8325-4cc1-b7f9-d... |
| 6 | `teamImageUrl` | `object` | https://media-cdn.cortextech.io/cbc49cb0-99ce-4462-bdb7-5... |
| 7 | `timePlayed` | `object` | 1150:52 |
| 8 | `averageTimePlayed` | `object` | 29:31 |
| 9 | `secondsPlayed` | `int64` | 69052 |
| 10 | `gamesPlayed` | `int64` | 39 |
| 11 | `possessions` | `float64` | 727.84 |
| 12 | `total` | `float64` | 284.0 |
| 13 | `averagePerGame` | `float64` | 7.282051282051282 |
| 14 | `averagePerMinute` | `float64` | 9.870821989225512 |
| 15 | `averagePer100Possessions` | `float64` | 39.01956473950319 |
| 16 | `clubCodes` | `object` | BAS |
| 17 | `clubNames` | `object` | Baskonia Vitoria-Gasteiz |
| 18 | `abbreviatedClubNames` | `object` | Baskonia |
| 19 | `tvCodes` | `object` | BKN |
| 20 | `clubSeasonCodes` | `object` | E2023 |

---

## Player Stats (Season — Traditional, PerGame)

**API Call:** `PlayerStats('E').get_player_stats_single_season({'endpoint': 'traditional', 'season': 2023, 'statistic_mode': 'PerGame'})`

**Shape:** 199 rows × 33 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `playerRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 22.0 |
| 3 | `gamesStarted` | `float64` | 2.0 |
| 4 | `minutesPlayed` | `float64` | 6.951515151515152 |
| 5 | `pointsScored` | `float64` | 0.8 |
| 6 | `twoPointersMade` | `float64` | 0.1 |
| 7 | `twoPointersAttempted` | `float64` | 0.4 |
| 8 | `twoPointersPercentage` | `object` | 37.5% |
| 9 | `threePointersMade` | `float64` | 0.2 |
| 10 | `threePointersAttempted` | `float64` | 1.0 |
| 11 | `threePointersPercentage` | `object` | 17.4% |
| 12 | `freeThrowsMade` | `float64` | 0.0 |
| 13 | `freeThrowsAttempted` | `float64` | 0.0 |
| 14 | `freeThrowsPercentage` | `object` | 0% |
| 15 | `offensiveRebounds` | `float64` | 0.5 |
| 16 | `defensiveRebounds` | `float64` | 0.6 |
| 17 | `totalRebounds` | `float64` | 1.1 |
| 18 | `assists` | `float64` | 0.1 |
| 19 | `steals` | `float64` | 0.3 |
| 20 | `turnovers` | `float64` | 0.0 |
| 21 | `blocks` | `float64` | 0.1 |
| 22 | `blocksAgainst` | `float64` | 0.0 |
| 23 | `foulsCommited` | `float64` | 1.4 |
| 24 | `foulsDrawn` | `float64` | 0.1 |
| 25 | `pir` | `float64` | 0.0 |
| 26 | `player.code` | `object` | 001518 |
| 27 | `player.name` | `object` | RICCI, GIAMPAOLO |
| 28 | `player.age` | `int64` | 32 |
| 29 | `player.imageUrl` | `object` | https://media-cdn.cortextech.io/f5767f49-5fc2-4639-95ef-0... |
| 30 | `player.team.code` | `object` | MIL |
| 31 | `player.team.tvCodes` | `object` | EA7 |
| 32 | `player.team.name` | `object` | EA7 Emporio Armani Milan |
| 33 | `player.team.imageUrl` | `object` | https://media-cdn.cortextech.io/9512ee73-a0f1-4647-a01e-3... |

---

## Player Stats (Season — Advanced, PerGame)

**API Call:** `PlayerStats('E').get_player_stats_single_season({'endpoint': 'advanced', 'season': 2023, 'statistic_mode': 'PerGame'})`

**Shape:** 199 rows × 23 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `playerRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 22.0 |
| 3 | `minutesPlayed` | `float64` | 6.951515151515152 |
| 4 | `effectiveFieldGoalPercentage` | `object` | 29% |
| 5 | `trueShootingPercentage` | `object` | 29% |
| 6 | `offensiveReboundsPercentage` | `object` | 9% |
| 7 | `defensiveReboundsPercentage` | `object` | 10.6% |
| 8 | `reboundsPercentage` | `object` | 9.8% |
| 9 | `assistsToTurnoversRatio` | `float64` | 2.0 |
| 10 | `assistsRatio` | `object` | 5.9% |
| 11 | `turnoversRatio` | `object` | 2.9% |
| 12 | `twoPointAttemptsRatio` | `object` | 23.5% |
| 13 | `threePointAttemptsRatio` | `object` | 67.6% |
| 14 | `freeThrowsRate` | `object` | 0% |
| 15 | `possesions` | `float64` | 1.5 |
| 16 | `player.code` | `object` | 001518 |
| 17 | `player.name` | `object` | RICCI, GIAMPAOLO |
| 18 | `player.age` | `int64` | 32 |
| 19 | `player.imageUrl` | `object` | https://media-cdn.cortextech.io/f5767f49-5fc2-4639-95ef-0... |
| 20 | `player.team.code` | `object` | MIL |
| 21 | `player.team.tvCodes` | `object` | EA7 |
| 22 | `player.team.name` | `object` | EA7 Emporio Armani Milan |
| 23 | `player.team.imageUrl` | `object` | https://media-cdn.cortextech.io/9512ee73-a0f1-4647-a01e-3... |

---

## Player Stats (Season — Traditional, Accumulated)

**API Call:** `PlayerStats('E').get_player_stats_single_season({'endpoint': 'traditional', 'season': 2023, 'statistic_mode': 'Accumulated'})`

**Shape:** 287 rows × 33 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `playerRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 10.0 |
| 3 | `gamesStarted` | `float64` | 3.0 |
| 4 | `minutesPlayed` | `float64` | 87.46666666666667 |
| 5 | `pointsScored` | `float64` | 9.0 |
| 6 | `twoPointersMade` | `float64` | 2.0 |
| 7 | `twoPointersAttempted` | `float64` | 6.0 |
| 8 | `twoPointersPercentage` | `object` | 33.3% |
| 9 | `threePointersMade` | `float64` | 1.0 |
| 10 | `threePointersAttempted` | `float64` | 17.0 |
| 11 | `threePointersPercentage` | `object` | 5.9% |
| 12 | `freeThrowsMade` | `float64` | 2.0 |
| 13 | `freeThrowsAttempted` | `float64` | 4.0 |
| 14 | `freeThrowsPercentage` | `object` | 50% |
| 15 | `offensiveRebounds` | `float64` | 2.0 |
| 16 | `defensiveRebounds` | `float64` | 3.0 |
| 17 | `totalRebounds` | `float64` | 5.0 |
| 18 | `assists` | `float64` | 4.0 |
| 19 | `steals` | `float64` | 1.0 |
| 20 | `turnovers` | `float64` | 5.0 |
| 21 | `blocks` | `float64` | 0.0 |
| 22 | `blocksAgainst` | `float64` | 3.0 |
| 23 | `foulsCommited` | `float64` | 9.0 |
| 24 | `foulsDrawn` | `float64` | 6.0 |
| 25 | `pir` | `float64` | -14.0 |
| 26 | `player.code` | `object` | 010516 |
| 27 | `player.name` | `object` | GAZI, ERTEN |
| 28 | `player.age` | `int64` | 26 |
| 29 | `player.imageUrl` | `object` | https://media-cdn.incrowdsports.com/714ae09c-4d14-4a17-bf... |
| 30 | `player.team.code` | `object` | IST |
| 31 | `player.team.tvCodes` | `object` | EFS |
| 32 | `player.team.name` | `object` | Anadolu Efes Istanbul |
| 33 | `player.team.imageUrl` | `object` | https://media-cdn.cortextech.io/9a463aa2-ceb2-481c-9a95-1... |

---

## Team Stats (Season — Traditional, PerGame)

**API Call:** `TeamStats('E').get_team_stats_single_season({'endpoint': 'traditional', 'season': 2023, 'statistic_mode': 'PerGame'})`

**Shape:** 18 rows × 28 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `teamRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 34.0 |
| 3 | `minutesPlayed` | `float64` | 40.0 |
| 4 | `pointsScored` | `float64` | 76.2 |
| 5 | `twoPointersMade` | `float64` | 18.9 |
| 6 | `twoPointersAttempted` | `float64` | 37.1 |
| 7 | `twoPointersPercentage` | `object` | 50.8% |
| 8 | `threePointersMade` | `float64` | 8.9 |
| 9 | `threePointersAttempted` | `float64` | 25.3 |
| 10 | `threePointersPercentage` | `object` | 35.1% |
| 11 | `freeThrowsMade` | `float64` | 11.9 |
| 12 | `freeThrowsAttempted` | `float64` | 15.0 |
| 13 | `freeThrowsPercentage` | `object` | 79.2% |
| 14 | `offensiveRebounds` | `float64` | 10.8 |
| 15 | `defensiveRebounds` | `float64` | 21.6 |
| 16 | `totalRebounds` | `float64` | 32.4 |
| 17 | `assists` | `float64` | 16.5 |
| 18 | `steals` | `float64` | 7.0 |
| 19 | `turnovers` | `float64` | 15.1 |
| 20 | `blocks` | `float64` | 2.1 |
| 21 | `blocksAgainst` | `float64` | 3.5 |
| 22 | `foulsCommited` | `float64` | 18.3 |
| 23 | `foulsDrawn` | `float64` | 18.2 |
| 24 | `pir` | `float64` | 77.7 |
| 25 | `team.code` | `object` | BER |
| 26 | `team.tvCodes` | `object` | BER |
| 27 | `team.name` | `object` | ALBA Berlin |
| 28 | `team.imageUrl` | `object` | https://media-cdn.incrowdsports.com/ccc34858-22b0-47dc-90... |

---

## Team Stats (Season — Advanced, PerGame)

**API Call:** `TeamStats('E').get_team_stats_single_season({'endpoint': 'advanced', 'season': 2023, 'statistic_mode': 'PerGame'})`

**Shape:** 18 rows × 20 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `teamRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 34.0 |
| 3 | `effectiveFieldGoalPercentage` | `object` | 51.6% |
| 4 | `trueShootingPercentage` | `object` | 55.2% |
| 5 | `offensiveReboundsPercentage` | `object` | 30.7% |
| 6 | `defensiveReboundsPercentage` | `object` | 66.3% |
| 7 | `reboundsPercentage` | `object` | 47.8% |
| 8 | `assistsToTurnoversRatio` | `float64` | 1.1 |
| 9 | `assistsRatio` | `object` | 16.4% |
| 10 | `turnoversRatio` | `object` | 15.1% |
| 11 | `twoPointRate` | `object` | 59.5% |
| 12 | `threePointRate` | `object` | 40.5% |
| 13 | `freeThrowsRate` | `object` | 24% |
| 14 | `pointsFromTwoPointersPercentage` | `object` | 49.5% |
| 15 | `pointsFromThreePointersPercentage` | `object` | 35% |
| 16 | `pointsFromFreeThrowsPercentage` | `object` | 15.6% |
| 17 | `team.code` | `object` | BER |
| 18 | `team.tvCodes` | `object` | BER |
| 19 | `team.name` | `object` | ALBA Berlin |
| 20 | `team.imageUrl` | `object` | https://media-cdn.incrowdsports.com/ccc34858-22b0-47dc-90... |

---

## Team Stats (Season — Opponents Advanced, PerGame)

**API Call:** `TeamStats('E').get_team_stats_single_season({'endpoint': 'opponentsAdvanced', 'season': 2023, 'statistic_mode': 'PerGame'})`

**Shape:** 18 rows × 20 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `teamRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 34.0 |
| 3 | `effectiveFieldGoalPercentage` | `object` | 58.1% |
| 4 | `trueShootingPercentage` | `object` | 60.9% |
| 5 | `offensiveReboundsPercentage` | `object` | 33.7% |
| 6 | `defensiveReboundsPercentage` | `object` | 69.3% |
| 7 | `reboundsPercentage` | `object` | 52.2% |
| 8 | `assistsToTurnoversRatio` | `float64` | 1.5 |
| 9 | `assistsRatio` | `object` | 19% |
| 10 | `turnoversRatio` | `object` | 12.7% |
| 11 | `twoPointRate` | `object` | 58.9% |
| 12 | `threePointRate` | `object` | 41.1% |
| 13 | `freeThrowsRate` | `object` | 25.9% |
| 14 | `pointsFromTwoPointersPercentage` | `object` | 50.1% |
| 15 | `pointsFromThreePointersPercentage` | `object` | 35.5% |
| 16 | `pointsFromFreeThrowsPercentage` | `object` | 14.4% |
| 17 | `team.code` | `object` | BER |
| 18 | `team.tvCodes` | `object` | BER |
| 19 | `team.name` | `object` | ALBA Berlin |
| 20 | `team.imageUrl` | `object` | https://media-cdn.incrowdsports.com/ccc34858-22b0-47dc-90... |

---

## Team Stats (Season — Opponents Traditional, PerGame)

**API Call:** `TeamStats('E').get_team_stats_single_season({'endpoint': 'opponentsTraditional', 'season': 2023, 'statistic_mode': 'PerGame'})`

**Shape:** 18 rows × 28 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `teamRanking` | `int64` | 1 |
| 2 | `gamesPlayed` | `float64` | 34.0 |
| 3 | `minutesPlayed` | `float64` | 40.0 |
| 4 | `pointsScored` | `float64` | 86.6 |
| 5 | `twoPointersMade` | `float64` | 21.7 |
| 6 | `twoPointersAttempted` | `float64` | 37.6 |
| 7 | `twoPointersPercentage` | `object` | 57.8% |
| 8 | `threePointersMade` | `float64` | 10.2 |
| 9 | `threePointersAttempted` | `float64` | 26.2 |
| 10 | `threePointersPercentage` | `object` | 39% |
| 11 | `freeThrowsMade` | `float64` | 12.5 |
| 12 | `freeThrowsAttempted` | `float64` | 16.5 |
| 13 | `freeThrowsPercentage` | `object` | 75.4% |
| 14 | `offensiveRebounds` | `float64` | 11.0 |
| 15 | `defensiveRebounds` | `float64` | 24.4 |
| 16 | `totalRebounds` | `float64` | 35.4 |
| 17 | `assists` | `float64` | 19.8 |
| 18 | `steals` | `float64` | 8.4 |
| 19 | `turnovers` | `float64` | 13.3 |
| 20 | `blocks` | `float64` | 3.5 |
| 21 | `blocksAgainst` | `float64` | 2.1 |
| 22 | `foulsCommited` | `float64` | 18.4 |
| 23 | `foulsDrawn` | `float64` | 18.1 |
| 24 | `pir` | `float64` | 102.2 |
| 25 | `team.code` | `object` | BER |
| 26 | `team.tvCodes` | `object` | BER |
| 27 | `team.name` | `object` | ALBA Berlin |
| 28 | `team.imageUrl` | `object` | https://media-cdn.incrowdsports.com/ccc34858-22b0-47dc-90... |

---

## Standings (Season, Round 1)

**API Call:** `Standings('E').get_standings({'season': 2023, 'round_number': 1})`

**Shape:** 18 rows × 24 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `position` | `int64` | 1 |
| 2 | `positionChange` | `object` | Equal |
| 3 | `gamesPlayed` | `int64` | 1 |
| 4 | `gamesWon` | `int64` | 1 |
| 5 | `gamesLost` | `int64` | 0 |
| 6 | `qualified` | `bool` | False |
| 7 | `winPercentage` | `object` | 100% |
| 8 | `pointsDifference` | `object` | +21 |
| 9 | `pointsFor` | `int64` | 94 |
| 10 | `pointsAgainst` | `int64` | 73 |
| 11 | `homeRecord` | `object` | 1-0 |
| 12 | `awayRecord` | `object` | 0-0 |
| 13 | `neutralRecord` | `object` | 0-0 |
| 14 | `overtimeRecord` | `object` | 0-0 |
| 15 | `lastTenRecord` | `object` | 1-0 |
| 16 | `groupName` | `object` | Regular Season |
| 17 | `last5Form` | `object` | ['W'] |
| 18 | `club.code` | `object` | RED |
| 19 | `club.name` | `object` | Crvena Zvezda Meridianbet Belgrade |
| 20 | `club.abbreviatedName` | `object` | Zvezda |
| 21 | `club.editorialName` | `object` | Crvena Zvezda |
| 22 | `club.tvCode` | `object` | CZV |
| 23 | `club.isVirtual` | `bool` | False |
| 24 | `club.images.crest` | `object` | https://media-cdn.incrowdsports.com/d2eef4a8-62df-4fdd-90... |

---

## Gamecodes for Season

**API Call:** `PlayByPlay('E').get_gamecodes_season({'season': 2023})`

**Shape:** 331 rows × 14 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `Phase` | `object` | RS |
| 2 | `Round` | `int64` | 1 |
| 3 | `date` | `object` | Oct 05, 2023 |
| 4 | `time` | `object` | 19:00 |
| 5 | `gameCode` | `int64` | 1 |
| 6 | `gamecode` | `object` | E2023_1 |
| 7 | `group` | `object` | Regular Season |
| 8 | `hometeam` | `object` | CRVENA ZVEZDA MERIDIANBET BELGRADE |
| 9 | `homecode` | `object` | RED |
| 10 | `homescore` | `int64` | 94 |
| 11 | `awayteam` | `object` | LDLC ASVEL VILLEURBANNE |
| 12 | `awaycode` | `object` | ASV |
| 13 | `awayscore` | `int64` | 73 |
| 14 | `played` | `bool` | True |

---

## Gamecodes for Round

**API Call:** `PlayByPlay('E').get_gamecodes_round({'season': 2023, 'round_number': 1})`

**Shape:** 9 rows × 97 columns

| # | Column Name | Dtype | Sample Value |
|---|-------------|-------|--------------|
| 1 | `id` | `object` | 7b4028a3-5e04-4c98-98b0-8b3579b8615c |
| 2 | `identifier` | `object` | E2023_1 |
| 3 | `gameCode` | `int64` | 1 |
| 4 | `Round` | `int64` | 1 |
| 5 | `roundAlias` | `object` | Round 1 |
| 6 | `roundName` | `object` | Round 1 |
| 7 | `played` | `bool` | True |
| 8 | `date` | `object` | 2023-10-05T19:00:00 |
| 9 | `confirmedDate` | `bool` | True |
| 10 | `confirmedHour` | `bool` | True |
| 11 | `localTimeZone` | `int64` | 2 |
| 12 | `localDate` | `object` | 2023-10-05T19:00:00 |
| 13 | `utcDate` | `object` | 2023-10-05T17:00:00Z |
| 14 | `audience` | `int64` | 18360 |
| 15 | `audienceConfirmed` | `bool` | True |
| 16 | `socialFeed` | `object` |  |
| 17 | `operationsCode` | `object` | None |
| 18 | `referee4` | `object` | None |
| 19 | `isNeutralVenue` | `bool` | False |
| 20 | `gameStatus` | `object` | Confirmed |
| 21 | `season.name` | `object` | EuroLeague 2023-24 |
| 22 | `season.code` | `object` | E2023 |
| 23 | `season.alias` | `object` | 2023-24 |
| 24 | `season.competitionCode` | `object` | E |
| 25 | `season.year` | `int64` | 2023 |
| 26 | `season.startDate` | `object` | 2023-06-29T00:00:00 |
| 27 | `group.id` | `object` | 1c8d3521-392b-4637-8aea-7b35f797bbba |
| 28 | `group.order` | `int64` | 1 |
| 29 | `group.name` | `object` | Group Regular Season |
| 30 | `group.rawName` | `object` | Regular Season |
| 31 | `Phase` | `object` | RS |
| 32 | `phaseType.alias` | `object` | REGULAR SEASON |
| 33 | `phaseType.name` | `object` | Regular Season |
| 34 | `phaseType.isGroupPhase` | `bool` | True |
| 35 | `local.club.code` | `object` | RED |
| 36 | `local.club.name` | `object` | Crvena Zvezda Meridianbet Belgrade |
| 37 | `local.club.abbreviatedName` | `object` | Zvezda |
| 38 | `local.club.editorialName` | `object` | Crvena Zvezda |
| 39 | `local.club.tvCode` | `object` | CZV |
| 40 | `local.club.isVirtual` | `bool` | False |
| 41 | `local.club.images.crest` | `object` | https://media-cdn.incrowdsports.com/d2eef4a8-62df-4fdd-90... |
| 42 | `local.score` | `int64` | 94 |
| 43 | `local.standingsScore` | `int64` | 94 |
| 44 | `local.partials.partials1` | `int64` | 29 |
| 45 | `local.partials.partials2` | `int64` | 23 |
| 46 | `local.partials.partials3` | `int64` | 17 |
| 47 | `local.partials.partials4` | `int64` | 25 |
| 48 | `road.club.code` | `object` | ASV |
| 49 | `road.club.name` | `object` | LDLC ASVEL Villeurbanne |
| 50 | `road.club.abbreviatedName` | `object` | ASVEL |
| 51 | `road.club.editorialName` | `object` | ASVEL |
| 52 | `road.club.tvCode` | `object` | ASV |
| 53 | `road.club.isVirtual` | `bool` | False |
| 54 | `road.club.images.crest` | `object` | https://media-cdn.incrowdsports.com/e33c6d1a-95ca-4dbc-b8... |
| 55 | `road.score` | `int64` | 73 |
| 56 | `road.standingsScore` | `int64` | 73 |
| 57 | `road.partials.partials1` | `int64` | 18 |
| 58 | `road.partials.partials2` | `int64` | 13 |
| 59 | `road.partials.partials3` | `int64` | 24 |
| 60 | `road.partials.partials4` | `int64` | 18 |
| 61 | `referee1.code` | `object` | OABC |
| 62 | `referee1.name` | `object` | ROCHA, FERNANDO |
| 63 | `referee1.alias` | `object` | ROCHA, F. |
| 64 | `referee1.country.code` | `object` | POR |
| 65 | `referee1.country.name` | `object` | Portugal |
| 66 | `referee1.images.verticalSmall` | `object` | 88654j47f8j5rrkt |
| 67 | `referee1.active` | `bool` | True |
| 68 | `referee2.code` | `object` | OAEM |
| 69 | `referee2.name` | `object` | PATERNICO, CARMELO |
| 70 | `referee2.alias` | `object` | PATERNICO, C. |
| 71 | `referee2.country.code` | `object` | ITA |
| 72 | `referee2.country.name` | `object` | Italy |
| 73 | `referee2.images.verticalSmall` | `object` | 885x49q73ymq7gg3 |
| 74 | `referee2.active` | `bool` | True |
| 75 | `referee3.code` | `object` | OJIY |
| 76 | `referee3.name` | `object` | PASTUSIAK, PIOTR |
| 77 | `referee3.alias` | `object` | PASTUSIAK, P. |
| 78 | `referee3.country.code` | `object` | POL |
| 79 | `referee3.country.name` | `object` | Poland |
| 80 | `referee3.images.verticalSmall` | `object` | 885wrb4rki89jxa3 |
| 81 | `referee3.active` | `bool` | True |
| 82 | `venue.name` | `object` | STARK ARENA |
| 83 | `venue.code` | `object` | ATM6 |
| 84 | `venue.capacity` | `int64` | 20094 |
| 85 | `venue.address` | `object` | Bulevar Arsenija Carnojevica 58, 11070 Belgrade - Serbia |
| 86 | `venue.images.medium` | `object` | None |
| 87 | `venue.active` | `bool` | True |
| 88 | `venue.notes` | `object` |  |
| 89 | `winner.code` | `object` | PAN |
| 90 | `winner.name` | `object` | Panathinaikos AKTOR Athens |
| 91 | `winner.abbreviatedName` | `object` | Panathinaikos |
| 92 | `winner.editorialName` | `object` | Panathinaikos |
| 93 | `winner.tvCode` | `object` | PAO |
| 94 | `winner.isVirtual` | `bool` | False |
| 95 | `winner.images.crest` | `object` | https://media-cdn.incrowdsports.com/e3dff28a-9ec6-4faf-9d... |
| 96 | `local.partials.extraPeriods.1` | `float64` | nan |
| 97 | `road.partials.extraPeriods.1` | `float64` | nan |

---

## Summary

- **Endpoints explored:** 18
- **Successful:** 18
- **Failed / Empty:** 0
