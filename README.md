<div align="center">

# 🏀 Euroleague Advanced Analytics Platform

**Enterprise-grade basketball analytics with DB-first architecture, async ETL pipelines, and sub-second query performance.**

[![Streamlit](https://img.shields.io/badge/Streamlit-1.31+-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Supabase](https://img.shields.io/badge/Supabase-Ready-3FCF8E?logo=supabase&logoColor=white)](https://supabase.com)
[![asyncio](https://img.shields.io/badge/asyncio-Powered-yellow?logo=python&logoColor=white)](https://docs.python.org/3/library/asyncio.html)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

[Features](#-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [ETL Pipeline](#-async-etl-engine) • [Tech Stack](#-tech-stack)

</div>

---

## 📋 Overview

A production-ready analytics platform that transforms raw Euroleague basketball data into actionable insights. What started as a simple API-fetching script has evolved into an **enterprise-grade, DB-First Analytics Platform** with a high-performance async ETL pipeline.

### The Core Innovation: DB-First Architecture

The Streamlit UI **never computes heavy statistics on-the-fly**. Instead:

1. **ETL Phase** — An async pipeline fetches raw data, computes all advanced metrics (On/Off splits, Lineup Net Ratings, TPC), and bulk-upserts them into PostgreSQL/Supabase.
2. **Query Phase** — The dashboard reads pre-aggregated tables, delivering **sub-second load times** even with 190k+ play-by-play rows.

| Approach | Load Time | Compute Location | Rate Limits |
|----------|-----------|------------------|-------------|
| ❌ API-Only (Old) | 5-15s per view | Pandas in UI | ⚠️ Throttled |
| ✅ **DB-First (Current)** | **<500ms** | Pre-computed in ETL | ✅ None |

---

## ✨ Features

### 📊 Advanced Statistics Engine

All metrics are **pre-computed during ETL** and stored in dedicated database tables:

| Metric | Formula | Description |
|--------|---------|-------------|
| **True Shooting % (TS%)** | `PTS / (2 × (FGA + 0.44 × FTA))` | Scoring efficiency across all shot types |
| **True Usage Rate (tUSG%)** | `(FGA + 0.44×FTA + TOV + AST + FD) / Team Poss` | Extended usage including assists & fouls drawn |
| **Total Points Created (TPC)** | `PTS + Assisted Points` | Complete offensive contribution |
| **Offensive Rating (ORtg)** | `(PTS / Poss) × 100` | Points scored per 100 possessions |
| **Defensive Rating (DRtg)** | `(Opp PTS / Poss) × 100` | Points allowed per 100 possessions |
| **On/Off Splits** | `Team NetRtg (On) - Team NetRtg (Off)` | Player impact differential |
| **Stop Rate** | `(STL + BLK + DRB) / Def Poss` | Defensive possession ending % |

### 🔄 Play-by-Play Analytics

- **Lineup Reconstruction** — Exact 5-man lineups from IN/OUT substitution events
- **Lineup Net Rating** — ORtg/DRtg per 5-man combination (stored in `lineup_stats` table)
- **Duo & Trio Synergy** — Performance when 2/3 players share the court vs. apart
- **Clutch Analysis** — Stats in last 5 min of Q4/OT with ≤5 point differential
- **Run Detection** — Identifies scoring runs and who stops them
- **Assist Network** — Passer → Scorer relationship matrix with shot quality

### 🗄️ Pre-Computed Aggregations

Heavy calculations are **not done dynamically by Pandas in the UI**. They are computed during ETL and stored in dedicated tables:

| Table | Contents | Query Time |
|-------|----------|------------|
| `season_on_off_splits` | Player On/Off differentials per season | <50ms |
| `season_lineup_stats` | 5-man lineup ORtg/DRtg/NetRtg | <50ms |
| `player_season_stats` | Aggregated player stats with advanced metrics | <30ms |
| `team_season_stats` | Team-level efficiency ratings | <20ms |

### 🖥️ Interactive Dashboard

| Page | Description |
|------|-------------|
| **🏠 Home** | Landing page with navigation cards |
| **🏆 Single Game** | Deep-dive into any game with all advanced metrics |
| **📊 Season Overview** | League efficiency landscape, team comparisons, form tracker |
| **⚡ Advanced Analytics** | Player-level advanced stats with filters |
| **🏅 Leaders** | Season leaderboards with minimum qualifier filters |
| **📋 Referee Analysis** | Referee tendencies and foul patterns |
| **📖 Glossary** | Metric definitions and formulas |

### 🌍 Internationalization

- **4 Languages** — English, Greek (Ελληνικά), German (Deutsch), Spanish (Español)
- **Dynamic Switching** — Real-time UI translation without page reload
- **Full Coverage** — Labels, tooltips, chart axes, table headers

### 🎨 UI/UX Polish

- **Custom Skeleton Loaders** — Shimmering CSS placeholders during data fetches (no more blank screens)
- **Breadcrumb Navigation** — Context trail in Season Overview (`🏠 Home › 📊 Season › OLY`)
- **Team Branding** — Dynamic accent colors based on selected team
- **Global Decimal Formatting** — Centralized `GLOBAL_DECIMALS` config (default: 2) applied via `format_df_decimals()` utility

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ASYNC ETL PIPELINE (load_to_db.py)                       │
│                                                                              │
│  ┌──────────────┐    ┌──────────────────┐    ┌────────────────────────────┐ │
│  │ Euroleague   │───→│  Async Extractors │───→│   Transformers (Pandas)    │ │
│  │ Live API     │    │  (aiohttp)        │    │   • compute_advanced_stats │ │
│  │              │    │  • 50 concurrent  │    │   • compute_on_off_splits  │ │
│  └──────────────┘    │    requests       │    │   • compute_lineup_stats   │ │
│                      └──────────────────┘    └─────────────┬──────────────┘ │
│                                                             │                │
│  Performance: 340+ games, 190k+ PBP rows in <2.5 minutes   │                │
│                                                             ▼                │
│                                              ┌────────────────────────────┐  │
│                                              │  PostgreSQL / Supabase     │  │
│                                              │  ┌──────────────────────┐  │  │
│                                              │  │ Pre-computed Tables: │  │  │
│                                              │  │ • season_on_off_splits│  │  │
│                                              │  │ • season_lineup_stats │  │  │
│                                              │  │ • player_season_stats │  │  │
│                                              │  └──────────────────────┘  │  │
│                                              └─────────────┬──────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
                                                             │
                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          STREAMLIT DASHBOARD                                  │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    queries.py (Data Access Layer)                        │ │
│  │                                                                          │ │
│  │   USE_DB=True (Primary)              USE_DB=False (Fallback)            │ │
│  │   ┌─────────────────────┐            ┌─────────────────────┐            │ │
│  │   │ SELECT * FROM       │            │ fetch from API +    │            │ │
│  │   │ season_on_off_splits│            │ compute on-the-fly  │            │ │
│  │   │ WHERE season=2025   │            │ (slow, rate-limited)│            │ │
│  │   │                     │            │                     │            │ │
│  │   │ Response: <50ms     │            │ Response: 5-15s     │            │ │
│  │   └─────────────────────┘            └─────────────────────┘            │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                               │
│  Views: Season Overview • Single Game • Leaders • Advanced • Referee         │
└──────────────────────────────────────────────────────────────────────────────┘
```

### DB-First, API-Fallback

1. **Primary Path (DB)**: Dashboard queries pre-aggregated tables — instant response
2. **Fallback Path (API)**: If `USE_DB=False` or DB unavailable, fetches live from Euroleague API
3. **Seamless Toggle**: Set `USE_DB=True` in `.env` to enable database mode

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL 16+ or Supabase account

### 1. Clone & Install

```bash
git clone https://github.com/yourusername/EuroleagueStats.git
cd EuroleagueStats

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
# ═══════════════════════════════════════════════════════════
# DATABASE MODE (Required for production performance)
# ═══════════════════════════════════════════════════════════
USE_DB=True

# ═══════════════════════════════════════════════════════════
# PostgreSQL / Supabase Credentials
# ═══════════════════════════════════════════════════════════
POSTGRES_HOST=db.xxxxxxxxxxxx.supabase.co
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your-secure-password

# ═══════════════════════════════════════════════════════════
# Optional Features
# ═══════════════════════════════════════════════════════════
OPENAI_API_KEY=sk-...        # For AI Chat feature
REQUIRE_LOGIN=False          # Enable authentication
```

### 3. Populate the Database

```bash
# Load the current season (340+ games in ~2.5 minutes)
python -m data_pipeline.load_to_db --season 2025

# Or reset and reload from scratch
python -m data_pipeline.load_to_db --season 2025 --reset
```

### 4. Launch the Dashboard

```bash
streamlit run streamlit_app/app.py
```

Navigate to `http://localhost:8501`. With `USE_DB=True`, all views load in **<500ms**.

---

## ⚡ Async ETL Engine

The crown jewel of this platform. The `load_to_db.py` pipeline uses `asyncio` and `aiohttp` to achieve massive throughput:

### Performance Benchmarks

| Metric | Value |
|--------|-------|
| **Full Season Sync** | ~2.5 minutes |
| **Games Processed** | 340+ per season |
| **Play-by-Play Rows** | 190,000+ per season |
| **Concurrent API Requests** | 50 (configurable) |
| **Bulk Upsert Batch Size** | 1,000 rows |

### CLI Usage

```bash
# Sync a single season
python -m data_pipeline.load_to_db --season 2025

# Sync multiple seasons
python -m data_pipeline.load_to_db --season 2025 2024 2023

# Reset tables and reload (fresh start)
python -m data_pipeline.load_to_db --season 2025 --reset

# Verbose logging
python -m data_pipeline.load_to_db --season 2025 --verbose
```

### What Gets Computed

During ETL, the pipeline computes and stores:

1. **Raw Data** — `boxscores`, `play_by_play`, `shots`, `schedules`
2. **Player Advanced Stats** — TS%, tUSG%, ORtg, DRtg, TPC per game
3. **Season Aggregations** — `player_season_stats`, `team_season_stats`
4. **Lineup Analytics** — `season_lineup_stats` (5-man combinations)
5. **On/Off Splits** — `season_on_off_splits` (player impact differentials)

### Programmatic Usage

```python
import asyncio
from data_pipeline.load_to_db import sync_season_async

# Sync all games from 2025 season
asyncio.run(sync_season_async(season=2025))
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Frontend** | Streamlit 1.31+, Plotly, AG-Grid | Interactive dashboards |
| **Backend** | Python 3.10+, Pandas, NumPy | Data processing |
| **Database** | PostgreSQL 16 / Supabase | Pre-computed storage |
| **Async I/O** | asyncio, aiohttp, asyncpg | High-throughput ETL |
| **ETL** | Custom pipeline | Idempotent bulk upserts |
| **Config** | YAML + dotenv | Environment-aware settings |
| **i18n** | JSON translation files | 4-language support |

---

## 🗂️ Project Structure

```
EuroleagueStats/
├── config/
│   └── config.yaml              # Seasons, team colors, GLOBAL_DECIMALS
├── data_pipeline/
│   ├── extractors.py            # Async API extraction (aiohttp)
│   ├── transformers.py          # 20+ stat computation functions
│   └── load_to_db.py            # CLI ETL orchestrator
├── database/
│   └── schema.sql               # PostgreSQL schema
├── streamlit_app/
│   ├── app.py                   # Main router + sidebar branding
│   ├── queries.py               # DB-first, API-fallback data layer
│   ├── shared.py                # Skeleton loaders, breadcrumbs, formatting
│   ├── translations.json        # i18n (en, el, de, es)
│   ├── utils/
│   │   ├── config_loader.py     # YAML + env config
│   │   ├── feature_flags.py     # Feature toggles
│   │   └── auth.py              # Authentication
│   └── views/
│       ├── home.py
│       ├── single_game.py
│       ├── season_overview.py   # Includes breadcrumbs
│       ├── advanced_analytics.py
│       ├── leaders.py
│       ├── referee.py
│       └── glossary.py
├── .env                         # Credentials (gitignored)
├── requirements.txt
└── README.md
```

---

## 📊 Database Schema

### Core Entities

```sql
teams (team_code PK, team_name, logo_url)
players (player_id PK, player_name, team_code FK)
games (season, gamecode PK, home_team FK, away_team FK, scores, date)
```

### Pre-Computed Analytics Tables

```sql
-- Per-game player stats with advanced metrics
player_advanced_stats (
    season, gamecode, player_id,
    ts_pct, true_usg_pct, ortg, drtg, tpc, stop_rate, ...
)

-- Season-aggregated On/Off splits (the heavy computation)
season_on_off_splits (
    season, player_id, team_code,
    on_ortg, on_drtg, on_net,
    off_ortg, off_drtg, off_net,
    diff_net, minutes_on, minutes_off
)

-- 5-man lineup performance
season_lineup_stats (
    season, team_code, lineup_hash, players[],
    ortg, drtg, net_rtg, possessions, minutes
)

-- Raw event data
play_by_play (game FK, event_id, playtype, player, period, clock, ...)
shots (game FK, player, x, y, zone, made, assisted_by, ...)
```

---

## 🔧 Configuration

### `config/config.yaml`

```yaml
app:
  page_title: "Euroleague Advanced Analytics"
  default_language: "en"
  languages:
    "🇬🇧 English": "en"
    "🇬🇷 Ελληνικά": "el"
    "🇩🇪 Deutsch": "de"
    "🇪🇸 Español": "es"

data:
  supported_seasons: [2025, 2024, 2023, 2022, 2021]
  default_season: 2025

ui:
  global_decimals: 2  # Applied via format_df_decimals()
  team_colors:
    OLY: { primary: "#D31145", secondary: "#FFFFFF", name: "Olympiacos" }
    PAO: { primary: "#007A33", secondary: "#FFFFFF", name: "Panathinaikos" }
    # ... 18 teams
```

---

## 📝 Data Source

Data sourced from the [euroleague-api](https://pypi.org/project/euroleague-api/) Python library, wrapping the official [Euroleague Live API](https://api-live.euroleague.net/swagger/index.html).

- **Boxscores** — Player-level game stats
- **Play-by-Play** — Every game action (190k+ events/season)
- **Shot Data** — X/Y coordinates, zones, assisted shots

---

## 📄 License

MIT License. Euroleague data is property of Euroleague Basketball. This project is for educational and analytical purposes.
