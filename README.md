# 🏀 Euroleague Advanced Analytics Platform

A production-ready data platform that extracts real basketball data from the **Euroleague API**, computes advanced statistics, stores them in PostgreSQL, and visualizes them through an interactive **Streamlit** dashboard.

![Dashboard Preview](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit) ![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql) ![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

## ✨ Features

### Advanced Statistics Engine
| Metric | Formula | Description |
|--------|---------|-------------|
| **Estimated Possessions** | `FGA + 0.44 × FTA − ORB + TOV` | Team possessions apportioned by playing time |
| **True Shooting % (TS%)** | `PTS / (2 × (FGA + 0.44 × FTA))` | Scoring efficiency across all shot types |
| **Offensive Rating** | `(Player PTS / Player Poss) × 100` | Points scored per 100 possessions |
| **Defensive Rating** | `(Opp PTS / Team Poss) × 100` | Points allowed per 100 possessions |
| **True Usage Rate (tUSG%)** | `(FGA + 0.44×FTA + TOV + AST + Fouls Drawn) / Weighted Team Poss` | Extended usage including assists & fouls drawn |
| **Stop Rate** | `(STL + BLK + DRB) / Def Possessions` | Defensive possession ending percentage |

### Play-by-Play Analytics
- **Lineup Tracking** — Reconstructs exact 5-man lineups from IN/OUT substitution events
- **Lineup Net Rating** — ORtg/DRtg per 5-man combination
- **Duo & Trio Synergy** — Performance when specific 2/3 players are on court together vs. apart
- **Clutch Factor** — Player stats in last 5 min of Q4/OT with ≤5 point differential
- **Run-Stopping Ability** — Identifies who breaks opponent 8+ point scoring runs
- **Foul Trouble Impact** — Team ORtg/DRtg change when the star gets early fouls
- **Assist Network** — Passer → Scorer relationship matrix

### Interactive Dashboard (7 Pages)
1. **Season Overview** — Aggregated league efficiency landscape & line-up metrics over the full season
2. **Player Advanced Stats** — Sortable table with team/player filters + ORtg vs DRtg scatter plot
3. **Shot Chart** — Precise FIBA-scaled half-court Plotly visualization with true coordinates
4. **Player Comparison Radar** — 5-axis normalized radar chart comparing any 2 players
5. **Lineup & Synergy** — Best/worst 5-man lineups, duo and trio synergy tables
6. **Assist Network** — Interactive heatmap of assist relationships per team
7. **Clutch & Momentum** — Clutch stats, run stoppers, foul trouble impact metrics

### 🌍 Internationalization (i18n)
- **Multi-language Support** — Built-in reactive localization for **English, Greek, German, and Spanish**.
- **Dynamic Translation Engine** — Real-time swapping of nested UI labels, Streamlit tabs, KPI metrics, Plotly axes, and DataFrames without reloading data.

---

## 🗂️ Project Structure

```
euroleague_advanced_stats/
├── data_pipeline/
│   ├── extractors.py          # Euroleague API data extraction (boxscore, PBP, shots)
│   ├── transformers.py        # Advanced stats computation (13 functions)
│   └── load_to_db.py          # SQLAlchemy loader with idempotent upserts
├── database/
│   └── schema.sql             # PostgreSQL schema (5 normalized tables)
├── streamlit_app/
│   ├── app.py                 # Multi-page interactive dashboard
│   ├── queries.py             # Data access layer (live API + DB modes)
│   ├── translations.json      # i18n dictionary (en, el, de, es)
├── docker-compose.yml         # PostgreSQL 16 container
├── requirements.txt           # Python dependencies
├── .env                       # Database credentials (gitignored)
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Docker & Docker Compose (optional, for PostgreSQL)

### 1. Install Dependencies

```bash
cd EuroleagueStats
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Launch the Dashboard (No Database Required)

The dashboard works in **live mode** — fetching directly from the Euroleague API:

```bash
streamlit run streamlit_app/app.py
```

Navigate to `http://localhost:8501`, select a season and game code, then click **Load Game Data**.

### 3. (Optional) Database Setup

To persist data in PostgreSQL:

```bash
# Start PostgreSQL (schema auto-applied on first run)
docker compose up -d

# Run the ETL pipeline for a game
python -c "
import logging; logging.basicConfig(level=logging.INFO)
from data_pipeline.load_to_db import run_pipeline
run_pipeline(season=2024, gamecode=1)
"
```

### 4. (Optional) Load Multiple Games

```python
from data_pipeline.load_to_db import run_pipeline

# Load first 10 games of the 2024-25 season
for gc in range(1, 11):
    run_pipeline(season=2024, gamecode=gc)
```

---

## 🔧 Configuration

Database credentials are stored in `.env` (gitignored):

```env
POSTGRES_USER=euroleague
POSTGRES_PASSWORD=euroleague_pass_2024
POSTGRES_DB=euroleague_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

---

## 📊 Database Schema

Five normalized tables with proper foreign keys and indexes:

```
teams ─────────────┐
  team_code (PK)   │
  team_name        │
                   │
players ───────────┤
  player_id (PK)   │
  player_name      │
  team_code (FK) ──┘
                        games
                          season + gamecode (PK)
                          home_team (FK) ──→ teams
                          away_team (FK) ──→ teams
                          home_score, away_score
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        play_by_play   player_advanced_stats
          (FK → games)   (FK → games, players, teams)
          playtype         possessions, ts_pct
          period           off_rating, def_rating
          markertime       true_usg_pct, stop_rate
```

---

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│ Euroleague  │────→│  Extractors  │────→│ Transformers │
│   Live API  │     │  (Python)    │     │  (Pandas)    │
└─────────────┘     └──────────────┘     └──────┬───────┘
                                                │
                         ┌──────────────────────┤
                         ▼                      ▼
                  ┌──────────────┐     ┌──────────────┐
                  │  PostgreSQL  │     │  Streamlit   │
                  │  (Docker)    │     │  Dashboard   │
                  └──────────────┘     └──────────────┘
```

The architecture supports two data access paradigms:

1. **Live Mode (Current Default)**
   - The Streamlit dashboard fetches real-time data directly from the Euroleague API.
   - Incurs network latency (~5-10s per team depending on the query).
   - No database or Docker setup required.

2. **DB Mode (Under Development)**
   - Reads pre-computed, persisted data from the PostgreSQL container.
   - Eliminates API rate limits and network latency (sub-second load times).
   - **How to populate:** Run `docker-compose up -d` to spin up postgres, then execute `data_pipeline/load_to_db.py` to run the ETL pipeline.
   - *Note: The database schema is fully updated to support all features, but the dashboard's query layer (`streamlit_app/queries.py`) is currently being migrated to support a global `.env` toggle for DB Mode.*

---

## 📝 Data Source

Data is sourced from the [euroleague-api](https://pypi.org/project/euroleague-api/) Python library, which wraps the official [Euroleague Live API](https://api-live.euroleague.net/swagger/index.html).

- **Boxscore**: Player-level game stats (points, rebounds, assists, etc.)
- **Play-by-Play**: Every game action (shots, subs, fouls, turnovers)
- **Shot Data**: X/Y coordinates, zone, fastbreak/second-chance flags

---

## 📄 License

This project is for educational and personal use. Euroleague data is property of Euroleague Basketball.
