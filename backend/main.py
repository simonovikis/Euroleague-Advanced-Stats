"""
FastAPI entry-point for the Euroleague Stats API.

Run with:
    uvicorn backend.main:app --reload --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.endpoints.season import router as season_router
from backend.api.endpoints.teams import router as teams_router

app = FastAPI(
    title="Euroleague Stats API",
    version="0.1.0",
)

# ── CORS — allow local Vue / Nuxt dev servers ───────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite default
        "http://localhost:3000",   # Nuxt default
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────────
app.include_router(season_router)
app.include_router(teams_router)
