"""
FastAPI entry-point for the Euroleague Stats API.

Run with:
    uvicorn backend.main:app --reload --port 8000
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.endpoints.season import router as season_router
from backend.api.endpoints.teams import router as teams_router
from backend.api.endpoints.predictor import router as predictor_router

app = FastAPI(
    title="Euroleague Stats API",
    version="0.1.0",
)

# ── CORS — allow local dev + production frontend ────────────────────
_allowed_origins = [
    "http://localhost:5173",   # Vite default
    "http://localhost:3000",   # Nuxt default
]

_frontend_url = os.getenv("FRONTEND_URL")  # e.g. https://my-euroleague-app.vercel.app
if _frontend_url:
    _allowed_origins.append(_frontend_url.rstrip("/"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────────
app.include_router(season_router)
app.include_router(teams_router)
app.include_router(predictor_router)
