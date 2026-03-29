#!/usr/bin/env bash
set -e
pip install -r backend/requirements.txt
uvicorn backend.main:app --host 0.0.0.0 --port "${PORT:-8000}"
