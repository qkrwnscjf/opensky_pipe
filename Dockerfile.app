# Lightweight image for the non-Spark app-layer processes (producer, FastAPI backend).
# Which script runs is chosen per-service via `command:` in docker-compose.yml.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/
