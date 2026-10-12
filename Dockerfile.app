# Lightweight image for the non-Spark app-layer processes (producer, FastAPI backend).
# Which script runs is chosen per-service via `command:` in docker-compose.yml.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
# scripts/(측정·검증·부하 시험 도구)는 git에 올리지 않으므로 이미지에 넣지 않는다 — 넣으면 새로 clone한 저장소에서
# 이 줄이 없는 폴더를 복사하려다 빌드가 실패했다(R-1, 2026-10-12). 로컬에서 쓸 때는 실행 시 마운트한다:
#   docker compose run --rm -v "$PWD/scripts:/app/scripts" producer python scripts/load_test_producer.py
