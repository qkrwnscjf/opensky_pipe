import asyncio
import os
import select
import threading

import psycopg2
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text

app = FastAPI()

# 1. CORS 설정 (React 포트 3000번에서 접속 허용)
# 이거 안 하면 React에서 에러 납니다!
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 배포시엔 ["http://localhost:3000"]으로 제한 추천
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. DB 연결 설정 (Docker의 Postgres)
# 환경변수 사용
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypassword")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "flightdb")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

FLIGHTS_QUERY = text("""
    SELECT DISTINCT ON (icao24)
        icao24, callsign, origin_country, latitude, longitude, velocity, geo_altitude as altitude, timestamp, true_track
    FROM flight_data
    WHERE timestamp >= NOW() - INTERVAL '5 minutes'
    ORDER BY icao24, timestamp DESC
""")


def fetch_flights():
    """REST(/flights)와 WebSocket(/ws/flights) 브로드캐스트가 공유하는 조회 로직."""
    with engine.connect() as conn:
        result = conn.execute(FLIGHTS_QUERY)
        return [
            {
                "icao24": row.icao24,
                "callsign": row.callsign,
                "origin_country": row.origin_country,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "velocity": row.velocity,
                "altitude": row.altitude,
                "timestamp": row.timestamp,
                "true_track": row.true_track,
            }
            for row in result
        ]


@app.get("/")
def read_root():
    return {"message": "Flight Tracker API is running!"}


@app.get("/flights")
def get_flights():
    """
    각 항공기별(icao24) 가장 최근 위치 데이터만 가져옵니다.
    최근 5분 이내에 업데이트된 데이터만 필터링합니다.
    """
    return fetch_flights()


# ---------------------------------------------------------------
# Phase 2 (docs/EXPANSION_PLAN.md): Polling → WebSocket 실시간 푸시
# ---------------------------------------------------------------
# src/spark_dual_write.py가 배치를 Postgres에 성공적으로 쓴 직후
# `SELECT pg_notify('flight_update', '')`를 호출한다. 이 프로세스는 별도 스레드에서
# `LISTEN flight_update`로 대기하다가, 알림이 오면 이벤트 루프에 브로드캐스트를 예약한다.
# psycopg2(동기)로 구현한 이유: asyncpg 같은 새 의존성을 추가하지 않기 위함.
_ws_clients: set[WebSocket] = set()


async def _broadcast_flights():
    payload = jsonable_encoder(fetch_flights())
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


def _listen_for_notifications(loop: asyncio.AbstractEventLoop):
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("LISTEN flight_update;")
            print("WS_LISTENER: listening for flight_update notifications")
            while True:
                if not select.select([conn], [], [], 5)[0]:
                    continue
                conn.poll()
                while conn.notifies:
                    conn.notifies.pop()
                    asyncio.run_coroutine_threadsafe(_broadcast_flights(), loop)
        except Exception as e:
            print(f"WS_LISTENER_WARNING: {e} — retrying in 5s")
            import time

            time.sleep(5)


@app.on_event("startup")
def start_listener():
    loop = asyncio.get_event_loop()
    threading.Thread(target=_listen_for_notifications, args=(loop,), daemon=True).start()


@app.websocket("/ws/flights")
async def websocket_flights(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        await websocket.send_json(jsonable_encoder(fetch_flights()))
        while True:
            # 클라이언트가 메시지를 보내진 않지만, 연결 종료(WebSocketDisconnect)를
            # 감지하려면 뭔가를 await 하고 있어야 함.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
