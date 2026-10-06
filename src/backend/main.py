import asyncio
import json
import os
import select
import threading

import psycopg2
import requests
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
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

# 드라이버를 명시한다(psycopg2). requirements.txt가 버전을 고정하지 않아 2026-10-06 재빌드 때
# SQLAlchemy 2.1이 깔렸고, 2.1은 "postgresql://"의 기본 드라이버를 psycopg(v3)로 바꿔 시작이 실패했다.
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

# 2026-09-17: flight_data(append-only, DISTINCT ON으로 매번 재계산) 대신
# flight_current(PK=icao24, spark_dual_write.py가 UPSERT로 채움)를 조회한다.
# 기본키가 "기체당 정확히 1행"을 DB 제약으로 보장하므로 DISTINCT ON이 필요 없다
# — 이전에는 이 보장이 조회 쪽 관례(쿼리를 이렇게 짜야 한다는 약속)에만 있었다.
# ORDER BY는 결과 순서를 안정적으로 만들 뿐 중복 제거와는 무관하다.
# 궤적(TRAIL_QUERY)은 이력이 필요하므로 flight_data를 그대로 쓴다 — 변경 없음.
FLIGHTS_QUERY = text("""
    SELECT icao24, callsign, origin_country, latitude, longitude, velocity, geo_altitude as altitude, timestamp, true_track
    FROM flight_current
    WHERE timestamp >= NOW() - INTERVAL '5 minutes'
    ORDER BY icao24
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


# 특정 기체의 최근 이동 궤적. WHERE 절이 (icao24, timestamp)를 모두 쓰기 때문에
# idx_flight_latest (icao24, timestamp DESC) 인덱스를 그대로 활용한다.
# db_cleanup DAG가 1시간 지난 행을 지우므로 조회 가능한 최대 창도 1시간이다.
# DISTINCT ON (timestamp): OpenSky가 10초 폴링 사이에 동일한 스냅샷(같은 time 값)을 돌려주면
# producer가 같은 레코드를 두 번 보내 완전히 동일한 행이 쌓인다. 궤적에 같은 좌표가 중복
# 정점으로 찍히지 않도록 타임스탬프당 한 점만 남긴다.
TRAIL_QUERY = text("""
    SELECT DISTINCT ON (timestamp) latitude, longitude, timestamp
    FROM flight_data
    WHERE icao24 = :icao24
      AND timestamp >= NOW() - (:minutes * INTERVAL '1 minute')
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
    ORDER BY timestamp ASC
""")


@app.get("/flights/{icao24}/trail")
def get_flight_trail(icao24: str, minutes: int = 30):
    """선택한 항공기의 최근 이동 경로를 시간순으로 반환합니다."""
    minutes = max(1, min(minutes, 60))
    with engine.connect() as conn:
        result = conn.execute(TRAIL_QUERY, {"icao24": icao24, "minutes": minutes})
        return [
            {
                "latitude": row.latitude,
                "longitude": row.longitude,
                "timestamp": row.timestamp,
            }
            for row in result
        ]


# ---------------------------------------------------------------
# 0-8 2단계 (2026-10-06): Model Report의 Retrain 버튼 → 여기 → ml-worker(내부망)
# ---------------------------------------------------------------
# backend는 요청을 넘기기만 한다. 학습·순서·대기·DAG 확인은 ml-worker가 판단한다(src/ml_worker.py).
#
# 【출처 검사】 위 CORS는 모든 출처를 허용한다(allow_origins=["*"]). 그대로면 사용자가 브라우저로 연
# 아무 웹사이트나 이 주소로 학습을 시작시킬 수 있다. 그래서 **학습 시작(POST)에만** 대시보드 출처를
# 검사한다. 브라우저는 다른 출처로 POST할 때 Origin 헤더를 항상 붙이므로, 이 검사로 남의 사이트가
# 보낸 요청은 막힌다. Origin이 없는 요청(같은 컴퓨터의 터미널 도구 등)은 통과한다 — 같은 네트워크의
# 다른 기기가 직접 호출하는 경우는 ml-worker의 "1개씩·120초 대기"로 완화한다(포트 설정은 바꾸지 않기로 함).
ML_WORKER_URL = os.getenv("ML_WORKER_URL", "http://ml-worker:8001")
ML_UI_ORIGINS = {"http://localhost:3000", "http://127.0.0.1:3000"}


def _ml_worker(method, path, payload=None):
    try:
        r = requests.request(method, f"{ML_WORKER_URL}{path}", json=payload, timeout=10)
        return JSONResponse(r.json(), status_code=r.status_code)
    except (requests.RequestException, ValueError):
        return JSONResponse({"error": "ml-worker unreachable"}, status_code=503)


@app.post("/ml/train")
async def ml_train(request: Request):
    origin = request.headers.get("origin")
    if origin is not None and origin not in ML_UI_ORIGINS:
        raise HTTPException(status_code=403, detail="origin not allowed")
    try:
        body = await request.json()
    except ValueError:
        body = {}
    target = body.get("target", "all") if isinstance(body, dict) else "all"
    return await asyncio.to_thread(_ml_worker, "POST", "/jobs", {"target": target})


@app.get("/ml/jobs/latest")
def ml_job_latest():
    return _ml_worker("GET", "/jobs/latest")


@app.get("/ml/jobs/{job_id}")
def ml_job(job_id: str):
    if not job_id.isalnum() or len(job_id) > 32:
        raise HTTPException(status_code=400, detail="bad job id")
    return _ml_worker("GET", f"/jobs/{job_id}")


# Phase 2 (docs/EXPANSION_PLAN.md): Polling → WebSocket 실시간 푸시
# ---------------------------------------------------------------
# src/spark_dual_write.py가 배치를 Postgres에 성공적으로 쓴 직후
# `SELECT pg_notify('flight_update', '')`를 호출한다. 이 프로세스는 별도 스레드에서
# `LISTEN flight_update`로 대기하다가, 알림이 오면 이벤트 루프에 브로드캐스트를 예약한다.
# psycopg2(동기)로 구현한 이유: asyncpg 같은 새 의존성을 추가하지 않기 위함.
_ws_clients: set[WebSocket] = set()


async def _broadcast_flights():
    # A-4 (docs/EXPANSION_PLAN.md): 직렬화는 한 번만 한다.
    #
    # send_json(payload)은 호출될 때마다 payload를 JSON으로 직렬화한다. 모든
    # 클라이언트에게 '같은' 데이터를 보내는데도 클라이언트 수만큼 같은 일을
    # 반복하던 구조였다. 한 번 dumps한 문자열을 send_text로 돌려쓴다.
    payload = json.dumps(jsonable_encoder(fetch_flights()))

    # 순차 await도 함께 고쳤다. 느린 클라이언트 하나가 뒤의 모두를 막고 있었다.
    # gather로 동시에 보내고, 예외는 개별로 회수해 죽은 연결만 정리한다.
    clients = list(_ws_clients)
    if not clients:
        return
    results = await asyncio.gather(
        *(ws.send_text(payload) for ws in clients), return_exceptions=True
    )
    for ws, result in zip(clients, results):
        if isinstance(result, Exception):
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
