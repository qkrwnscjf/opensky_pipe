"""ML 재학습 작업자 — 화면의 Retrain 버튼이 보낸 요청을 받아 학습을 돌린다. (0-8 2단계, 2026-10-06)

  [Retrain] → backend POST /ml/train → (내부망) ml-worker POST /jobs → python src/ml_jobs.py all

상주하지만 대기 중에는 가볍다: 표준 라이브러리 HTTP 서버만 떠 있고, PyTorch·scikit-learn·MLflow는
학습을 **별도 프로세스**(`ml_jobs.py`)로 실행할 때만 올라왔다가 끝나면 메모리와 함께 사라진다.
새 의존성을 늘리지 않으려고 웹 프레임워크 없이 `http.server`를 쓴다.

**포트를 공개하지 않는다.** compose 내부망에서 backend만 부른다(`ml-worker:8001`).

【안전장치】
- 한 번에 작업 1개. 실행 중 요청은 409.
- 작업이 끝난 뒤 `ML_WORKER_COOLDOWN_S`(기본 120초) 동안은 새 요청을 429로 거절 — backend 포트가
  모든 인터페이스에 열려 있어 같은 네트워크에서 반복 호출될 수 있기 때문(포트 설정은 바꾸지 않기로 함).
- **DAG가 돌고 있으면 409.** Hadoop 카탈로그는 쓰는 쪽이 하나라는 전제라, Silver/Gold가 쓰는 도중에 읽으면
  어긋난 상태를 읽을 수 있다. Airflow REST API는 켜지 않고(8080이 모든 인터페이스에 열려 있어 접근 경로가
  하나 더 생김) Airflow 메타DB의 `dag_run`을 읽기 전용으로 조회한다. 메타DB에 닿지 못하면 Airflow 자체가
  내려가 있어 DAG도 돌 수 없으므로 실행을 허용하고, 그 사실을 작업 기록(`dag_check`)에 남긴다.
- 작업 시간 상한 `ML_WORKER_TIMEOUT_S`(기본 900초) — 넘으면 프로세스를 끝낸다.

작업 기록은 메모리에만 있다(최근 20개). 학습 결과의 원본은 MLflow이고, 이것은 진행 상태 표시용이다.
"""

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.getenv("ML_WORKER_PORT", "8001"))
COOLDOWN_S = int(os.getenv("ML_WORKER_COOLDOWN_S", "120"))
TIMEOUT_S = int(os.getenv("ML_WORKER_TIMEOUT_S", "900"))
DAG_ID = os.getenv("ML_WORKER_DAG_ID", "flight_lakehouse_etl")
AIRFLOW_DB = {
    "host": os.getenv("AIRFLOW_DB_HOST", "airflow-postgres"),
    "port": int(os.getenv("AIRFLOW_DB_PORT", "5432")),
    "dbname": os.getenv("AIRFLOW_DB_NAME", "airflow"),
    "user": os.getenv("AIRFLOW_DB_USER", "airflow"),
    "password": os.getenv("AIRFLOW_DB_PASSWORD", "airflow"),
}
TARGETS = ("all", "next_report", "compare_10s")
LOG_KEEP = 300      # 작업당 보관하는 출력 줄 수
LOG_TAIL = 40       # 응답에 싣는 마지막 줄 수
JOBS_KEEP = 20

_lock = threading.Lock()
_jobs = OrderedDict()   # id -> job dict (최근 JOBS_KEEP개)
_current_id = None
_last_end = 0.0


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def dag_check():
    """(active: bool|None, note: str). None은 확인 불가."""
    try:
        import psycopg2  # 메타DB 조회 때만 불러온다

        conn = psycopg2.connect(connect_timeout=3, **AIRFLOW_DB)
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM dag_run WHERE dag_id = %s AND state IN ('running', 'queued')",
                    (DAG_ID,),
                )
                n = cur.fetchone()[0]
        finally:
            conn.close()
        return n > 0, f"{n} active run(s) of {DAG_ID}"
    except Exception as e:  # noqa: BLE001
        return None, f"airflow metadata unreachable ({type(e).__name__}) — Airflow is down, so no DAG can be writing"


def public(job):
    out = {k: v for k, v in job.items() if k not in ("log", "proc")}
    out["log_tail"] = list(job["log"])[-LOG_TAIL:]
    if job["state"] == "running" and job.get("started_ts"):
        out["elapsed_s"] = round(time.time() - job["started_ts"], 1)
    out.pop("started_ts", None)
    return out


def run_job(job):
    global _current_id, _last_end
    cmd = [sys.executable, os.path.join(HERE, "ml_jobs.py"), job["target"]]
    try:
        proc = subprocess.Popen(
            cmd, cwd=os.path.dirname(HERE), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        with _lock:
            job["proc"] = proc
            job["state"] = "running"
            job["started_at"] = now_iso()
            job["started_ts"] = time.time()
        killer = threading.Timer(TIMEOUT_S, lambda: proc.kill())
        killer.start()
        for line in proc.stdout:
            line = line.rstrip("\n")
            with _lock:
                job["log"].append(line)
                if line.startswith("ML_GATE["):
                    job["gates"].append(line)
        code = proc.wait()
        killer.cancel()
        with _lock:
            job["exit_code"] = code
            if code == 0:
                job["state"] = "succeeded"
            else:
                job["state"] = "failed"
                job["error"] = "timeout" if code < 0 and time.time() - job["started_ts"] >= TIMEOUT_S else f"exit code {code}"
    except Exception as e:  # noqa: BLE001
        with _lock:
            job["state"] = "failed"
            job["error"] = type(e).__name__
    finally:
        with _lock:
            job["ended_at"] = now_iso()
            if job.get("started_ts"):
                job["duration_s"] = round(time.time() - job["started_ts"], 1)
            job.pop("proc", None)
            _current_id = None
            _last_end = time.time()
        print(f"ML_WORKER_JOB_END id={job['id']} state={job['state']} duration_s={job.get('duration_s')}", flush=True)


def submit(target):
    """반환: (HTTP 상태, 응답 dict)."""
    global _current_id
    if target not in TARGETS:
        return 400, {"error": f"target must be one of {list(TARGETS)}"}
    with _lock:
        if _current_id is not None:
            return 409, {"error": "a training job is already running", "job": public(_jobs[_current_id])}
        wait = COOLDOWN_S - (time.time() - _last_end)
        if _last_end and wait > 0:
            return 429, {"error": "cooldown after the previous job", "retry_after_s": int(wait) + 1}
    active, note = dag_check()
    if active:
        return 409, {"error": "the lakehouse DAG is running — retry after it finishes", "dag_check": note}
    with _lock:
        if _current_id is not None:  # 확인하는 사이 다른 요청이 들어온 경우
            return 409, {"error": "a training job is already running", "job": public(_jobs[_current_id])}
        job = {
            "id": uuid.uuid4().hex[:12], "target": target, "state": "queued",
            "created_at": now_iso(), "started_at": None, "ended_at": None, "duration_s": None,
            "exit_code": None, "error": None, "dag_check": note, "gates": [],
            "log": deque(maxlen=LOG_KEEP),
        }
        _jobs[job["id"]] = job
        while len(_jobs) > JOBS_KEEP:
            _jobs.popitem(last=False)
        _current_id = job["id"]
    threading.Thread(target=run_job, args=(job,), daemon=True).start()
    print(f"ML_WORKER_JOB_START id={job['id']} target={target} dag_check={note!r}", flush=True)
    with _lock:
        return 202, public(job)


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        path = self.path.split("?", 1)[0].rstrip("/")
        with _lock:
            if path == "/health":
                return self._send(200, {"ok": True, "busy": _current_id is not None})
            if path == "/jobs/latest":
                if not _jobs:
                    return self._send(404, {"error": "no job yet"})
                return self._send(200, public(next(reversed(_jobs.values()))))
            if path.startswith("/jobs/"):
                job = _jobs.get(path[len("/jobs/"):])
                return self._send(200, public(job)) if job else self._send(404, {"error": "unknown job"})
        return self._send(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path.split("?", 1)[0].rstrip("/") != "/jobs":
            return self._send(404, {"error": "not found"})
        try:
            length = min(int(self.headers.get("Content-Length") or 0), 4096)
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            return self._send(400, {"error": "invalid JSON"})
        status, payload = submit(str(body.get("target", "all")))
        return self._send(status, payload)

    def log_message(self, fmt, *args):  # 요청마다 찍히는 접속 로그는 끈다(상태 조회가 2초마다 온다)
        pass


if __name__ == "__main__":
    print(f"ML_WORKER_READY port={PORT} cooldown_s={COOLDOWN_S} timeout_s={TIMEOUT_S}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
