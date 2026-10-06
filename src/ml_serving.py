"""모델 서빙 + 실시간 모니터링 — 0-8 3단계 (2026-10-06, 사용자 결정: 구축·검증만, 운용하지 않음)

  docker compose --profile serving up -d ml-serving     # 켜기 (평소엔 꺼져 있다)
  docker compose --profile serving stop ml-serving      # 끄기

**무엇을 하나**
1. 서빙: MLflow 레지스트리의 `trajectory-hgb-10s@champion`을 불러와, 10초마다 실시간 Postgres
   `flight_data`에서 항공기별 최근 보고를 읽고 **각 항공기의 다음 10초 위치**를 예측한다.
   같은 순간 기준선(직진 가정) 예측도 함께 만든다.
2. 실시간 채점: 10초 뒤 실제 보고가 들어오면 예측을 채점한다. 정답은 학습 라벨과 같은 방식(앞뒤 보고로
   보간, 보고 공백 30초 이하일 때만)이다. 이 데이터는 정답이 10초 만에 도착하므로 모니터링이 추정이
   아니라 실제 적중률이다.
3. 모니터링: `ML_MONITOR_WINDOW_S`(기본 300초)마다 모델·기준선의 적중률(50/100/200 m)과 오차를
   집계하고, 입력 분포가 학습 데이터와 얼마나 달라졌는지(PSI, 속도·고도)를 계산해
   MLflow 실험 `trajectory-10s-monitoring`에 기록하고 화면용 `ml_monitoring.json`을 쓴다.
4. champion이 바뀌면(재학습으로 승격) 다음 집계 때 새 버전으로 갈아 끼운다.

**학습과 같은 입력을 쓴다.** 특성은 `ml_jobs.origin_features()` — 학습의 `build_samples()`가 쓰는 바로
그 함수다(분리 전후 학습 표본 지문 동일 확인). 차이 하나: 동쪽 미터 환산의 기준 위도가 학습은 구간 시작점,
서빙은 현재 이어진 보고 구간의 시작점이다(cos 값 차이 수 % 미만, 10초 이동량에는 무시할 수준).

**하지 않는 것**: 예측을 DB나 화면 지도에 내보내지 않는다(운용하지 않음). 결과는 MLflow 지표와 집계 JSON뿐.
예측 대기열은 메모리에만 있다. 포트를 열지 않는다.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
import psycopg2
from mlflow.tracking import MlflowClient

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import ml_jobs as J  # noqa: E402

MODEL_NAME = J.JOBS["compare_10s"]["model"]
EXPERIMENT = "trajectory-10s-monitoring"
REPORT_FILE = "ml_monitoring.json"

CYCLE_S = int(os.getenv("ML_SERVING_CYCLE_S", "10"))
WINDOW_S = int(os.getenv("ML_MONITOR_WINDOW_S", "300"))
LOOKBACK_S = 900          # 예측에 쓰는 과거 보고 범위(입력 최대 64칸 ≈ 10.7분보다 길게)
FRESH_S = 60              # 마지막 보고가 이보다 오래된 항공기는 예측하지 않는다(레이더에서 사라짐)
GIVEUP_S = 120            # 이 시간 안에 정답 보고가 안 오면 채점 불가로 센다
NO_CHAMPION_RETRY_S = 60
WINDOWS_KEEP = 48
PSI_FEATURES = ("velocity", "baro_altitude")

DB = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "flightdb"),
    "user": os.getenv("DB_USER", "myuser"),
    "password": os.getenv("DB_PASSWORD", "mypassword"),
}

LIVE_SQL = """
    SELECT DISTINCT ON (icao24, time_position)
           icao24, time_position, latitude, longitude, velocity, true_track, baro_altitude, vertical_rate
    FROM flight_data
    WHERE time_position >= %s
      AND on_ground = false
      AND latitude IS NOT NULL AND longitude IS NOT NULL
    ORDER BY icao24, time_position, timestamp
"""  # Gold와 같은 규칙: 같은 위치 보고(time_position)가 여러 스냅샷에 있으면 가장 이른 것


def now_iso(ts=None):
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ───────────────────────── 기준 분포 (드리프트 비교용) ─────────────────────────

def reference_bins():
    """Gold 궤적(학습 데이터)의 속도·고도 십분위 경계. 실패하면 드리프트 계산만 끈다."""
    try:
        con = J.connect()
        df = con.execute(
            f"SELECT velocity, baro_altitude FROM {J.scan('trajectory')} "
            "WHERE velocity IS NOT NULL AND baro_altitude IS NOT NULL").df()
        dates = J.trajectory_dates(con)
        edges = {f: np.unique(np.quantile(df[f].to_numpy(), np.linspace(0, 1, 11))) for f in PSI_FEATURES}
        ref = {f: np.histogram(df[f].to_numpy(), bins=_open_edges(edges[f]))[0] / len(df) for f in PSI_FEATURES}
        return {"edges": edges, "ref": ref, "rows": int(len(df)), "dates": dates}
    except Exception as e:  # noqa: BLE001
        print(f"SERVING_WARN reference unavailable ({type(e).__name__}) — drift disabled", flush=True)
        return None


def _open_edges(e):
    e = e.astype(float).copy()
    e[0], e[-1] = -np.inf, np.inf
    return e


def psi(values, f, reference):
    """Population Stability Index. <0.1 안정, 0.1~0.25 주의, >0.25 뚜렷한 변화(관례적 기준)."""
    if reference is None or len(values) < 20:
        return None
    live = np.histogram(values, bins=_open_edges(reference["edges"][f]))[0] / len(values)
    ref = reference["ref"][f]
    eps = 1e-4
    live, ref = np.clip(live, eps, None), np.clip(ref, eps, None)
    return round(float(np.sum((live - ref) * np.log(live / ref))), 4)


# ───────────────────────── 실시간 데이터 → 예측 입력 ─────────────────────────

def fetch_live(conn, since_epoch):
    with conn.cursor() as cur:
        cur.execute(LIVE_SQL, (int(since_epoch),))
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["icao24", "time_position", "latitude", "longitude", "velocity",
                                       "true_track", "baro_altitude", "vertical_rate"])


def aircraft_runs(df):
    """항공기별 '현재 이어진 보고 구간'(공백 30초 이하)의 배열들."""
    out = {}
    for icao, g in df.groupby("icao24", sort=False):
        t = g["time_position"].to_numpy(dtype=np.float64)
        cut = np.where(np.diff(t) > J.MAX_RAW_GAP_S)[0]
        s = cut[-1] + 1 if len(cut) else 0  # 마지막 구간의 시작
        g = g.iloc[s:]
        lat = g["latitude"].to_numpy(dtype=np.float64)
        lon = g["longitude"].to_numpy(dtype=np.float64)
        lat0 = lat[0]
        out[icao] = {
            "t": g["time_position"].to_numpy(dtype=np.float64),
            "lat": lat, "lon": lon, "lat0": lat0,
            "n": (lat - lat0) * J.M_PER_DEG,
            "e": (lon - lon[0]) * J.M_PER_DEG * np.cos(np.radians(lat0)),
            "vel": g["velocity"].to_numpy(dtype=np.float64),
            "trk": g["true_track"].to_numpy(dtype=np.float64),
            "alt": g["baro_altitude"].to_numpy(dtype=np.float64),
            "vr": g["vertical_rate"].to_numpy(dtype=np.float64),
        }
    return out


def make_origins(runs, last_origin, now):
    """예측할 항공기: 새 보고가 있고, 최근이며, 입력 1분 이상이 쌓인 경우."""
    rows, meta = [], []
    for icao, r in runs.items():
        t = r["t"]
        j = len(t) - 1
        ti = t[j]
        if ti <= last_origin.get(icao, -1) or now - ti > FRESH_S or ti - t[0] < J.CTX_MIN * J.STEP_S:
            continue
        if np.isnan(r["vel"][j]) or np.isnan(r["trk"][j]):
            continue
        prev_trk = r["trk"][j - 1] if j >= 1 else np.nan
        prev_vel = r["vel"][j - 1] if j >= 1 else np.nan
        f, _, _, (drn, dre) = J.origin_features(
            r["t"], r["n"], r["e"], j, r["vel"][j], r["trk"][j], r["alt"][j], r["vr"][j], prev_trk, prev_vel)
        rows.append(f)
        meta.append({"icao24": icao, "ti": ti, "lat_i": r["lat"][j], "lon_i": r["lon"][j], "lat0": r["lat0"],
                     "dr": (drn, dre), "velocity": r["vel"][j], "baro_altitude": r["alt"][j]})
    return rows, meta


def score_pending(pending, runs, now):
    """정답(ti+10초 위치)이 도착한 예측을 채점. 반환: (채점된 것, 남은 것, 채점 불가 수)."""
    scored, keep, giveup = [], [], 0
    for p in pending:
        target = p["ti"] + J.STEP_S
        r = runs.get(p["icao24"])
        ok = False
        # 예측 시각이 지금의 이어진 구간 안에 있고(그 사이 30초 넘는 공백 없음) 정답 시각 뒤 보고가 왔을 때만.
        # 위치는 절대 좌표로 비교한다 — 15분 조회 창이 밀리면 구간 시작점(미터 기준점)이 바뀌기 때문.
        if r is not None and len(r["t"]) and r["t"][0] <= p["ti"] and r["t"][-1] >= target:
            lat_a = np.interp(target, r["t"], r["lat"])
            lon_a = np.interp(target, r["t"], r["lon"])
            yn = (lat_a - p["lat_i"]) * J.M_PER_DEG
            ye = (lon_a - p["lon_i"]) * J.M_PER_DEG * np.cos(np.radians(p["lat0"]))
            p.update(err_model=float(np.hypot(p["pred"][0] - yn, p["pred"][1] - ye)),
                     err_dr=float(np.hypot(p["dr"][0] - yn, p["dr"][1] - ye)),
                     moved=float(np.hypot(yn, ye)))
            scored.append(p)
            ok = True
        if not ok:
            if now - target > GIVEUP_S:
                giveup += 1
            else:
                keep.append(p)
    return scored, keep, giveup


def summarize(errs, moved):
    errs, moved = np.asarray(errs), np.asarray(moved)
    return J.summary(errs, moved)


# ───────────────────────── 메인 루프 ─────────────────────────

class Server:
    def __init__(self):
        mlflow.set_tracking_uri(J.TRACKING_URI)
        self.client = MlflowClient()
        self.stop = False
        self.model, self.version = None, None
        self.reference = reference_bins()
        self.started = time.time()
        self.run_id = None
        self.windows = []
        self.totals = {"predictions": 0, "scored": 0, "unscorable": 0}
        self.window_scored, self.window_origins, self.window_giveup = [], [], 0
        self.window_idx = 0
        self.last_cycle = {}
        self.notes = []

    # champion 불러오기 / 바뀌었으면 갈아 끼우기
    def load_champion(self):
        try:
            v = self.client.get_model_version_by_alias(MODEL_NAME, J.CHAMPION).version
        except mlflow.exceptions.MlflowException:
            return False
        if v != self.version:
            self.model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@{J.CHAMPION}")
            if self.version is not None:
                self.notes.append(f"{now_iso()} champion v{self.version} → v{v}")
            self.version = v
            print(f"SERVING_MODEL {MODEL_NAME} v{v}", flush=True)
        return True

    def start_mlflow_run(self):
        try:
            exp = J.experiment_id(EXPERIMENT)
            run = self.client.create_run(exp, run_name=f"serving-{now_iso().replace(':', '')}", tags={
                "model": MODEL_NAME, "model_version": str(self.version),
                "cycle_s": str(CYCLE_S), "window_s": str(WINDOW_S),
                "reference_dates": ",".join(self.reference["dates"]) if self.reference else "",
                "mlflow.note.content": "Online scoring of the 10 s champion against dead reckoning, plus input drift (PSI)",
            })
            self.run_id = run.info.run_id
        except Exception as e:  # noqa: BLE001 — 기록 실패가 서빙을 멈추지 않게
            print(f"SERVING_WARN mlflow run not created ({type(e).__name__})", flush=True)

    def close_window(self):
        sc = self.window_scored
        w = {"idx": self.window_idx, "end": now_iso(), "model_version": self.version,
             "scored": len(sc), "unscorable": self.window_giveup, "predictions": len(self.window_origins)}
        if sc:
            moved = [p["moved"] for p in sc]
            w["model"] = summarize([p["err_model"] for p in sc], moved)
            w["baseline"] = summarize([p["err_dr"] for p in sc], moved)
        w["psi"] = {f: psi(np.array([o[f] for o in self.window_origins if o[f] is not None and not np.isnan(o[f])]),
                           f, self.reference) for f in PSI_FEATURES}
        self.windows.append(w)
        self.windows = self.windows[-WINDOWS_KEEP:]
        if self.run_id and sc:
            try:
                metrics = J.flat_metrics({"model": w["model"], "baseline": w["baseline"]})
                metrics["scored"] = w["scored"]
                metrics["unscorable"] = w["unscorable"]
                for f, v in w["psi"].items():
                    if v is not None:
                        metrics[f"psi/{f}"] = v
                for k, v in metrics.items():
                    self.client.log_metric(self.run_id, k, v, step=self.window_idx)
            except Exception as e:  # noqa: BLE001
                print(f"SERVING_WARN mlflow log failed ({type(e).__name__})", flush=True)
        m, b = w.get("model", {}), w.get("baseline", {})
        print(f"SERVING_WINDOW idx={self.window_idx} v{self.version} scored={w['scored']} "
              f"hit100 model={m.get('hit_rate_pct', {}).get('100')} baseline={b.get('hit_rate_pct', {}).get('100')} "
              f"psi={w['psi']}", flush=True)
        self.window_idx += 1
        self.window_scored, self.window_origins, self.window_giveup = [], [], 0

    def write_report(self, status):
        d = os.getenv("ML_REPORT_DIR", "/app/ml_report")
        if not os.path.isdir(d):
            return
        report = {
            "generated_at": now_iso(), "status": status, "started_at": now_iso(self.started),
            "model": {"name": MODEL_NAME, "version": int(self.version) if self.version else None},
            "cycle_s": CYCLE_S, "window_s": WINDOW_S,
            "experiment": EXPERIMENT, "mlflow_run_id": self.run_id,
            "reference": {k: self.reference[k] for k in ("rows", "dates")} if self.reference else None,
            "totals": self.totals, "last_cycle": self.last_cycle, "notes": self.notes[-10:],
            "windows": self.windows,
        }
        path = os.path.join(d, REPORT_FILE)
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=float)
        os.replace(path + ".tmp", path)

    def run(self):
        while not self.load_champion():
            print(f"SERVING_WAIT no '{J.CHAMPION}' alias on {MODEL_NAME} yet", flush=True)
            self.write_report("waiting_for_champion")
            if self._sleep(NO_CHAMPION_RETRY_S):
                return
        self.start_mlflow_run()
        conn = psycopg2.connect(connect_timeout=5, **DB)
        conn.autocommit = True
        pending, last_origin = [], {}
        next_window = time.time() + WINDOW_S
        self.write_report("running")
        while not self.stop:
            t0 = time.time()
            try:
                df = fetch_live(conn, t0 - LOOKBACK_S)
                runs = aircraft_runs(df)
                scored, pending, giveup = score_pending(pending, runs, t0)
                rows, meta = make_origins(runs, last_origin, t0)
                if rows:
                    pred = self.model.predict(pd.DataFrame(rows, columns=J.HGB_FEATURES))
                    for m, pn, pe in zip(meta, pred["north_m"].to_numpy(), pred["east_m"].to_numpy()):
                        m["pred"] = (float(pn), float(pe))
                        last_origin[m["icao24"]] = m["ti"]
                    pending.extend(meta)
                self.window_scored.extend(scored)
                self.window_origins.extend(meta)
                self.window_giveup += giveup
                self.totals["predictions"] += len(meta)
                self.totals["scored"] += len(scored)
                self.totals["unscorable"] += giveup
                self.last_cycle = {"at": now_iso(), "aircraft": len(runs), "predicted": len(meta),
                                   "scored": len(scored), "pending": len(pending),
                                   "latency_ms": round((time.time() - t0) * 1000, 1)}
            except psycopg2.Error as e:
                print(f"SERVING_WARN db ({type(e).__name__}) — reconnecting", flush=True)
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = psycopg2.connect(connect_timeout=5, **DB)
                conn.autocommit = True
            if time.time() >= next_window:
                self.close_window()
                self.load_champion()  # 재학습으로 champion이 바뀌었으면 다음 창부터 새 버전
                next_window += WINDOW_S
            self.write_report("running")
            if self._sleep(max(0.0, CYCLE_S - (time.time() - t0))):
                break
        if self.window_scored:
            self.close_window()
        if self.run_id:
            try:
                self.client.set_terminated(self.run_id)
            except Exception:  # noqa: BLE001
                pass
        self.write_report("stopped")
        print("SERVING_STOPPED", flush=True)

    def _sleep(self, s):
        end = time.time() + s
        while time.time() < end and not self.stop:
            time.sleep(0.5)
        return self.stop


if __name__ == "__main__":
    srv = Server()
    signal.signal(signal.SIGTERM, lambda *_: setattr(srv, "stop", True))
    signal.signal(signal.SIGINT, lambda *_: setattr(srv, "stop", True))
    print(f"SERVING_START model={MODEL_NAME} cycle_s={CYCLE_S} window_s={WINDOW_S}", flush=True)
    srv.run()
