"""MLOps 학습 작업 — MLflow로 기록·등록·승격하고 화면용 리포트를 내보낸다. (0-8, 2026-10-05)

  docker compose run --rm ml                                   # 두 작업 모두 → 리포트 내보내기
  docker compose run --rm ml python src/ml_jobs.py next_report
  docker compose run --rm ml python src/ml_jobs.py compare_10s
  docker compose run --rm ml python src/ml_jobs.py export      # 학습 없이 리포트만

ML 전용 이미지(Dockerfile.ml, Python 3.11)에서 돈다. 예전 두 스크립트를 하나로 합쳤다:
  - ml_train_trajectory.py (airflow 이미지, 다음 보고 시점 HGB) → 작업 `next_report`  (원본 제거)
  - ml_compare_models.py  (10초 뒤 위치, 기준선·HGB·Chronos)   → 작업 `compare_10s` (원본 제거)
데이터 준비·평가 함수는 두 스크립트의 것을 그대로 옮겼다(검증된 로직이라 같은 데이터면 숫자가 같아야 한다).

【MLflow — 서버 없이 쓴다】 (사용자 결정)
추적 서버를 띄우지 않고 MLflow API가 직접 기록한다:
  - 메타데이터(실험·실행·설정값·지표·모델 레지스트리) → SQLite `mlflow.db`, 이름 있는 볼륨 `mlflow_data`
  - 파일(모델·metrics.json·gate.json) → MinIO `s3://flight-data-lake/mlflow/<실험>/`
웹 화면은 평소 꺼 두고, 볼 때만 `mlflow-ui` 서비스를 읽기 전용으로 켠다(docker-compose.yml 참고).

【모델 레지스트리와 자동 승격 관문】 (사용자 결정 — 권장안)
학습한 HGB는 매번 새 버전으로 등록한다. 같은 평가 표본에서 100 m 적중률이
  ① 기준선(직진 가정)보다 높고
  ② 현재 champion을 **같은 표본으로 다시 채점한 값**보다 높을 때만 `champion` 별칭을 옮긴다.
champion의 예전 점수와 비교하지 않는 이유: 데이터가 날마다 늘어 평가 표본이 바뀌므로 옛 점수와 새 점수는
같은 시험이 아니다. 판단 결과와 이유는 모델 버전 태그(`promotion`, `promotion_reason`)와 실행의
`gate.json`에 남는다. Chronos는 학습하지 않는 모델(zero-shot)이라 등록하지 않고 기록만 한다.

**구축하고 검증만 하며, 운용하지 않는다** — DAG에 넣지 않는다.
"""

import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
import sklearn
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import GroupShuffleSplit

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from lake_duckdb import connect, scan  # noqa: E402
from ml_pyfunc import NorthEastModel  # noqa: E402

LAKE_BUCKET = os.getenv("LAKE_BUCKET", "flight-data-lake")
TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "sqlite:////mlflow/mlflow.db")
ARTIFACT_ROOT = os.getenv("MLFLOW_ARTIFACT_ROOT", f"s3://{LAKE_BUCKET}/mlflow")
ML_REPORT_DIR = os.getenv("ML_REPORT_DIR", "/app/ml_report")

JOBS = {
    "next_report": {
        "experiment": "trajectory-next-report",
        "model": "trajectory-hgb-next-report",
        "report_file": "ml_runs.json",
    },
    "compare_10s": {
        "experiment": "trajectory-10s-compare",
        "model": "trajectory-hgb-10s",
        "report_file": "ml_compare.json",
    },
}
GATE_HIT_KEY = "100"  # 승격 관문 지표: 100 m 이내 적중률
CHAMPION = "champion"

M_PER_DEG = 111_320.0
SEED = 42

# ───────────────────────── 작업 1 데이터: 다음 보고 시점 (ml_train_trajectory.py에서 이전) ─────────────────────────

# 한 점 뒤 다음 보고까지 60초를 넘는 경우는 뺀다. 측정상 보고 간격 p90이 20초라
# 60초 초과는 수신 공백이고, 그 사이 기동을 맞히는 것은 다른 문제다.
MAX_NEXT_DT_S = 60

FEATURES = [
    "latitude", "longitude", "baro_altitude", "geo_altitude",
    "velocity", "track_sin", "track_cos", "vertical_rate",
    "dt_s", "calc_speed_mps", "heading_change_deg", "alt_change_m", "velocity_change_mps",
    "next_dt_s", "dr_north_m", "dr_east_m",
]


def load_next_report(con):
    sql = f"""
        SELECT segment_id, latitude, longitude, baro_altitude, geo_altitude,
               velocity, true_track, vertical_rate,
               dt_s, calc_speed_mps, heading_change_deg, alt_change_m, velocity_change_mps,
               next_latitude, next_longitude, next_dt_s
        FROM {scan('trajectory')}
        WHERE next_latitude IS NOT NULL
          AND next_dt_s BETWEEN 1 AND {MAX_NEXT_DT_S}
          AND velocity IS NOT NULL AND true_track IS NOT NULL
    """
    return con.execute(sql).df()


def build_xy_next_report(df):
    lat_rad = np.radians(df["latitude"].to_numpy())
    track_rad = np.radians(df["true_track"].to_numpy())

    # 방향은 각도 그대로 넣으면 359°와 1°가 멀어 보인다. sin/cos로 펼친다.
    df["track_sin"] = np.sin(track_rad)
    df["track_cos"] = np.cos(track_rad)

    # 기준선: 지금 속도·방향으로 next_dt_s초 직진 (방위각은 북쪽에서 시계방향)
    travel = df["velocity"].to_numpy() * df["next_dt_s"].to_numpy()
    df["dr_north_m"] = travel * np.cos(track_rad)
    df["dr_east_m"] = travel * np.sin(track_rad)

    # 정답: 실제 이동량(미터)
    y_north = (df["next_latitude"].to_numpy() - df["latitude"].to_numpy()) * M_PER_DEG
    y_east = (df["next_longitude"].to_numpy() - df["longitude"].to_numpy()) * M_PER_DEG * np.cos(lat_rad)

    X = df[FEATURES].astype("float64").to_numpy()
    return X, y_north, y_east


def errors_m(pred_n, pred_e, y_n, y_e):
    return np.sqrt((pred_n - y_n) ** 2 + (pred_e - y_e) ** 2)


# ───────────────────────── 작업 2 데이터: 10초 뒤 위치 (ml_compare_models.py에서 이전) ─────────────────────────

CHRONOS_MODEL_ID = "amazon/chronos-bolt-tiny"
STEP_S = 10            # 격자 간격 = 예측 거리(10초 뒤)
MAX_RAW_GAP_S = 30     # 이보다 긴 보고 공백은 구간을 끊는다
CTX_MIN = 6            # 최소 입력 6칸(1분) — 세 방법이 같은 표본을 쓰도록 공통 조건
CTX_MAX = 64           # Chronos 입력 최대 64칸(약 10.7분)
MAX_TEST_SAMPLES = 8000  # CPU에서 Chronos 시간을 묶어 두는 상한(넘으면 시드 고정 무작위 추출)
CHRONOS_BATCH = 256


def load_trajectory(con):
    sql = f"""
        SELECT segment_id, time_position, latitude, longitude, baro_altitude,
               velocity, true_track, vertical_rate
        FROM {scan('trajectory')}
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND time_position IS NOT NULL
        ORDER BY segment_id, time_position
    """
    return con.execute(sql).df()


def wrap_deg(a):
    return (a + 180.0) % 360.0 - 180.0


def origin_features(rt, rn, re_, j, vel_i, trk_i, alt_i, vr_i, prev_trk, prev_vel):
    """보고 시각 rt[j]에서 끝나는 10초 격자로 만든 입력 — 학습(build_samples)과 서빙(ml_serving)이 같이 쓴다.

    rt/rn/re_는 한 구간(보고 공백 30초 이하로 이어진 보고들)의 시각·북쪽·동쪽(미터) 배열이다.
    격자점은 모두 rt[j] 이하라 rt[j] 이하 보고들로만 보간된다(미래 정보 없음).
    반환: (HGB 특성 15개, Chronos용 북·동 이동량 수열, 기준선 이동량 (북, 동))
    """
    ti, t_start = rt[j], rt[0]
    L = int(min(CTX_MAX, (ti - t_start) // STEP_S))
    grid = ti - STEP_S * np.arange(L, -1, -1)  # L+1개, 마지막이 ti
    pn = np.interp(grid, rt[: j + 1], rn[: j + 1])
    pe = np.interp(grid, rt[: j + 1], re_[: j + 1])
    dn, de = np.diff(pn), np.diff(pe)  # 10초 이동량 L칸

    rad = np.radians(trk_i)
    drn, dre = vel_i * STEP_S * np.cos(rad), vel_i * STEP_S * np.sin(rad)
    feats = [
        dn[-1], de[-1], dn[-2], de[-2], dn[-3], de[-3],
        vel_i, np.sin(rad), np.cos(rad), alt_i, vr_i,
        wrap_deg(trk_i - prev_trk), vel_i - prev_vel,
        drn, dre,
    ]
    return feats, dn, de, (drn, dre)


def build_samples(df):
    """보고 하나 = 표본 하나. 입력은 t에서 끝나는 10초 격자, 정답은 t+10초 위치."""
    feats, ctx_n, ctx_e, tgt, dr, groups = [], [], [], [], [], []

    for seg_id, g in df.groupby("segment_id", sort=False):
        g = g.drop_duplicates("time_position")
        if len(g) < 3:
            continue
        t = g["time_position"].to_numpy(dtype=np.float64)
        lat = g["latitude"].to_numpy(dtype=np.float64)
        lon = g["longitude"].to_numpy(dtype=np.float64)
        lat0, lon0 = lat[0], lon[0]
        north = (lat - lat0) * M_PER_DEG
        east = (lon - lon0) * M_PER_DEG * np.cos(np.radians(lat0))
        vel = g["velocity"].to_numpy(dtype=np.float64)
        trk = g["true_track"].to_numpy(dtype=np.float64)
        alt = g["baro_altitude"].to_numpy(dtype=np.float64)
        vr = g["vertical_rate"].to_numpy(dtype=np.float64)

        # 보고 공백이 30초를 넘으면 구간을 끊는다
        run_id = np.concatenate([[0], np.cumsum(np.diff(t) > MAX_RAW_GAP_S)])
        for r in np.unique(run_id):
            idx = np.where(run_id == r)[0]
            rt, rn, re_ = t[idx], north[idx], east[idx]
            t_start, t_end = rt[0], rt[-1]
            for j, i in enumerate(idx):
                ti = t[i]
                if ti + STEP_S > t_end or ti - CTX_MIN * STEP_S < t_start:
                    continue
                if np.isnan(vel[i]) or np.isnan(trk[i]):
                    continue
                prev_trk = trk[i - 1] if i - 1 >= idx[0] else np.nan
                prev_vel = vel[i - 1] if i - 1 >= idx[0] else np.nan
                f, dn, de, (drn, dre) = origin_features(
                    rt, rn, re_, j, vel[i], trk[i], alt[i], vr[i], prev_trk, prev_vel)

                # 정답: ti+10초 위치(앞뒤 보고로 보간 — 정답이라 미래 사용 가능)
                yn = np.interp(ti + STEP_S, rt, rn) - north[i]
                ye = np.interp(ti + STEP_S, rt, re_) - east[i]

                feats.append(f)
                ctx_n.append(dn.astype(np.float32))
                ctx_e.append(de.astype(np.float32))
                tgt.append([yn, ye])
                dr.append([drn, dre])
                groups.append(seg_id)

    return (
        np.asarray(feats, dtype=np.float64), ctx_n, ctx_e,
        np.asarray(tgt), np.asarray(dr), np.asarray(groups),
    )


HGB_FEATURES = [
    "move_n_t-1", "move_e_t-1", "move_n_t-2", "move_e_t-2", "move_n_t-3", "move_e_t-3",
    "velocity", "track_sin", "track_cos", "baro_altitude", "vertical_rate",
    "heading_change_deg", "velocity_change_mps", "dr_north_m", "dr_east_m",
]


def dist(pred, true):
    return np.hypot(pred[:, 0] - true[:, 0], pred[:, 1] - true[:, 1])


def run_chronos(ctx_n, ctx_e):
    from importlib.metadata import version

    import torch
    from chronos import BaseChronosPipeline

    torch.manual_seed(SEED)
    pipe = BaseChronosPipeline.from_pretrained(CHRONOS_MODEL_ID, device_map="cpu", torch_dtype=torch.float32)
    model = getattr(pipe, "model", None)
    n_params = int(sum(p.numel() for p in model.parameters())) if model is not None else None

    def predict(series):
        out = []
        for k in range(0, len(series), CHRONOS_BATCH):
            batch = [torch.from_numpy(s) for s in series[k:k + CHRONOS_BATCH]]
            q, _ = pipe.predict_quantiles(context=batch, prediction_length=1, quantile_levels=[0.5])
            out.append(q[:, 0, 0].numpy())
        return np.concatenate(out)

    t0 = time.perf_counter()
    pn, pe = predict(ctx_n), predict(ctx_e)
    elapsed = time.perf_counter() - t0
    info = {
        "model_id": CHRONOS_MODEL_ID,
        "package": f"chronos-forecasting {version('chronos-forecasting')}",
        "torch": torch.__version__,
        "device": "cpu",
        "parameters": n_params,
        "mode": "zero-shot (no training on this data)",
        "input": "10 s displacement series, north and east predicted separately (univariate)",
        "output": "median (quantile 0.5) of the next value",
        "series_predicted": 2 * len(ctx_n),
        "inference_s": round(elapsed, 2),
        "inference_ms_per_series": round(1000 * elapsed / max(2 * len(ctx_n), 1), 3),
        "threads": torch.get_num_threads(),
    }
    return np.column_stack([pn, pe]), info


# ───────────────────────── 평가 공통 ─────────────────────────

# 정확도 — 회귀라 "맞음/틀림"이 없으므로 두 가지로 정의한다(사용자 결정 2026-10-04).
#   적중률: 오차가 기준 거리 이내인 예측의 비율(%) — 주 지표
#   상대 정확도: 1 − (오차 ÷ 실제 이동 거리)의 중앙값(%) — 보조. 이동 거리에 비해 오차가 작으면 높게 나와
#   모델 간 차이가 작아 보인다는 점을 화면에도 적는다.
HIT_THRESHOLDS_M = (50, 100, 200)


def summary(err, actual):
    ratio = err / np.maximum(actual, 1e-9)
    return {
        "mean_m": round(float(np.mean(err)), 1),
        "p50_m": round(float(np.median(err)), 1),
        "p90_m": round(float(np.percentile(err, 90)), 1),
        "hit_rate_pct": {str(t): round(float(np.mean(err <= t)) * 100, 1) for t in HIT_THRESHOLDS_M},
        "relative_accuracy_p50_pct": round((1 - float(np.median(ratio))) * 100, 1),
    }


def hit_rate(pred_df, y_n, y_e, threshold_m):
    err = np.hypot(pred_df["north_m"].to_numpy() - y_n, pred_df["east_m"].to_numpy() - y_e)
    return round(float(np.mean(err <= threshold_m)) * 100, 1)


# ───────────────────────── MLflow 공통 ─────────────────────────

def code_version():
    """학습 코드 파일들의 내용 해시. 컨테이너에는 .git이 없어 커밋 해시 대신 이것으로 코드 버전을 남긴다."""
    h = hashlib.sha256()
    for name in ("ml_jobs.py", "ml_pyfunc.py", "lake_duckdb.py"):
        with open(os.path.join(HERE, name), "rb") as f:
            h.update(f.read())
    return h.hexdigest()[:12]


def trajectory_dates(con):
    return sorted(str(d)[:10] for d in con.execute(
        f"SELECT DISTINCT event_date FROM {scan('trajectory')}").df()["event_date"])


def experiment_id(name):
    exp = mlflow.get_experiment_by_name(name)
    if exp is not None:
        return exp.experiment_id
    return mlflow.create_experiment(name, artifact_location=f"{ARTIFACT_ROOT}/{name}")


def hgb_spec(model_n, model_e, features):
    hp = model_n.get_params()
    return {
        "library": f"scikit-learn {sklearn.__version__}",
        "estimator": "HistGradientBoostingRegressor",
        "targets": ["north_m", "east_m"],
        "features": list(features),
        "params": {k: hp[k] for k in (
            "max_iter", "learning_rate", "max_leaf_nodes", "min_samples_leaf",
            "l2_regularization", "early_stopping", "validation_fraction", "n_iter_no_change",
            "random_state")},
        "n_iter": {"north": int(model_n.n_iter_), "east": int(model_e.n_iter_)},
    }


def fit_hgb(X, y):
    return HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, random_state=SEED).fit(X, y)


def flat_metrics(results):
    """{"hgb": {"p50_m": .., "hit_rate_pct": {"100": ..}}} → {"hgb/p50_m": .., "hgb/hit_100m_pct": ..}"""
    out = {}
    for method, r in results.items():
        for k in ("mean_m", "p50_m", "p90_m", "relative_accuracy_p50_pct"):
            out[f"{method}/{k}"] = r[k]
        for t, v in r["hit_rate_pct"].items():
            out[f"{method}/hit_{t}m_pct"] = v
    return out


def new_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def log_register_gate(job, metrics, params, tags, model, X_te_df, y_n_te, y_e_te, base_hit, cand_hit):
    """실행 기록 → 모델 등록 → 자동 승격 관문. 반환: (MLflow run id, 버전, 관문 결과)."""
    cfg = JOBS[job]
    client = MlflowClient()
    with mlflow.start_run(experiment_id=experiment_id(cfg["experiment"]), run_name=metrics["run_id"]) as run:
        mlflow.log_params(params)
        mlflow.log_metrics(flat_metrics(metrics["results"]))
        mlflow.set_tags(tags)
        mlflow.log_dict(metrics, "metrics.json")
        sample = X_te_df.head(5)
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=model,
            code_paths=[os.path.join(HERE, "ml_pyfunc.py")],
            signature=infer_signature(sample, model.predict(None, sample)),
            # 의존성을 직접 적는다 — 자동 추론에 기대지 않고, 실행 중 외부 조회도 생기지 않게
            pip_requirements=[
                f"mlflow=={mlflow.__version__}", f"scikit-learn=={sklearn.__version__}",
                f"numpy=={np.__version__}", f"pandas=={pd.__version__}",
            ],
        )
        run_id = run.info.run_id

    version = mlflow.register_model(f"runs:/{run_id}/model", cfg["model"]).version

    # 관문: 현재 champion을 같은 평가 표본으로 다시 채점
    champ_version, champ_hit, champ_note = None, None, "no champion yet"
    try:
        champ_version = client.get_model_version_by_alias(cfg["model"], CHAMPION).version
    except mlflow.exceptions.MlflowException:
        pass
    if champ_version is not None:
        try:
            champ_model = mlflow.pyfunc.load_model(f"models:/{cfg['model']}@{CHAMPION}")
            champ_hit = hit_rate(champ_model.predict(X_te_df), y_n_te, y_e_te, float(GATE_HIT_KEY))
            champ_note = f"champion v{champ_version} re-scored on this test set"
        except (KeyError, ValueError, mlflow.exceptions.MlflowException) as e:
            # 특성이 바뀌어 옛 champion을 같은 입력으로 채점할 수 없으면 기준선만으로 판단한다
            champ_note = f"champion v{champ_version} not comparable ({type(e).__name__}) — baseline only"

    beats_base = cand_hit > base_hit
    beats_champ = champ_hit is None or cand_hit > champ_hit
    promote = beats_base and beats_champ
    if not beats_base:
        reason = f"hit@{GATE_HIT_KEY}m {cand_hit}% <= baseline {base_hit}%"
    elif not beats_champ:
        reason = f"hit@{GATE_HIT_KEY}m {cand_hit}% <= champion v{champ_version} {champ_hit}%"
    else:
        reason = f"hit@{GATE_HIT_KEY}m {cand_hit}% > baseline {base_hit}%" + (
            f" and > champion v{champ_version} {champ_hit}%" if champ_hit is not None else " (first champion)")

    gate = {
        "metric": f"hit_rate_within_{GATE_HIT_KEY}m_pct",
        "candidate_version": int(version), "candidate": cand_hit, "baseline": base_hit,
        "champion_version": int(champ_version) if champ_version else None, "champion": champ_hit,
        "champion_note": champ_note, "promoted": promote, "reason": reason,
    }
    client.set_model_version_tag(cfg["model"], version, "promotion", "promoted" if promote else "rejected")
    client.set_model_version_tag(cfg["model"], version, "promotion_reason", reason)
    client.set_tag(run_id, "promotion", "promoted" if promote else "rejected")
    client.log_dict(run_id, gate, "gate.json")
    if promote:
        client.set_registered_model_alias(cfg["model"], CHAMPION, version)
    print(f"ML_GATE[{job}] v{version} {'PROMOTED' if promote else 'REJECTED'} — {reason}")
    return run_id, version, gate


# ───────────────────────── 작업 1: 다음 보고 시점 ─────────────────────────

def job_next_report(con, dates):
    df = load_next_report(con)
    if len(df) < 100:
        raise SystemExit(f"ML_ABORT[next_report]: rows={len(df)} — 학습할 데이터가 너무 적다")
    X, y_n, y_e = build_xy_next_report(df)
    groups = df["segment_id"].to_numpy()
    tr, te = next(GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=SEED).split(X, y_n, groups))
    overlap = len(set(groups[tr]) & set(groups[te]))

    model_n, model_e = fit_hgb(X[tr], y_n[tr]), fit_hgb(X[tr], y_e[tr])
    fi = FEATURES.index
    err_model = errors_m(model_n.predict(X[te]), model_e.predict(X[te]), y_n[te], y_e[te])
    err_base = errors_m(X[te][:, fi("dr_north_m")], X[te][:, fi("dr_east_m")], y_n[te], y_e[te])
    actual = np.hypot(y_n[te], y_e[te])
    m_model, m_base = summary(err_model, actual), summary(err_base, actual)
    print(f"ML_DATA[next_report] rows={len(df)} segments={len(set(groups))} "
          f"train={len(tr)} test={len(te)} overlap={overlap}")
    print(f"ML_RESULT[next_report] baseline {m_base}")
    print(f"ML_RESULT[next_report] hgb      {m_model}")

    spec = hgb_spec(model_n, model_e, FEATURES)
    spec["split"] = f"GroupShuffleSplit by segment_id, test_size=0.2, random_state={SEED}"
    metrics = {
        "run_id": new_run_id(),
        "task": "next position displacement (north/east metres)",
        "rows": int(len(df)), "train_rows": int(len(tr)), "test_rows": int(len(te)),
        "segments": int(len(set(groups))), "segment_overlap": int(overlap),
        "max_next_dt_s": MAX_NEXT_DT_S,
        "features": FEATURES,
        # 화면(ModelReport.js)이 읽는 키 이름은 예전과 같게 유지한다
        "baseline_dead_reckoning": m_base,
        "model_hist_gradient_boosting": m_model,
        "results": {"baseline_dead_reckoning": m_base, "hgb": m_model},
        "model": spec,
        "event_dates": dates,
        "code_version": code_version(),
    }
    params = {f"hgb.{k}": v for k, v in spec["params"].items()}
    params.update({"max_next_dt_s": MAX_NEXT_DT_S, "split": spec["split"], "n_features": len(FEATURES)})
    tags = {
        "task": metrics["task"], "event_dates": ",".join(dates), "code_version": metrics["code_version"],
        "rows": str(len(df)), "segments": str(len(set(groups))), "segment_overlap": str(overlap),
        "mlflow.note.content": "HGB x2 (north/east) vs dead reckoning — error at the next position report",
    }
    return log_register_gate(
        "next_report", metrics, params, tags, NorthEastModel(model_n, model_e, FEATURES),
        pd.DataFrame(X[te], columns=FEATURES), y_n[te], y_e[te],
        m_base["hit_rate_pct"][GATE_HIT_KEY], m_model["hit_rate_pct"][GATE_HIT_KEY],
    )


# ───────────────────────── 작업 2: 10초 뒤 위치, 3개 모델 비교 ─────────────────────────

def job_compare_10s(con, dates):
    df = load_trajectory(con)
    X, ctx_n, ctx_e, Y, DR, groups = build_samples(df)
    if len(X) < 500:
        raise SystemExit(f"ML_ABORT[compare_10s]: samples={len(X)} — 비교할 표본이 너무 적다")
    tr, te = next(GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=SEED).split(X, Y[:, 0], groups))
    overlap = len(set(groups[tr]) & set(groups[te]))
    te_all = len(te)
    if len(te) > MAX_TEST_SAMPLES:
        te = np.sort(np.random.default_rng(SEED).choice(te, MAX_TEST_SAMPLES, replace=False))
    print(f"ML_DATA[compare_10s] rows={len(df)} samples={len(X)} segments={len(set(groups))} "
          f"train={len(tr)} test={len(te)} overlap={overlap}")

    err_dr = dist(DR[te], Y[te])
    hgb_n, hgb_e = fit_hgb(X[tr], Y[tr, 0]), fit_hgb(X[tr], Y[tr, 1])
    err_hgb = dist(np.column_stack([hgb_n.predict(X[te]), hgb_e.predict(X[te])]), Y[te])
    pred_c, chronos_info = run_chronos([ctx_n[i] for i in te], [ctx_e[i] for i in te])
    err_c = dist(pred_c, Y[te])

    actual = np.hypot(Y[te, 0], Y[te, 1])
    results = {
        "baseline_dead_reckoning": summary(err_dr, actual),
        "hgb": summary(err_hgb, actual),
        "chronos_bolt_tiny": summary(err_c, actual),
    }
    for k, v in results.items():
        print(f"ML_RESULT[compare_10s] {k:24s} {v}")

    metrics = {
        "run_id": new_run_id(),
        "task": f"position {STEP_S} s after each position report (north/east metres)",
        "framing": {
            "step_s": STEP_S, "horizon_s": STEP_S, "max_raw_gap_s": MAX_RAW_GAP_S,
            "context_min_steps": CTX_MIN, "context_max_steps": CTX_MAX,
            "origin": "every position report; context grid ends at the report (no future leakage)",
        },
        "event_dates": dates,
        "rows": int(len(df)), "samples": int(len(X)),
        "train_samples": int(len(tr)), "test_samples": int(len(te)), "test_samples_before_cap": int(te_all),
        "segments": int(len(set(groups))), "segment_overlap": int(overlap),
        "results": results,
        "models": {
            "baseline_dead_reckoning": {"description": "hold reported speed and heading for 10 s"},
            "hgb": hgb_spec(hgb_n, hgb_e, HGB_FEATURES),
            "chronos_bolt_tiny": chronos_info,
        },
        "code_version": code_version(),
    }
    params = {f"hgb.{k}": v for k, v in metrics["models"]["hgb"]["params"].items()}
    params.update({f"framing.{k}": v for k, v in metrics["framing"].items() if k != "origin"})
    params.update({"chronos.model_id": CHRONOS_MODEL_ID, "test_cap": MAX_TEST_SAMPLES})
    tags = {
        "task": metrics["task"], "event_dates": ",".join(dates), "code_version": metrics["code_version"],
        "samples": str(len(X)), "segments": str(len(set(groups))), "segment_overlap": str(overlap),
        "chronos.package": chronos_info["package"],
        "chronos.inference_ms_per_series": str(chronos_info["inference_ms_per_series"]),
        "mlflow.note.content": "Dead reckoning vs HGB vs zero-shot Chronos-Bolt tiny — position 10 s after each report",
    }
    return log_register_gate(
        "compare_10s", metrics, params, tags, NorthEastModel(hgb_n, hgb_e, HGB_FEATURES),
        pd.DataFrame(X[te], columns=HGB_FEATURES), Y[te, 0], Y[te, 1],
        results["baseline_dead_reckoning"]["hit_rate_pct"][GATE_HIT_KEY], results["hgb"]["hit_rate_pct"][GATE_HIT_KEY],
    )


# ───────────────────────── 화면용 리포트 ─────────────────────────

def export_report():
    """MLflow 기록을 화면이 읽는 정적 JSON(ml_runs.json, ml_compare.json)으로 내보낸다.

    실행마다 저장한 metrics.json(예전 화면이 읽던 것과 같은 모양)에 MLflow 정보(실행 ID, 모델 버전,
    별칭, 승격 판단)를 붙인다. 원본은 MLflow(mlflow.db + MinIO)이고, 이 파일은 언제든 다시 만들 수 있다.
    """
    if not os.path.isdir(ML_REPORT_DIR):
        print(f"ML_REPORT_SKIPPED dir_missing={ML_REPORT_DIR}")
        return
    client = MlflowClient()
    for job, cfg in JOBS.items():
        exp = mlflow.get_experiment_by_name(cfg["experiment"])
        runs_out, champion = [], None
        if exp is not None:
            versions = {}
            try:
                for mv in client.search_model_versions(f"name='{cfg['model']}'"):
                    versions[mv.run_id] = mv
                champion = client.get_model_version_by_alias(cfg["model"], CHAMPION).version
            except mlflow.exceptions.MlflowException:
                pass
            for r in client.search_runs([exp.experiment_id], order_by=["attributes.start_time ASC"], max_results=500):
                try:
                    m = mlflow.artifacts.load_dict(f"{r.info.artifact_uri}/metrics.json")
                except Exception:  # noqa: BLE001 — metrics.json이 없는 실행(중간에 멈춘 실행)은 건너뛴다
                    continue
                try:
                    gate = mlflow.artifacts.load_dict(f"{r.info.artifact_uri}/gate.json")
                except Exception:  # noqa: BLE001
                    gate = None
                mv = versions.get(r.info.run_id)
                m["mlflow"] = {
                    "run_id": r.info.run_id,
                    "experiment": cfg["experiment"],
                    "registered_model": cfg["model"],
                    "version": int(mv.version) if mv else None,
                    "aliases": list(getattr(mv, "aliases", []) or []) if mv else [],
                    "promotion": (mv.tags or {}).get("promotion") if mv else None,
                    "promotion_reason": (mv.tags or {}).get("promotion_reason") if mv else None,
                    "gate": gate,
                }
                runs_out.append(m)
        report = {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "mlflow",
            "model_prefix": f"MLflow experiment '{cfg['experiment']}'",
            "registered_model": cfg["model"],
            "champion_version": int(champion) if champion else None,
            "runs": runs_out,
        }
        path = os.path.join(ML_REPORT_DIR, cfg["report_file"])
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        os.replace(tmp, path)
        print(f"ML_REPORT_EXPORTED[{job}] runs={len(runs_out)} champion=v{champion} file={cfg['report_file']}")


# 1-A M-1 (2026-10-10, 사용자 채택): 학습이 끝날 때마다 mlflow.db를 MinIO에 백업한다.
# 영구 저장소가 MinIO와 mlflow_data 두 개인데, mlflow_data만 지워지면(down -v, volume prune --all)
# MinIO의 모델 파일은 남아도 "어느 실행의 어떤 버전인지"라는 기록이 사라져 짝이 끊긴다. 백업이 MinIO에
# 있으면 MinIO 하나로 둘 다 되살릴 수 있다. 파일을 그대로 복사하지 않고 SQLite 백업 API로 사본을 만든다
# — 서빙이 동시에 쓰고 있어도 일관된 스냅샷이 된다. 수 MB 파일이라 메모리·시간 부담은 미미하고,
# 디스크가 계속 늘지 않도록 최근 MLFLOW_BACKUP_KEEP개만 남긴다.
MLFLOW_BACKUP_PREFIX = "mlflow_backups"
MLFLOW_BACKUP_KEEP = int(os.getenv("MLFLOW_BACKUP_KEEP", "5"))


def backup_mlflow_db():
    import sqlite3
    import tempfile

    import boto3

    if not TRACKING_URI.startswith("sqlite:///"):
        print("MLFLOW_BACKUP_SKIPPED not a sqlite tracking store")
        return
    src_path = TRACKING_URI[len("sqlite:///"):]
    if not os.path.exists(src_path):
        print("MLFLOW_BACKUP_SKIPPED db file missing")
        return
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )
    with tempfile.TemporaryDirectory() as d:
        snap = os.path.join(d, "mlflow.db")
        src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
        dst = sqlite3.connect(snap)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        key = f"{MLFLOW_BACKUP_PREFIX}/mlflow-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.db"
        s3.upload_file(snap, LAKE_BUCKET, key)
        size = os.path.getsize(snap)
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=LAKE_BUCKET, Prefix=f"{MLFLOW_BACKUP_PREFIX}/").get("Contents", []))
    old = keys[:-MLFLOW_BACKUP_KEEP] if len(keys) > MLFLOW_BACKUP_KEEP else []
    for k in old:
        s3.delete_object(Bucket=LAKE_BUCKET, Key=k)
    print(f"MLFLOW_BACKUP_SAVED key={key} bytes={size} kept={len(keys) - len(old)} removed={len(old)}")


def main(argv):
    target = argv[1] if len(argv) > 1 else "all"
    if target not in ("all", "next_report", "compare_10s", "export"):
        raise SystemExit("usage: ml_jobs.py [all|next_report|compare_10s|export]")
    mlflow.set_tracking_uri(TRACKING_URI)
    if target != "export":
        con = connect()
        dates = trajectory_dates(con)
        t0 = time.perf_counter()
        if target in ("all", "next_report"):
            job_next_report(con, dates)
        if target in ("all", "compare_10s"):
            job_compare_10s(con, dates)
        print(f"ML_JOBS_DONE target={target} seconds={time.perf_counter() - t0:.1f}")
    export_report()
    if target != "export":
        try:
            backup_mlflow_db()
        except Exception as e:  # noqa: BLE001 — 백업 실패가 학습 성공을 뒤집지 않게
            print(f"MLFLOW_BACKUP_FAILED {type(e).__name__}")


if __name__ == "__main__":
    main(sys.argv)
