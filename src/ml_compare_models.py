"""기준선·HGB·Chronos-Bolt tiny를 같은 조건으로 비교한다. (2026-10-04, 사용자 결정)

  docker compose run --rm ml                                        # 비교 → 저장 → 리포트 내보내기
  docker compose run --rm ml python src/ml_compare_models.py --export-only

전용 이미지(Dockerfile.ml, Python 3.11 + PyTorch CPU)에서 돈다. airflow 이미지는 Python 3.8이라
PyTorch·Chronos를 쓸 수 없다. 이 이미지에는 pyspark가 없으므로 lakehouse_common(맨 위에서
pyspark를 import)을 쓰지 않고, MinIO 접속은 여기서 직접 만든다. 레이크 읽기는 lake_duckdb를 쓴다.
**구축하고 비교만 하며, 운용하지 않는다** — DAG에 넣지 않는다.

【왜 평가 조건을 새로 정했나】
Chronos 같은 시계열 사전학습 모델은 "일정 간격"의 수열을 전제로 한다. 우리 위치 보고는 1~60초로
들쭉날쭉하다. 그래서 세 방법 모두 다음 조건 하나로 다시 평가한다:

  각 위치 보고 시각 t에서, t+10초의 위치를 맞힌다.
  입력은 t부터 거꾸로 10초 간격으로 다시 맞춘 위치 수열(최대 64칸 ≈ 10.7분, 최소 6칸 = 1분).

ml_train_trajectory.py의 "다음 보고 시점" 오차(57.8m 등)와는 조건이 달라 숫자를 직접 비교할 수 없다.

【미래 정보가 섞이지 않게】
10초 격자를 t "이후"까지 선형 보간으로 만들면, 격자점이 t 뒤의 보고를 이용해 계산되어 정답 일부가
입력에 섞인다(직전 이동량을 쓰는 HGB·Chronos만 유리해진다). 그래서 격자를 **보고 시각 t에서 끝나도록
거꾸로** 만든다. t는 실제 보고 지점이므로 t 이전 격자점은 t 이전 보고들로만 보간된다.
정답(t+10초 위치)만 t 앞뒤 보고로 보간한다 — 정답이니 미래를 써도 된다.

또 보고 간격이 30초를 넘는 구간은 끊는다(MAX_RAW_GAP_S). 긴 공백을 직선으로 보간하면 직진 가정
기준선에 유리한 가짜 직선 궤적이 생기기 때문이다.

【세 방법】
- 기준선(dead reckoning): t의 보고 속도·방향으로 10초 직진.
- HGB: scikit-learn HistGradientBoostingRegressor 2개(북·동). 직전 10초 이동량 3칸, 속도·방향·고도·
  상승률, 방향·속도 변화, 기준선 이동량을 입력. 학습 비행으로 학습, 평가 비행으로 채점.
- Chronos-Bolt tiny: 학습 없이(zero-shot) 10초 이동량 수열(북·동 각각, 단변량)의 다음 값을 예측.
  중앙값(분위 0.5)을 예측값으로 쓴다.

평가는 세 방법 모두 **같은 평가 표본**(비행 단위로 나눈 20%)에서 한다.
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
import numpy as np
import sklearn
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import GroupShuffleSplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake_duckdb import connect, scan  # noqa: E402

LAKE_BUCKET = os.getenv("LAKE_BUCKET", "flight-data-lake")
COMPARE_PREFIX = "ml/model_comparison"
ML_REPORT_DIR = os.getenv("ML_REPORT_DIR", "/app/ml_report")
ML_COMPARE_FILE = "ml_compare.json"

CHRONOS_MODEL_ID = "amazon/chronos-bolt-tiny"

M_PER_DEG = 111_320.0
STEP_S = 10            # 격자 간격 = 예측 거리(10초 뒤)
MAX_RAW_GAP_S = 30     # 이보다 긴 보고 공백은 구간을 끊는다
CTX_MIN = 6            # 최소 입력 6칸(1분) — 세 방법이 같은 표본을 쓰도록 공통 조건
CTX_MAX = 64           # Chronos 입력 최대 64칸(약 10.7분)
MAX_TEST_SAMPLES = 8000  # CPU에서 Chronos 시간을 묶어 두는 상한(넘으면 시드 고정 무작위 추출)
CHRONOS_BATCH = 256
SEED = 42


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )


def load(con):
    sql = f"""
        SELECT segment_id, time_position, latitude, longitude, baro_altitude,
               velocity, true_track, vertical_rate
        FROM {scan('trajectory')}
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND time_position IS NOT NULL
        ORDER BY segment_id, time_position
    """
    df = con.execute(sql).df()
    dates = sorted(str(d)[:10] for d in con.execute(
        f"SELECT DISTINCT event_date FROM {scan('trajectory')}").df()["event_date"])
    return df, dates


def wrap_deg(a):
    return (a + 180.0) % 360.0 - 180.0


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
                L = int(min(CTX_MAX, (ti - t_start) // STEP_S))
                grid = ti - STEP_S * np.arange(L, -1, -1)  # L+1개, 마지막이 ti
                # 격자점은 모두 ti 이하 → ti 이하 보고들로만 보간된다(ti 자신이 실제 보고점)
                pn = np.interp(grid, rt[: j + 1], rn[: j + 1])
                pe = np.interp(grid, rt[: j + 1], re_[: j + 1])
                dn, de = np.diff(pn), np.diff(pe)  # 10초 이동량 L칸

                # 정답: ti+10초 위치(앞뒤 보고로 보간 — 정답이라 미래 사용 가능)
                yn = np.interp(ti + STEP_S, rt, rn) - north[i]
                ye = np.interp(ti + STEP_S, rt, re_) - east[i]

                rad = np.radians(trk[i])
                drn, dre = vel[i] * STEP_S * np.cos(rad), vel[i] * STEP_S * np.sin(rad)
                prev_trk = trk[i - 1] if i - 1 >= idx[0] else np.nan
                prev_vel = vel[i - 1] if i - 1 >= idx[0] else np.nan

                feats.append([
                    dn[-1], de[-1], dn[-2], de[-2], dn[-3], de[-3],
                    vel[i], np.sin(rad), np.cos(rad), alt[i], vr[i],
                    wrap_deg(trk[i] - prev_trk), vel[i] - prev_vel,
                    drn, dre,
                ])
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


def main():
    con = connect()
    df, dates = load(con)
    X, ctx_n, ctx_e, Y, DR, groups = build_samples(df)
    if len(X) < 500:
        raise SystemExit(f"COMPARE_ABORT: samples={len(X)} — 비교할 표본이 너무 적다")

    split = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=SEED)
    tr, te = next(split.split(X, Y[:, 0], groups))
    overlap = len(set(groups[tr]) & set(groups[te]))
    te_all = len(te)
    if len(te) > MAX_TEST_SAMPLES:
        te = np.sort(np.random.default_rng(SEED).choice(te, MAX_TEST_SAMPLES, replace=False))
    print(
        f"COMPARE_DATA rows={len(df)} samples={len(X)} segments={len(set(groups))} "
        f"train={len(tr)} test={len(te)} (of {te_all}) segment_overlap={overlap} dates={','.join(dates)}"
    )

    # 1) 기준선
    err_dr = dist(DR[te], Y[te])

    # 2) HGB — ml_train_trajectory.py와 같은 설정
    params = dict(max_iter=300, learning_rate=0.05, random_state=SEED)
    hgb_n = HistGradientBoostingRegressor(**params).fit(X[tr], Y[tr, 0])
    hgb_e = HistGradientBoostingRegressor(**params).fit(X[tr], Y[tr, 1])
    err_hgb = dist(np.column_stack([hgb_n.predict(X[te]), hgb_e.predict(X[te])]), Y[te])

    # 3) Chronos-Bolt tiny (zero-shot)
    pred_c, chronos_info = run_chronos([ctx_n[i] for i in te], [ctx_e[i] for i in te])
    err_c = dist(pred_c, Y[te])

    actual = np.hypot(Y[te, 0], Y[te, 1])  # 실제 10초 이동 거리 — 상대 정확도의 분모
    results = {
        "baseline_dead_reckoning": summary(err_dr, actual),
        "hgb": summary(err_hgb, actual),
        "chronos_bolt_tiny": summary(err_c, actual),
    }
    for k, v in results.items():
        print(f"COMPARE_RESULT {k:24s} {v}")

    hp = hgb_n.get_params()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    metrics = {
        "run_id": run_id,
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
            "hgb": {
                "library": f"scikit-learn {sklearn.__version__}",
                "estimator": "HistGradientBoostingRegressor",
                "targets": ["north_m", "east_m"],
                "features": HGB_FEATURES,
                "params": {k: hp[k] for k in (
                    "max_iter", "learning_rate", "max_leaf_nodes", "min_samples_leaf",
                    "l2_regularization", "early_stopping", "validation_fraction", "n_iter_no_change",
                    "random_state")},
                "n_iter": {"north": int(hgb_n.n_iter_), "east": int(hgb_e.n_iter_)},
            },
            "chronos_bolt_tiny": chronos_info,
        },
    }

    s3 = s3_client()
    key = f"{COMPARE_PREFIX}/{run_id}/metrics.json"
    s3.put_object(Bucket=LAKE_BUCKET, Key=key, Body=json.dumps(metrics, indent=2).encode("utf-8"))
    print(f"COMPARE_SAVED s3://{LAKE_BUCKET}/{key}")
    print(
        f"COMPARE_DONE run_id={run_id} chronos_inference_s={chronos_info['inference_s']} "
        f"({chronos_info['inference_ms_per_series']} ms/series)"
    )


def export_report(s3):
    """MinIO의 모든 비교 실행을 모아 화면용 ml_compare.json으로 쓴다."""
    if not os.path.isdir(ML_REPORT_DIR):
        print(f"COMPARE_REPORT_SKIPPED dir_missing={ML_REPORT_DIR}")
        return
    runs = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LAKE_BUCKET, Prefix=f"{COMPARE_PREFIX}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/metrics.json"):
                runs.append(json.loads(s3.get_object(Bucket=LAKE_BUCKET, Key=obj["Key"])["Body"].read()))
    runs.sort(key=lambda r: r["run_id"])
    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "model_prefix": COMPARE_PREFIX,
        "runs": runs,
    }
    path = os.path.join(ML_REPORT_DIR, ML_COMPARE_FILE)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    os.replace(tmp, path)
    print(f"COMPARE_REPORT_EXPORTED runs={len(runs)} file={ML_COMPARE_FILE}")


if __name__ == "__main__":
    if "--export-only" in sys.argv[1:]:
        export_report(s3_client())
    else:
        main()
        export_report(s3_client())
