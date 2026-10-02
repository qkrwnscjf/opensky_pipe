"""Gold 궤적으로 '다음 위치' 예측 모델을 학습·평가·저장한다. (0-6d)

  python src/ml_train_trajectory.py

**구축하고 작동만 검증하며, 운용하지 않는다**(사용자 결정). 그래서 DAG에 넣지 않고 손으로
실행하는 스크립트로 둔다. 자동 재학습·서빙 루프는 만들지 않는다.

【무엇을 예측하나】
gold_flight_trajectory의 한 행(지금 위치)에서 **다음 위치 보고까지의 이동량**을 맞힌다.
라벨(next_latitude/next_longitude/next_dt_s)은 Gold가 이미 만들어 두었다.

이동량은 위경도 차이가 아니라 **북쪽·동쪽 방향 미터**로 바꿔 예측한다. 위경도 1도의 실제
거리는 위도에 따라 달라져(경도 방향은 cos(위도)에 비례) 모델이 그 변환까지 배워야 하기
때문이다. 오차도 미터로 바로 읽힌다.

【무엇과 비교하나 — 기준선】
물리 기반 추측항법(dead reckoning): 지금 속도와 방향 그대로 next_dt_s초 동안 직진한다고
가정한다. 항공기는 대부분 직선 비행이라 강한 기준선이다. 모델이 이것을 못 이기면 그렇다고
보고한다 — 이 단계의 목적은 성능이 아니라 파이프라인이 끝까지 도는지 확인하는 것이다.

【데이터 누수 방지】
같은 비행(segment_id)의 점들은 서로 매우 비슷하다. 행 단위로 나누면 같은 비행이 학습과
평가 양쪽에 들어가 점수가 부풀려진다. 그래서 **비행 구간 단위로** 나눈다(GroupShuffleSplit).

【저장】
모델과 지표를 MinIO `ml/trajectory_next_position/<run_id>/`에 저장한다. 영구 저장소가
MinIO뿐이기 때문이다. 저장한 모델을 다시 내려받아 같은 예측이 나오는지까지 확인한다.
"""

import io
import json
import sys
import uuid
from datetime import datetime, timezone

import joblib
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import GroupShuffleSplit

sys.path.insert(0, "/opt/airflow/src")

from lake_duckdb import connect, scan  # noqa: E402
from lakehouse_common import LAKE_BUCKET, s3_client  # noqa: E402

MODEL_PREFIX = "ml/trajectory_next_position"
M_PER_DEG = 111_320.0  # 위도 1도 ≈ 111.32km

# 한 점 뒤 다음 보고까지 60초를 넘는 경우는 뺀다. 측정상 보고 간격 p90이 20초라
# 60초 초과는 수신 공백이고, 그 사이 기동을 맞히는 것은 다른 문제다.
MAX_NEXT_DT_S = 60

FEATURES = [
    "latitude", "longitude", "baro_altitude", "geo_altitude",
    "velocity", "track_sin", "track_cos", "vertical_rate",
    "dt_s", "calc_speed_mps", "heading_change_deg", "alt_change_m", "velocity_change_mps",
    "next_dt_s", "dr_north_m", "dr_east_m",
]


def load(con):
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


def build_xy(df):
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


def summary(err):
    return {
        "mean_m": round(float(np.mean(err)), 1),
        "p50_m": round(float(np.median(err)), 1),
        "p90_m": round(float(np.percentile(err, 90)), 1),
    }


def main():
    con = connect()
    df = load(con)
    if len(df) < 100:
        raise SystemExit(f"ML_ABORT: rows={len(df)} — 학습할 데이터가 너무 적다")

    X, y_n, y_e = build_xy(df)
    groups = df["segment_id"].to_numpy()

    split = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    tr, te = next(split.split(X, y_n, groups))
    overlap = len(set(groups[tr]) & set(groups[te]))
    print(
        f"ML_DATA rows={len(df)} segments={len(set(groups))} "
        f"train={len(tr)} test={len(te)} segment_overlap={overlap}"
    )

    # 북쪽·동쪽 이동량을 각각 회귀한다. HistGradientBoosting은 결측(NaN)을 그대로 다뤄,
    # 구간 첫 점처럼 직전 값이 없는 행도 버리지 않아도 된다.
    model_n = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, random_state=42)
    model_e = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, random_state=42)
    model_n.fit(X[tr], y_n[tr])
    model_e.fit(X[tr], y_e[tr])

    pred_n, pred_e = model_n.predict(X[te]), model_e.predict(X[te])
    fi = FEATURES.index
    base_n, base_e = X[te][:, fi("dr_north_m")], X[te][:, fi("dr_east_m")]

    err_model = errors_m(pred_n, pred_e, y_n[te], y_e[te])
    err_base = errors_m(base_n, base_e, y_n[te], y_e[te])
    m_model, m_base = summary(err_model), summary(err_base)
    print(f"ML_BASELINE_DEAD_RECKONING {m_base}")
    print(f"ML_MODEL_HGB              {m_model}")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    metrics = {
        "run_id": run_id,
        "task": "next position displacement (north/east metres)",
        "rows": int(len(df)), "train_rows": int(len(tr)), "test_rows": int(len(te)),
        "segments": int(len(set(groups))), "segment_overlap": int(overlap),
        "max_next_dt_s": MAX_NEXT_DT_S,
        "features": FEATURES,
        "baseline_dead_reckoning": m_base,
        "model_hist_gradient_boosting": m_model,
        "event_dates": sorted({str(d) for d in con.execute(
            f"SELECT DISTINCT event_date FROM {scan('trajectory')}").df()["event_date"]}),
    }

    # 저장: 모델 두 개와 특성 목록을 한 파일로, 지표는 JSON으로
    buf = io.BytesIO()
    joblib.dump({"north": model_n, "east": model_e, "features": FEATURES}, buf)
    model_bytes = buf.getvalue()
    s3 = s3_client()
    model_key = f"{MODEL_PREFIX}/{run_id}/model.joblib"
    metrics_key = f"{MODEL_PREFIX}/{run_id}/metrics.json"
    s3.put_object(Bucket=LAKE_BUCKET, Key=model_key, Body=model_bytes)
    s3.put_object(Bucket=LAKE_BUCKET, Key=metrics_key, Body=json.dumps(metrics, indent=2).encode("utf-8"))
    print(f"ML_SAVED s3://{LAKE_BUCKET}/{model_key} bytes={len(model_bytes)}")
    print(f"ML_SAVED s3://{LAKE_BUCKET}/{metrics_key}")

    # 다시 내려받아 같은 예측이 나오는지 — 저장·적재 경로가 실제로 쓸 수 있는 상태인지 확인
    loaded = joblib.load(io.BytesIO(s3.get_object(Bucket=LAKE_BUCKET, Key=model_key)["Body"].read()))
    sample = X[te][:200]
    same = (
        np.allclose(loaded["north"].predict(sample), model_n.predict(sample))
        and np.allclose(loaded["east"].predict(sample), model_e.predict(sample))
        and loaded["features"] == FEATURES
    )
    print(f"ML_RELOAD_CHECK {'OK' if same else 'MISMATCH'} sample={len(sample)}")
    if not same:
        sys.exit(1)

    better = m_model["p50_m"] < m_base["p50_m"]
    print(
        f"ML_DONE run_id={run_id} model_vs_baseline_p50="
        f"{m_model['p50_m']}m vs {m_base['p50_m']}m ({'model better' if better else 'baseline better or equal'})"
    )


if __name__ == "__main__":
    main()
