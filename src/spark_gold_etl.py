"""Silver(Iceberg) → Gold(Iceberg) 일배치 ETL. 하루치를 처리한다. (0-6c)

  spark-submit ... src/spark_gold_etl.py --date 2026-09-22

Silver와 마찬가지로 **날짜 하나**만 처리하고, 어떤 날짜를 돌릴지는 DAG가 정한다.
결과는 테이블 세 개다:

  gold_flight_trajectory  ML용. 위치 보고 1건 = 1행. 궤적 구간과 직전·다음 위치 기준 피처
  gold_aircraft_daily     DA용. 날짜 × 기체 요약
  gold_traffic_hourly     DA용. 날짜 × 시간 × 국가별 교통량

【Silver의 중복 제거만으로는 부족한 이유 — 2026-09-23 실측】

Silver는 (icao24, timestamp)로 중복을 제거한다. 그런데 timestamp는 OpenSky가
**스냅샷을 찍은 시각**이고, 기체가 **위치를 보고한 시각**은 time_position이다.
기체가 새 위치를 보내지 않으면 스냅샷만 바뀌고 위치는 그대로 남는다.

  실 Bronze 24,896행 중 같은 (icao24, time_position)의 반복: 6,336행 (25.4%)
  스냅샷 시각 − 위치 시각: p50 1초, p95 199초, p99 466초

즉 행의 4분의 1이 "이동 0, 시간 0"이고, timestamp를 궤적 시각으로 쓰면 최대 몇
분씩 틀린다. 그대로 두면 속도·방향 피처가 오염되고 0으로 나누게 된다. 그래서
Gold는 (icao24, time_position)으로 한 번 더 줄이고 time_position을 사건 시각으로 쓴다.

Silver 자체는 바꾸지 않는다. 스냅샷 단위의 기록도 그 자체로 사실이고("원본 그대로"가
Silver의 결정이었다), 위치 단위로 정리하는 건 Gold의 몫이다.

【자정(UTC) 경계 — 전날을 읽어 잇는다 (사용자 결정, 2026-09-23)】

날짜별로만 처리하면 UTC 자정(= 09:00 KST, 한국 영공이 바쁜 시간)에서 궤적이 끊긴다.
그래서 날짜 D를 처리할 때 D-1의 Silver도 함께 읽는다.

"끝부분"이 아니라 **D-1 전체**를 읽는다. 끝 몇 분만 읽으면 직전 위치는 이을 수 있지만
23시에 시작한 비행의 **구간 시작점**을 알 수 없어, 같은 비행이 날짜마다 다른
segment_id를 받게 된다. 하루치 Silver는 약 1.7만 행이라 비용은 무시할 만하다.

결과는 **D에 속하는 행만** 쓴다. D-1 행은 맥락으로만 쓴다.

【날짜를 가로지르는 중복을 막는 소유 규칙】

같은 위치 보고가 23:59:55 스냅샷(D-1)과 00:00:05 스냅샷(D)에 모두 찍힐 수 있다.
"가장 이른 스냅샷이 속한 날짜가 그 위치를 소유한다"로 정한다. 이 판단에는 과거만
필요하므로 D를 처리할 때 D-1을 읽는 것으로 충분하고, 각 위치는 정확히 한 파티션에만
들어간다. 예외는 같은 위치가 24시간 넘게 반복되는 경우뿐인데, 비행 중인 기체에서는
일어나지 않는다(지상 행은 궤적에서 뺀다).

【알려진 한계】
- 자정을 넘는 구간에서 D의 마지막 위치는 다음 위치 라벨(next_*)이 비어 있다. D를
  처리하는 시점엔 D+1이 없기 때문이다. D+1을 처리할 때 그 첫 행의 직전 값은 채워진다.
- D-1의 Silver가 나중에 다시 처리되더라도 D의 Gold는 자동으로 다시 계산되지 않는다.
- 구간 길이는 테이블에 넣지 않는다. 자정을 넘는 구간은 날짜마다 일부만 보이므로
  하루 단위로 세면 틀린다. 필요하면 Gold 전체를 segment_id로 묶어 센다.
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse_common import build_spark, validate_date, write_marker, write_partition

SILVER_TABLE = os.getenv("SILVER_TABLE", "lake.db.flight_features")
GOLD_TRAJECTORY_TABLE = os.getenv("GOLD_TRAJECTORY_TABLE", "lake.db.gold_flight_trajectory")
GOLD_AIRCRAFT_DAILY_TABLE = os.getenv("GOLD_AIRCRAFT_DAILY_TABLE", "lake.db.gold_aircraft_daily")
GOLD_TRAFFIC_HOURLY_TABLE = os.getenv("GOLD_TRAFFIC_HOURLY_TABLE", "lake.db.gold_traffic_hourly")

# Silver 마커(etl_markers/)와 겹치지 않게 따로 둔다. Silver만 끝나고 Gold가 실패한
# 날짜도 다음 실행에서 Gold만 따라잡을 수 있어야 하기 때문이다.
GOLD_MARKER_PREFIX = os.getenv("GOLD_MARKER_PREFIX", "etl_markers_gold")

# 같은 기체라도 위치 보고 사이 공백이 이보다 길면 새 구간(= 다른 비행)으로 본다.
# 실측(09-23): 연속 위치 간격 p50 11초, p90 20초. 300초 초과 공백은 87건이고,
# 30분 초과가 48건, 최대 약 24시간 — 다음 날 다시 나타난 같은 기체다. 끊지 않으면
# "24시간 걸린 이동"이 속도 피처로 들어간다.
SEGMENT_GAP_SEC = int(os.getenv("GOLD_SEGMENT_GAP_SEC", "300"))

EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1, lon1, lat2, lon2):
    """두 위경도 사이 대원 거리(미터). UDF 없이 Spark 내장 함수만 쓴다."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * F.asin(F.sqrt(a))


def dedup_positions(silver):
    """스냅샷 단위(Silver) → 위치 보고 단위.

    같은 (icao24, time_position) 중 **가장 이른 스냅샷**을 남긴다. 그 행의 event_date가
    이 위치의 소유 날짜다(모듈 설명 참고). Silver가 (icao24, timestamp)로 이미 줄였으므로
    같은 기체 안에서 timestamp는 유일하고, 정렬이 결정적이다.
    """
    w = Window.partitionBy("icao24", "time_position").orderBy(F.col("timestamp").asc())
    return (
        silver.filter(F.col("time_position").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumnRenamed("event_date", "owner_date")
        .withColumn("event_time", F.timestamp_seconds("time_position"))
    )


def build_trajectory(positions):
    """비행 중 위치 → 구간 분할 → 직전·다음 위치 기준 피처.

    지상 행은 뺀다(사용자 결정). 지상에서는 고도·상승률이 대부분 null이고
    (실측: 고도 null 약 20% ≈ 지상 19.7%) 움직임의 성격도 다르다.
    on_ground가 null인 행도 뺀다 — 비행 중이라고 확인되지 않은 것이다.
    """
    air = positions.filter(F.col("on_ground") == F.lit(False))

    # 1) 구간 나누기: 기체별 시간순으로 직전 위치와의 공백이 기준을 넘으면 새 구간
    by_aircraft = Window.partitionBy("icao24").orderBy("time_position")
    running = by_aircraft.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    air = (
        air.withColumn("_prev_tp", F.lag("time_position").over(by_aircraft))
        .withColumn(
            "_new_seg",
            F.when(
                F.col("_prev_tp").isNull()
                | ((F.col("time_position") - F.col("_prev_tp")) > SEGMENT_GAP_SEC),
                1,
            ).otherwise(0),
        )
        .withColumn("_seg_no", F.sum("_new_seg").over(running))
    )

    # segment_id = 기체 + 구간 시작 시각. 번호(_seg_no)는 읽은 범위에 따라 달라지지만
    # 시작 시각은 D-1을 읽는 한 날짜와 무관하게 같다 — 그래서 날짜를 넘어 이어진다.
    seg_start = Window.partitionBy("icao24", "_seg_no")
    air = air.withColumn(
        "segment_id",
        F.concat_ws("_", F.col("icao24"), F.min("time_position").over(seg_start).cast("string")),
    )

    # 2) 구간 안에서 직전·다음 위치
    s = Window.partitionBy("segment_id").orderBy("time_position")
    prev = lambda c: F.lag(c).over(s)  # noqa: E731
    nxt = lambda c: F.lead(c).over(s)  # noqa: E731

    traj = (
        air.withColumn("point_idx", F.row_number().over(s) - 1)
        .withColumn("prev_time_position", prev("time_position"))
        .withColumn("prev_latitude", prev("latitude"))
        .withColumn("prev_longitude", prev("longitude"))
        .withColumn("prev_true_track", prev("true_track"))
        .withColumn("prev_baro_altitude", prev("baro_altitude"))
        .withColumn("prev_velocity", prev("velocity"))
        .withColumn("next_time_position", nxt("time_position"))
        .withColumn("next_latitude", nxt("latitude"))
        .withColumn("next_longitude", nxt("longitude"))
        .withColumn("next_baro_altitude", nxt("baro_altitude"))
    )

    # 3) 피처. 구간의 첫 행은 직전 값이 없으므로 전부 null이다.
    # (icao24, time_position)으로 줄였으므로 구간 안에서 time_position은 엄격히
    # 증가한다 — dt_s는 0이 될 수 없어 나눗셈이 안전하다.
    traj = (
        traj.withColumn("dt_s", F.col("time_position") - F.col("prev_time_position"))
        .withColumn(
            "dist_m",
            haversine_m(
                F.col("prev_latitude"), F.col("prev_longitude"),
                F.col("latitude"), F.col("longitude"),
            ),
        )
        .withColumn("calc_speed_mps", F.col("dist_m") / F.col("dt_s"))
        # 방향 변화는 -180~+180으로 접는다. 350°→10°는 +20°이지 -340°가 아니다.
        .withColumn(
            "heading_change_deg",
            F.pmod(F.col("true_track") - F.col("prev_true_track") + 540, 360) - 180,
        )
        .withColumn("alt_change_m", F.col("baro_altitude") - F.col("prev_baro_altitude"))
        .withColumn("velocity_change_mps", F.col("velocity") - F.col("prev_velocity"))
        # 라벨: 다음 위치까지의 시간. 다음 위치 자체는 next_latitude/next_longitude.
        .withColumn("next_dt_s", F.col("next_time_position") - F.col("time_position"))
    )

    return traj.select(
        "icao24", "segment_id", "point_idx", "time_position", "event_time",
        "callsign", "origin_country",
        "latitude", "longitude", "baro_altitude", "geo_altitude",
        "velocity", "true_track", "vertical_rate", "squawk",
        "dt_s", "dist_m", "calc_speed_mps", "heading_change_deg",
        "alt_change_m", "velocity_change_mps",
        "next_latitude", "next_longitude", "next_baro_altitude", "next_dt_s",
        "owner_date",
    )


def build_aircraft_daily(positions_today, trajectory_today):
    """날짜 × 기체. 지상 행도 포함한다(사용자 결정) — 공항 체류도 DA 대상이다."""
    base = positions_today.groupBy("icao24").agg(
        # 하루 사이에도 편명이 바뀔 수 있어 마지막 보고 기준으로 고른다.
        F.max_by("callsign", "time_position").alias("callsign"),
        F.max_by("origin_country", "time_position").alias("origin_country"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.count("*").alias("obs_count"),
        F.sum(F.when(F.col("on_ground") == F.lit(False), 1).otherwise(0)).alias("airborne_obs"),
        F.sum(F.when(F.col("on_ground") == F.lit(True), 1).otherwise(0)).alias("ground_obs"),
        F.max("baro_altitude").alias("max_baro_altitude"),
        F.avg(F.when(F.col("on_ground") == F.lit(False), F.col("velocity"))).alias("avg_airborne_velocity"),
    )
    # 비행 거리·구간 수는 궤적에서 가져온다(비행 중 위치 기준).
    flown = trajectory_today.groupBy("icao24").agg(
        F.countDistinct("segment_id").alias("segment_count"),
        F.sum("dist_m").alias("distance_m"),
    )
    return base.join(flown, "icao24", "left").fillna({"segment_count": 0, "distance_m": 0.0})


def build_traffic_hourly(positions_today):
    """날짜 × 시간(UTC) × 국가. 지상 포함.

    시간은 스냅샷 시각(timestamp) 기준이다. 파티션 소유도 스냅샷 날짜로 정하므로
    둘을 맞춰야 "D 파티션 안에 23시 전날 데이터" 같은 어긋남이 생기지 않는다.
    """
    return positions_today.groupBy(
        F.hour("timestamp").alias("hour_utc"), "origin_country"
    ).agg(
        F.countDistinct("icao24").alias("aircraft_count"),
        F.countDistinct(F.when(F.col("on_ground") == F.lit(False), F.col("icao24"))).alias("airborne_aircraft"),
        F.count("*").alias("obs_count"),
    )


def main():
    parser = argparse.ArgumentParser(description="Silver → Gold 일배치 ETL")
    parser.add_argument("--date", required=True, help="처리할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()
    validate_date(args.date)

    date_str = args.date
    prev_str = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    midnight_epoch = int(
        datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )
    print(f"GOLD_START: date={date_str} context={prev_str} source={SILVER_TABLE}")

    spark = build_spark(f"flight_gold_etl_{date_str}")
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not spark.catalog.tableExists(SILVER_TABLE):
            print(f"GOLD_SOURCE_MISSING: {SILVER_TABLE} 없음 — Silver가 먼저 돌아야 한다")
            sys.exit(1)

        # ── 1. Silver 읽기: D와 D-1 ──────────────────────────────
        # event_date는 Iceberg 파티션 키라 이 필터는 파일 수준에서 걸러진다.
        silver = spark.table(SILVER_TABLE).filter(
            F.col("event_date").isin(
                F.lit(prev_str).cast("date"), F.lit(date_str).cast("date")
            )
        )
        today_rows = silver.filter(F.col("event_date") == F.lit(date_str).cast("date")).count()
        context_rows = silver.filter(F.col("event_date") == F.lit(prev_str).cast("date")).count()
        print(f"GOLD_SOURCE: {date_str}={today_rows} rows, context {prev_str}={context_rows} rows")

        if today_rows == 0:
            # Silver와 같은 이유로 마커를 쓰지 않는다 — "정말 없음"과 "읽기 실패"를
            # 구분할 수 없고, 마커를 안 쓰는 쪽의 비용이 훨씬 싸다.
            print(f"GOLD_EMPTY: {date_str} — 마커를 쓰지 않으므로 다음 실행에서 재시도된다")
            return

        # ── 2. 위치 보고 단위로 줄이기 ───────────────────────────
        positions = dedup_positions(silver).cache()
        today = F.col("owner_date") == F.lit(date_str).cast("date")
        pos_today = positions.filter(today)
        n_pos_today = pos_today.count()
        print(
            f"GOLD_DEDUP: silver_rows={today_rows} positions={n_pos_today} "
            f"stale_repeats_removed={today_rows - n_pos_today}"
        )

        # ── 3. 궤적 (D-1을 맥락으로 계산한 뒤 D 소유 행만 남긴다) ──
        trajectory = build_trajectory(positions).filter(today).cache()
        n_traj = trajectory.count()
        n_segments = trajectory.select("segment_id").distinct().count()
        # 자정 이전에 시작해 D로 넘어온 구간 = D-1 읽기가 실제로 이어준 구간
        n_cross = (
            trajectory.filter(F.split("segment_id", "_").getItem(1).cast("long") < midnight_epoch)
            .select("segment_id").distinct().count()
        )
        min_dt = trajectory.agg(F.min("dt_s")).first()[0]
        print(
            f"GOLD_TRAJECTORY: rows={n_traj} segments={n_segments} "
            f"crossing_midnight={n_cross} min_dt_s={min_dt}"
        )

        traj_out = trajectory.withColumnRenamed("owner_date", "event_date")
        mode = write_partition(traj_out, GOLD_TRAJECTORY_TABLE, "event_date")
        print(f"GOLD_WRITE: {mode} {GOLD_TRAJECTORY_TABLE} rows={n_traj}")

        # ── 4. DA 집계 (지상 포함) ─────────────────────────────
        daily = build_aircraft_daily(pos_today, trajectory).withColumn(
            "event_date", F.lit(date_str).cast("date")
        )
        n_daily = daily.count()
        mode = write_partition(daily, GOLD_AIRCRAFT_DAILY_TABLE, "event_date")
        print(f"GOLD_WRITE: {mode} {GOLD_AIRCRAFT_DAILY_TABLE} rows={n_daily}")

        hourly = build_traffic_hourly(pos_today).withColumn(
            "event_date", F.lit(date_str).cast("date")
        )
        n_hourly = hourly.count()
        mode = write_partition(hourly, GOLD_TRAFFIC_HOURLY_TABLE, "event_date")
        print(f"GOLD_WRITE: {mode} {GOLD_TRAFFIC_HOURLY_TABLE} rows={n_hourly}")

        # ── 5. 마커 — 세 테이블 쓰기가 모두 끝난 뒤 ──────────────
        # 세 커밋은 원자적이지 않다. 중간에 죽으면 마커가 없어 다음 실행에서 날짜
        # 전체를 다시 하고, 세 쓰기 모두 overwritePartitions라 무해하다.
        write_marker(
            GOLD_MARKER_PREFIX, date_str, n_traj,
            f"{GOLD_TRAJECTORY_TABLE},{GOLD_AIRCRAFT_DAILY_TABLE},{GOLD_TRAFFIC_HOURLY_TABLE}",
        )
        print(f"GOLD_DONE: date={date_str}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
