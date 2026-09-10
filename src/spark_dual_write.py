import json
import os
import sys
import threading
import time

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, IntegerType

# ---------------------------------------------------------------
# 1. Spark 세션 생성
# ---------------------------------------------------------------
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

spark = SparkSession.builder \
    .appName("FlightDataLakeETL") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 2. 스키마 정의 (Kafka에서 들어오는 raw JSON 형태)
# ---------------------------------------------------------------
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("sensors", StringType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", IntegerType()),
    StructField("timestamp", LongType()) # Raw epoch timestamp
])

# ---------------------------------------------------------------
# 3. Kafka 읽기
# ---------------------------------------------------------------
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# failOnDataLoss=false: 체크포인트가 가리키는 오프셋이 Kafka retention(1시간)에 밀려 이미
# 삭제됐을 때, 기본값(true)은 OffsetOutOfRangeException으로 스트림 자체를 죽인다. 유실분은
# 건너뛰고 남아있는 가장 이른 오프셋부터 이어가게 한다.
#
# 존재 근거가 2026-09-09에 바뀌었다. 원래는 "스택을 1시간 넘게 꺼뒀다 켜면 항상 재현"되는
# 세션 '간' 문제 때문이었는데, 세션 시작 초기화(EXPANSION_PLAN 0.4)로 체크포인트와 Kafka가
# 항상 함께 비워지므로 그 시나리오는 이제 발생하지 않는다. 남은 근거는 세션 '내'다 —
# spark가 1시간 넘게 죽어 있는 동안 producer가 계속 쓰면 retention이 앞질러 간다.
# 실제로 2026-09-09 OOM으로 spark가 장시간 내려간 적이 있으므로 가상의 경우가 아니다.
#
# (원본 이력은 MinIO 콜드 패스에 이미 보관되어 있으므로 여기서의 skip은 복구 불가능한 손실이 아니다.)
# maxOffsetsPerTrigger (A-3): 한 트리거가 가져올 수 있는 오프셋 수 상한.
#
# 없으면 다운타임 뒤 재기동할 때 밀린 물량을 한 배치에 전부 삼킨다. 2026-09-10 실측:
# spark를 5분 멈췄다 켜니 첫 배치가 **2,742행**이었다 — 정상(95행)의 29배, 7.9초.
# 과거에도 761·945행 스파이크가 관측됐다. 배치가 커지면 persist()가 붙들 메모리가
# 그만큼 커지므로 A-4(메모리)와 직결되고, 처리 시간이 트리거를 넘겨 지연이 쌓인다.
#
# 500을 고른 근거는 실측이다 (2026-09-10, 실트래픽):
#   정상 배치 p50 87행 / max 91행, producer 1회 전송 83행
# 평상시를 절대 조이지 않으려면 정상 max보다 충분히 커야 하고(5.5배 여유), 동시에
# 위 스파이크는 잘라야 한다. 임의값이 아니라 이 두 조건의 교집합이다.
#
# 소스에 걸었으므로 핫·콜드 두 쿼리 모두에 적용된다 — 메모리 안전 관점에서는
# 그게 맞다. 다만 콜드 패스는 백로그 구간에서 파일이 더 잘게 쪼개진다.
#
# B-1(아시아 확장)으로 트래픽이 수십 배가 되면 이 값이 평상시를 조이게 되므로
# 반드시 재산정해야 한다. KAFKA_MAX_OFFSETS 환경변수로 조정 가능.
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "flight_data_raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", os.getenv("KAFKA_MAX_OFFSETS", "500")) \
    .load()

# ---------------------------------------------------------------
# 4. 데이터 가공 (Postgres 형식에 맞게 변환)
# ---------------------------------------------------------------
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# timestamp(Long)를 timestamp_ts(Timestamp)로 변환 후, 원래의 timestamp 컬럼을 대체
df_processed = df_parsed \
    .withColumn("timestamp_fixed", to_timestamp(from_unixtime(col("timestamp")))) \
    .drop("timestamp") \
    .withColumnRenamed("timestamp_fixed", "timestamp") \
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull())

# ---------------------------------------------------------------
# 5. 핵심 함수: 배치 처리 로직 (Dual Write)
# ---------------------------------------------------------------

# flight_data 테이블은 Spark JDBC writer가 최초 append 시 자동 생성하지만 인덱스는 만들지 않는다.
# 문서에만 "적용됨"으로 적혀 있던 걸 실제로 코드화. 첫 성공 배치 이후엔 매번 재확인할 필요 없어 플래그로 가드.
#
# 두 인덱스는 역할이 다르다 (Phase 0 벤치마크에서 실측 확인, docs/BENCHMARKS.md 참고):
# - idx_flight_latest (icao24, timestamp DESC): icao24 단건 조회용 (향후 궤적 조회 등). icao24가
#   선행 컬럼이라 /flights의 `WHERE timestamp >= ...` 필터에는 쓰이지 않는다 — Seq Scan을 못 없앰.
# - idx_flight_timestamp (timestamp DESC): timestamp가 선행 컬럼이라 /flights의 WHERE 필터가
#   실제로 이 인덱스를 타서 Seq Scan → Index Scan 전환이 가능하다 (Phase 1에서 합성 부하로 실측).
_index_ready = False


def ensure_index():
    global _index_ready
    if _index_ready:
        return
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "flightdb"),
            user=os.getenv("DB_USER", "myuser"),
            password=os.getenv("DB_PASSWORD", "mypassword"),
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_flight_latest ON flight_data (icao24, timestamp DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_flight_timestamp ON flight_data (timestamp DESC)"
            )
        conn.close()
        _index_ready = True
        print("INDEX_READY: idx_flight_latest, idx_flight_timestamp ensured on flight_data")
    except Exception as e:
        print(f"INDEX_SETUP_WARNING: {e}")


# Phase 2 (docs/EXPANSION_PLAN.md): 배치가 Postgres에 성공적으로 쓰인 직후 호출.
# src/backend/main.py가 `LISTEN flight_update`로 대기하다가 이 신호를 받으면
# 연결된 WebSocket 클라이언트에 최신 /flights 결과를 브로드캐스트한다.
# A-4: 커넥션을 배치마다 새로 열지 않고 재사용한다.
#
# 이전에는 배치마다 connect → pg_notify → close를 반복했다. 한 번이 10~15ms로
# 크진 않지만, 그 비용이 배치 경로 '안'에 있고 TCP 핸드셰이크와 Postgres 백엔드
# 프로세스 생성을 매번 유발한다. 트리거를 3초로 줄인 뒤(A-5) 호출 빈도가 늘어
# 더 아깝게 됐다.
#
# 커넥션은 끊길 수 있으므로(Postgres 재시작, 유휴 타임아웃) 실패하면 한 번
# 버리고 다시 연결한다 — 재사용이 '끊기면 알림을 잃는' 구조가 되면 안 된다.
_notify_conn = None


def _open_notify_conn():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "flightdb"),
        user=os.getenv("DB_USER", "myuser"),
        password=os.getenv("DB_PASSWORD", "mypassword"),
    )
    conn.autocommit = True
    return conn


def notify_flight_update():
    global _notify_conn
    for attempt in (1, 2):
        try:
            if _notify_conn is None or _notify_conn.closed:
                _notify_conn = _open_notify_conn()
            with _notify_conn.cursor() as cur:
                cur.execute("SELECT pg_notify('flight_update', '')")
            return
        except Exception as e:
            # 1차 실패는 끊긴 커넥션일 가능성이 높다. 버리고 한 번만 재시도한다.
            try:
                if _notify_conn is not None:
                    _notify_conn.close()
            except Exception:
                pass
            _notify_conn = None
            if attempt == 2:
                print(f"NOTIFY_WARNING: {e}")


# 콜드 패스 적재 경로. 기본값이 실데이터 경로이고, 합성 부하 테스트일 때만 덮어쓴다.
#
# 2026-09-09 데이터 수명 정책(docs/EXPANSION_PLAN.md 0.4)에서 MinIO가 이 시스템의
# 유일한 영구 저장소이자 B-2(궤적 예측) 학습 데이터의 원천으로 확정됐다. 그런데
# scripts/load_test_producer.py는 같은 flight_data_raw 토픽에 랜덤 icao24의 가짜
# 레코드를 흘려보내므로, 그대로 두면 존재하지 않는 항공기가 학습 데이터에 섞인다.
# 날짜 파티셔닝(Phase 1-1)이 아직 없어 나중에 경로로 걸러낼 수단도 없다.
#
# 따라서 부하 테스트 시에는 경로 자체를 분리한다:
#   MINIO_COLD_PATH=s3a://flight-data-lake/synthetic docker compose up -d spark
COLD_PATH = os.getenv("MINIO_COLD_PATH", "s3a://flight-data-lake/raw_data")


def save_to_hot(batch_df, batch_id):
    # 핫 패스(Postgres)만 담당한다. 콜드 패스(MinIO)는 별도 스트리밍 쿼리로 분리됐다.
    #
    # 분리 근거 (2026-09-09 실측, 184배치/200행, docs/BENCHMARKS.md):
    #   count 746ms(50.7%) / minio 539ms(36.6%) / postgres 175ms(11.9%)
    # MinIO가 Postgres의 3.1배였다. 두 싱크를 병렬화하면 539+175 → max(539,175)로
    # 175ms만 줄지만, 콜드 패스를 배치 경로에서 빼면 539ms가 통째로 빠진다.
    #
    # count()는 남긴다. persist() 후 첫 액션이라 Kafka 읽기·JSON 파싱을 실제로
    # 수행하는 구간이고, 없애면 그 비용이 Postgres 쓰기로 옮겨갈 뿐이다.
    timings = {}

    def _timed(name, fn):
        started = time.perf_counter()
        try:
            return fn()
        finally:
            timings[name] = round((time.perf_counter() - started) * 1000, 1)

    batch_df.persist()
    count = _timed("count", batch_df.count)

    if count > 0:
        print(f"[{batch_id}] {count} records processing...")
        try:
            # PostgreSQL 저장
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME", "flightdb")
            db_user = os.getenv("DB_USER", "myuser")
            db_password = os.getenv("DB_PASSWORD", "mypassword")
            jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

            _timed("postgres", lambda: batch_df.write
                   .mode("append")
                   .format("jdbc")
                   .option("url", jdbc_url)
                   .option("dbtable", "flight_data")
                   .option("user", db_user)
                   .option("password", db_password)
                   .option("driver", "org.postgresql.Driver")
                   .option("batchsize", "1000")
                   .save())

            _timed("ensure_index", ensure_index)
            _timed("notify", notify_flight_update)

        except Exception as e:
            print(f"CRITICAL_SINK_ERROR: {e}")

    _timed("unpersist", batch_df.unpersist)

    # 예외로 중단됐어도 그 시점까지의 구간은 남긴다 — 어디서 끊겼는지가 곧 단서다.
    print("SINK_METRIC " + json.dumps({
        "batchId": batch_id,
        "numRows": count,
        "ms": timings,
    }, default=str))

# ---------------------------------------------------------------
# A-1 (docs/EXPANSION_PLAN.md): 배치 메트릭 관측
# ---------------------------------------------------------------
# 이전에는 "다음 배치 로그와의 시각 차이"로 처리 시간을 역산했는데, 그 값에는 트리거
# 대기가 섞여 있어 같은 크기(~2,100건) 배치에서도 6.65~19.33초로 3배 흔들렸다.
# Spark는 durationMs.addBatch로 실제 처리 시간을, numInputRows로 입력 행 수를 이미
# 계산해두므로 역산 대신 그 값을 읽는다.
#
# StreamingQueryListener(py4j 콜백) 대신 폴링을 쓴 이유: 콜백 서버 설정 없이 동일한
# 지표를 얻을 수 있고 실패 지점이 적다.
#
# 폴링은 lastProgress(1개)로 하고, 배치가 건너뛴 것이 감지됐을 때만 recentProgress로
# 되메운다. 매초 recentProgress를 훑으면 안 되는 이유는 PySpark 3.5.0 구현 때문이다:
#
#   recentProgress → [json.loads(p.json()) for p in self._jsq.recentProgress()]  # 최대 100개
#   lastProgress   → json.loads(self._jsq.lastProgress().json())                  # 1개
#
# 즉 매초 JVM이 progress 객체 100개를 JSON으로 직렬화하고 Python이 100개를 파싱한다
# (분당 6,000회). 2026-09-09 이 방식을 배포한 뒤 24분 만에 addBatch가 3.7초→124초로
# 악화되며 드라이버가 OOM으로 종료됐고, 직전 lastProgress 버전은 38분간 멀쩡했다.
# 계측기가 관측 대상을 망가뜨리면 안 된다.
#
# 그래도 lastProgress '하나만' 보면 폴링 간격 안에 두 배치가 끝났을 때 중간 배치를
# 놓친다. 그래서 batchId가 건너뛴 것이 보일 때만 recentProgress를 한 번 읽어 되메운다.
#
# 【2026-09-10 정정】 원래 이 방어의 근거로 "백로그를 소화하는 동안 Spark가 트리거
# 간격을 무시하고 배치를 연달아 실행한다"고 적었는데, **틀렸다.** A-3 백프레셔 검증
# 중 실측한 결과 5분치 백로그를 소화하는 구간에서도 배치 완료 간격이 정확히 3.00초
# — 트리거 간격 그대로였다. processingTime 트리거는 백로그가 있어도 페이스를 지킨다
# (배치가 트리거를 넘겨야만 다음 배치가 곧바로 시작된다).
#
# 따라서 현재 설정(3초 트리거)에서 '폴링 간격 안에 두 배치'는 사실상 일어나지 않는다.
# 이 방어를 남겨두는 이유는 트리거를 1초 미만으로 낮추거나 Trigger.AvailableNow처럼
# 페이싱이 없는 모드로 바꿀 때를 위한 것이며, 비용이 0에 가까우므로 유지한다.
#
# 첫 폴링에서는 recentProgress로 되메운다. 그러지 않으면 리포터가 뜨기 전에 끝난
# 배치가 조용히 사라진다 — 2026-09-10 실측: 재기동 후 batch 191이 SINK_METRIC에는
# 있는데 SPARK_METRIC에는 없었고 METRIC_GAP도 뜨지 않았다. 기동 시 1회뿐이라
# 비싼 호출을 해도 된다. (첫 폴링에는 비교 기준이 없으므로 간극 보고는 하지 않는다.)
def start_metrics_reporter(query, label="hot", interval_sec=1):
    def _emit(progress):
        sources = progress.get("sources") or [{}]
        print("SPARK_METRIC " + json.dumps({
            "query": label,
            "batchId": progress.get("batchId"),
            "timestamp": progress.get("timestamp"),
            "numInputRows": progress.get("numInputRows"),
            "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
            "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
            "durationMs": progress.get("durationMs"),
            "endOffset": sources[0].get("endOffset"),
        }, default=str))

    def _run():
        last_emitted = -1
        while query.isActive:
            try:
                progress = query.lastProgress
                batch_id = progress.get("batchId") if progress else None

                if batch_id is not None and batch_id > last_emitted:
                    first_poll = last_emitted < 0
                    if not first_poll and batch_id == last_emitted + 1:
                        # 정상 경로: 바로 다음 배치 → 비싼 호출 없이 끝낸다.
                        _emit(progress)
                        last_emitted = batch_id
                    else:
                        # 첫 폴링이거나 batchId가 건너뛰었다. 이때만 recentProgress를
                        # 한 번 읽어 되메운다.
                        backfill = sorted(
                            (p for p in (query.recentProgress or [])
                             if p.get("batchId") is not None
                             and last_emitted < p["batchId"] <= batch_id),
                            key=lambda p: p["batchId"],
                        )
                        recovered = {p["batchId"] for p in backfill}
                        # 첫 폴링에는 "직전에 무엇이 있었어야 하는지"의 기준이 없다.
                        # 체크포인트에서 이어받은 batchId는 0부터 시작하지 않으므로,
                        # 여기서 간극을 계산하면 전부 누락으로 오보한다.
                        if not first_poll:
                            # recentProgress 버퍼(기본 100개)보다 빨리 지나간 구간은
                            # 되메울 수 없다. 조용히 넘어가면 측정값이 완전한 것처럼
                            # 보이므로, 계측기가 자기 사각지대를 스스로 보고하게 한다.
                            missing = [b for b in range(last_emitted + 1, batch_id + 1)
                                       if b not in recovered]
                            if missing:
                                # 불연속일 수 있으므로 범위로 뭉뚱그리지 않고 목록을 낸다.
                                print(f"METRIC_GAP[{label}]: batch {missing} 누락 "
                                      f"({len(missing)}건)")
                        # recentProgress가 비어 있어도 lastProgress만은 남긴다.
                        for p in (backfill or [progress]):
                            _emit(p)
                        last_emitted = batch_id
            except Exception as e:
                print(f"METRIC_WARNING: {e}")
            time.sleep(interval_sec)

    threading.Thread(target=_run, daemon=True).start()


# ---------------------------------------------------------------
# 6. 실행 (Checkpoint 관리)
# ---------------------------------------------------------------
checkpoint_dir = "/tmp/spark_checkpoints_final"
cold_checkpoint_dir = "/tmp/spark_checkpoints_cold"
for d in (checkpoint_dir, cold_checkpoint_dir):
    if not os.path.exists(d):
        os.makedirs(d)

# 핫 패스 — 서빙용.
#
# 트리거를 10초 → 3초로 줄였다 (A-5, 2026-09-10). 이전에는 줄일 수 없었다:
# 배치가 잦아지면 MinIO에 작은 Parquet이 폭증하기 때문이었다. Phase 1-1에서 콜드
# 패스를 300초 트리거의 별도 쿼리로 분리하면서 **핫 패스 트리거가 콜드 패스 파일
# 크기와 무관해졌고**, 그래서 비로소 줄일 수 있게 됐다.
#
# 3초를 고른 근거는 실측이다 (2026-09-10, 117행/배치):
#   배치 작업시간 p50 1,161ms / p95 1,666ms / max 1,916ms
# 배치가 트리거를 넘기면 밀려서 오히려 지연이 늘므로 max 대비 1.5배 이상의 여유가
# 필요하다. 3초는 1.57배. 2초는 1.04배로 여유가 없어 배제했다.
#
# 이 값이 줄이는 것은 'Kafka에 도착한 레코드가 처리될 때까지의 대기'다. producer
# 폴링 주기(10초)는 건드리지 않는다 — 줄이면 OpenSky 크레딧 소모가 늘고 실제로
# 2026-09-09에 429(일일 한도 소진)를 겪었다.
hot_query = df_processed.writeStream \
    .foreachBatch(save_to_hot) \
    .outputMode("append") \
    .trigger(processingTime=os.getenv("HOT_TRIGGER", "3 seconds")) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# 콜드 패스 — 학습 데이터용. 핫 패스와 완전히 분리된 별도 쿼리다.
#
# 왜 분리했나: 같은 배치 안에서 순차로 쓰던 구조에서 MinIO가 Postgres의 3.1배를
# 먹고 있었다(539ms vs 175ms). 분리하면 그 539ms가 핫 패스 배치에서 통째로 빠진다.
#
# 왜 트리거가 긴가: 콜드 패스는 아무도 실시간으로 조회하지 않는다(B-2 학습 데이터
# 원천). 10초마다 쓰면 하루 8,600개의 작은 Parquet이 생기는데, 300초로 늘리면
# 파일이 30배 적고 30배 커진다 — 학습 데이터 로딩에서 소파일 문제가 완화된다.
#
# 왜 partitionBy("dt")인가: 2026-09-09 데이터 수명 정책(0.4)에서 MinIO가 유일한
# 영구 저장소로 확정됐다. 평면 경로로는 (1) A-2 순서 수정 이전/이후 데이터를
# 구분할 수 없고 (2) 학습 시 기간을 골라 읽을 수 없다.
#
# 대가: Kafka를 두 번 읽는다(쿼리마다 독립 컨슈머). 현재 트래픽에서는 무시할 수
# 있지만, B-1 지리 확장 시 재검토가 필요하다.
cold_query = df_processed \
    .withColumn("dt", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("dt") \
    .option("path", COLD_PATH) \
    .option("checkpointLocation", cold_checkpoint_dir) \
    .trigger(processingTime=os.getenv("COLD_TRIGGER", "300 seconds")) \
    .start()

start_metrics_reporter(hot_query, label="hot")
start_metrics_reporter(cold_query, label="cold")

print("TACTICAL_ETL_SYSTEM: OPERATIONAL")
hot_query.awaitTermination()
