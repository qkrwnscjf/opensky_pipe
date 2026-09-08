import json
import os
import sys
import threading
import time

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime
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

# failOnDataLoss=false: 체크포인트에 남은 오프셋이 Kafka retention(1시간)에 밀려 이미 삭제된
# 경우, 기본값(true)이면 OffsetOutOfRangeException으로 스트림 자체가 죽는다. 스택을 한 시간 넘게
# 꺼뒀다 다시 켜면 항상 재현되는 문제라, 유실분은 건너뛰고 남아있는 가장 이른 오프셋부터 이어가게 한다.
# (원본 이력은 MinIO 콜드 패스에 이미 보관되어 있으므로 여기서의 skip은 복구 불가능한 손실이 아니다.)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "flight_data_raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
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
def notify_flight_update():
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
            cur.execute("SELECT pg_notify('flight_update', '')")
        conn.close()
    except Exception as e:
        print(f"NOTIFY_WARNING: {e}")


def save_to_sinks(batch_df, batch_id):
    batch_df.persist()
    count = batch_df.count()
    
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

            batch_df.write \
                .mode("append") \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "flight_data") \
                .option("user", db_user) \
                .option("password", db_password) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", "1000") \
                .save()

            ensure_index()
            notify_flight_update()

            # MinIO 저장
            batch_df.write \
                .mode("append") \
                .format("parquet") \
                .save("s3a://flight-data-lake/raw_data")
                
        except Exception as e:
            print(f"CRITICAL_SINK_ERROR: {e}")
    
    batch_df.unpersist()

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
# lastProgress가 아니라 recentProgress(최근 N개 배치 배열, 기본 100)를 훑는다.
# lastProgress 하나만 보면 폴링 간격 안에 배치가 두 개 끝났을 때 중간 배치가 조용히
# 사라진다. 트리거 간격이 10초라 정상 상태에서는 문제가 없지만, 백로그를 소화하는
# 동안 Spark는 트리거 간격을 무시하고 배치를 연달아 실행한다 — 즉 A-3(백프레셔)
# 측정처럼 데이터가 가장 필요한 국면에서 하필 구멍이 난다.
def start_metrics_reporter(query, interval_sec=1):
    def _emit(progress):
        sources = progress.get("sources") or [{}]
        print("SPARK_METRIC " + json.dumps({
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
                pending = sorted(
                    (p for p in (query.recentProgress or [])
                     if p.get("batchId") is not None and p["batchId"] > last_emitted),
                    key=lambda p: p["batchId"],
                )
                for progress in pending:
                    batch_id = progress["batchId"]
                    # recentProgress 버퍼(기본 100개)보다 빨리 배치가 지나가면 여기서도
                    # 놓친다. 조용히 넘어가면 측정값이 완전한 것처럼 보이므로, 누락
                    # 구간을 명시해 계측기가 자기 사각지대를 스스로 보고하게 한다.
                    if last_emitted >= 0 and batch_id > last_emitted + 1:
                        print(f"METRIC_GAP: batch {last_emitted + 1}~{batch_id - 1} 누락")
                    _emit(progress)
                    last_emitted = batch_id
            except Exception as e:
                print(f"METRIC_WARNING: {e}")
            time.sleep(interval_sec)

    threading.Thread(target=_run, daemon=True).start()


# ---------------------------------------------------------------
# 6. 실행 (Checkpoint 관리)
# ---------------------------------------------------------------
checkpoint_dir = "/tmp/spark_checkpoints_final"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

query = df_processed.writeStream \
    .foreachBatch(save_to_sinks) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

start_metrics_reporter(query)

print("TACTICAL_ETL_SYSTEM: OPERATIONAL")
query.awaitTermination()
