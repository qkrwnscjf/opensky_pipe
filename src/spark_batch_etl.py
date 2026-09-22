"""Bronze(Parquet) → Silver(Iceberg) 일배치 ETL. 하루치 파티션 하나를 처리한다.

docs/TASK_ORDER.md 0-6에서 채택한 설계의 구현이다. 이 스크립트는 **날짜 하나**만
처리하고 끝나는 배치다. 어떤 날짜들을 돌릴지 고르는 일은 여기서 하지 않는다 —
dags/flight_lakehouse_etl.py가 마커 파일을 비교해 고르고, 날짜마다 이 스크립트를
한 번씩 spark-submit 한다.

  spark-submit ... src/spark_batch_etl.py --date 2026-09-18

【이 배치가 지켜야 하는 것】

1. 중복 없음. Bronze는 at-least-once다 — 콜드 패스가 foreachBatch라서 "쓰기는
   끝났는데 체크포인트 커밋 전에 죽는" 구간이 있고, 그때 같은 (icao24, timestamp)
   행이 Parquet에 두 번 남는다. 그래서 읽은 직후 dropDuplicates로 턴다.

2. 재실행해도 결과가 같을 것. 같은 날짜를 두 번 돌려도 행이 불어나면 안 된다.
   append가 아니라 overwritePartitions()를 쓰는 이유다 — 이 DataFrame에 들어 있는
   파티션(= event_date 하루)만 통째로 갈아끼운다. 실패 후 재시도, 마커가 안 써진
   채 죽은 경우, 늦게 도착한 데이터로 다시 도는 경우가 전부 같은 경로로 수렴한다.

3. 마커는 **쓰기가 성공한 뒤에만** 남길 것. 순서가 반대면 ETL이 실패한 날짜가
   완료로 표시되어 영영 처리되지 않는다. 반대 순서(쓰기 성공 → 마커 실패)는
   다음 날 그 날짜를 한 번 더 돌리게 되는데, 2번 덕분에 무해하다. 두 작업이
   원자적이지 않은 이상 한쪽으로 기울여야 하고, "중복 처리 > 누락"이 맞다.
"""

import argparse
import os
import sys
from datetime import datetime

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# ---------------------------------------------------------------
# 설정
# ---------------------------------------------------------------
# Bronze: 스트리밍 콜드 패스가 쓰는 곳 (spark_dual_write.py의 MINIO_COLD_PATH와 같은 값)
BRONZE_PATH = os.getenv("MINIO_COLD_PATH", "s3a://flight-data-lake/positions")
# Silver: Iceberg 테이블. Hadoop 카탈로그라 warehouse 아래 디렉터리로 존재한다.
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://flight-data-lake/warehouse")
SILVER_TABLE = os.getenv("SILVER_TABLE", "lake.db.flight_features")

# 마커. DAG가 "이 날짜는 이미 했다"를 판단하는 유일한 근거다.
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "flight-data-lake")
MARKER_PREFIX = os.getenv("ETL_MARKER_PREFIX", "etl_markers")

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def build_spark(app_name):
    """S3A + Iceberg 설정을 얹은 SparkSession.

    S3A 설정은 spark_dual_write.py와 같은 값이다. 두 잡이 같은 MinIO를 본다.

    Iceberg 쪽은 카탈로그 `lake` 하나만 등록한다. type=hadoop이라 별도 카탈로그
    서버(Hive Metastore, REST, Nessie)가 필요 없고, 메타데이터가 warehouse 경로
    안의 파일로만 존재한다 — 컨테이너를 늘리지 않겠다는 제약(0-6)에 맞다.

    Hadoop 카탈로그의 알려진 한계: 커밋이 "원자적 rename"에 기대는데 S3에는 그런
    연산이 없다. 동시에 두 writer가 같은 테이블에 커밋하면 한쪽을 덮어쓸 수 있다.
    여기서는 writer가 이 배치 하나뿐이고 DAG가 날짜당 한 태스크만 띄우므로 해당
    조건이 성립하지 않는다. 동시 쓰기가 생기면 카탈로그를 바꿔야 한다.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", ICEBERG_WAREHOUSE)
        .getOrCreate()
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )


def write_marker(date_str, row_count):
    """`etl_markers/dt=<날짜>/_SUCCESS`를 남긴다.

    내용은 진단용이다. DAG는 객체의 **존재 여부**만 본다.
    """
    key = f"{MARKER_PREFIX}/dt={date_str}/_SUCCESS"
    body = (
        f"date={date_str}\n"
        f"rows={row_count}\n"
        f"table={SILVER_TABLE}\n"
        f"completed_at={datetime.utcnow().isoformat()}Z\n"
    )
    s3_client().put_object(Bucket=LAKE_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"ETL_MARKER_WRITTEN: s3://{LAKE_BUCKET}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver 일배치 ETL")
    parser.add_argument("--date", required=True, help="처리할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    # 이 값은 그대로 경로와 파티션 값이 된다. 형식이 틀리면 조용히 빈 경로를
    # 읽고 "데이터 없음"으로 끝나므로, 여기서 먼저 깨뜨린다.
    datetime.strptime(args.date, "%Y-%m-%d")

    date_str = args.date
    source = f"{BRONZE_PATH}/dt={date_str}"
    print(f"ETL_START: date={date_str} source={source} target={SILVER_TABLE}")

    spark = build_spark(f"flight_batch_etl_{date_str}")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ── 1. Bronze 읽기 ────────────────────────────────────────
        # dt= 디렉터리를 직접 지정하므로 Spark가 파티션 컬럼 dt를 만들지 않는다.
        # (상위 positions/를 읽고 filter하면 전체 파티션을 나열하게 된다.)
        #
        # positions/에는 _spark_metadata가 없다(콜드 패스가 foreachBatch인 이유).
        # 따라서 평범한 디렉터리 읽기가 맞다.
        try:
            df_raw = spark.read.parquet(source)
        except Exception as e:
            # 경로 자체가 없는 경우. DAG는 존재하는 dt= 접두사만 넘기므로 정상
            # 흐름에서는 나오지 않지만, 넘긴 뒤 지워졌을 수 있다.
            print(f"ETL_SOURCE_MISSING: {source} ({e})")
            sys.exit(1)

        raw_count = df_raw.count()

        # ── 2. 중복 제거 ──────────────────────────────────────────
        # (icao24, timestamp)가 한 기체의 한 관측을 유일하게 식별한다. Bronze의
        # at-least-once가 만드는 중복은 전부 이 쌍이 같은 완전 동일 행이므로,
        # 어느 쪽을 남겨도 결과가 같다.
        df_dedup = df_raw.dropDuplicates(["icao24", "timestamp"])
        dedup_count = df_dedup.count()
        print(
            f"ETL_DEDUP: raw={raw_count} deduped={dedup_count} "
            f"removed={raw_count - dedup_count}"
        )

        if dedup_count == 0:
            # 마커를 쓰지 않고 끝낸다. 빈 결과는 "정말 데이터가 없었다"와 "S3
            # 나열이 순간적으로 실패했다"를 구분할 수 없다. 마커를 쓰면 후자가
            # 영구 누락이 되고, 안 쓰면 전자는 다음 날 한 번 더 헛도는 것으로
            # 끝난다. 비용이 훨씬 싼 쪽을 고른다.
            print(f"ETL_EMPTY: {date_str} — 마커를 쓰지 않으므로 다음 실행에서 재시도된다")
            return

        # ── 3. 피처 ──────────────────────────────────────────────
        # 수집 스키마 전체를 그대로 피처로 넘긴다(0-6 결정). icao24는 피처가
        # 아니라 키지만, 조인·그룹 기준으로 필요하므로 컬럼으로는 남긴다.
        #
        # event_date는 Iceberg 파티션 키다. 소스의 dt(문자열)를 쓰지 않고
        # 인자에서 만드는 이유: 이 배치가 처리한다고 선언한 날짜와 테이블에
        # 실제로 들어가는 파티션 값이 정의상 일치해야 overwritePartitions()가
        # 의도한 하루만 갈아끼운다.
        df_silver = df_dedup.withColumn("event_date", lit(date_str).cast("date"))
        if "dt" in df_silver.columns:
            df_silver = df_silver.drop("dt")

        # ── 4. Silver 쓰기 ───────────────────────────────────────
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")

        if spark.catalog.tableExists(SILVER_TABLE):
            # 이 DataFrame에 있는 파티션(= event_date 하루)만 교체한다.
            # 다른 날짜 파티션은 건드리지 않는다.
            df_silver.writeTo(SILVER_TABLE).overwritePartitions()
            print(f"ETL_WRITE: overwritePartitions event_date={date_str} rows={dedup_count}")
        else:
            # 최초 1회. 파티션 명세는 테이블 속성이라 여기서만 정해진다.
            #
            # Iceberg는 hidden partitioning이다 — 조회 시 event_date 컬럼으로
            # 그냥 WHERE를 걸면 되고, 디렉터리 구조를 쿼리에 드러낼 필요가 없다.
            df_silver.writeTo(SILVER_TABLE).partitionedBy(col("event_date")).create()
            print(f"ETL_CREATE: {SILVER_TABLE} created, event_date={date_str} rows={dedup_count}")

        # ── 5. 마커 ─────────────────────────────────────────────
        # 반드시 쓰기 성공 뒤. 위에서 예외가 나면 여기 도달하지 않는다.
        write_marker(date_str, dedup_count)
        print(f"ETL_DONE: date={date_str} rows={dedup_count}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
