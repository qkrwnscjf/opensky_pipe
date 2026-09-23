"""레이크하우스 배치(Silver·Gold)가 공유하는 설정과 도구.

Silver와 Gold가 같은 MinIO, 같은 Iceberg 카탈로그를 본다. SparkSession 설정을
각 스크립트에 따로 두면 한쪽만 고쳐져 갈라진다 — flight_schema.py가 생긴 이유
(producer와 Spark 스키마가 실제로 어긋났던 일)와 같은 문제다. 그래서 여기에만 둔다.
"""

import os
from datetime import datetime

import boto3
from pyspark.sql import SparkSession

ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://flight-data-lake/warehouse")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "flight-data-lake")

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
    여기서는 DAG가 태스크를 한 번에 하나만 띄우므로(max_active_tis_per_dag=1)
    해당 조건이 성립하지 않는다. 동시 쓰기가 생기면 카탈로그를 바꿔야 한다.
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


def validate_date(date_str):
    """--date 값은 그대로 경로와 파티션 값이 된다. 형식이 틀리면 조용히 빈 경로를
    읽고 "데이터 없음"으로 끝나므로, 여기서 먼저 깨뜨린다."""
    datetime.strptime(date_str, "%Y-%m-%d")


def write_partition(df, table, partition_col):
    """테이블이 있으면 해당 파티션만 교체하고, 없으면 파티션 명세와 함께 만든다.

    overwritePartitions()는 이 DataFrame에 들어 있는 파티션만 통째로 갈아끼운다.
    같은 날짜를 몇 번 돌려도 결과가 같다 — 재시도, 마커 누락, 늦게 도착한 데이터가
    전부 같은 경로로 수렴한다.
    """
    spark = df.sparkSession
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
    if spark.catalog.tableExists(table):
        df.writeTo(table).overwritePartitions()
        return "overwrite"
    # 파티션 명세는 테이블 속성이라 최초 생성 때만 정해진다. Iceberg는 hidden
    # partitioning이라 조회 쪽은 그냥 WHERE를 걸면 된다.
    from pyspark.sql.functions import col

    df.writeTo(table).partitionedBy(col(partition_col)).create()
    return "create"


def write_marker(marker_prefix, date_str, rows, target):
    """`<marker_prefix>/dt=<날짜>/_SUCCESS`를 남긴다.

    반드시 쓰기 성공 **뒤에** 호출한다. 반대 순서면 실패한 날짜가 완료로 찍혀 영영
    처리되지 않는다. 이 순서의 최악은 "한 번 더 처리"이고, 쓰기가 멱등이라 무해하다.

    내용은 진단용이다. DAG는 객체의 **존재 여부**만 본다.
    """
    key = f"{marker_prefix}/dt={date_str}/_SUCCESS"
    body = (
        f"date={date_str}\n"
        f"rows={rows}\n"
        f"target={target}\n"
        f"completed_at={datetime.utcnow().isoformat()}Z\n"
    )
    s3_client().put_object(Bucket=LAKE_BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"ETL_MARKER_WRITTEN: s3://{LAKE_BUCKET}/{key}")
