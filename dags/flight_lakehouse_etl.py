"""Bronze → Silver 배치 ETL의 스케줄러. docs/TASK_ORDER.md 0-6 구현.

【이 DAG가 푸는 문제】

하루 한 번 돌면서 "아직 ETL 안 된 날짜"를 전부 찾아 처리한다. 어제 하루치만
처리하는 게 아니다.

그렇게 만든 이유는 이 프로젝트의 운용 방식 때문이다. 컨테이너를 켜 둔 시간에만
Bronze에 데이터가 쌓이고, 껐다 켜기를 반복한다. 즉 Bronze에 존재하는 날짜가
연속적이지 않고, 스케줄러가 꺼져 있던 날에는 DAG도 안 돈다.

  - catchup=True로 밀린 분을 메우는 방법은 못 쓴다. Airflow 메타데이터 DB가
    세션마다 초기화되므로(airflow-postgres에 볼륨 선언이 없다) "어디까지 돌았나"
    라는 기록 자체가 남지 않는다. 매 세션 start_date부터 전부 다시 돌게 되고,
    그 비용이 날마다 커진다.
  - 수동 트리거는 채택하지 않기로 했다(사용자 결정). 자동이어야 한다.

그래서 진행 상태를 Airflow 밖에 둔다. 세션을 넘어 살아남는 저장소는 MinIO뿐이니,
날짜별 완료 마커를 MinIO에 쓴다. DAG는 매번 다음을 비교한다:

    Bronze에 있는 날짜 집합  −  마커가 있는 날짜 집합  =  처리할 날짜 집합

이러면 Airflow가 아무것도 기억하지 못해도 되고, 며칠을 꺼 뒀다 켜도 첫 실행에서
밀린 날짜가 한꺼번에 따라잡힌다. 상태의 근거가 "Airflow가 그 태스크를 성공으로
기록했는가"가 아니라 "결과물이 실제로 저장소에 있는가"로 바뀐다.

【알려진 약점】 (0-6에 기록)
  - 마커 쓰기와 Iceberg 커밋이 원자적이지 않다. 쓰기 성공 후 마커 실패 시 그
    날짜를 한 번 더 처리하지만, ETL이 overwritePartitions()라 무해하다.
  - ETL 로직을 바꿔도 마커는 그대로라 과거 날짜가 다시 계산되지 않는다.
    로직 변경 시에는 마커를 수동으로 지워야 한다.
  - 오늘 날짜는 아직 수집 중이므로 제외한다. 그러지 않으면 하루의 일부만 담긴
    파티션에 마커가 붙어 나머지가 영영 누락된다.
"""

import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

LAKE_BUCKET = os.getenv("LAKE_BUCKET", "flight-data-lake")
# spark_batch_etl.py의 BRONZE_PATH와 같은 위치를 가리켜야 한다. 저쪽은 s3a:// URI,
# 여기는 boto3라 버킷과 접두사로 나눠 쓴다.
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "positions")
MARKER_PREFIX = os.getenv("ETL_MARKER_PREFIX", "etl_markers")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Iceberg 런타임. Spark 3.5 / Scala 2.12용 빌드다.
#
# Silver 쓰기는 S3FileIO가 아니라 HadoopFileIO(= s3a://)를 쓴다. 스트리밍 잡이
# 이미 쓰고 있는 hadoop-aws 설정을 그대로 재사용하기 위해서다 — 엔드포인트,
# 자격증명, path-style 설정이 한 군데에만 있으면 된다. 그래서 iceberg-aws-bundle은
# 넣지 않는다.
ICEBERG_VERSION = os.getenv("ICEBERG_VERSION", "1.6.1")
SPARK_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    ]
)

default_args = {
    "owner": "airflow",
    "retries": 1,
}


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _dates_under(client, prefix):
    """`<prefix>/dt=YYYY-MM-DD/...` 형태의 하위 디렉터리에서 날짜만 뽑는다.

    Delimiter="/"를 주면 S3가 객체 전체가 아니라 '공통 접두사'만 돌려준다.
    파티션 안의 파일 수와 무관하게 응답이 날짜 수만큼이라 가볍다.
    """
    dates = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=LAKE_BUCKET, Prefix=f"{prefix}/", Delimiter="/"
    ):
        for common in page.get("CommonPrefixes", []):
            # "positions/dt=2026-09-18/" → "2026-09-18"
            leaf = common["Prefix"].rstrip("/").split("/")[-1]
            if not leaf.startswith("dt="):
                continue
            value = leaf[len("dt="):]
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                # __HIVE_DEFAULT_PARTITION__ 등. 콜드 패스가 timestamp null 행을
                # 거르므로 나오지 않아야 하지만, 나오면 조용히 건너뛴다.
                continue
            dates.add(value)
    return dates


def _completed_dates(client):
    """마커가 **완결된** 날짜만 센다.

    dt= 디렉터리의 존재가 아니라 그 안의 _SUCCESS 객체의 존재를 본다. 마커를
    쓰다 만 상태(디렉터리만 있고 객체 없음)를 완료로 오인하지 않기 위해서다.
    """
    dates = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=LAKE_BUCKET, Prefix=f"{MARKER_PREFIX}/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) >= 3 and parts[-1] == "_SUCCESS" and parts[-2].startswith("dt="):
                dates.add(parts[-2][len("dt="):])
    return dates


with DAG(
    dag_id="flight_lakehouse_etl",
    description="Bronze(Parquet) → Silver(Iceberg) 일배치 ETL",
    default_args=default_args,
    # 매일 01:00 UTC. 어제 날짜가 완전히 닫힌 뒤에 돈다.
    schedule_interval="0 1 * * *",
    # 고정 과거 날짜. catchup=False와 짝이므로 이 값으로 과거를 소급하지는
    # 않는다. "이 날짜 이후로만 유효한 DAG"라는 표시에 가깝다.
    start_date=datetime(2026, 9, 1),
    # 밀린 구간은 Airflow가 아니라 마커 비교로 따라잡는다(위 설명).
    catchup=False,
    # 앞 실행이 아직 밀린 날짜를 돌고 있는데 다음 스케줄이 겹쳐 같은 날짜를
    # 두 번 처리하는 것을 막는다.
    max_active_runs=1,
    tags=["lakehouse", "cold-path", "silver"],
) as dag:

    @task
    def build_etl_commands() -> list:
        """처리할 날짜를 찾아 날짜별 spark-submit 명령 문자열을 만든다.

        Spark를 띄우지 않고 S3 나열만 한다 — 처리할 날짜가 없는 날(대부분)에
        JVM을 띄우지 않기 위해서다.
        """
        client = _s3()
        bronze = _dates_under(client, BRONZE_PREFIX)
        done = _completed_dates(client)

        # 오늘은 아직 수집 중이라 제외한다. 일부만 담긴 파티션에 마커가 붙으면
        # 그날의 나머지가 영구 누락된다.
        today = datetime.utcnow().strftime("%Y-%m-%d")
        pending = sorted(d for d in (bronze - done) if d < today)

        print(f"BRONZE_DATES: {sorted(bronze)}")
        print(f"COMPLETED_DATES: {sorted(done)}")
        print(f"PENDING_DATES: {pending}")

        # 반환이 빈 리스트면 아래 매핑 태스크는 인스턴스 0개로 skip된다.
        return [
            "/home/airflow/.local/bin/spark-submit "
            f"--packages {SPARK_PACKAGES} "
            f"/opt/airflow/src/spark_batch_etl.py --date {d}"
            for d in pending
        ]

    # 날짜 하나당 태스크 하나(동적 태스크 매핑). 한 잡이 여러 날짜를 처리하지
    # 않게 쪼개는 이유: 3일치 중 2일차에서 실패해도 1일차의 마커는 이미 남아
    # 재실행 때 다시 하지 않는다. 실패의 영향 범위가 하루로 갇힌다.
    run_etl = BashOperator.partial(
        task_id="run_etl_for_date",
        # 밀린 날짜가 여러 개여도 Spark 드라이버는 한 번에 하나만 뜬다.
        # 로컬 메모리 한계(spark 서비스 mem_limit 2g와 같은 호스트)를 고려한 값.
        max_active_tis_per_dag=1,
    ).expand(bash_command=build_etl_commands())
