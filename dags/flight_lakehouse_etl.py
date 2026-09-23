"""Bronze → Silver → Gold 배치 ETL의 스케줄러. docs/TASK_ORDER.md 0-6 구현.

  build_silver_commands → run_silver_for_date (날짜별, Bronze → Silver)
                                   ↓ (전부 끝난 뒤)
  build_gold_commands   → run_gold_for_date   (날짜별, Silver → Gold)
                                   ↓ (결과와 무관하게 끝난 뒤)
  build_merge_commands  → run_bronze_merge_for_date (날짜별, Bronze 파일 병합)

단계마다 마커를 따로 둔다(etl_markers/, etl_markers_gold/, etl_markers_bronze_merge/).
그래서 한 단계만 실패한 날짜도 다음 실행에서 그 단계만 따라잡는다.

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
# 단계별 완료 마커. 스크립트 쪽 기본값과 같아야 한다
# (spark_batch_etl.MARKER_PREFIX, spark_gold_etl.GOLD_MARKER_PREFIX).
MARKER_PREFIX = os.getenv("ETL_MARKER_PREFIX", "etl_markers")
GOLD_MARKER_PREFIX = os.getenv("GOLD_MARKER_PREFIX", "etl_markers_gold")
MERGE_MARKER_PREFIX = os.getenv("BRONZE_MERGE_MARKER_PREFIX", "etl_markers_bronze_merge")

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


def _completed_dates(client, marker_prefix):
    """마커가 **완결된** 날짜만 센다.

    dt= 디렉터리의 존재가 아니라 그 안의 _SUCCESS 객체의 존재를 본다. 마커를
    쓰다 만 상태(디렉터리만 있고 객체 없음)를 완료로 오인하지 않기 위해서다.

    접두사 끝에 "/"를 붙여 나열하므로 etl_markers/와 etl_markers_gold/가 섞이지 않는다.
    """
    dates = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=LAKE_BUCKET, Prefix=f"{marker_prefix}/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) >= 3 and parts[-1] == "_SUCCESS" and parts[-2].startswith("dt="):
                dates.add(parts[-2][len("dt="):])
    return dates


def _pending(marker_prefix, label):
    """Bronze에 있는 날짜 − 이 단계의 마커가 있는 날짜 − 오늘.

    Silver와 Gold가 **같은 Bronze 날짜 집합**을 기준으로 삼는다. Gold를 Silver 마커
    기준으로 고르면, 이번 실행에서 막 Silver를 처리할 날짜가 아직 마커가 없어 Gold
    대상에서 빠진다(하루 늦게 따라잡게 됨). Bronze 기준이면 같은 실행 안에서 둘 다 된다.
    """
    client = _s3()
    bronze = _dates_under(client, BRONZE_PREFIX)
    done = _completed_dates(client, marker_prefix)

    # 오늘은 아직 수집 중이라 제외한다. 일부만 담긴 파티션에 마커가 붙으면
    # 그날의 나머지가 영구 누락된다.
    today = datetime.utcnow().strftime("%Y-%m-%d")
    pending = sorted(d for d in (bronze - done) if d < today)

    print(f"[{label}] BRONZE_DATES: {sorted(bronze)}")
    print(f"[{label}] COMPLETED_DATES: {sorted(done)}")
    print(f"[{label}] PENDING_DATES: {pending}")
    return pending


def _pending_merge():
    """병합 대상 = Silver 마커가 있는 날짜 − 병합 마커가 있는 날짜 − 오늘.

    Bronze 기준이 아니라 **Silver 완료** 기준이다. Silver가 그 날짜의 Bronze를 다
    읽었다는 것이 병합해도 되는 조건이기 때문이다. 오늘은 스트리밍이 아직 쓰고 있다.
    Bronze 디렉터리가 실제로 남아 있는 날짜로 한 번 더 좁힌다.
    """
    client = _s3()
    bronze = _dates_under(client, BRONZE_PREFIX)
    silver_done = _completed_dates(client, MARKER_PREFIX)
    merged = _completed_dates(client, MERGE_MARKER_PREFIX)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    pending = sorted(d for d in ((silver_done & bronze) - merged) if d < today)

    print(f"[merge] SILVER_DONE: {sorted(silver_done)}")
    print(f"[merge] MERGED: {sorted(merged)}")
    print(f"[merge] PENDING_DATES: {pending}")
    return pending


def _spark_submit(script, date):
    return (
        "/home/airflow/.local/bin/spark-submit "
        f"--packages {SPARK_PACKAGES} "
        f"/opt/airflow/src/{script} --date {date}"
    )


with DAG(
    dag_id="flight_lakehouse_etl",
    description="Bronze(Parquet) → Silver(Iceberg) → Gold(Iceberg) 일배치 ETL",
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
    tags=["lakehouse", "cold-path", "silver", "gold", "bronze-merge"],
) as dag:

    # Spark를 띄우지 않고 S3 나열만 한다 — 처리할 날짜가 없는 날(대부분)에
    # JVM을 띄우지 않기 위해서다. 반환이 빈 리스트면 매핑 태스크는 인스턴스 0개로 skip된다.
    @task
    def build_silver_commands() -> list:
        return [_spark_submit("spark_batch_etl.py", d) for d in _pending(MARKER_PREFIX, "silver")]

    @task
    def build_gold_commands() -> list:
        return [_spark_submit("spark_gold_etl.py", d) for d in _pending(GOLD_MARKER_PREFIX, "gold")]

    # 날짜 하나당 태스크 하나(동적 태스크 매핑). 한 잡이 여러 날짜를 처리하지
    # 않게 쪼개는 이유: 3일치 중 2일차에서 실패해도 1일차의 마커는 이미 남아
    # 재실행 때 다시 하지 않는다. 실패의 영향 범위가 하루로 갇힌다.
    #
    # max_active_tis_per_dag=1: 밀린 날짜가 여러 개여도 Spark 드라이버는 한 번에
    # 하나만 뜬다. 로컬 메모리 한계(spark 서비스 mem_limit 2g와 같은 호스트)와
    # Hadoop 카탈로그의 단일 writer 전제를 함께 지킨다.
    run_silver = BashOperator.partial(
        task_id="run_silver_for_date",
        max_active_tis_per_dag=1,
    ).expand(bash_command=build_silver_commands())

    # Gold는 **모든** Silver 태스크가 끝난 뒤에 돈다. 날짜 D의 Gold가 D-1의 Silver를
    # 맥락으로 읽기 때문에, 날짜별로 Silver→Gold를 번갈아 돌리는 것보다 Silver를 먼저
    # 다 채우는 편이 정확하다.
    #
    # trigger_rule="none_failed": Silver가 이미 다 끝나 있으면 run_silver는 인스턴스
    # 0개로 skipped가 된다. 기본값(all_success)이면 그때 Gold까지 skip되어, "Silver는
    # 됐는데 Gold가 실패한 날짜"를 영영 따라잡지 못한다. none_failed는 skipped는
    # 통과시키고, Silver가 하나라도 실패하면 Gold를 막는다 — Silver가 불완전한 채로
    # Gold를 만들지 않는다.
    run_gold = BashOperator.partial(
        task_id="run_gold_for_date",
        max_active_tis_per_dag=1,
        trigger_rule="none_failed",
    ).expand(bash_command=build_gold_commands())

    # Bronze 병합. Gold 뒤에 한 줄로 잇는 이유: max_active_tis_per_dag=1은 **태스크
    # 하나 단위** 제한이라, 병합을 Silver 바로 뒤에 두면 Gold와 동시에 돌아 Spark가
    # 두 개 뜬다. 한 줄로 이어야 "Spark는 한 번에 하나"가 지켜진다.
    #
    # 대상 목록을 Gold가 끝난 **뒤에** 만든다. 시작 시점에 만들면 이번 실행에서 막
    # Silver가 끝난 날짜가 아직 마커가 없어 빠지고, 병합이 하루 늦어진다.
    #
    # trigger_rule="all_done": 병합은 Gold의 성공과 무관하다(Gold는 Bronze를 읽지
    # 않는다). 대상을 "Silver 마커가 있는 날짜"로 골라 두었으므로, 앞 단계가 일부
    # 실패했어도 병합해도 되는 날짜만 처리된다.
    @task(trigger_rule="all_done")
    def build_merge_commands() -> list:
        return [_spark_submit("bronze_merge.py", d) for d in _pending_merge()]

    merge_commands = build_merge_commands()
    run_merge = BashOperator.partial(
        task_id="run_bronze_merge_for_date",
        max_active_tis_per_dag=1,
    ).expand(bash_command=merge_commands)

    run_silver >> run_gold >> merge_commands
