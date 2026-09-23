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

from pyspark.sql.functions import lit

from lakehouse_common import build_spark, validate_date, write_marker, write_partition

# ---------------------------------------------------------------
# 설정
# ---------------------------------------------------------------
# Bronze: 스트리밍 콜드 패스가 쓰는 곳 (spark_dual_write.py의 MINIO_COLD_PATH와 같은 값)
BRONZE_PATH = os.getenv("MINIO_COLD_PATH", "s3a://flight-data-lake/positions")
# Silver: Iceberg 테이블. Hadoop 카탈로그라 warehouse 아래 디렉터리로 존재한다.
# SparkSession·카탈로그 설정은 Gold와 같아야 하므로 lakehouse_common.py에 있다.
SILVER_TABLE = os.getenv("SILVER_TABLE", "lake.db.flight_features")

# 마커. DAG가 "이 날짜는 이미 했다"를 판단하는 유일한 근거다.
MARKER_PREFIX = os.getenv("ETL_MARKER_PREFIX", "etl_markers")


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver 일배치 ETL")
    parser.add_argument("--date", required=True, help="처리할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    validate_date(args.date)

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
        # 이 DataFrame에 있는 파티션(= event_date 하루)만 교체한다. 테이블이
        # 없으면 event_date 파티션 명세로 만든다(lakehouse_common.write_partition).
        mode = write_partition(df_silver, SILVER_TABLE, "event_date")
        print(f"ETL_WRITE: {mode} {SILVER_TABLE} event_date={date_str} rows={dedup_count}")

        # ── 5. 마커 ─────────────────────────────────────────────
        # 반드시 쓰기 성공 뒤. 위에서 예외가 나면 여기 도달하지 않는다.
        write_marker(MARKER_PREFIX, date_str, dedup_count, SILVER_TABLE)
        print(f"ETL_DONE: date={date_str} rows={dedup_count}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
