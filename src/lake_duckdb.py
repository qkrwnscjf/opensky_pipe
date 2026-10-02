"""DuckDB로 Iceberg 레이크를 그 자리에서 조회한다. (0-6d)

DuckDB는 **저장소가 아니라 조회 창구**다. 데이터를 옮기거나 복사하지 않고, Spark가 쓴
Iceberg 테이블(Hadoop 카탈로그)의 메타데이터 파일을 직접 읽는다. 서버가 아니라 프로세스
안에서 도는 임베디드 엔진이라 새 컨테이너가 필요 없다 — 0-6 설계에서 Trino 대신 DuckDB를
고른 이유(동시 사용자 없음, 인프라 무게, OOM 전례)가 그대로 여기 반영된다.

  python src/lake_duckdb.py counts      # 네 테이블의 행 수
  python src/lake_duckdb.py examples    # DA 애드혹 분석 예시 쿼리

다른 스크립트(ML 학습 등)는 connect()와 scan()만 가져다 쓴다.

【Hadoop 카탈로그를 DuckDB가 읽을 수 있는 이유】
Hadoop 카탈로그는 테이블 폴더 안의 `metadata/version-hint.text`가 최신 메타데이터 버전을
가리킨다. DuckDB의 iceberg_scan('<테이블 폴더>')은 바로 이 규약을 따라 최신 스냅샷을 찾는다.
카탈로그 서버(REST·Hive)가 필요했다면 Hadoop 카탈로그를 고를 수 없었다(0-6 논의 참고).

【쓰기는 하지 않는다】
Iceberg 쓰기는 Spark 배치만 한다(Hadoop 카탈로그는 writer 하나를 전제한다). 여기서는
읽기만 한다.
"""

import os
import sys

import duckdb

WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://flight-data-lake/warehouse")

TABLES = {
    "silver": "flight_features",
    "trajectory": "gold_flight_trajectory",
    "aircraft_daily": "gold_aircraft_daily",
    "traffic_hourly": "gold_traffic_hourly",
}


def _quote(value):
    """SQL 문자열 리터럴로 감싼다. CREATE SECRET은 바인딩 파라미터를 받지 않는다."""
    return "'" + str(value).replace("'", "''") + "'"


def connect():
    """MinIO와 Iceberg를 읽을 수 있게 설정된 DuckDB 연결(메모리 내).

    httpfs·iceberg 확장은 이미지 빌드 때 미리 설치해 둔다(Dockerfile). 실행할 때마다
    인터넷에서 받지 않게 하기 위해서다.

    자격증명은 컨테이너에 이미 주어진 값(MINIO_ACCESS_KEY 등)을 그대로 쓰고, 출력하지 않는다.
    """
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute("LOAD iceberg")

    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    host = endpoint.split("://", 1)[-1]
    use_ssl = endpoint.startswith("https://")
    con.execute(
        "CREATE SECRET lake ("
        " TYPE S3,"
        f" KEY_ID {_quote(os.getenv('MINIO_ACCESS_KEY', 'minioadmin'))},"
        f" SECRET {_quote(os.getenv('MINIO_SECRET_KEY', 'minioadmin'))},"
        f" ENDPOINT {_quote(host)},"
        " URL_STYLE 'path',"
        f" USE_SSL {'true' if use_ssl else 'false'},"
        " REGION 'us-east-1'"
        ")"
    )
    return con


def table_path(name):
    return f"{WAREHOUSE}/db/{TABLES[name]}"


def scan(name):
    """FROM 절에 그대로 넣을 수 있는 iceberg_scan(...) 식."""
    return f"iceberg_scan({_quote(table_path(name))})"


EXAMPLES = {
    "시간대·국가별 교통량 (상위 10)": f"""
        SELECT event_date, hour_utc, origin_country, aircraft_count, airborne_aircraft, obs_count
        FROM {scan('traffic_hourly')}
        ORDER BY aircraft_count DESC
        LIMIT 10
    """,
    "하루 비행 거리 상위 기체 (상위 10)": f"""
        SELECT event_date, icao24, callsign, origin_country,
               round(distance_m / 1000, 1) AS distance_km, segment_count, obs_count
        FROM {scan('aircraft_daily')}
        ORDER BY distance_m DESC
        LIMIT 10
    """,
    "고도대별 계산 속도와 보고 속도": f"""
        SELECT CASE WHEN baro_altitude < 3000 THEN '1) <3,000m'
                    WHEN baro_altitude < 9000 THEN '2) 3,000-9,000m'
                    ELSE '3) >=9,000m' END AS altitude_band,
               count(*) AS points,
               round(median(calc_speed_mps), 1) AS calc_speed_p50,
               round(median(velocity), 1) AS reported_speed_p50
        FROM {scan('trajectory')}
        WHERE dt_s IS NOT NULL AND dt_s <= 60 AND baro_altitude IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """,
}


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "counts"
    con = connect()
    if cmd == "counts":
        for name in TABLES:
            n = con.execute(f"SELECT count(*) FROM {scan(name)}").fetchone()[0]
            dates = con.execute(
                f"SELECT string_agg(DISTINCT CAST(event_date AS VARCHAR), ',' ORDER BY CAST(event_date AS VARCHAR)) FROM {scan(name)}"
            ).fetchone()[0]
            print(f"DUCKDB_COUNT {TABLES[name]} rows={n} event_dates={dates}")
    elif cmd == "examples":
        for title, sql in EXAMPLES.items():
            print(f"\n## {title}")
            print(con.execute(sql).df().to_string(index=False))
    else:
        raise SystemExit(f"unknown command: {cmd}")


if __name__ == "__main__":
    main()
