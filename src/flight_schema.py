"""Kafka `flight_data_raw` 메시지 스키마의 **단일 출처**.

이 파일이 생기기 전에는 producer의 dict와 spark_dual_write의 StructType이 따로
관리됐고, 실제로 어긋나 있었다 — 2026-09-10 실측: producer는 15개 필드를 보내는데
스키마는 18개를 선언해 `sensors`/`spi`/`position_source`가 **30,528행 전부 null**로
적재되고 있었다. Postgres와 MinIO 콜드 패스(B-2 학습 데이터) 양쪽에 빈 컬럼 3개가
쌓인 셈이다.

학습 데이터에서 스키마 드리프트는 조용히 라벨 품질을 망친다. 그래서 필드 정의와
레코드 생성을 모두 여기로 모으고, 양쪽이 이 파일에서만 읽게 한다.

**pyspark를 import하지 않는다** — producer 이미지(python:3.11-slim)에는 pyspark가
없다. 타입은 문자열로 두고 spark 쪽에서 StructField로 번역한다.
"""

# (필드명, 타입, OpenSky /states/all state 벡터 인덱스)
# 인덱스가 None이면 state 벡터가 아니라 응답 최상위(data['time'])에서 온다.
# 인덱스는 OpenSky API 문서의 state vector 순서를 그대로 따른다.
FLIGHT_FIELDS = [
    ("icao24",          "string",  0),
    ("callsign",        "string",  1),
    ("origin_country",  "string",  2),
    ("time_position",   "long",    3),
    ("last_contact",    "long",    4),
    ("longitude",       "double",  5),
    ("latitude",        "double",  6),
    ("baro_altitude",   "double",  7),
    ("on_ground",       "boolean", 8),
    ("velocity",        "double",  9),
    ("true_track",      "double",  10),
    ("vertical_rate",   "double",  11),
    ("sensors",         "string",  12),
    ("geo_altitude",    "double",  13),
    ("squawk",          "string",  14),
    ("spi",             "boolean", 15),
    ("position_source", "int",     16),
    ("timestamp",       "long",    None),
]

FIELD_NAMES = [name for name, _, _ in FLIGHT_FIELDS]


def build_record(state, snapshot_time):
    """OpenSky state 벡터 하나를 Kafka에 실을 dict로 만든다.

    변환 규칙도 여기 둔다. producer에만 있으면 스키마와 또 갈라진다.
    """
    record = {}
    for name, _type, idx in FLIGHT_FIELDS:
        if idx is None:
            record[name] = snapshot_time
        else:
            record[name] = state[idx] if idx < len(state) else None

    # callsign은 공백 패딩이 붙어 오고 빈 문자열도 흔하다.
    # isinstance로 거르는 이유: 예상 못 한 타입이 오면 .strip()에서 예외가 나고,
    # 그 예외가 폴링 주기 전체를 날린다 — 레코드 하나 때문에 100대분이 버려진다.
    callsign = record.get("callsign")
    record["callsign"] = callsign.strip() if isinstance(callsign, str) and callsign.strip() else "N/A"

    # sensors는 배열로 오는데 스키마상 문자열이다. 익명 티어에서는 거의 null.
    sensors = record.get("sensors")
    if isinstance(sensors, (list, tuple)):
        record["sensors"] = ",".join(str(x) for x in sensors) or None

    return record
