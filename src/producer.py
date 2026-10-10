import requests
import json
import os
from flight_schema import build_record
from kafka import KafkaProducer
from datetime import datetime
import time

# 1. 환경 변수 및 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")

# 1크레딧 최적화 범위 (Area < 25 sq deg)
# 대한민국 중심부: Lat(33~38.5), Lon(126~130) -> 5.5 * 4 = 22 sq deg
LAT_MIN, LAT_MAX = 33.0, 38.5
LON_MIN, LON_MAX = 126.0, 130.0

URL = "https://opensky-network.org/api/states/all"
PARAMS = {
    "lamin": LAT_MIN,
    "lomin": LON_MIN,
    "lamax": LAT_MAX,
    "lomax": LON_MAX
}

# 2. Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # 키는 icao24(기체 식별자). 키가 있으면 기본 파티셔너가 murmur2 해시로
    # 파티션을 고르므로 같은 기체는 항상 같은 파티션에 실린다.
    key_serializer=lambda k: k.encode('utf-8') if k is not None else None,
    acks=1, # 안정성을 위해 전송 확인
    # 파티션 키만으로는 순서가 보장되지 않는다. kafka-python 기본값
    # max_in_flight_requests_per_connection=5에서 retries를 켜면, 실패한 요청이
    # 뒤이어 성공한 요청보다 '나중에' 재전송되어 같은 파티션 안에서 순서가 뒤집힌다.
    # 1로 낮추면 요청이 직렬화되어 재정렬 자체가 불가능해진다.
    #
    # retries=0(기본)으로 두면 재정렬은 없지만 전송 실패가 조용한 유실이 된다.
    # 궤적 예측 학습 데이터에서는 결측이 순서 붕괴만큼 치명적이므로 재시도는 필요하다.
    # kafka-python은 idempotent producer를 지원하지 않아, 순서를 지키면서 재시도하는
    # 수단은 이 조합뿐이다. 폴링당 메시지가 수십 건 수준이라 직렬화 비용은 무시 가능.
    retries=5,
    max_in_flight_requests_per_connection=1,
    api_version=(2, 5, 0) # API 버전 명시
)

# 3. API 세션 설정 (성능 최적화)
session = requests.Session()
if OPENSKY_USER and OPENSKY_PASSWORD:
    session.auth = (OPENSKY_USER, OPENSKY_PASSWORD)
    print(f"인증된 계정({OPENSKY_USER})으로 수집을 시작합니다.")
else:
    print("익명 계정으로 수집을 시작합니다. (일일 제한 주의)")

# 지금까지 전송에 성공한 스냅샷 time의 최댓값(워터마크). 이보다 같거나 이른 스냅샷은 보내지 않는다.
#
# OpenSky(특히 익명 티어)는 10초 폴링 사이에 아직 갱신되지 않은 "같은 스냅샷"을 그대로 돌려주는
# 경우가 있다(2026-08-20 실측: 3분당 수십 쌍 중복). 처음에는 "직전 스냅샷과 같으면 생략"만 했는데,
# 2026-10-10 점검에서 그걸로는 부족하다는 것이 드러났다:
#   - Bronze 7일 중 3일에서 "스냅샷 하나의 항공기 전부가 정확히 2번" 들어간 경우가 8건.
#   - OpenSky 응답이 서버마다 엇갈려 **더 오래된 스냅샷이 나중에 도착**하는 일이 실제로 관측됨
#     (1초 차, Kafka 6개 파티션에서 동시에 역행).
#   - 그러면 T → X(더 오래된 것) → T 순서로 오고, 세 번째 T는 "직전(X)과 다르다"고 판단돼 다시 전송된다.
# 워터마크 하나로 이 두 가지(같은 스냅샷 재등장, 오래된 스냅샷 늦게 도착)를 함께 막는다. 기억하는 것은
# 정수 하나라 메모리 사용이 늘지 않는다(최근 N개를 기억하는 방식은 N이 커질수록 늘고, N을 넘는 엇갈림은
# 놓친다). 오래된 스냅샷을 버려도 잃는 정보는 없다 — 그보다 새 위치가 이미 나갔다. 오히려 그걸 보내면
# 한 파티션 안에서 기체별 시간이 거꾸로 가 A-2(기체별 순서 보장)를 깬다.
# 막지 못하는 경우: producer 재시작 직후(워터마크가 비어 있음), Kafka가 응답을 잃고 요청을 재시도한 경우.
# 둘 다 드물고, 남은 중복은 Silver 중복 제거·flight_current 기본키·궤적 조회 DISTINCT ON이 흡수한다.
_sent_watermark = None
_skipped_repeat_total = 0
_skipped_older_total = 0


def snapshot_decision(snapshot_time, watermark):
    """'send' | 'repeat'(이미 보낸 것과 같음) | 'older'(이미 보낸 것보다 이름)."""
    if snapshot_time is None or watermark is None:
        return "send"
    if snapshot_time == watermark:
        return "repeat"
    if snapshot_time < watermark:
        return "older"
    return "send"

# icao24가 없는 레코드는 key_serializer가 None을 돌려주어 라운드로빈으로 '안전하게'
# 강등된다 — 예외로 수집이 끊기지 않는다. 문제는 그게 **조용하다**는 것이다.
#
# 강등된 레코드는 기체별 순서 보장(A-2)에서 빠지고, 그대로 콜드 패스에 실려
# B-2 학습 데이터에 섞인다. 시퀀스 순서가 곧 라벨인데 순서가 깨진 레코드가
# 표시 없이 들어가는 셈이다. 스키마가 바뀌어 icao24 필드명이 달라지기라도 하면
# 전량이 조용히 강등되는데 아무도 모른다.
#
# 그래서 센다. 로그는 폴링마다가 아니라 '발생했을 때만' 남긴다 — 정상일 때
# 0을 계속 찍으면 아무도 읽지 않게 되고, 그러면 카운터가 있으나 마나다.
_null_key_total = 0


def fetch_and_send():
    global _sent_watermark, _null_key_total, _skipped_repeat_total, _skipped_older_total
    null_key_batch = 0
    try:
        response = session.get(URL, params=PARAMS, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data.get('states'):
            print(f"[{datetime.now()}] 감지된 항공기 없음.")
            return

        snapshot_time = data.get('time')
        decision = snapshot_decision(snapshot_time, _sent_watermark)
        if decision == "repeat":
            _skipped_repeat_total += 1
            print(f"[{datetime.now()}] 이전과 동일한 스냅샷(time={snapshot_time}) — 전송 생략.")
            return
        if decision == "older":
            # 원인 판별용 기록(2026-10-10): 오래된 스냅샷이 늦게 온 사례만 따로 센다. 발생할 때만 남긴다.
            _skipped_older_total += 1
            print(f"SNAPSHOT_OLDER_SKIPPED time={snapshot_time} watermark={_sent_watermark} "
                  f"behind_s={_sent_watermark - snapshot_time} total={_skipped_older_total}")
            return

        states = data['states']
        for state in states:
            # 필드 정의와 변환 규칙은 src/flight_schema.py 한 곳에만 있다.
            # 여기에 dict를 직접 쓰면 Spark의 StructType과 또 갈라진다
            # (2026-09-10 실측: 3개 필드가 30,528행 전부 null이었다).
            flight_info = build_record(state, data['time'])
            # icao24를 파티션 키로 사용 — Kafka는 파티션 '내부' 순서만 보장하므로,
            # 키 없이 보내면 라운드로빈으로 같은 기체의 연속 위치가 6개 파티션에
            # 흩어져 기체별 시간 순서가 깨진다. (2026-09-09 실측: 2회 이상 등장한
            # 기체의 94.7%가 복수 파티션에 분산 — docs/BENCHMARKS.md 참고)
            # 콜드 패스를 궤적 예측 학습 데이터로 쓸 때 시퀀스 순서가 곧 라벨이다.
            if not flight_info["icao24"]:
                _null_key_total += 1
                null_key_batch += 1
            producer.send('flight_data_raw', key=flight_info["icao24"], value=flight_info)

        if null_key_batch:
            print(f"NULL_KEY_WARNING: icao24 없는 레코드 {null_key_batch}건을 "
                  f"키 없이(라운드로빈) 전송했습니다 — 기체별 순서 보장에서 제외됨. "
                  f"누적 {_null_key_total}건")

        producer.flush() # 메시지 전송 보장
        # flush 성공 후에만 갱신 — 전송에 실패했다면 다음 주기에 같은 스냅샷을 다시 시도해야 한다.
        # 'send' 판정은 워터마크보다 늦은 경우뿐이라 그대로 덮어써도 최댓값이 유지된다.
        if snapshot_time is not None:
            _sent_watermark = snapshot_time
        print(f"[{datetime.now()}] {len(states)}개의 항공기 정보를 전송했습니다.")

    except Exception as e:
        print(f"데이터 수집 중 오류 발생: {e}")

if __name__ == "__main__":
    print(f"수집 범위: Lat({LAT_MIN}~{LAT_MAX}), Lon({LON_MIN}~{LON_MAX})")
    try:
        while True:
            fetch_and_send()
            time.sleep(10) # 10초 주기로 수집
    except KeyboardInterrupt:
        print("Producer 종료")
    finally:
        producer.close()