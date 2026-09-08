import requests
import json
import os
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

# 직전에 전송한 스냅샷의 time 값. OpenSky(특히 익명 티어)는 10초 폴링 사이에 아직 갱신되지
# 않은 "같은 스냅샷"(동일한 data['time'])을 그대로 돌려주는 경우가 있다. 이를 걸러내지 않으면
# 좌표·속도까지 완전히 동일한 레코드가 Kafka에 두 번 실려 flight_data에 중복 행으로 쌓인다.
# (2026-08-20 실측: 정상 동작 중에도 3분당 수십 쌍씩 누적 — docs/BENCHMARKS.md 참고)
_last_snapshot_time = None


def fetch_and_send():
    global _last_snapshot_time
    try:
        response = session.get(URL, params=PARAMS, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data.get('states'):
            print(f"[{datetime.now()}] 감지된 항공기 없음.")
            return

        snapshot_time = data.get('time')
        if snapshot_time is not None and snapshot_time == _last_snapshot_time:
            print(f"[{datetime.now()}] 이전과 동일한 스냅샷(time={snapshot_time}) — 전송 생략.")
            return

        states = data['states']
        for state in states:
            flight_info = {
                "icao24": state[0],
                "callsign": state[1].strip() if state[1] else "N/A",
                "origin_country": state[2],
                "time_position": state[3],
                "last_contact": state[4],
                "longitude": state[5],
                "latitude": state[6],
                "baro_altitude": state[7],
                "on_ground": state[8],
                "velocity": state[9],
                "true_track": state[10],
                "vertical_rate": state[11],
                "geo_altitude": state[13],
                "squawk": state[14],
                "timestamp": data['time']
            }
            # icao24를 파티션 키로 사용 — Kafka는 파티션 '내부' 순서만 보장하므로,
            # 키 없이 보내면 라운드로빈으로 같은 기체의 연속 위치가 6개 파티션에
            # 흩어져 기체별 시간 순서가 깨진다. (2026-09-09 실측: 2회 이상 등장한
            # 기체의 94.7%가 복수 파티션에 분산 — docs/BENCHMARKS.md 참고)
            # 콜드 패스를 궤적 예측 학습 데이터로 쓸 때 시퀀스 순서가 곧 라벨이다.
            producer.send('flight_data_raw', key=flight_info["icao24"], value=flight_info)
        
        producer.flush() # 메시지 전송 보장
        # flush 성공 후에만 갱신 — 전송에 실패했다면 다음 주기에 같은 스냅샷을 다시 시도해야 한다.
        _last_snapshot_time = snapshot_time
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