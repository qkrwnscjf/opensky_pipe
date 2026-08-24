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
    acks=1, # 안정성을 위해 전송 확인
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
            producer.send('flight_data_raw', value=flight_info)
        
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