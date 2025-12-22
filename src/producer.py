import requests
import json
from kafka import KafkaProducer # KafkaProducer 임포트
from datetime import datetime
import time # time.sleep을 위해 임포트

# 1. Kafka Producer 설정
# Kafka 서버 주소 (Docker로 실행했으므로 localhost:9092)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # 데이터를 JSON 형식으로 직렬화 (bytes로 변환)
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

# 2. API 설정 (이전과 동일)
LAT_MIN = 33.0
LAT_MAX = 39.0
LON_MIN = 124.5
LON_MAX = 132.0
url = f"https://opensky-network.org/api/states/all?lamin={LAT_MIN}&lomin={LON_MIN}&lamax={LAT_MAX}&lomax={LON_MAX}"

print("Kafka Producer를 시작합니다. 10초마다 데이터를 전송합니다...")

# 3. 무한 루프를 돌면서 데이터 전송
try:
    while True:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data['states']:
            for state in data['states']:
                # 전송할 데이터 가공 (필요한 정보만 선택)
                flight_info = {
                    "icao24": state[0],
                    "callsign": state[1].strip() if state[1] else "N/A",
                    "longitude": state[5],
                    "latitude": state[6],
                    "baro_altitude": state[7],
                    "on_ground": state[8],
                    "velocity": state[9],
                    "true_track": state[10],
                    "vertical_rate": state[11],
                    "last_updated": data['time'] # 수집된 시간
                }
                
                # Kafka의 'flight_data_raw' 토픽으로 데이터 전송
                producer.send('flight_data_raw', value=flight_info)
                
            print(f"[{datetime.now()}] {len(data['states'])}개의 항공기 정보를 Kafka로 전송했습니다.")
        else:
            print(f"[{datetime.now()}] 감지된 항공기 없음.")
            
        # 10초 대기 (API에 과도한 요청 방지)
        time.sleep(10) 

except KeyboardInterrupt:
    print("Producer를 종료합니다.")
except Exception as e:
    print(f"오류 발생: {e}")
finally:
    producer.close() # Producer 종료