import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime, timezone # timezone 추가 임포트

# 1. Elasticsearch 연결
es = Elasticsearch("http://localhost:9200") 

# 2. Kafka Consumer 연결
consumer = KafkaConsumer(
    'flight_data_raw',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Kafka -> Elasticsearch 데이터 전송을 시작합니다...")

try:
    for message in consumer:
        flight_data = message.value
        
        # 데이터 수신 확인용 로그 (필요 없으면 주석 처리)
        # print(f"수신: {flight_data.get('callsign')}")

        if flight_data.get('longitude') and flight_data.get('latitude'):
            location = {
                "lon": flight_data['longitude'],
                "lat": flight_data['latitude']
            }
            
            # UTC 시간 처리 방식 수정 (Warning 해결)
            # 기존: datetime.utcfromtimestamp(...) -> 삭제
            # 변경: datetime.fromtimestamp(..., timezone.utc)
            timestamp = flight_data['last_updated']
            iso_time = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()

            doc = {
                "callsign": flight_data.get('callsign'),
                "velocity": flight_data.get('velocity'),
                "baro_altitude": flight_data.get('baro_altitude'),
                "on_ground": flight_data.get('on_ground'),
                "last_updated": iso_time, # 수정된 시간 변수 사용
                "location": location
            }
            
            today_index = f"flight-data-{datetime.now().strftime('%Y-%m-%d')}"
            
            es.index(index=today_index, document=doc)
            
            print(f"[{doc['callsign']}] ES 저장 완료: {doc['location']}")
            
except KeyboardInterrupt:
    print("스크립트를 종료합니다.")
except Exception as e:
    print(f"오류 발생: {e}")
finally:
    consumer.close()