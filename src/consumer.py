import json
from kafka import KafkaConsumer

# 'flight_data_raw' 토픽을 구독(subscribe)하는 Consumer 생성
consumer = KafkaConsumer(
    'flight_data_raw',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest', # 가장 최신 데이터부터 읽기
    # JSON 데이터를 다시 파이썬 딕셔너리로 변환
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Kafka Consumer를 시작합니다. (Topic: flight_data_raw)")

# Kafka에서 메시지를 계속 읽어와서 출력
try:
    for message in consumer:
        print(f"수신: {message.value}")
except KeyboardInterrupt:
    print("Consumer를 종료합니다.")
finally:
    consumer.close()