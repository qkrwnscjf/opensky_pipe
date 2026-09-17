# SkyStream: Real-time Flight Data Pipeline

Kafka, Spark, MinIO, PostgreSQL을 활용한 **실시간 항공 데이터 파이프라인** 프로젝트입니다. OpenSky Network API에서 항공기 텔레메트리를 수집해 Kafka로 스트리밍하고, Spark Structured Streaming이 이를 Hot Path(PostgreSQL, 실시간 서빙)와 Cold Path(MinIO 기반 레이크하우스, 분석·학습용)에 나눠 적재합니다. FastAPI가 REST와 WebSocket으로 최신 위치를 서빙하고, React/Leaflet 기반 관제 UI가 이를 시각화합니다.

## 아키텍처

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         OpenSky API (10초+처리시간 폴링)                      │
└──────────────────────────────────┬────────────────────────────────────────┘
                                    ▼
                          producer.py → Kafka
                     flight_data_raw (KRaft, 6파티션, key=icao24)
                                    │
              ┌─────────────────────┴─────────────────────┐
              ▼                                             ▼
┌───────────────────────────────┐          ┌───────────────────────────────────┐
│  HOT 쿼리 (3초 트리거)  [✅]      │          │  COLD 쿼리 (120초 트리거)  [✅]        │
│  from_json → _valid 플래그       │          │  from_json → _valid 필터            │
└───────────┬───────────────────┘          └───────────┬─────────────────────┘
            ▼                                           ▼
   ┌─────────────────┐                        dt=YYYY-MM-DD 파티션, coalesce(1)
   │  save_to_hot()   │                                │
   │ ① flight_data    │                                ▼
   │    append (이력)  │                    ┌──────────────────────────┐
   │ ② flight_current │                    │ MinIO — Bronze  [✅]       │
   │    UPSERT(PK     │                    │ positions/dt=.../*.parquet│
   │    icao24, 조건절) │                    │ (영구 보관, at-least-once) │
   └────┬────────┬────┘                    └────────────┬──────────────┘
        │        │                                       │
        ▼        ▼                                       │ 📋 여기부터 설계만
┌──────────┐ ┌──────────────┐                             ▼
│flight_data│ │flight_current│            ┌───────────────────────────────┐
│(이력, 1시간)│ │(상태, PK=    │            │ Airflow DAG  [📋]                │
│           │ │ icao24)      │            │ schedule_interval=None (수동)     │
└─────┬─────┘ └──────┬───────┘            │  → spark-submit spark_batch_etl.py│
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ Spark 배치 ETL  [📋]              │
      │              │                    │ Bronze dt= 파티션 읽음(spark.read) │
      │              │                    │ 피처 엔지니어링(내용 미정)          │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ MinIO — Silver/Gold  [📋]         │
      │              │                    │ Iceberg 테이블                    │
      │              │                    │ (Hadoop 카탈로그, 새 컨테이너 없음)  │
      │              │                    │ df.writeTo(...).append()          │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ DuckDB  [📋] — 저장 아님, 조회 창구  │
      │              │                    │ iceberg_scan()으로 그 자리에서 읽음  │
      │              │                    │ (새 컨테이너 없음, 임베디드)          │
      │              │                    └───────────────┬───────────────────┘
      │              │                              ┌───────┴───────┐
      │              │                              ▼               ▼
      │              │                        DA 애드혹 분석    ML 학습 스크립트
      │              │                                          (코드만, 미운영)
      ▼              ▼
┌─────────────────────────────────┐
│ backend (FastAPI)  [✅]            │
│ TRAIL_QUERY → flight_data          │
│ FLIGHTS_QUERY → flight_current     │
│ (DISTINCT ON 불필요, PK가 보장)      │
└───────────────┬─────────────────┘
                │ pg_notify → LISTEN
                ▼
        WebSocket 브로드캐스트 → flight-ui (React/Leaflet)  [✅]
```
