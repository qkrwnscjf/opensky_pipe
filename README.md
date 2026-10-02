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
              │   쿼리마다 readStream을 따로 잡는다 (0-6b)      │
              ▼                                             ▼
┌───────────────────────────────┐          ┌───────────────────────────────────┐
│  HOT 쿼리  [✅]                  │          │  COLD 쿼리  [✅]                     │
│  3초 트리거 / 500건 상한          │          │  60초 트리거 / 5,000건 상한           │
│  = 166행/s                     │          │  = 83.3행/s                        │
│  parse_and_flag() → _valid 플래그 │          │  parse_and_flag() → _valid 필터      │
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
        ▼        ▼                                       │ ✅ 09-29 첫 실운영
┌──────────┐ ┌──────────────┐                             ▼
│flight_data│ │flight_current│            ┌───────────────────────────────┐
│(이력, 1시간)│ │(상태, PK=    │            │ Airflow DAG  [✅ 기동 시 자동 실행]  │
│ pg_cron이  │ │ icao24)      │            │ flight_lakehouse_etl            │
│ 매시 정리   │ │ 조건부 UPSERT │            │ "0 1 * * *" UTC, catchup=False  │
└─────┬─────┘ └──────┬───────┘            │ Bronze dt= − 마커 − 오늘         │
      │              │                    │  → 날짜당 1태스크 동적 매핑        │
      │              │                    │ 순서: Silver → Gold → Bronze 병합  │
      │              │                    │ (병합 [✅]: Silver가 끝난 날짜의     │
      │              │                    │  Bronze 파일을 하루 1개로)          │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ Spark 배치 ETL  [✅ 검증]          │
      │              │                    │ spark_batch_etl.py --date <날짜>  │
      │              │                    │ dropDuplicates(icao24,timestamp) │
      │              │                    │ event_date 부여 → 성공 후 마커     │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ MinIO — Silver (Iceberg)  [✅]    │
      │              │                    │ lake.db.flight_features          │
      │              │                    │ Hadoop 카탈로그(새 컨테이너 없음)   │
      │              │                    │ PARTITIONED BY (event_date)      │
      │              │                    │ .overwritePartitions() ← 멱등     │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ Gold 배치  [✅ 검증]               │
      │              │                    │ spark_gold_etl.py --date <날짜>   │
      │              │                    │ (icao24, time_position)로 재중복제거│
      │              │                    │ 전날(D-1)을 맥락으로 읽어 자정 연결  │
      │              │                    │ 300초 공백 기준 비행 구간 분할       │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ MinIO — Gold (Iceberg)  [✅]      │
      │              │                    │ gold_flight_trajectory  (ML)     │
      │              │                    │ gold_aircraft_daily     (DA)     │
      │              │                    │ gold_traffic_hourly     (DA)     │
      │              │                    └───────────────┬───────────────────┘
      │              │                                    ▼
      │              │                    ┌───────────────────────────────┐
      │              │                    │ DuckDB  [✅] lake_duckdb.py       │
      │              │                    │ DuckDB는 저장이 아니라 조회 창구     │
      │              │                    │ iceberg_scan()으로 그 자리에서 읽음  │
      │              │                    │ (새 컨테이너 없음, 임베디드)          │
      │              │                    └───────────────┬───────────────────┘
      │              │                              ┌───────┴───────┐
      │              │                              ▼               ▼
      │              │                        DA 애드혹 분석    ML 학습 [✅ 검증]
      │              │                        (예시 쿼리)      ml_train_trajectory.py
      │              │                                         다음 위치 예측 → MinIO ml/
      │              │                                         (수동 실행, 운용 안 함)
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

**범례** — `[✅]` 구현·검증 완료

Hot Path와 Cold Path의 Bronze 적재는 상시 가동됩니다. Bronze → Silver → Gold 배치와 Bronze 병합은 2026-09-29에 첫 실운영을 마쳤고, 스택을 띄우면 DAG가 켜진 채 등록되어 밀린 날짜를 자동으로 따라잡습니다. DuckDB는 Iceberg 테이블을 그 자리에서 조회하고, ML 파이프라인은 학습·평가·저장까지 동작을 확인했으며 실제 운용은 하지 않습니다.

상세한 작업 순서와 상태는 `docs/TASK_ORDER.md`, 모든 Before/After 측정치는 `docs/BENCHMARKS.md`에 있습니다.
