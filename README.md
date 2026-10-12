# SkyStream: Real-time Flight Data Pipeline

Kafka, Spark, MinIO, PostgreSQL을 활용한 **실시간 항공 데이터 파이프라인** 프로젝트입니다. OpenSky Network API에서 항공기 텔레메트리를 수집해 Kafka로 스트리밍하고, Spark Structured Streaming이 이를 Hot Path(PostgreSQL, 실시간 서빙)와 Cold Path(MinIO 기반 레이크하우스, 분석·학습용)에 나눠 적재합니다. FastAPI가 REST와 WebSocket으로 최신 위치를 서빙하고, React/Leaflet 기반 관제 UI가 이를 시각화합니다.

## 아키텍처

```
┌───────────────────────────────────────────────────────────────────────────┐
│       OpenSky API (익명, 약 10초 — 스냅샷 시각에 맞춰 요청)                       │
└──────────────────────────────────┬────────────────────────────────────────┘
                                    ▼
                   producer.py — 이미 보낸/더 오래된 스냅샷 생략(워터마크),
                                 재시작 시 Kafka 마지막 메시지로 워터마크 복원
                                    ▼
                     Kafka flight_data_raw (KRaft, 6파티션, key=icao24, 보존 3시간)
                                    │
              ┌─────────────────────┴─────────────────────┐
              │        쿼리마다 readStream을 따로 잡는다          │
              ▼                                             ▼
┌───────────────────────────────┐          ┌───────────────────────────────────┐
│  HOT 쿼리                       │          │  COLD 쿼리                          │
│  3초 트리거 / 500건 상한          │          │  60초 트리거 / 5,000건 상한           │
│  유효성: 좌표·시각·icao24         │          │  유효성: 좌표·시각                    │
│  save_to_hot() — 파티션별 연결    │          │  dt=YYYY-MM-DD, coalesce(1)       │
│  재사용·트랜잭션 하나로 두 테이블    │          │  종료 신호 시 남은 분량 마무리 쓰기     │
│  쓰기 실패 → 60초 재시도 →        │          │  (availableNow)                    │
│  재시작 후 재처리(멱등 쓰기)        │          └───────────┬─────────────────────┘
└───────┬───────────────┬───────┘                      ▼
        ▼               ▼                     ┌──────────────────────────┐
┌──────────────┐ ┌──────────────┐             │ MinIO — Bronze            │
│ flight_data   │ │flight_current│             │ positions/dt=.../*.parquet│
│ 10컬럼,        │ │ 13컬럼,       │             │ (영구 보관, 받은 그대로)     │
│ 위치 바뀐 행만, │ │ PK icao24,   │             └────────────┬──────────────┘
│ 35분 보존      │ │ 최신값만,      │                          ▼
│ (pg_cron 10분)│ │ 30분 미관측 정리│      ┌───────────────────────────────────┐
└──────┬───────┘ └──────┬───────┘      │ Airflow (scheduler만 상주, 웹 UI는 필요 시) │
       │                │              │ flight_lakehouse_etl — 기동 시 1회 + 매일 01:00 UTC│
       │                │              │ Bronze dt= − 단계별 완료 마커 − 오늘          │
       │                │              │  → 최대 10일씩 묶어 spark-submit 하나로       │
       │                │              │ 순서: Silver → Gold(Silver D·D-1 끝난 날짜만) → 병합│
       │                │              │ 날짜별 실행 기록(etl_runs/)·코드 버전(마커)      │
       │                │              └───────────────┬───────────────────┘
       │                │                              ▼
       │                │              ┌───────────────────────────────────┐
       │                │              │ Silver — spark_batch_etl.py         │
       │                │              │ (icao24, timestamp) 중복 제거         │
       │                │              │ 품질 지표 기록, 날짜 불일치면 그 날짜 실패  │
       │                │              │ → Iceberg lake.db.flight_features   │
       │                │              │   (Hadoop 카탈로그, overwritePartitions)│
       │                │              └───────────────┬───────────────────┘
       │                │                              ▼
       │                │              ┌───────────────────────────────────┐
       │                │              │ Gold — spark_gold_etl.py            │
       │                │              │ (icao24, time_position) 재중복 제거    │
       │                │              │ 전날(D-1)을 맥락으로, 300초 공백으로 구간 분할│
       │                │              │ 이상치 표시(is_outlier), 편명은 N/A 제외   │
       │                │              │ → gold_flight_trajectory (ML)       │
       │                │              │   gold_aircraft_daily / traffic_hourly (DA)│
       │                │              └───────────────┬───────────────────┘
       │                │                              ▼
       │                │              ┌───────────────────────────────────┐
       │                │              │ Bronze 병합 — bronze_merge.py        │
       │                │              │ Silver가 끝난 지난 날짜를 하루 1파일로     │
       │                │              │ (행 수·해시 검증, 중단 후 복구)           │
       │                │              └───────────────┬───────────────────┘
       │                │                              ▼
       │                │              DuckDB(lake_duckdb.py, 조회 창구) ─ DA 분석
       │                │              ML(ml_jobs.py, 수동) ─ 기준선·HGB·Chronos → MLflow
       │                │                레지스트리·자동 승격, 재학습 버튼(ml-worker),
       │                │                서빙·실시간 채점(평소 꺼짐)
       ▼                ▼
┌─────────────────────────────────┐
│ backend (FastAPI)                 │
│ /flights → flight_current         │
│ /flights/{icao24}/trail → flight_data │
│ /health (starting·ok·stale·503)    │
└───────────────┬─────────────────┘
                │ pg_notify → LISTEN
                ▼
  WebSocket(갱신 리듬 포함) → flight-ui (React/Leaflet, 표시는 한국 시간)
```

운영 도구: `src/lake_reprocess.py`(단계·날짜별 완료 마커 삭제와 연쇄 재처리, 늦게 들어온 Bronze 파일 점검, 옛 코드 버전으로 처리된 날짜 목록 — 기본은 미리보기).

## 설계 원칙

- **진행 상태는 MinIO의 완료 마커로** — Airflow 메타데이터는 세션마다 비워지므로, "결과물이 실제로 저장소에 있는가"를 기준으로 처리할 날짜를 고릅니다. 평소 운용과 며칠치 따라잡기가 같은 경로입니다.
- **모든 쓰기는 멱등** — Postgres는 `ON CONFLICT`와 시각 조건, Iceberg는 날짜 파티션 덮어쓰기, 병합은 해시 검증. 그래서 실패한 배치를 다시 처리해도 중복·역행이 생기지 않습니다.
- **조용한 실패를 드러나는 실패로** — 쓰기 실패는 재시도 후 재시작, scheduler는 감시 루프와 헬스체크, 배치는 날짜별 실행 기록을 남깁니다.
- **영구 저장소는 두 곳** — MinIO(레이크)와 MLflow 메타데이터. 나머지(Kafka, 체크포인트, Postgres, Airflow 메타DB)는 세션마다 새로 시작합니다.
- **저장은 UTC, 한국 시간은 표시할 때만** — 화면과 DuckDB 조회에서 변환합니다.

## 실행

```
docker compose up -d --build                     # 수집 → 처리 → 서빙 전체 (DAG는 기동 시 자동 실행)
cd flight-ui && npm install && HOST=127.0.0.1 npm start   # 화면 http://localhost:3000

docker compose --profile airflow-ui up -d airflow-webserver   # (필요할 때) Airflow UI 127.0.0.1:8080
docker compose --profile mlflow-ui up -d mlflow-ui            # (필요할 때) MLflow UI 127.0.0.1:5000
docker compose run --rm ml                                     # (수동) 모델 학습·비교
docker compose --profile airflow-ui down                       # 종료 — 기본 자격 증명으로 포트가 열리므로 쓰고 나면 내린다
```

OpenSky 익명 계정은 하루 요청 한도가 있습니다. 인증 계정을 쓰려면 저장소 루트의 `.env`에 `OPENSKY_USER`·`OPENSKY_PASSWORD`를 넣습니다(git 제외 파일).

## 상태

Hot Path와 Cold Path는 스택을 띄우면 상시 가동됩니다. Bronze → Silver → Gold 배치와 Bronze 병합은 2026-09-29에 첫 실운영을 마쳤고, 스택을 띄울 때마다 DAG가 밀린 날짜를 자동으로 따라잡습니다. ML 파이프라인은 학습·평가·저장·서빙까지 동작을 확인했으며 실제 운용은 하지 않습니다.

상세한 작업 순서와 결정은 `docs/TASK_ORDER.md`, 모든 Before/After 측정치는 `docs/BENCHMARKS.md`에 있습니다(로컬 문서).
