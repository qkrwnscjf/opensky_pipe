# SkyStream — 실시간 항공 데이터 파이프라인

한반도 상공 항공기의 실시간 위치를 수집해 **실시간 관제 화면**과 **분석·학습용 레이크하우스**로 동시에 흘려보내는 데이터 파이프라인입니다.

---

## 1. Tech Stack

**Ingestion · Streaming**

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9%20(KRaft)-231F20?logo=apachekafka&logoColor=white)
![OpenSky](https://img.shields.io/badge/OpenSky%20Network-REST%20API-1F6FEB)

**Processing**

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%20Structured%20Streaming-E25A1C?logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.6-4E8EE9)
![DuckDB](https://img.shields.io/badge/DuckDB-1.1-FFF000?logo=duckdb&logoColor=black)

**Storage**

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%20%2B%20pg__cron-4169E1?logo=postgresql&logoColor=white)
![S3](https://img.shields.io/badge/S3-Data%20Lake-569A31?logo=amazons3&logoColor=white)

**Orchestration**

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)

**Serving · UI**

![FastAPI](https://img.shields.io/badge/FastAPI-REST%20%2B%20WebSocket-009688?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)
![Leaflet](https://img.shields.io/badge/Leaflet-react--leaflet%205-199900?logo=leaflet&logoColor=white)

**ML · MLOps**

![MLflow](https://img.shields.io/badge/MLflow-2.17-0194E2?logo=mlflow&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-HistGradientBoosting-F7931E?logo=scikitlearn&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-2.5%20CPU-EE4C2C?logo=pytorch&logoColor=white)
![Chronos](https://img.shields.io/badge/Chronos--Bolt-tiny-FFD21E?logo=huggingface&logoColor=black)

**Infrastructure**

![Docker](https://img.shields.io/badge/Docker%20Compose-Full%20Stack-2496ED?logo=docker&logoColor=white)

---

## 2. 프로젝트 소개

OpenSky Network가 공개하는 항공기 상태 정보(위치·고도·속도·방향)를 약 10초마다 받아, 두 갈래로 나눠 처리합니다.

- **Hot Path — 지금 하늘을 보여 주기**: 수 초 안에 PostgreSQL에 반영하고, WebSocket으로 화면에 바로 밀어 줍니다.
- **Cold Path — 쌓아서 분석하기**: 받은 그대로를 MinIO에 영구 보관(Bronze)하고, 하루 단위 배치로 정제(Silver)·가공(Gold)해 분석과 궤적 예측 학습에 씁니다.

실시간 서빙과 영구 이력을 한 흐름에서 동시에 다루는 **람다 아키텍처**를 로컬 환경(Docker Compose)에서 끝까지 구현하고, 모든 개선을 측정 전후 비교로 검증하는 것이 목표입니다.

---

## 3. 구현 기능

**실시간 관제 화면**
- 지도 위 항공기 실시간 위치(고도대별 색, 진행 방향, 지상 항공기 구분)
- 항공기 선택 시 최근 30분 궤적, 따라가기 모드
- 검색 · 국가 필터(편명 기반 항공사 국가) · 정렬 · 고정, 한국 시간 표시
- 갱신 리듬 표시(마지막 갱신 n초 전 / 다음 갱신 예상), 연결 상태 표시
- 갱신 사이에도 속도·방향으로 추정한 위치로 부드럽게 이동(끌 수 있음, 궤적은 실제 보고만)

**데이터 파이프라인**
- 중복·역순 스냅샷 차단, 항공기별 순서 보장(Kafka 키 = 항공기 ID)
- Hot/Cold 독립 스트리밍 쿼리 — 서로의 장애·속도에 영향 없음
- 멱등 쓰기 + 실패 시 재시도·재처리 → 장애가 나도 데이터 유실 없음
- 종료 시 미처리 데이터 마무리 저장, 재시작 시 체크포인트로 이어서 처리
- 수집 시각(`ingested_at`) 기록 — OpenSky 자체 지연과 파이프라인 지연을 구분

**레이크하우스 배치**
- Bronze → Silver → Gold 메달리온 구조(Iceberg 테이블)
- 스택을 켤 때마다 밀린 날짜를 자동으로 따라잡기
- 날짜별 품질 지표 · 날짜 불일치 검사 · 실행 기록 · 코드 버전 기록
- 재처리 도우미(완료 표시 삭제, 늦은 파일 점검), DuckDB로 테이블 즉시 조회

**ML · MLOps**
- 항공기 10초 뒤 위치 예측: 추측항법 기준선 vs HGB vs Chronos-Bolt 비교
- MLflow 기록 · 모델 레지스트리 · 자동 승격(champion) · 화면의 재학습 버튼
- 실시간 예측 채점과 데이터 변화(PSI — 항공기 200대 이상 모였을 때만 판정) 모니터링, 모델 리포트 페이지

---

## 4. 아키텍처

```
                        OpenSky Network API
                                │  약 10초 (스냅샷 시각에 맞춰 요청)
                                ▼
                        ┌───────────────┐
                        │  producer.py   │  중복·역순 스냅샷 차단
                        └───────┬───────┘
                                ▼
                 Kafka  flight_data_raw  (파티션 6, 키 = icao24, 보존 3시간)
                                │
             ┌──────────────────┴──────────────────┐
             ▼                                     ▼
   ┌───────────────────┐                 ┌───────────────────┐
   │ Spark  HOT 쿼리     │                 │ Spark  COLD 쿼리    │
   │ 3초마다             │                 │ 60초마다            │
   └─────────┬─────────┘                 └─────────┬─────────┘
             ▼                                     ▼
   ┌───────────────────┐                 ┌───────────────────┐
   │ PostgreSQL          │                 │ MinIO  Bronze       │
   │ flight_current 최신 │                 │ 받은 그대로 영구 보관  │
   │ flight_data   35분  │                 └─────────┬─────────┘
   └─────────┬─────────┘                           │ Airflow (켤 때 + 매일)
             │ NOTIFY                              ▼
             ▼                         Silver ─▶ Gold ─▶ Bronze 병합
   ┌───────────────────┐               (정제)    (궤적·집계)  (하루 1파일)
   │ FastAPI             │                   │        │
   │ REST + WebSocket    │                   ▼        ▼
   └─────────┬─────────┘               DuckDB 조회   ML 학습 → MLflow
             ▼                                        (모델 리포트)
      React 관제 화면  ◀───────────────────────────────┘
```

### 계층별 역할

| 계층 | 구성 요소 | 하는 일 |
| :--- | :--- | :--- |
| 수집 | `producer.py` | OpenSky 스냅샷을 받아 항공기별 메시지로 Kafka에 전송. 이미 보낸·더 오래된 스냅샷은 생략하고, 재시작해도 Kafka의 마지막 기록으로 이어 간다 |
| 스트리밍 | Kafka (KRaft) | 항공기 ID를 키로 써서 같은 항공기의 위치가 순서대로 한 파티션에 쌓인다 |
| 실시간 처리 | Spark HOT 쿼리 | 3초마다 새 메시지를 검증해 PostgreSQL 두 테이블에 한 번에 기록. 쓰기에 실패하면 재시도 후 재처리 |
| 실시간 저장 | PostgreSQL | `flight_current`(항공기당 최신 1행, 화면용), `flight_data`(위치가 바뀐 행만 35분, 궤적용). pg_cron이 10분마다 정리 |
| 서빙 | FastAPI | 새 데이터가 기록되면 NOTIFY 신호를 받아 WebSocket으로 즉시 전송 |
| 원본 보관 | Spark COLD 쿼리 → MinIO | 60초마다 받은 그대로를 날짜별 Parquet로 저장(Bronze). 종료 시 남은 분량까지 저장 |
| 배치 | Airflow + Spark | 완료 표시가 없는 날짜를 찾아 Silver(중복 제거·품질 검사) → Gold(궤적·일별·시간별 집계) → Bronze 파일 병합 |
| 조회 · 학습 | DuckDB, MLflow | Iceberg 테이블을 DuckDB로 바로 조회, Gold 궤적으로 위치 예측 모델 학습·비교·등록 |

### 설계 원칙

- **진행 상태는 저장소의 완료 표시로** — 배치는 "결과가 실제로 저장돼 있는가"로 처리할 날짜를 고릅니다. 며칠 꺼 뒀다 켜도 같은 방식으로 따라잡습니다.
- **모든 쓰기는 멱등** — 같은 데이터를 다시 써도 결과가 같아서, 실패한 작업을 안심하고 재처리할 수 있습니다.
- **조용한 실패를 드러나는 실패로** — 실패는 재시도·재시작·헬스체크·실행 기록으로 드러냅니다.
- **영구 저장소는 두 곳** — MinIO(레이크)와 MLflow 메타데이터. 나머지는 켤 때마다 새로 시작합니다.
- **저장은 UTC, 표시는 한국 시간**

---

## 5. 실행 가이드

### 5-1. 사전 준비

| 항목 | 요구 사항 |
| :--- | :--- |
| Docker Desktop | Docker Compose v2 포함. **메모리 6GB 이상 할당**(권장 8GB), 디스크 여유 약 10GB(첫 빌드 이미지) |
| Node.js | 18 이상(화면 실행용) |
| 비어 있어야 하는 포트 | 5432, 5433, 8000, 9000, 9001, 9092, 3000 (선택: 8080 Airflow UI, 5000 MLflow UI) |
| OpenSky 계정 | 없어도 됩니다(익명, 하루 요청 한도 있음). 계정이 있으면 아래 설정 파일에 넣습니다 |

선택 — 저장소 루트에 설정 파일(`.env`, git 제외)을 만들어 계정이나 비밀번호를 바꿀 수 있습니다.

```
OPENSKY_USER=계정
OPENSKY_PASSWORD=비밀번호
```

### 5-2. 파이프라인 실행

```
git clone <저장소 주소>
cd airpipe
docker compose up -d --build
```

- 첫 빌드는 Spark·Airflow 이미지 때문에 수 분 걸립니다.
- 수집 → 스트리밍 → 저장 → 서빙이 자동으로 시작되고, Airflow가 밀린 날짜 배치를 한 번 실행합니다.
- 상태 확인: `docker compose ps`, 브라우저에서 `http://localhost:8000/health` (`ok`면 데이터가 흐르는 중)

### 5-3. 화면 실행

```
cd flight-ui
npm install
HOST=127.0.0.1 npm start
```

브라우저에서 `http://localhost:3000` — 실시간 지도, 아래로 내리면 항공기 목록, 상단 **Model** 메뉴는 모델 리포트입니다.

### 5-4. 필요할 때만 켜는 것

| 목적 | 명령 | 접속 |
| :--- | :--- | :--- |
| Airflow UI | `docker compose --profile airflow-ui up -d airflow-webserver` | `127.0.0.1:8080` (admin / admin) |
| MLflow UI | `docker compose --profile mlflow-ui up -d mlflow-ui` | `127.0.0.1:5000` |
| MinIO 콘솔 | 기본 실행에 포함 | `localhost:9001` (minioadmin / minioadmin) |
| 모델 학습 · 비교 | `docker compose run --rm ml` (또는 화면의 Retrain 버튼) | 결과는 모델 리포트 |
| 실시간 예측 · 모니터링 | `docker compose --profile serving up -d ml-serving` | 모델 리포트의 모니터링 구역 |

### 5-5. 종료

```
docker compose --profile airflow-ui --profile mlflow-ui --profile serving stop
docker compose --profile airflow-ui --profile mlflow-ui --profile serving rm -fv
docker compose --profile airflow-ui --profile mlflow-ui --profile serving down
```

- 쓰고 나면 내려 주세요. 기본 비밀번호로 여러 포트가 열립니다.
- `rm -fv`는 이 프로젝트 컨테이너가 쓰던 이름 없는 볼륨(Kafka, PostgreSQL, 체크포인트)만 지웁니다. 다음 실행은 어차피 새 볼륨으로 시작하므로, 이 단계를 빼면 쓰지 않는 볼륨이 실행할 때마다 쌓입니다.
- 레이크(MinIO)와 MLflow 기록은 이름 있는 볼륨에 남습니다. `down -v`는 이것까지 지우니 쓰지 마세요.
- 실시간 데이터(Kafka, PostgreSQL)는 다음 실행 때 새로 시작합니다(의도된 설계).

---

## 6. 한계점

- **수집 범위·한도**: OpenSky 익명 계정이라 하루 요청 한도가 있고, 비용이 가장 적은 범위(한반도 주변 22 평방도)만 수집합니다.
- **로컬 전용 보안**: 기본 비밀번호와 여러 포트가 열린 상태로 동작합니다. 외부에 공개하는 배포에는 맞지 않습니다.
- **단일 노드**: Kafka 브로커 1개·복제 없음, Spark 로컬 모드 — 확장성보다 재현성을 택했습니다.
- **실시간 데이터는 세션 단위**: 스택을 내렸다 올리면 Kafka·PostgreSQL이 비워집니다(영구 이력은 MinIO에 있음).
- **ML은 운용하지 않음**: 학습·평가·서빙·모니터링까지 동작을 검증했지만 상시 운용은 하지 않습니다.
- **화면 테스트**: `npm test`는 Create React App 기본 설정이 `react-leaflet`을 읽지 못해 실패합니다.
- **측정 도구 미포함**: 성능 측정·검증 스크립트(`scripts/`)는 저장소에 올리지 않았습니다.
