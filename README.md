# SkyStream: Real-time Flight Data Pipeline

Kafka, Spark, MinIO, PostgreSQL을 활용한 **실시간 항공 데이터 파이프라인** 프로젝트입니다. OpenSky Network API에서 항공기 텔레메트리를 10초 주기로 수집해 Kafka로 스트리밍하고, Spark Structured Streaming이 이를 Hot Path(PostgreSQL, 실시간 서빙)와 Cold Path(MinIO/Parquet, 영구 이력)에 동시 적재합니다. FastAPI가 REST와 WebSocket으로 최신 위치를 서빙하고, React/Leaflet 기반 관제 UI가 이를 시각화합니다.

## 핵심 특징

- **Lambda Architecture** — 실시간 처리(Speed Layer)와 영구 이력 보관(Batch Layer)을 하나의 파이프라인에서 동시에 처리
- **완전 자동화** — `docker-compose up` 한 번으로 수집부터 서빙까지 전 구간이 자동 기동 (수동 단계 없음)
- **실시간 푸시** — 10초 Polling이 아니라 Postgres `LISTEN`/`NOTIFY` 기반 WebSocket으로 밀리초 단위 반영
- **실측 기반 트러블슈팅** — 발생한 문제마다 Before/After 수치를 직접 측정해 기록 (아래 트러블슈팅 섹션 참고)

---

## 아키텍처

### 데이터 흐름

1. **수집 (Ingestion)** — `producer.py`가 OpenSky Network API를 10초 주기로 폴링해 항공기 텔레메트리(위치·고도·속도 등)를 가져옴
2. **메시징 (Messaging)** — 수집한 데이터를 Kafka `flight_data_raw` 토픽에 발행
3. **처리 (Processing)** — Spark Structured Streaming(`spark_dual_write.py`)이 10초 마이크로배치로 토픽을 구독해 파싱·정제
4. **저장 (Storage)** — 정제된 데이터를 **Dual Write** 방식으로 두 저장소에 동시 적재
   - **Hot Path** — PostgreSQL `flight_data` 테이블에 append (실시간 서빙용)
   - **Cold Path** — MinIO(S3 호환)에 Parquet 파일로 append (영구 이력 보관용)
5. **서빙 (Serving)** — FastAPI 백엔드가 REST(`GET /flights`)와 WebSocket(`/ws/flights`)으로 최신 위치 제공
6. **시각화 (Client)** — React + Leaflet 기반 `flight-ui`가 실시간 관제 대시보드로 렌더링

### 구조 설명

| 설계 포인트 | 이유 |
| :--- | :--- |
| **PostgreSQL은 append-only** | 매 폴링마다 새 행을 쌓기만 함. 항공기별 최신 위치만 보려면 `icao24` 기준 `DISTINCT ON`이 필수 (`/flights`가 이미 구현) — 안 하면 지도에 과거 위치가 중복 표시됨. |
| **MinIO는 모든 이력을 영구 보관** | Postgres는 `db_cleanup` DAG가 1시간마다 오래된 행을 정리하지만, MinIO의 Parquet은 지워지지 않음 — 실시간 서빙과 장기 분석용 데이터를 분리. 현재는 **적재 전용**이며, 향후 **항공기 궤적 예측 모델의 학습 데이터셋**으로 사용할 목적으로 보존 중. |
| **Polling 대신 LISTEN/NOTIFY** | Spark가 배치를 쓴 직후 `pg_notify`를 호출하면 백엔드가 즉시 연결된 WebSocket 클라이언트에 브로드캐스트 — 화면 반영 지연을 평균 5초에서 630ms로 단축. |

---

## 사용 스택

| 영역 | 기술 |
| :--- | :--- |
| Ingestion | Python, Kafka, Zookeeper |
| Processing | Apache Spark (Structured Streaming) |
| Hot Path (실시간 서빙 DB) | PostgreSQL |
| Cold Path (데이터 레이크) | MinIO (S3 Compatible), Parquet |
| Orchestration | Apache Airflow |
| Serving API | FastAPI (REST `/flights` + WebSocket `/ws/flights`) |
| Frontend | React, Leaflet |
| Infra | Docker / docker-compose |

---

## 실행 방법

### 사전 준비
* Docker
* (프론트엔드 로컬 개발 시) Node.js — `flight-ui/`는 컨테이너화되어 있지 않습니다.

### 전체 파이프라인 기동

```bash
docker-compose up -d --build
```

`zookeeper`, `kafka`, `minio`, `postgres`, `airflow`, `airflow-postgres`와 함께 `producer`/`spark`/`backend` 앱 서비스까지 전부 자동 기동됩니다 — 수집부터 서빙까지 별도 수동 단계가 없습니다.

| 서비스 | 주소 |
| :--- | :--- |
| Airflow UI | `localhost:8080` (admin/admin) |
| MinIO 콘솔 | `localhost:9001` |
| PostgreSQL | `localhost:5432` (`flightdb`) |
| Backend REST | `localhost:8000/flights` |
| Backend WebSocket | `ws://localhost:8000/ws/flights` |

### 프론트엔드 (로컬 실행)

```bash
cd flight-ui
npm start
```

`localhost:3000`에서 접속, `ws://localhost:8000/ws/flights`를 구독해 실시간으로 갱신됩니다.

### 종료

```bash
docker-compose down
```

---

## 트러블슈팅 (문제 → Before 지표 → After 지표)

파이프라인을 컨테이너 자동화하고 확장 작업을 진행하면서 실제로 마주친 문제들과, 가능한 경우 실측한 정량 지표입니다.

### 1. 컨테이너 재시작 시 체크포인트 유실 → 전체 재처리·중복 데이터

Spark 체크포인트가 컨테이너 내부(휘발성 레이어)에만 있어, `spark` 컨테이너를 재시작할 때마다 Kafka `earliest` 오프셋부터 전체를 다시 읽고 있었습니다. 체크포인트 디렉터리를 호스트 볼륨(`./checkpoints/spark_checkpoints_final`)으로 마운트해 해결했습니다.

| 항목 | Before | After |
| :--- | :--- | :--- |
| 재시작 시 재처리 레코드 수 | 761~945건 (Kafka 전체 재생) | 26건 (다운타임 동안 누락분만, **-97%**) |
| 재시작 시 중복 데이터 | 매번 100% 발생 (`(icao24, timestamp)` 중복) | 0건 |

### 2. arm64(Apple Silicon)에서 Spark 기동 실패

`Dockerfile`의 `JAVA_HOME`이 `.../java-17-openjdk-amd64`로 하드코딩되어 있어, arm64 호스트에서는 Debian 패키지가 `...-arm64` 경로에 설치되는데도 amd64 경로를 찾다가 `spark-submit`이 즉시 실패했습니다. 빌드 시점에 아키텍처를 감지해 심볼릭 링크를 만드는 방식으로 고쳤습니다(`ln -sfn .../java-17-openjdk-$(dpkg --print-architecture) ...`). 정량 지표보다는 "기동 자체가 되느냐 마느냐"의 문제였습니다 — arm64 Mac에서 실제로 재현·수정 확인.

### 3. DB 인덱스가 문서에만 존재 → 코드화했지만 처음엔 엉뚱한 인덱스

기존 문서에는 `(icao24, timestamp DESC)` 복합 인덱스가 "적용 완료"로 적혀 있었지만, 실제로는 어느 코드에도 없어서 저장소를 새로 클론하면 인덱스 없이 시작했습니다. 이를 코드화했는데, 막상 측정해보니 `/flights` 쿼리(`WHERE timestamp >= ...`)는 `icao24`가 선행 컬럼인 이 인덱스를 전혀 타지 못했습니다 — 진짜 필요한 건 `timestamp`가 선행 컬럼인 인덱스였습니다.

| 항목 | Before (인덱스 없음) | After (`timestamp` 선행 인덱스) |
| :--- | :--- | :--- |
| `/flights` 쿼리 플랜 (28k행 규모) | `Seq Scan` | `Index Scan using idx_flight_timestamp` |
| `/flights` 쿼리 실행 시간 | 39.979ms | 16.828ms (**-57.9%**) |

같은 인덱스를 데이터가 적을 때(~8천 행)와 많을 때(~28k행) 각각 측정해봤는데, 적을 때는 플랜이 전혀 안 바뀌었습니다 — **인덱스 효과가 데이터 규모에 좌우된다**는 걸 같은 프로젝트 안에서 직접 재현했습니다.

### 4. Kafka 단일 파티션 → 대용량 배치 처리 지연

실제 OpenSky 인증 계정 없이도 정량적 결과를 남기기 위해 합성 부하 생성기로 아시아 스케일 트래픽을 흉내내 측정했습니다.

| 항목 | Before (1 파티션) | After (6 파티션) |
| :--- | :--- | :--- |
| 대용량 배치(~2,100건) 처리 지연 (10초 트리거 기준) | +30% (13.0초) | 사실상 0% (9.8초) |

1파티션에서는 Kafka 읽기가 직렬화되어 대용량 배치가 트리거 윈도우를 초과했지만, 6파티션에서는 병렬로 읽어 여유 있게 끝났습니다.

### 5. 10초 Polling → WebSocket 실시간 푸시

프론트엔드가 10초마다 `/flights`를 폴링하던 방식을, Spark가 배치를 쓴 직후 `pg_notify`로 알리고 백엔드가 즉시 브로드캐스트하는 WebSocket 방식으로 교체했습니다.

| 항목 | Before (Polling) | After (WebSocket) |
| :--- | :--- | :--- |
| 데이터 발생 → 화면 반영 지연시간 (평균) | 5,000ms (이론값) | 630ms (실측, **-87%**) |
| 지연시간 (최악의 경우) | 10,000ms | 실측 최대 745ms |

### 6. 체크포인트가 Kafka 보존 기간보다 오래 살아남으면 Spark 기동 실패

1번에서 체크포인트를 호스트 볼륨에 영속화했더니, 그 조치의 2차 부작용이 드러났습니다. Kafka는 1시간 retention이라 오래된 메시지를 지우는데, 체크포인트는 그대로 남아 이미 삭제된 오프셋을 가리킵니다. Spark 기본값(`failOnDataLoss=true`)은 이를 치명적 오류로 처리해 스트림을 종료합니다. 즉 **스택을 1시간 넘게 꺼뒀다 켜면 Spark가 아예 뜨지 않는** 상태였습니다.

| 항목 | Before | After (`failOnDataLoss=false`) |
| :--- | :--- | :--- |
| 1시간+ 중단 후 재기동 | `OffsetOutOfRangeException` → 스트림 종료 | 유실분 skip 후 정상 재개 |

원본 이력은 MinIO 콜드 패스에 남아 있으므로 skip이 복구 불가능한 손실은 아닙니다.

### 7. 중복 데이터의 진짜 원인 — 재시작이 아니라 수집 폴링

1번에서 "재시작 시 중복 0건"을 달성했지만, 정상 동작 중에도 중복이 계속 쌓이는 것을 뒤늦게 발견했습니다. 크래시 없는 상태에서 45초 간격으로 측정한 결과 최근 3분 내 중복 쌍이 **33 → 44건으로 증가** → 재시작과 무관한 상시 현상이었습니다.

원인은 `producer.py`가 10초마다 폴링하는데 OpenSky(익명 티어)가 아직 갱신되지 않은 **같은 스냅샷**을 돌려주는 경우였습니다. 좌표·속도까지 완전히 동일한 행이 두 번 적재됩니다.

**해결**: `producer.py`가 직전 스냅샷의 `time` 값을 기억했다가 같은 값이면 전송을 생략하도록 했습니다. 실제로 skip이 발동해 73건의 중복 전송을 차단하는 것을 로그로 확인했고, 이후 약 6분간(36회 폴링, 2,424행 적재) **중복 쌍 0건**을 유지했습니다.

| 항목 | Before | After |
| :--- | :--- | :--- |
| 최근 3분 내 중복 쌍 (정상 동작 중) | 33 → 44건으로 계속 증가 | **0건** |
| 중복 차단 지점 | 없음 (조회 시 `DISTINCT ON`으로 우회만) | 수집 단계에서 차단 — 애초에 적재되지 않음 |

`/flights`는 원래 `DISTINCT ON (icao24)` 덕에 영향이 없었고, trail 쿼리의 `DISTINCT ON (timestamp)`은 이미 쌓인 과거 중복과 producer 재시작 경계를 방어하기 위해 유지합니다.

1번의 "중복 0건"은 **재시작 기인 중복**에 한정된 수치로는 여전히 유효하지만, 그것만으로 "중복이 전혀 없다"고 읽힐 여지가 있어 여기에 함께 남겨둡니다.

---

*모든 정량 지표는 로컬 개발 머신에서 진행한 1회성 측정입니다 — 정식 부하 테스트가 아니라 방향성 확인용으로 참고하세요.*
