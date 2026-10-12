# flight-ui — SkyStream 실시간 관제 화면

SkyStream 파이프라인의 화면입니다. 백엔드(FastAPI)가 WebSocket으로 보내는 항공기 위치를 지도와 카드 목록으로 보여 주고, `#/model` 페이지에서 궤적 예측 모델의 학습·비교·모니터링 결과를 보여 줍니다. 로컬 개발 전용이며 컨테이너에 넣지 않았습니다.

## 실행

```
# 1) 저장소 루트에서 파이프라인을 먼저 띄운다 (백엔드: localhost:8000)
docker compose up -d --build

# 2) 화면
cd flight-ui
npm install
HOST=127.0.0.1 npm start        # http://localhost:3000
```

`HOST=127.0.0.1`을 주지 않으면 개발 서버가 모든 네트워크 인터페이스에 열립니다. 백엔드 주소(`localhost:8000`)는 코드에 고정돼 있습니다(로컬 전용).

## 화면

| 구역 | 내용 |
| :--- | :--- |
| 상단 바 | `LIVE`/`OFFLINE`, 마지막 갱신 후 경과와 다음 갱신 예상(`Xs ago · next ≈Ns`), 한국 시간 시계, 라이트/다크 |
| 히어로 | 항공기 수, 비행 중 항공기의 평균 고도 등 — 데이터가 올 때마다 갱신 |
| Live Airspace | Leaflet 지도(Esri Gray Canvas), 고도대 색(저·중·고 + 지상 회색), 호버 정보, 클릭 시 최근 30분 궤적·따라가기, HUD(데이터 나이, 갱신 주기) |
| Fleet | 검색(편명·icao24·국가·항공사), 국가 필터, 정렬, 고정(핀), "Airborne only" 토글 |
| Model Report (`#/model`) | 기준선 대비 모델 결과, 3개 모델 비교(기준선·HGB·Chronos-Bolt tiny), 정확도(적중률), MLflow 레지스트리, 재학습 버튼, 실시간 모니터링 |

- 모든 시각은 한국 시간으로 표시합니다(저장은 UTC).
- 국가는 편명 앞 3글자(항공사 코드)로 정하고, 모르면 기체 등록국으로 표시합니다(`src/airlines.js`).
- Model Report는 백엔드가 아니라 `public/ml-report/*.json`(학습 작업이 내보내는 파일, git 제외)을 읽습니다.

## 스크립트

- `npm start` — 개발 서버
- `npm run build` — 배포용 빌드(`build/`, git 제외)
- `npm test` — 현재 실패합니다. Create React App의 기본 Jest 설정이 `react-leaflet`의 ESM 코드를 읽지 못합니다(`transformIgnorePatterns` 설정 필요, 이전부터 있던 문제).

## 코드 구성

- `src/App.js` — 실시간 화면(WebSocket, 지도, 목록, 상태 표시)
- `src/ModelReport.js` — 모델 리포트 페이지
- `src/airlines.js` — 항공사 코드 → 항공사·국가, 국기 코드
- `src/App.css` — 스타일. 모든 색은 맨 위 CSS 변수(`:root` / `[data-theme='dark']`)
