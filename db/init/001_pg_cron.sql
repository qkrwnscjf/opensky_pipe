-- db_cleanup — Airflow DAG에서 pg_cron으로 이전 (2026-09-18, docs/TASK_ORDER.md 0-5)
--
-- 이 파일은 postgres 공식 이미지의 /docker-entrypoint-initdb.d/ 규약으로 실행된다:
-- 데이터 디렉터리가 비어 있을 때(= 최초 초기화) 한 번 실행된다.
--
-- 이 프로젝트에서는 그게 정확히 "세션 시작"과 같다. Postgres 볼륨이 익명 볼륨이라
-- `docker compose down` 후 `up` 할 때마다 빈 상태로 시작하므로, 이 스크립트도 매
-- 세션 다시 실행되어 cron 작업이 새로 등록된다. 이전 세션의 스케줄이 남아 어긋날
-- 일이 없다 — 데이터 수명 정책(EXPANSION_PLAN 0.4)과 같은 방향이다.
-- (그래서 이 파일을 고친 내용은 세션을 새로 시작해야 적용된다.)

-- 기동 로그에 아래 FATAL이 세션마다 한 번 찍히는 것은 정상이다(무시해도 된다):
--   FATAL: database "flightdb" does not exist
--   LOG:   background worker "pg_cron launcher" exited with exit code 1
-- 공식 이미지는 initdb 부트스트랩 단계에서 임시 서버를 먼저 띄우는데, 그 시점엔
-- 아직 flightdb가 없어 pg_cron 런처가 한 번 붙었다 떨어진다. 본 서버가 올라온 뒤
-- 다시 붙으며 바로 뒤에 `LOG: pg_cron scheduler started`가 찍힌다 (2026-09-18 확인).
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- ① flight_data(이력) 정리 — 10분마다 70분 지난 행을 지운다. (1-A D-1, 2026-10-10)
--
-- 이전에는 매시 정각에 1시간 지난 행을 지웠다. 그러면 정각 직전에는 거의 2시간치가 남아
-- 보존량이 1~2시간 사이를 오갔다. 이 테이블을 읽는 궤적 조회(/flights/{icao24}/trail)는 최대
-- 60분만 쓴다. 10분마다 70분 기준으로 지우면 보존량이 70~80분으로 일정해지고(최대치 약 2시간 →
-- 80분), 한 번에 지우는 양도 1시간치에서 10분치로 줄어든다. 70분은 60분 조회에 10분 여유를 둔 값.
--
-- 지워도 되는 근거는 그대로다: 전체 이력은 Cold Path(MinIO)에 영구 보관되고,
-- Postgres는 실시간 서빙용 임시 저장소다.
--
-- 테이블 존재를 먼저 확인하는 이유 (2026-09-18 실측으로 확인한 엣지 케이스):
-- flight_data는 Spark JDBC writer가 첫 append 때 만든다. 세션을 시작하고 Spark가
-- 첫 배치를 쓰기 전에 작업이 돌면 테이블이 아직 없어서
--   ERROR: relation "flight_data" does not exist
-- 로 작업이 실패하고 cron.job_run_details에 실패로 남는다. 무해하지만, 실패 기록이
-- 쌓이면 나중에 진짜 실패를 가려버린다. 없으면 조용히 건너뛰게 한다.
SELECT cron.schedule(
    'cleanup_flight_data',
    '*/10 * * * *',
    $job$
    DO $guard$
    BEGIN
        IF to_regclass('public.flight_data') IS NOT NULL THEN
            DELETE FROM flight_data WHERE timestamp < NOW() - INTERVAL '70 minutes';
        END IF;
    END
    $guard$;
    $job$
);

-- ② flight_current(현재 상태) 정리 — 10분마다 30분 넘게 안 보인 항공기를 지운다. (1-A D-2, 2026-10-10)
--
-- flight_current는 기체당 1행을 UPSERT로 유지하는 상태 테이블이라 ①의 "N분 지난 행" 정리와는
-- 성격이 다르다. 다만 레이더를 벗어난(착륙·범위 밖) 항공기도 행이 그대로 남아, 세션 동안 본 모든
-- 항공기가 쌓인다. /flights는 어차피 최근 5분만 보여 주므로(WHERE timestamp >= NOW() - 5분),
-- 30분 넘게 갱신이 없는 행은 아무도 읽지 않는다. 30분은 그 5분에 넉넉한 여유를 둔 값이다.
-- 다시 나타나면 다음 배치의 UPSERT가 새로 넣으므로 잃는 것이 없다. 궤적은 flight_data를 읽어 무관.
--
-- 같은 이유로 테이블이 아직 없으면(Spark의 ensure_current_table()이 첫 배치 때 만든다) 건너뛴다.
SELECT cron.schedule(
    'cleanup_flight_current',
    '*/10 * * * *',
    $job$
    DO $guard$
    BEGIN
        IF to_regclass('public.flight_current') IS NOT NULL THEN
            DELETE FROM flight_current WHERE timestamp < NOW() - INTERVAL '30 minutes';
        END IF;
    END
    $guard$;
    $job$
);
