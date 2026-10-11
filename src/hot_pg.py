"""1-22 (2026-10-11): Spark Python worker 안에서 Hot DB 연결을 재사용한다.

write_hot_tables()의 파티션 함수는 배치마다·파티션마다 psycopg2 연결을 새로 열고 닫았다(배치당 최대 6개 —
매번 TCP 연결 + Postgres 백엔드 프로세스 생성). Spark는 Python worker 프로세스를 태스크끼리 재사용하므로
(spark.python.worker.reuse 기본 true) 연결을 이 모듈의 전역에 두면 worker가 살아 있는 동안 계속 쓸 수 있다.

드라이버 스크립트(__main__)의 전역은 태스크마다 함수와 함께 직렬화돼 새로 만들어지므로, 연결은 별도 모듈에
둬야 worker의 sys.modules에 남는다 — 그래서 이 파일이 따로 있고 SparkContext.addPyFile로 worker에 보낸다.
"""
import psycopg2

_conn = None
_params = None


def get(params):
    """params(dict)로 연결을 돌려준다. 끊겼거나 접속 정보가 바뀌었으면 새로 연다."""
    global _conn, _params
    if _conn is not None and (_conn.closed or _params != params):
        discard()
    if _conn is None:
        _conn = psycopg2.connect(**params)
        _params = dict(params)
    return _conn


def discard():
    """연결을 버린다 — 다음 get()이 새로 연다. Postgres 재시작 뒤 죽은 연결을 치울 때 쓴다."""
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
    _conn = None
