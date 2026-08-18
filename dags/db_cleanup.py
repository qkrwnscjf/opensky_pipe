import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# 1. DB 데이터 정리 함수
def cleanup_old_data():
    """
    PostgreSQL 데이터베이스에서 1시간 이상 경과한 데이터를 삭제합니다.
    Data Lake(MinIO)에는 이미 저장되어 있으므로 실시간 DB 부하를 줄이기 위해 정리합니다.
    """
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")

    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        print("ERROR: DB 연결 설정이 환경변수에 누락되었습니다.")
        return

    # DB 연결 (SQLAlchemy 사용)
    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DB_URL)
    
    # 최근 1시간 데이터만 남기고 나머지는 삭제하는 쿼리
    query = text("DELETE FROM flight_data WHERE timestamp < NOW() - INTERVAL '1 hour'")
    
    try:
        with engine.connect() as conn:
            # 트랜잭션 시작 (SQLAlchemy 2.0 권장 방식)
            with conn.begin():
                result = conn.execute(query)
                deleted_count = result.rowcount
                print(f"SUCCESS: {deleted_count}개의 오래된 데이터를 성공적으로 정리했습니다.")
    except Exception as e:
        print(f"FAILURE: 데이터 정리 중 오류 발생: {e}")

# 2. Airflow DAG 설정 (매시간 정각 실행)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'db_cleanup_scheduler',
    default_args=default_args,
    description='실시간 항공 데이터베이스 최적화를 위해 1시간마다 오래된 데이터 삭제',
    schedule_interval='@hourly', # 매시간 반복 실행
    catchup=False,
    tags=['maintenance', 'db', 'cleanup'],
) as dag:

    cleanup_task = PythonOperator(
        task_id='delete_old_records',
        python_callable=cleanup_old_data,
    )

    cleanup_task
