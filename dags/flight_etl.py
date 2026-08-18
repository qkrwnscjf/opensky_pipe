import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. DAG 설정 (5분마다 실행)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'flight_data_pipeline',
    default_args=default_args,
    description='Spark Streaming을 상주 실행하여 실시간 데이터 적재',
    schedule_interval=None, # 상주 실행 방식으로 변경하여 자동 스케줄링 비활성화
    catchup=False,
    tags=['flight', 'spark', 'streaming'],
) as dag:

    # 2. 할 일 정의 (Spark Submit 명령어 실행)
    # Airflow 컨테이너 안에는 Java와 Spark가 이미 설치되어 있습니다 (Dockerfile 덕분)
    run_spark_etl = BashOperator(
        task_id='run_spark_job',
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 \
        /opt/airflow/src/spark_dual_write.py
        """,
        env={
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY"),
            "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY"),
            "DB_HOST": os.getenv("DB_HOST", "postgres"),
            "DB_USER": os.getenv("DB_USER"),
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
            "DB_NAME": os.getenv("DB_NAME")
        }
    )

    run_spark_etl