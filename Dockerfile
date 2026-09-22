# Dockerfile
FROM apache/airflow:2.8.1

# 1. 관리자 권한으로 전환 (Java 설치를 위해)
USER root

# 2. Java (OpenJDK 17) 및 필수 패키지 설치
# Spark를 실행하려면 Java가 필수입니다.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Java 환경변수 설정
# amd64/arm64 양쪽 모두에서 동작하도록 실제 설치 경로(아키텍처별로 다름)에 심볼릭 링크를 만들어 고정 경로로 참조
RUN ln -sfn /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PYTHONUNBUFFERED=1

# Spark 체크포인트 디렉터리를 이미지 안에 미리 만들고 airflow 소유로 넘긴다.
# docker-compose에서 이 경로들은 익명 볼륨이다(세션마다 초기화 — EXPANSION_PLAN 0.4).
# Docker는 이미지에 없는 경로에 볼륨을 붙일 때 root:root 0755로 만드는데, 컨테이너는
# airflow(uid 50000)로 돌기 때문에 체크포인트를 쓰지 못해 스트림이 죽고 재시작을
# 반복한다(2026-09-09 실측: RestartCount 43). 이미지에 미리 있으면 볼륨이 그 소유권을
# 물려받아 문제가 사라진다.
RUN mkdir -p /tmp/spark_checkpoints_final /tmp/spark_checkpoints_cold \
  && chown -R airflow:root /tmp/spark_checkpoints_final /tmp/spark_checkpoints_cold

# 3. Airflow 계정으로 다시 전환 (보안상 필수)
USER airflow

# 4. 필요한 Python 라이브러리 설치
#
# boto3: 콜드 패스 배치 ETL이 MinIO를 직접 다룬다 (0-6).
#  - dags/flight_lakehouse_etl.py: Bronze의 dt= 목록과 완료 마커 목록을 나열해
#    처리할 날짜를 고른다. Spark를 띄우지 않고 판단하려면 S3 API가 필요하다.
#  - src/spark_batch_etl.py: 쓰기 성공 후 _SUCCESS 마커를 남긴다.
# Spark의 s3a로도 되지만 그쪽은 JVM이 떠야 하고, 여기 쓰임은 객체 나열과
# 작은 put 하나뿐이다.
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    apache-airflow-providers-apache-spark \
    kafka-python \
    psycopg2-binary \
    pandas \
    python-dotenv \
    boto3