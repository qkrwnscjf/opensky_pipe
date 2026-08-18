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

# 3. Airflow 계정으로 다시 전환 (보안상 필수)
USER airflow

# 4. 필요한 Python 라이브러리 설치
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    apache-airflow-providers-apache-spark \
    kafka-python \
    psycopg2-binary \
    pandas \
    python-dotenv