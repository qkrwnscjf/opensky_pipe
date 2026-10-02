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

# 5. 레이크 조회(DuckDB)와 ML 학습(scikit-learn) — 0-6d
#
# 위 설치 줄에 섞지 않고 따로 둔다. 위 줄을 고치면 그 층의 캐시가 깨져 pyspark(수백 MB)까지
# 다시 받는다.
#
# 버전은 이 이미지의 Python 3.8에서 설치되는 마지막 대에 맞춘다(apache/airflow:2.8.1 기본
# 파이썬). scikit-learn은 1.3.x가 3.8을 지원하는 마지막 버전이다.
#
# DuckDB 확장(httpfs: MinIO/S3 읽기, iceberg: Iceberg 메타데이터 해석)은 빌드 때 미리 받아 둔다.
# 실행할 때마다 인터넷에서 확장을 내려받지 않게 하기 위해서다.
RUN pip install --no-cache-dir \
    duckdb==1.1.3 \
    scikit-learn==1.3.2 \
  && python -c "import duckdb; duckdb.sql('INSTALL httpfs'); duckdb.sql('INSTALL iceberg')"

# 6. Spark 의존 라이브러리를 이미지에 미리 받아 둔다 — 0-6i (2026-10-02, 사용자 결정)
#
# spark-submit --packages는 컨테이너가 새로 만들어질 때마다 Maven에서 라이브러리를 다시
# 받는다. 2026-10-02 실측: 기동 직후 배치 태스크가 255초, 스트리밍 컨테이너가 276초를
# 같은 시각에 다운로드로 썼다(대부분 AWS SDK 번들 약 280MB). 세션마다 4~5분과 약 0.6GB
# 네트워크를 쓰고, 그동안 Maven 저장소가 막히면 파이프라인이 뜨지 않는다.
#
# 빌드 때 빈 스크립트로 spark-submit을 한 번 돌려 Ivy 캐시(~/.ivy2/cache)를 채운다. 실행 시
# 같은 --packages 좌표는 캐시에서 해석되어 다운로드가 일어나지 않는다. 런타임 동작과
# 메모리 사용량은 그대로다 — 같은 파일을 어디서 가져오느냐만 바뀐다.
#
# ~/.ivy2/jars는 지운다. Ivy가 캐시의 파일을 그 폴더로 한 번 더 복사해 두는데, 실행 시
# 캐시에서 다시 복사(로컬, 수 초)되므로 이미지에 두 벌을 남길 이유가 없다.
#
# 좌표는 아래 두 곳과 같아야 캐시가 쓰인다. 어긋나면 그 라이브러리만 실행 시 다시 받는다
# (동작은 하지만 느려진다):
#   docker-compose.yml  spark 서비스의 --packages (스트리밍)
#   dags/flight_lakehouse_etl.py  SPARK_PACKAGES (배치)
#
# 이 단계를 맨 뒤에 두는 이유: 기존 레이어를 그대로 재사용해, 다시 빌드해도 이전 이미지가
# 디스크를 거의 차지하지 않게 하기 위해서다.
RUN touch /tmp/noop.py \
  && /home/airflow/.local/bin/spark-submit \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
       /tmp/noop.py \
  && rm /tmp/noop.py \
  && rm -r /home/airflow/.ivy2/jars