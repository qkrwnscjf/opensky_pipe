# ML 비교 전용 이미지 — 2026-10-04 (사용자 결정, A안)
#
# 왜 airflow 이미지에 넣지 않나:
#   - apache/airflow:2.8.1은 Python 3.8이다. PyTorch는 2.5부터 3.8을 지원하지 않고,
#     Chronos-Bolt를 지원하는 chronos-forecasting도 최신 파이썬을 전제로 한다.
#   - 그 이미지는 Spark 스트리밍·배치도 같이 쓴다. PyTorch(약 수백 MB)를 넣으면
#     파이프라인 이미지가 함께 무거워진다.
# 그래서 손으로 실행하는 ML 비교만 이 이미지에서 돈다. compose의 `ml` 서비스는 profile로
# 묶여 있어 `docker compose up`에 뜨지 않고, 포트도 열지 않는다.
#
#   docker compose run --rm ml                          # 학습 2종 → MLflow 기록·등록·승격 → 리포트
#   docker compose --profile mlflow-ui up -d mlflow-ui  # MLflow 웹(읽기 전용, 127.0.0.1:5000) — 볼 때만
#
# 2026-10-05(0-8)부터 MLOps 학습 작업(src/ml_jobs.py)과 MLflow 웹 화면도 이 이미지를 쓴다.
# pyspark는 넣지 않는다. 그래서 학습 코드는 lakehouse_common(맨 위에서 pyspark를 import)을
# 쓰지 않는다. 레이크 읽기는 lake_duckdb(duckdb만 필요)를 쓴다.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    HF_HOME=/opt/hf

WORKDIR /app

# PyTorch는 CPU 전용 인덱스에서 받는다. 일반 PyPI의 x86_64 휠은 CUDA 라이브러리까지 포함해
# 수 GB가 된다. 이 프로젝트는 CPU 로컬 실행이 조건이다(사용자).
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu

# 나머지 — duckdb는 airflow 이미지와 같은 버전으로 맞춰 Iceberg 읽기 동작을 같게 한다.
# chronos-forecasting은 2.x(Chronos-2)에서 API가 바뀔 수 있어 1.x로 묶는다.
RUN pip install --no-cache-dir \
    "chronos-forecasting>=1.4,<2" \
    duckdb==1.1.3 \
    "numpy<2" \
    "pandas<2.3" \
    scikit-learn==1.5.2 \
    boto3

# MLflow — 0-8 (2026-10-05, 사용자 결정). 위 줄에 섞지 않고 따로 둬서, MLflow만 바꿀 때
# PyTorch·Chronos 층 캐시를 깨지 않게 한다. 학습 기록(서버 없이 API로 직접 기록)과
# 필요할 때만 켜는 웹 화면(`mlflow-ui` 서비스)이 같은 이 이미지를 쓴다.
# SQLAlchemy는 2.0대로 묶는다. MLflow 2.17은 "3 미만"만 요구해 2.1이 깔리는데, 2.1에서 사라진
# FallbackAsyncAdaptedQueuePool을 MLflow가 import해 기록 단계에서 ImportError가 났다(2026-10-05 실측).
RUN pip install --no-cache-dir "mlflow==2.17.2" "sqlalchemy>=2.0,<2.1"

# 재학습 작업자(src/ml_worker.py, 0-8 2단계) — 학습 전에 Airflow 메타DB의 dag_run을 읽기 전용으로
# 조회해 DAG가 돌고 있는지 확인한다. Airflow REST API를 켜지 않기 위한 선택(그 포트는 모든
# 인터페이스에 열려 있다). 웹 서버는 표준 라이브러리 http.server라 추가 의존성은 이것뿐이다.
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10"

# 모델 가중치를 빌드 때 이미지에 넣는다(Spark 라이브러리를 미리 받아 두는 0-6i와 같은 이유).
# 실행할 때는 아래 HF_HUB_OFFLINE=1로 외부 접속을 막아, 실행 중 다운로드가 생기지 않음을 보장한다.
RUN python -c "import torch; from chronos import BaseChronosPipeline; \
BaseChronosPipeline.from_pretrained('amazon/chronos-bolt-tiny', device_map='cpu', torch_dtype=torch.float32)" \
  && chmod -R a+rX /opt/hf

ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

# root로 돌지 않는다
RUN useradd --create-home --uid 1000 ml \
  && mkdir -p /app/ml_report && chown ml /app/ml_report \
  && chown -R ml /opt/hf \
  && mkdir -p /mlflow && chown ml /mlflow \
  && chown ml /app
# /app을 ml 소유로: MLflow의 SQLite 저장소는 시작할 때 작업 폴더에 ./mlruns를 무조건 만든다
# (기본 실험용 자리). 우리 실험은 파일을 MinIO에 두므로 이 폴더는 비어 있고 컨테이너와 함께 사라진다.
# /mlflow는 이름 있는 볼륨 mlflow_data의 연결 지점이다. 빈 볼륨은 처음 연결될 때 이미지의 이
# 폴더 소유자를 물려받으므로, 미리 ml 소유로 만들어 둔다 — 안 그러면 root 소유로 생겨
# mlflow.db를 쓰지 못한다(spark 체크포인트 폴더에서 겪은 것과 같은 문제).
USER ml

# DuckDB 확장은 설치한 사용자의 홈(~/.duckdb)에 들어가므로, 실행 사용자(ml)로 바꾼 뒤에 받는다.
# root로 받으면 실행 때 확장을 못 찾고 인터넷에서 다시 받으려 한다.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs'); duckdb.sql('INSTALL iceberg')"

CMD ["python", "src/ml_jobs.py", "all"]
