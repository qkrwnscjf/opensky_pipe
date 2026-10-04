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
#   docker compose run --rm ml                 # 3개 모델 비교 → MinIO 저장 → 리포트 내보내기
#
# pyspark는 넣지 않는다. 그래서 비교 스크립트는 lakehouse_common(맨 위에서 pyspark를
# import)을 쓰지 않고 MinIO 접속을 직접 만든다. 레이크 읽기는 lake_duckdb(duckdb만 필요)를 쓴다.
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
  && chown -R ml /opt/hf
USER ml

# DuckDB 확장은 설치한 사용자의 홈(~/.duckdb)에 들어가므로, 실행 사용자(ml)로 바꾼 뒤에 받는다.
# root로 받으면 실행 때 확장을 못 찾고 인터넷에서 다시 받으려 한다.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs'); duckdb.sql('INSTALL iceberg')"

CMD ["python", "src/ml_compare_models.py"]
