# SkyStream: Real-time Flight Data Pipeline & Anomaly Detection

Kafka, Spark, MinIO, PostgreSQL을 활용한 **실시간 항공 데이터 파이프라인** 및 **이상 탐지(Anomaly Detection)** 프로젝트입니다.
OpenSky API 데이터를 수집하여 실시간 대시보드(Hot Path)와 데이터 레이크(Cold Path)로 동시에 적재합니다.

## Architecture
**Lambda Architecture**를 기반으로 데이터 파이프라인을 구축했습니다.

* **Ingestion:** Python, Kafka, Zookeeper
* **Processing:** Apache Spark (Structured Streaming)
* **Storage:** * Data Lake: MinIO (S3 Compatible) - `Parquet`
  * Serving DB: PostgreSQL
* **Serving:** Streamlit (Real-time Dashboard)

---

### 사전 준비 (Prerequisites)
* Docker 
* Python 3.9+
* Java 17 (Spark 실행용)
