import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

# 1. Spark 세션 생성 (S3 연결 설정 포함)
spark = SparkSession.builder \
    .appName("FlightDataLakeETL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Data Lake ETL check(Kafka -> Spark -> MinIO(S3))")

# 2. Kafka에서 데이터 읽기
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight_data_raw") \
    .option("startingOffsets", "latest") \
    .load()

# 3. 데이터 구조(Schema) 정의
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("last_updated", LongType())
])

# 4. 데이터 파싱
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. 타임스탬프 변환 (Partitioning을 위해 필요)
df_processed = df_parsed \
    .withColumn("timestamp", to_timestamp(col("last_updated"))) \
    .withColumn("date", col("timestamp").cast("date")) # 나중에 날짜별로 폴더를 나누기 위함

# 6. MinIO(S3)에 Parquet 파일로 저장 (Write)
query = df_processed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://flight-data-lake/raw_data") \
    .option("checkpointLocation", "s3a://flight-data-lake/checkpoints") \
    .trigger(processingTime="10 seconds") \
    .start()
    # processingTime="10 seconds": 10초마다 데이터를 모아서 파일 하나로 만듦 (파일 개수 폭발 방지)

query.awaitTermination()