import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

# 1. Spark 세션 생성 (S3 및 Postgres 연결 설정)
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

print("🚀 Dual ETL 시작: Kafka -> Spark -> [MinIO(S3) + PostgreSQL]")

# 2. Kafka 읽기
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight_data_raw") \
    .option("startingOffsets", "latest") \
    .load()

# 3. 스키마 정의
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

# 4. 데이터 가공
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

df_processed = df_parsed \
    .withColumn("timestamp", to_timestamp(col("last_updated"))) \
    .withColumn("date", col("timestamp").cast("date"))

# ★ 핵심 함수: 데이터 묶음(Batch)마다 실행될 로직
def save_to_sinks(batch_df, batch_id):
    print(f"Batch {batch_id} 처리 중... 데이터 수: {batch_df.count()}")
    
    # 1) MinIO(S3)에 Parquet로 저장 (Append)
    batch_df.write \
        .mode("append") \
        .format("parquet") \
        .save("s3a://flight-data-lake/raw_data")
    
    # 2) PostgreSQL에 저장 (Append - 계속 쌓음)
    # 실제 운영에선 최신 상태만 유지(Overwrite)하거나 Upsert를 하지만, 지금은 단순하게 쌓겠습니다.
    batch_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/flightdb") \
        .option("dbtable", "flight_realtime") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .option("driver", "org.postgresql.Driver") \
        .save()
        
    print(f"Batch {batch_id} 저장 완료 (S3 & DB)")

# 5. 실행 (foreachBatch 사용)
query = df_processed.writeStream \
    .foreachBatch(save_to_sinks) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "s3a://flight-data-lake/checkpoints_dual") \
    .start()

query.awaitTermination()