from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

# 1. Spark 세션 생성
# kafka와 elasticsearch 라이브러리 두 개를 모두 로드합니다.
spark = SparkSession.builder \
    .appName("FlightDataAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Streaming: Kafka -> Processing -> Elasticsearch 전송 시작")

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

# 4. 데이터 파싱 및 변환 (Transform)
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# [중요] Elasticsearch Geo-point 형식에 맞게 'location' 필드 생성
# 위도(lat)와 경도(lon)를 묶어서 하나의 구조체(Struct)로 만듭니다.
df_processed = df_parsed \
    .withColumn("location", struct(col("latitude").alias("lat"), col("longitude").alias("lon"))) \
    .withColumn("timestamp", to_timestamp(col("last_updated"))) # 시간 필드 변환

# ★ 분석 로직: 예) 고도가 5,000m 이상인 비행기만 필터링
df_filtered = df_processed.filter(col("baro_altitude") > 5000)

# 5. Elasticsearch로 전송 (Write)
# checkpointLocation은 Spark가 중단되어도 어디까지 보냈는지 기억하는 장소입니다.
query = df_filtered.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "./spark-checkpoints") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .option("es.index.auto.create", "true") \
    .start("flight-data-spark")  # 저장할 인덱스 이름 (기존과 다르게 설정)

query.awaitTermination()