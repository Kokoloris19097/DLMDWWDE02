from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, mean, from_json, to_json, struct, lit, date_format
from pyspark.sql.types import StructType, StringType, DoubleType

KAFKA_BOOTSTRAP = "kafka-broker.messaging.svc.cluster.local:9092"
SOURCE_TOPIC = "raw-data"
TARGET_TOPIC = "analytics-data"

# Schema-Definition für Kafka Connect JSON Format
SCHEMA_JSON = '{"type":"struct","fields":[{"field":"sensor_id","type":"string"},{"field":"timestamp","type":"string"},{"field":"temperature","type":"double"},{"field":"humidity","type":"double"}]}'

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", SOURCE_TOPIC) \
        .load()

    schema = StructType() \
        .add("sensor_id", StringType()) \
        .add("timestamp", StringType()) \
        .add("temperature", DoubleType()) \
        .add("humidity", DoubleType())

    df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    agg = df_parsed \
        .groupBy(window(col("timestamp"), "30 seconds"), col("sensor_id")) \
        .agg(
            mean("temperature").alias("temperature"),
            mean("humidity").alias("humidity")
        )

    output = agg.select(
        col("sensor_id"),
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
        col("temperature"),
        col("humidity")
    )

    # Mit Schema für Kafka Connect
    output_with_schema = output.select(
        to_json(
            struct(
                lit(SCHEMA_JSON).alias("schema"),
                to_json(struct("sensor_id", "timestamp", "temperature", "humidity")).alias("payload")
            )
        ).alias("value")
    )

    query = output_with_schema \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", TARGET_TOPIC) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .outputMode("update") \
        .start()

    query.awaitTermination()
