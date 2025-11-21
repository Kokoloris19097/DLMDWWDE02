# Apache Spark Main Script
# DLMDWWDE02 Master Project
# Entry point for Spark job
# Apache Spark Main Script
# DLMDWWDE02 Master Project
# Entry point for Spark job


# Apache Spark Analytics mit confluent-kafka
# DLMDWWDE02 Master Project
import json
import time
from confluent_kafka import Consumer, Producer
from statistics import mean
# Apache Spark Streaming mit pyspark
# DLMDWWDE02 Master Project

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, mean, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

KAFKA_BOOTSTRAP = "kafka-broker.messaging.svc.cluster.local:9092"
SOURCE_TOPIC = "sensor-data"
TARGET_TOPIC = "analytics-data"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    # Lese von Kafka Topic 'sensor-data'
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", SOURCE_TOPIC) \
        .load()

    # Konvertiere Value zu String und parse JSON
    schema = StructType() \
        .add("sensor_id", StringType()) \
        .add("timestamp", StringType()) \
        .add("temperature", DoubleType()) \
        .add("humidity", DoubleType())

    df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Aggregation über 10s Fenster
    agg = df_parsed \
        .groupBy(window(col("timestamp"), "10 seconds"), col("sensor_id")) \
        .agg(mean("temperature").alias("mean_temperature"), mean("humidity").alias("mean_humidity"))

    # Schreibe aggregierte Daten in neues Kafka Topic 'analytics-data'
    query = agg.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", TARGET_TOPIC) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .outputMode("update") \
        .start()

    query.awaitTermination()
