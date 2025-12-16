from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, mean, from_json, to_json, struct, lit, date_format, current_timestamp, array
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging
import os
import json

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfiguration aus Umgebungsvariablen
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker.messaging.svc.cluster.local:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "sensor-data")
TARGET_TOPIC = os.getenv("TARGET_TOPIC", "analytics-data")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "30 seconds")

def create_kafka_topics_if_not_exist(bootstrap_servers, topics):
    """
    Erstellt Kafka Topics, falls sie noch nicht existieren.

    Args:
        bootstrap_servers: Kafka Bootstrap Server String
        topics: Liste von Topic-Namen (str) oder Liste von (name, num_partitions, replication_factor) Tupeln
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='spark-topic-creator'
        )

        # Prüfe welche Topics bereits existieren
        existing_topics = admin_client.list_topics()
        logger.info(f"Existierende Topics: {existing_topics}")

        # Erstelle NewTopic Objekte für fehlende Topics
        new_topics = []
        for topic in topics:
            if isinstance(topic, str):
                topic_name = topic
                num_partitions = 1
                replication_factor = 1
            else:
                topic_name, num_partitions, replication_factor = topic

            if topic_name not in existing_topics:
                logger.info(f"Topic '{topic_name}' existiert nicht, wird erstellt...")
                new_topics.append(
                    NewTopic(
                        name=topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor
                    )
                )
            else:
                logger.info(f"Topic '{topic_name}' existiert bereits")

        # Erstelle neue Topics
        if new_topics:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            logger.info(f"Topics erfolgreich erstellt: {[t.name for t in new_topics]}")
        else:
            logger.info("Alle benötigten Topics existieren bereits")

        admin_client.close()

    except TopicAlreadyExistsError as e:
        logger.warning(f"Topic existiert bereits: {e}")
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der Topics: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starte Spark Streaming Application...")
    logger.info(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    logger.info(f"Source Topic: {SOURCE_TOPIC}")
    logger.info(f"Target Topic: {TARGET_TOPIC}")
    logger.info(f"Window Duration: {WINDOW_DURATION}")

    # Erstelle Topics falls sie nicht existieren
    logger.info("Prüfe und erstelle benötigte Kafka Topics...")
    create_kafka_topics_if_not_exist(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topics=[
            (SOURCE_TOPIC, 1, 2),
            (TARGET_TOPIC, 1, 2)
        ]
    )

    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation.cleanup", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session erfolgreich erstellt")

    logger.info(f"Lese Stream von Kafka Topic: {SOURCE_TOPIC}")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Kafka Stream erfolgreich initialisiert")

    schema = StructType() \
        .add("sensor_id", StringType()) \
        .add("timestamp", StringType()) \
        .add("temperature", DoubleType()) \
        .add("humidity", DoubleType())

    logger.info("Parse JSON-Daten aus Kafka Stream")
    df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Konvertiere String-Timestamp zu TimestampType und füge Watermark hinzu
    df_with_timestamp = df_parsed.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    df_with_watermark = df_with_timestamp.withWatermark("timestamp", "1 minute")

    logger.info(f"Starte Aggregation mit {WINDOW_DURATION} Fenstern")
    agg = df_with_watermark \
        .groupBy(window(col("timestamp"), WINDOW_DURATION), col("sensor_id")) \
        .agg(
            mean("temperature").alias("temperature"),
            mean("humidity").alias("humidity")
        )

    output = agg.select(
        col("sensor_id"),
        (col("window.start").cast("long") * 1000).alias("timestamp"),  # Unix-Timestamp in Millisekunden
        col("temperature"),
        col("humidity")
    )

    # Konvertiere zu JSON MIT Schema (Kafka Connect JDBC Sink kompatibel)
    # Format: {"schema": {...}, "payload": {...}}
    output_json = output.select(
        to_json(
            struct(
                struct(
                    lit("struct").alias("type"),
                    array(
                        struct(lit("sensor_id").alias("field"), lit("string").alias("type"), lit(False).alias("optional")),
                        struct(lit("timestamp").alias("field"), lit("int64").alias("type"), lit(False).alias("optional")),
                        struct(lit("temperature").alias("field"), lit("double").alias("type"), lit(False).alias("optional")),
                        struct(lit("humidity").alias("field"), lit("double").alias("type"), lit(False).alias("optional"))
                    ).alias("fields"),
                    lit(False).alias("optional"),
                    lit("analytics_data").alias("name")
                ).alias("schema"),
                struct(
                    col("sensor_id"),
                    col("timestamp"),
                    col("temperature"),
                    col("humidity")
                ).alias("payload")
            )
        ).alias("value")
    )

    logger.info(f"Schreibe Ergebnisse zu Kafka Topic: {TARGET_TOPIC}")
    query = output_json \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", TARGET_TOPIC) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("Streaming Query gestartet, warte auf Terminierung...")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming wird gestoppt (KeyboardInterrupt)...")
        query.stop()
    except Exception as e:
        logger.error(f"Fehler im Streaming Query: {e}")
        query.stop()
        raise
    finally:
        logger.info("Spark Session wird beendet")
        spark.stop()
