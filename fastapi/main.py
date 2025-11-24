"""
FastAPI Ingestion Service - Data Engineering Master Project
Empfängt Sensor-Daten via HTTP POST und schreibt in Kafka Topic
"""
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer
from confluent_kafka import KafkaException
import json
import logging
import os
from datetime import datetime
from typing import Optional
import uvicorn

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Umgebungsvariablen
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka.messaging.svc.cluster.local:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'raw-data')
SERVICE_NAME = os.getenv('SERVICE_NAME', 'fastapi')

# FastAPI App
app = FastAPI(
    title="Data Ingestion API",
    description="Empfängt Sensor-Daten und schreibt in Kafka",
    version="1.0.0"
)

def get_kafka_producer() -> Producer:
    """Erstellt oder gibt existierenden Confluent Kafka Producer zurück"""
    if not hasattr(get_kafka_producer, "producer"):
        logger.info(f"Initialisiere Confluent Kafka Producer: {KAFKA_BOOTSTRAP_SERVERS}")
        try:
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'compression.codec': 'gzip',
                'acks': 'all',
                'retries': 3,
                'socket.keepalive.enable': True,
                'queue.buffering.max.messages': 100000,
                'queue.buffering.max.ms': 1000,
                'enable.idempotence': True
            }
            get_kafka_producer.producer = Producer(conf)
            logger.info("Confluent Kafka Producer erfolgreich initialisiert")
        except Exception as e:
            logger.error(f"Fehler beim Initialisieren des Kafka Producers: {e}")
            raise
    return get_kafka_producer.producer

class SensorData(BaseModel):
    """Schema für eingehende Sensor-Daten"""
    sensor_id: str = Field(..., description="Eindeutige Sensor-ID", min_length=1, max_length=100)
    timestamp: datetime = Field(..., description="Zeitstempel der Messung")
    temperature: float = Field(..., description="Temperatur in Celsius", ge=-273.15, le=200.0)
    humidity: float = Field(..., description="Luftfeuchtigkeit in Prozent", ge=0.0, le=100.0)

    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        """Validiert dass Timestamp nicht in der Zukunft liegt"""
        now = datetime.now(v.tzinfo) if v.tzinfo else datetime.now()
        if v > now:
            raise ValueError('Timestamp darf nicht in der Zukunft liegen')
        return v

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "sensor_id": "SENSOR-001",
                    "timestamp": "2025-11-15T10:30:00",
                    "temperature": 22.5,
                    "humidity": 65.0
                }
            ]
        }
    }


class IngestionResponse(BaseModel):
    """Response nach erfolgreicher Datenaufnahme"""
    status: str = Field(..., description="Status der Operation")
    message: str = Field(..., description="Statusmeldung")
    sensor_id: str = Field(..., description="Sensor-ID")
    kafka_partition: int = Field(..., description="Kafka Partition")
    kafka_offset: int = Field(..., description="Kafka Offset")
    timestamp: datetime = Field(..., description="Server-Zeitstempel")


@app.on_event("startup")
async def startup_event():
    """Initialisiert Kafka Producer beim App-Start"""
    logger.info(f"{SERVICE_NAME} startet...")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")

    try:
        get_kafka_producer()
        logger.info("Startup erfolgreich abgeschlossen")
    except Exception as e:
        logger.error(f"Startup fehlgeschlagen: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Schließt Kafka Producer beim Herunterfahren"""
    logger.info(f"{SERVICE_NAME} fährt herunter...")
    producer = getattr(get_kafka_producer, "producer", None)
    if producer:
        try:
            producer.flush()
            logger.info("Kafka Producer erfolgreich geschlossen")
        except Exception as e:
            logger.error(f"Fehler beim Schließen des Kafka Producers: {e}")
    logger.info("Shutdown abgeschlossen")


@app.get("/health")
async def health_check():
    """Health Check Endpoint für Kubernetes Liveness Probe"""
    return {
        "status": "healthy",
        "service": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/ready")
async def readiness_check():
    """Readiness Check Endpoint für Kubernetes Readiness Probe"""
    try:
        producer = get_kafka_producer()
        # Test: Metadata abfragen (wirft Exception, wenn Broker nicht erreichbar)
        metadata = producer.list_topics(timeout=5)
        return {
            "status": "ready",
            "service": SERVICE_NAME,
            "kafka_connected": True,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Readiness Check fehlgeschlagen: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service nicht bereit: {str(e)}"
        )


@app.post("/ingest", response_model=IngestionResponse, status_code=status.HTTP_201_CREATED)
async def ingest_sensor_data(data: SensorData):
    """
    Hauptendpoint: Empfängt Sensor-Daten und schreibt in Kafka
    """
    try:
        producer = get_kafka_producer()

        # Payload vorbereiten
        message_value = {
            "sensor_id": data.sensor_id,
            "timestamp": data.timestamp.isoformat(),
            "temperature": data.temperature,
            "humidity": data.humidity,
            "ingestion_time": datetime.now().isoformat()
        }

        # Kafka Key = Sensor-ID (für Partitionierung)
        message_key = data.sensor_id.encode('utf-8')
        message_bytes = json.dumps(message_value).encode('utf-8')

        logger.info(f"Sende Daten an Kafka Topic '{KAFKA_TOPIC}' - Sensor: {data.sensor_id}")

        # Callback für Delivery-Report
        delivery_report = {}
        def acked(err, msg):
            if err is not None:
                delivery_report['error'] = err
            else:
                delivery_report['partition'] = msg.partition()
                delivery_report['offset'] = msg.offset()

        # Sende an Kafka (asynchron, dann flush für Blockierung)
        producer.produce(
            topic=KAFKA_TOPIC,
            key=message_key,
            value=message_bytes,
            callback=acked
        )
        producer.flush(10)

        if 'error' in delivery_report:
            logger.error(f"Kafka Fehler beim Senden: {delivery_report['error']}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Kafka nicht verfügbar: {str(delivery_report['error'])}"
            )

        logger.info(
            f"Erfolgreich gesendet - Partition: {delivery_report.get('partition')}, "
            f"Offset: {delivery_report.get('offset')}"
        )

        return IngestionResponse(
            status="success",
            message="Sensor-Daten erfolgreich aufgenommen",
            sensor_id=data.sensor_id,
            kafka_partition=delivery_report.get('partition', -1),
            kafka_offset=delivery_report.get('offset', -1),
            timestamp=datetime.now()
        )

    except KafkaException as e:
        logger.error(f"Kafka Fehler beim Senden: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Kafka nicht verfügbar: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unerwarteter Fehler: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Interner Serverfehler: {str(e)}"
        )


@app.get("/")
async def root():
    """Root Endpoint mit API-Informationen"""
    return {
        "service": SERVICE_NAME,
        "version": "1.0.0",
        "description": "Data Ingestion API für Sensor-Daten",
        "endpoints": {
            "POST /ingest": "Sensor-Daten aufnehmen",
            "GET /health": "Health Check",
            "GET /ready": "Readiness Check",
            "GET /docs": "API Dokumentation (Swagger UI)",
            "GET /redoc": "API Dokumentation (ReDoc)"
        },
        "kafka": {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_TOPIC
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
