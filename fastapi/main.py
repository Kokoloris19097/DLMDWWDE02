"""
FastAPI Ingestion & Output Service - Data Engineering Master Project
Empfängt Sensor-Daten via HTTP POST und bietet Datenabfrage-Endpunkte
"""
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer, KafkaException
import json
import logging
import os
from datetime import datetime, timedelta, UTC
from typing import Optional, List
import uvicorn
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# UMGEBUNGSVARIABLEN
# =============================================================================
timezone = UTC

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka.messaging.svc.cluster.local:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sensor-data')
SERVICE_NAME = os.getenv('SERVICE_NAME', 'fastapi')

# PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgresql.data.svc.cluster.local')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sensordata')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'appuser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'appuser-secure-pw')

# FastAPI App
app = FastAPI(
    title="Data Ingestion & Query API",
    description="Empfängt Sensor-Daten und bietet Datenabfrage-Endpunkte",
    version="2.0.0"
)

# =============================================================================
# DATABASE CONNECTION MANAGEMENT
# =============================================================================

@contextmanager
def get_db_connection():
    """Context manager für PostgreSQL Verbindung"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10
        )
        yield conn
    except psycopg2.OperationalError as e:
        logger.error(f"DB Verbindung fehlgeschlagen: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Datenbankverbindung nicht verfügbar"
        )
    finally:
        if conn:
            conn.close()


# =============================================================================
# KAFKA PRODUCER
# =============================================================================

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
        """
        Validiert Timestamp auf Plausibilität:
        - Ablehnung: > 1 Tag in Zukunft oder > 1 Tag in Vergangenheit
        - Warnung: > 5 Minuten in Zukunft oder > 10 Stunden in Vergangenheit
        - Akzeptiert: Alle anderen Werte
        """
        now = datetime.now(v.tzinfo) if v.tzinfo else datetime.now(timezone)

        # Unplausible Werte: Ablehnen
        max_future = timedelta(days=1)
        max_past = timedelta(days=1)

        if v > now + max_future:
            raise ValueError(f"Timestamp liegt zu weit in der Zukunft (>{max_future.days} Tage)")

        if v < now - max_past:
            raise ValueError(f"Timestamp liegt zu weit in der Vergangenheit (>{max_past.days} Tage)")

        # Auffällige Werte: Loggen (aber akzeptieren)
        warning_future = timedelta(minutes=5)
        warning_past = timedelta(hours=10)

        import inspect
        try:
            frame = inspect.currentframe()
            # Gehe zwei Frames zurück (diese Funktion -> validate_* -> pydantic)
            outer = frame.f_back.f_back
            values = outer.f_locals.get('values', {})
            sensor_id = values.get('sensor_id', None)
        except Exception:
            sensor_id = None

        if v > now + warning_future:
            logger.warning(
                f"Auffälliger Timestamp in der Zukunft: {v.isoformat()} "
                f"(Differenz: {(v - now).total_seconds():.1f}s) "
                f"[sensor_id={sensor_id}]"
            )

        if v < now - warning_past:
            logger.warning(
                f"Auffälliger Timestamp in der Vergangenheit: {v.isoformat()} "
                f"(Alter: {(now - v).days} Tage) "
                f"[sensor_id={sensor_id}]"
            )

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


# =============================================================================
# OUTPUT MODELS (ABFRAGE)
# =============================================================================

class SensorReading(BaseModel):
    """Einzelne Sensor-Messung"""
    sensor_id: str
    timestamp: datetime
    temperature: float
    humidity: float


class SensorStats(BaseModel):
    """Statistiken für einen Sensor"""
    sensor_id: str
    period_start: datetime
    period_end: datetime
    temperature_min: float
    temperature_max: float
    temperature_avg: float
    humidity_min: float
    humidity_max: float
    humidity_avg: float
    reading_count: int


class SensorInfo(BaseModel):
    """Sensor-Übersicht"""
    sensor_id: str
    first_reading: datetime
    latest_reading: datetime
    reading_count: int


@app.on_event("startup")
async def startup_event():
    """Initialisiert Kafka Producer beim App-Start"""
    logger.info(f"{SERVICE_NAME} startet...")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

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
        "timestamp": datetime.now(timezone).isoformat()
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
            "timestamp": datetime.now(timezone).isoformat()
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
            "humidity": data.humidity
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

        # Sende asynchron an Kafka
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
            timestamp=datetime.now(timezone)
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


# =============================================================================
# OUTPUT ENDPOINTS (ABFRAGE)
# =============================================================================

@app.get("/sensors", response_model=List[SensorInfo])
async def list_all_sensors(limit: int = 50):
    """
    Liefert Übersicht aller Sensoren mit Statistiken

    Query Parameter:
    - limit: Maximal zu liefernde Sensoren (Default: 50, Max: 1000)
    """
    if limit < 1 or limit > 1000:
        limit = 50

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        sensor_id,
                        MIN(timestamp) AS first_ts,
                        MAX(timestamp) AS latest_ts,
                        COUNT(*) AS reading_count
                    FROM analytics_data
                    GROUP BY sensor_id
                    ORDER BY latest_ts DESC
                    LIMIT %s
                    """,
                    (limit,)
                )
                rows = cur.fetchall()

                # Berechnung der Timestamps in Python
                for row in rows:
                    row['first_reading'] = datetime.fromtimestamp(row['first_ts'] / 1000.0, tz=timezone)
                    row['latest_reading'] = datetime.fromtimestamp(row['latest_ts'] / 1000.0, tz=timezone)

        if not rows:
            return []

        return [SensorInfo(**row) for row in rows]

    except Exception as e:
        logger.error(f"Fehler beim Abrufen von Sensor-Übersicht: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Fehler beim Abrufen der Sensor-Übersicht"
        )


@app.get("/sensors/{sensor_id}/data", response_model=List[SensorReading])
async def get_sensor_data(
    sensor_id: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = 100
):
    """
    Liefert Zeitreihen-Daten eines Sensors

    Path Parameter:
    - sensor_id: Eindeutige Sensor-ID

    Query Parameter:
    - start: ISO-Timestamp (z.B. 2025-12-01T10:00:00), Default: 7 Tage zurück
    - end: ISO-Timestamp, Default: jetzt
    - limit: Max. Anzahl Datensätze (Default: 100, Max: 10000)
    """
    # Default: letzte 7 Tage
    if not end:
        end = datetime.now(timezone)
    if not start:
        start = end - timedelta(days=7)

    if limit < 1 or limit > 10000:
        limit = 100

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        sensor_id,
                        timestamp,
                        temperature,
                        humidity
                    FROM analytics_data
                    WHERE sensor_id = %s
                        AND timestamp BETWEEN %s AND %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                    """,
                    (sensor_id, int(start.timestamp() * 1000), int(end.timestamp() * 1000), limit)
                )
                rows = cur.fetchall()

        if not rows:
            logger.info(f"Keine Daten für Sensor {sensor_id} im Zeitfenster gefunden")
            return []

        return [SensorReading(**row) for row in rows]

    except Exception as e:
        logger.error(f"Fehler beim Abrufen von Sensor-Daten für {sensor_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Fehler beim Abrufen der Sensor-Daten"
        )


@app.get("/sensors/{sensor_id}/stats", response_model=SensorStats)
async def get_sensor_stats(
    sensor_id: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None
):
    """
    Liefert aggregierte Statistiken für einen Sensor

    Path Parameter:
    - sensor_id: Eindeutige Sensor-ID

    Query Parameter:
    - start: ISO-Timestamp, Default: 7 Tage zurück
    - end: ISO-Timestamp, Default: jetzt

    Liefert: min, max, avg für Temperatur und Luftfeuchtigkeit
    """
    if not end:
        end = datetime.now(timezone)
    if not start:
        start = end - timedelta(days=7)

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        sensor_id,
                        MIN(temperature) AS temperature_min,
                        MAX(temperature) AS temperature_max,
                        AVG(temperature) AS temperature_avg,
                        MIN(humidity) AS humidity_min,
                        MAX(humidity) AS humidity_max,
                        AVG(humidity) AS humidity_avg,
                        COUNT(*) AS reading_count
                    FROM analytics_data
                    WHERE sensor_id = %s
                      AND timestamp BETWEEN %s AND %s
                    GROUP BY sensor_id
                    """,
                    (sensor_id, int(start.timestamp() * 1000), int(end.timestamp() * 1000))
                )
                row = cur.fetchone()

        if not row:
            logger.info(f"Keine Daten für Sensor {sensor_id} im Zeitfenster gefunden")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Keine Statistiken für Sensor {sensor_id} gefunden"
            )

        # Konvertiere float-Werte sauber
        return SensorStats(
            sensor_id=row['sensor_id'],
            period_start=start,
            period_end=end,
            temperature_min=float(row['temperature_min']) if row['temperature_min'] else 0.0,
            temperature_max=float(row['temperature_max']) if row['temperature_max'] else 0.0,
            temperature_avg=float(row['temperature_avg']) if row['temperature_avg'] else 0.0,
            humidity_min=float(row['humidity_min']) if row['humidity_min'] else 0.0,
            humidity_max=float(row['humidity_max']) if row['humidity_max'] else 0.0,
            humidity_avg=float(row['humidity_avg']) if row['humidity_avg'] else 0.0,
            reading_count=int(row['reading_count']) if row['reading_count'] else 0
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fehler beim Abrufen von Statistiken für {sensor_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Fehler beim Abrufen der Statistiken"
        )


@app.get("/")
async def root():
    """Root Endpoint mit API-Informationen"""
    return {
        "service": SERVICE_NAME,
        "version": "2.0.0",
        "description": "Data Ingestion & Query API für Sensor-Daten",
        "endpoints": {
            "Ingestion": {
                "POST /ingest": "Sensor-Daten aufnehmen"
            },
            "Query": {
                "GET /sensors": "Übersicht aller Sensoren",
                "GET /sensors/{sensor_id}/data": "Zeitreihen-Daten",
                "GET /sensors/{sensor_id}/stats": "Aggregierte Statistiken"
            },
            "Health": {
                "GET /health": "Health Check",
                "GET /ready": "Readiness Check"
            },
            "Documentation": {
                "GET /docs": "Swagger UI",
                "GET /redoc": "ReDoc"
            }
        },
        "kafka": {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_TOPIC
        },
        "database": {
            "host": POSTGRES_HOST,
            "database": POSTGRES_DB,
            "table": "analytics_data"
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
