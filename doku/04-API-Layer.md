# 04 - API Layer

## Übersicht

Der API Layer stellt externe HTTP-Schnittstellen bereit für **Daten-Ingestion** (POST) und **Daten-Abfrage** (GET). FastAPI wird via NGINX Ingress Controller exponiert und kommuniziert mit Kafka (Producer) sowie PostgreSQL (Query).

**Namespace:** `api`

## FastAPI Service

### Technologie-Stack

**Image:** Custom-built `localhost/fastapi:latest`

**Base:** `python:3.13.9-slim-bookworm`

**Dockerfile:** [`fastapi/Dockerfile.fastapi`](../fastapi/Dockerfile.fastapi)

**Dependencies (`requirements.txt`):**
```txt
fastapi==0.115.6        # Web Framework
uvicorn==0.34.0         # ASGI Server
pydantic==2.10.5        # Data Validation
confluent-kafka==2.7.0  # Kafka Producer
psycopg2-binary==2.9.10 # PostgreSQL Client
python-dateutil==2.9.0  # Date Parsing
```

### Application Code

**Quellcode:** [`fastapi/main.py`](../fastapi/main.py)

#### Startup & Configuration

```python
from fastapi import FastAPI
from confluent_kafka import Producer
import psycopg2
import os

app = FastAPI(
    title="Sensor Data Ingestion API",
    version="1.0.0",
    description="REST API für Sensor-Daten Ingestion & Query"
)

# Kafka Producer
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS",
    "kafka.messaging.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 1,
    'linger.ms': 10,
    'compression.type': 'lz4'
})

# PostgreSQL Connection Pool (geplant)
POSTGRES_DSN = "postgresql://appuser:appuser-secure-pw@postgresql.data.svc.cluster.local:5432/sensordata"
```

### Endpoints

#### 1. POST /ingest - Daten-Aufnahme

**Request:**
```python
from pydantic import BaseModel, Field, validator

class SensorData(BaseModel):
    sensor_id: str = Field(..., min_length=1, max_length=50)
    timestamp: str = Field(..., pattern=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')
    temperature: float = Field(..., ge=-273.15, le=200.0)
    humidity: float = Field(..., ge=0.0, le=100.0)

    @validator('timestamp')
    def validate_timestamp(cls, v):
        from dateutil import parser
        try:
            parser.isoparse(v)
        except ValueError:
            raise ValueError("Invalid ISO 8601 timestamp")
        return v
```

**Handler:**
```python
import json
from datetime import datetime

@app.post("/ingest", status_code=201)
async def ingest_sensor_data(data: SensorData):
    # Kafka Message
    message = {
        "sensor_id": data.sensor_id,
        "timestamp": data.timestamp,
        "temperature": data.temperature,
        "humidity": data.humidity
    }

    # Produce zu Kafka
    producer.produce(
        topic=KAFKA_TOPIC,
        key=data.sensor_id.encode('utf-8'),  # Partitionierung nach Sensor
        value=json.dumps(message).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)  # Trigger Callbacks

    return {
        "status": "success",
        "message": "Sensor-Daten erfolgreich aufgenommen",
        "sensor_id": data.sensor_id,
        "timestamp": datetime.now().isoformat()
    }

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
```

**Beispiel-Request:**
```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_id": "SENSOR-001",
    "timestamp": "2025-12-14T10:30:00",
    "temperature": 22.5,
    "humidity": 65.0
  }'
```

**Response:**
```json
{
  "status": "success",
  "message": "Sensor-Daten erfolgreich aufgenommen",
  "sensor_id": "SENSOR-001",
  "timestamp": "2025-12-14T10:30:05.123456"
}
```

#### 2. GET /sensors - Sensor-Übersicht

**Handler:**
```python
@app.get("/sensors")
async def get_sensors(limit: int = 50):
    conn = psycopg2.connect(POSTGRES_DSN)
    cursor = conn.cursor()

    query = """
        SELECT
            sensor_id,
            MIN(timestamp) as first_reading,
            MAX(timestamp) as latest_reading,
            COUNT(*) as reading_count
        FROM analytics_data
        GROUP BY sensor_id
        ORDER BY latest_reading DESC
        LIMIT %s
    """

    cursor.execute(query, (limit,))
    results = cursor.fetchall()

    sensors = [
        {
            "sensor_id": row[0],
            "first_reading": row[1].isoformat(),
            "latest_reading": row[2].isoformat(),
            "reading_count": row[3]
        }
        for row in results
    ]

    cursor.close()
    conn.close()

    return {"sensors": sensors}
```

**Beispiel-Response:**
```json
{
  "sensors": [
    {
      "sensor_id": "SENSOR-001",
      "first_reading": "2025-12-14T10:00:00",
      "latest_reading": "2025-12-14T12:30:00",
      "reading_count": 150
    }
  ]
}
```

#### 3. GET /sensors/{sensor_id}/data - Zeitreihen-Daten

**Handler:**
```python
from typing import Optional

@app.get("/sensors/{sensor_id}/data")
async def get_sensor_data(
    sensor_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 100
):
    conn = psycopg2.connect(POSTGRES_DSN)
    cursor = conn.cursor()

    query = """
        SELECT sensor_id, timestamp, temperature, humidity
        FROM analytics_data
        WHERE sensor_id = %s
    """
    params = [sensor_id]

    if start:
        query += " AND timestamp >= %s"
        params.append(start)
    if end:
        query += " AND timestamp <= %s"
        params.append(end)

    query += " ORDER BY timestamp DESC LIMIT %s"
    params.append(limit)

    cursor.execute(query, params)
    results = cursor.fetchall()

    data = [
        {
            "sensor_id": row[0],
            "timestamp": row[1].isoformat(),
            "temperature": row[2],
            "humidity": row[3]
        }
        for row in results
    ]

    cursor.close()
    conn.close()

    return {"data": data}
```

**Beispiel-Request:**
```bash
curl "http://localhost:8000/sensors/SENSOR-001/data?start=2025-12-14T00:00:00&end=2025-12-14T23:59:59&limit=10"
```

#### 4. GET /sensors/{sensor_id}/stats - Aggregierte Statistiken

```python
@app.get("/sensors/{sensor_id}/stats")
async def get_sensor_stats(
    sensor_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None
):
    conn = psycopg2.connect(POSTGRES_DSN)
    cursor = conn.cursor()

    query = """
        SELECT
            sensor_id,
            MIN(timestamp) as period_start,
            MAX(timestamp) as period_end,
            MIN(temperature) as temperature_min,
            MAX(temperature) as temperature_max,
            AVG(temperature) as temperature_avg,
            MIN(humidity) as humidity_min,
            MAX(humidity) as humidity_max,
            AVG(humidity) as humidity_avg,
            COUNT(*) as reading_count
        FROM analytics_data
        WHERE sensor_id = %s
    """
    params = [sensor_id]

    if start:
        query += " AND timestamp >= %s"
        params.append(start)
    if end:
        query += " AND timestamp <= %s"
        params.append(end)

    query += " GROUP BY sensor_id"

    cursor.execute(query, params)
    row = cursor.fetchone()

    if not row:
        return {"error": "No data found"}

    stats = {
        "sensor_id": row[0],
        "period_start": row[1].isoformat() if row[1] else None,
        "period_end": row[2].isoformat() if row[2] else None,
        "temperature": {
            "min": row[3],
            "max": row[4],
            "avg": round(row[5], 2) if row[5] else None
        },
        "humidity": {
            "min": row[6],
            "max": row[7],
            "avg": round(row[8], 2) if row[8] else None
        },
        "reading_count": row[9]
    }

    cursor.close()
    conn.close()

    return stats
```

#### 5. GET /health - Health Check

```python
@app.get("/health")
async def health():
    return {"status": "healthy"}
```

#### 6. GET /ready - Readiness Probe

```python
@app.get("/ready")
async def readiness():
    # Prüfe Kafka Connection
    try:
        metadata = producer.list_topics(timeout=5)
        if KAFKA_TOPIC not in metadata.topics:
            return {"status": "not ready", "reason": "Kafka topic not found"}, 503
    except Exception as e:
        return {"status": "not ready", "reason": str(e)}, 503

    # Prüfe PostgreSQL Connection
    try:
        conn = psycopg2.connect(POSTGRES_DSN, connect_timeout=3)
        conn.close()
    except Exception as e:
        return {"status": "not ready", "reason": str(e)}, 503

    return {"status": "ready"}
```

### Deployment Konfiguration

**Template:** [`helm-charts/system-cluster/templates/fastapi.yaml`](../helm-charts/system-cluster/templates/fastapi.yaml)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fastapi
  namespace: {{ .Values.fastapi.namespace }}
spec:
  replicas: {{ .Values.fastapi.replicaCount }}
  selector:
    matchLabels:
      app: fastapi
  template:
    metadata:
      labels:
        app: fastapi
    spec:
      containers:
      - name: fastapi
        image: {{ .Values.fastapi.image }}
        imagePullPolicy: {{ .Values.fastapi.pullPolicy }}
        ports:
        - containerPort: 8000
          name: http
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: {{ .Values.fastapi.env.kafkaBootstrapServers }}
        - name: KAFKA_TOPIC
          value: {{ .Values.fastapi.env.kafkaTopic }}
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          {{- toYaml .Values.fastapi.resources | nindent 10 }}
```

**Service:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: fastapi
  namespace: {{ .Values.fastapi.namespace }}
spec:
  type: ClusterIP
  selector:
    app: fastapi
  ports:
    - port: 8000
      targetPort: 8000
      name: http
```

## NGINX Ingress

### Ingress-Konfiguration

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: fastapi-ingress
  namespace: {{ .Values.fastapi.namespace }}
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  ingressClassName: nginx
  rules:
  - host: {{ .Values.fastapi.ingress.host }}
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: fastapi
            port:
              number: 8000
```

**Values (values.yaml):**
```yaml
fastapi:
  ingress:
    enabled: true
    host: "fastapi.local"
    tls:
      enabled: false  # Für lokale Entwicklung
```

### Zugriff auf API

**Via Ingress (mit Host-Header):**
```bash
curl -H "Host: fastapi.local" http://localhost/health
```

**Via Port-Forwarding (empfohlen für lokal):**
```bash
kubectl port-forward -n api svc/fastapi 8000:8000
curl http://localhost:8000/health
```

**Automatisches Port-Forwarding:** [`port-forward.ps1`](../port-forward.ps1)

## Sensor Simulator

### Funktionsweise

**Quellcode:** [`simulator/sensor_simulator.py`](../simulator/sensor_simulator.py)

**Dependencies:**
```txt
requests==2.32.3
```

#### Sensor-Typen

```python
SENSOR_TYPES = [
    "Datacenter Rack 1",
    "Server Room A",
    "Cooling Unit 3",
    "Storage Area B",
    "Network Equipment Room"
]
```

#### Daten-Generierung

```python
import random
from datetime import datetime

class SensorSimulator:
    def __init__(self, sensor_id: str, sensor_type: str):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type

        # Initiale Werte mit Drift
        self.temperature = random.uniform(-10, 40)
        self.humidity = random.uniform(30, 99)

    def generate_reading(self):
        # Simuliere Drift
        self.temperature += random.uniform(-2, 2)
        self.humidity += random.uniform(-8, 8)

        # Schwankungen
        temp = self.temperature + random.uniform(-0.2, 0.2)
        hum = self.humidity + random.uniform(-1, 1)

        # Clamp Ranges
        temp = max(-10, min(40, temp))
        hum = max(30, min(99, hum))

        return {
            "sensor_id": self.sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "temperature": round(temp, 2),
            "humidity": round(hum, 2)
        }
```

#### Haupt-Loop

```python
import requests
import time

API_URL = "http://localhost:8000/ingest"
INTERVAL = 2.0  # Sekunden
NUM_SENSORS = 5

sensors = [
    SensorSimulator(f"SENSOR-{i:03d}", SENSOR_TYPES[i % len(SENSOR_TYPES)])
    for i in range(1, NUM_SENSORS + 1)
]

total_sent = 0
total_failed = 0

while True:
    for sensor in sensors:
        data = sensor.generate_reading()

        try:
            response = requests.post(API_URL, json=data, timeout=5)
            if response.status_code == 201:
                total_sent += 1
            else:
                total_failed += 1
        except Exception as e:
            total_failed += 1
            print(f"Error: {e}")

    print(f"Sent: {total_sent} | Failed: {total_failed} | Rate: {total_sent / (time.time() - start_time):.2f} msg/s")
    time.sleep(INTERVAL)
```

### Kommandozeilen-Optionen

```bash
python simulator/sensor_simulator.py --help

# Optionen:
--interval 1.0      # Intervall zwischen Batches (Sekunden)
--sensors 15        # Anzahl simulierter Sensoren
--api-url http://localhost:8000/ingest  # FastAPI Endpoint
```

**Automatischer Start:** `init.ps1` startet Simulator mit 15 Sensoren @ 1.0s Intervall

## API-Sequenzdiagramm

```mermaid
sequenceDiagram
    participant Sim as Sensor Simulator
    participant Ing as NGINX Ingress
    participant FA as FastAPI Pod
    participant K as Kafka Broker
    participant PG as PostgreSQL

    Note over Sim: Generate Sensor Reading
    Sim->>Ing: POST /ingest<br/>{sensor_id, timestamp, temp, hum}
    Ing->>FA: Forward Request

    Note over FA: Validate with Pydantic
    FA->>FA: Validate Schema<br/>(temp: -273.15-200°C, hum: 0-100%)

    Note over FA: Kafka Produce
    FA->>K: Produce to 'sensor-data'<br/>Key: sensor_id
    K-->>FA: ACK (partition, offset)

    FA-->>Ing: 201 Created<br/>{status, timestamp}
    Ing-->>Sim: HTTP Response

    Note over Sim: Log Success/Failure

    rect rgb(200, 220, 255)
    Note right of Sim: Query Flow (separater Request)
    Sim->>Ing: GET /sensors/SENSOR-001/data
    Ing->>FA: Forward Request
    FA->>PG: SQL Query<br/>SELECT * FROM analytics_data<br/>WHERE sensor_id = 'SENSOR-001'
    PG-->>FA: Rows
    FA-->>Ing: 200 OK<br/>[{timestamp, temp, hum}, ...]
    Ing-->>Sim: HTTP Response
    end
```

## Swagger UI

FastAPI generiert automatisch interaktive API-Dokumentation:

**URL:** http://localhost:8000/docs

**Features:**
- Alle Endpoints mit Request/Response Schemas
- Try-it-out Funktionalität
- Schema-Validierung (Pydantic Models)
- Authentifizierung (wenn konfiguriert)

**Alternative (ReDoc):** http://localhost:8000/redoc

## Performance & Skalierung

### Horizontal Scaling

**Replicas erhöhen:**
```yaml
# values.yaml
fastapi:
  replicaCount: 3  # Statt 1
```

**Load-Balancing:** NGINX Ingress verteilt automatisch Traffic auf alle Pods

### Kafka Producer Tuning

```python
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 1,          # Leader Ack (schneller als acks=all)
    'linger.ms': 10,    # Batching: 10ms warten
    'batch.size': 16384,  # 16 KB Batches
    'compression.type': 'lz4',  # Schnelle Kompression
    'max.in.flight.requests.per.connection': 5
})
```

### PostgreSQL Connection Pooling (geplant)

```python
import psycopg2.pool

# Connection Pool statt neue Connections
pg_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=POSTGRES_DSN
)

@app.get("/sensors")
async def get_sensors():
    conn = pg_pool.getconn()
    try:
        # Query
        pass
    finally:
        pg_pool.putconn(conn)
```

### Caching (Redis geplant)

```python
from redis import Redis

redis_client = Redis(host='redis.api.svc.cluster.local', port=6379)

@app.get("/sensors")
async def get_sensors(limit: int = 50):
    cache_key = f"sensors:list:{limit}"

    # Cache Hit?
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # Cache Miss → PostgreSQL
    # ...
    redis_client.setex(cache_key, 60, json.dumps(result))  # 60s TTL
    return result
```

## Monitoring

### Health Checks

**Liveness:** `/health` - Prüft ob FastAPI-Prozess läuft
**Readiness:** `/ready` - Prüft Kafka + PostgreSQL Connectivity

**Kubernetes Probes:**
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /ready
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 2
```

### Logs

**Uvicorn Logging:**
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("fastapi")
logger.info(f"Received sensor data from {data.sensor_id}")
```

**Pod Logs anschauen:**
```bash
kubectl logs -n api deployment/fastapi -f
```

## Troubleshooting

### Häufige Probleme

**Problem: Connection Refused (Kafka)**
```bash
# Kafka Service DNS prüfen
kubectl exec -n api deployment/fastapi -- \
  nslookup kafka.messaging.svc.cluster.local

# Network Connectivity
kubectl exec -n api deployment/fastapi -- \
  nc -zv kafka.messaging.svc.cluster.local 9092
```

**Problem: PostgreSQL Connection Failed**
```bash
# PostgreSQL Service DNS
kubectl exec -n api deployment/fastapi -- \
  nslookup postgresql.data.svc.cluster.local

# psql Test
kubectl exec -n api deployment/fastapi -- \
  apt-get update && apt-get install -y postgresql-client
kubectl exec -n api deployment/fastapi -- \
  psql "postgresql://appuser:appuser-secure-pw@postgresql.data.svc.cluster.local:5432/sensordata" -c "\dt"
```

**Problem: Ingress 404**
```bash
# Ingress Status
kubectl get ingress -n api

# NGINX Logs
kubectl logs -n api deployment/ingress-nginx-controller -f

# Host-Header testen
curl -v -H "Host: fastapi.local" http://localhost/health
```

**Problem: Slow Response Times**
```bash
# Prüfe Pod Resources
kubectl top pod -n api

# PostgreSQL Slow Queries (siehe 03-Data-Processing.md)
```

## Weiterführende Dokumentation

- **[03 - Data Processing](03-Data-Processing.md)** - PostgreSQL Schema & Queries
- **[02 - Kafka Messaging](02-Kafka-Messaging.md)** - Kafka Producer Best Practices
- **[07 - Testing](07-Testing.md)** - FastAPI Endpoint Tests
- **[FastAPI Documentation](https://fastapi.tiangolo.com/)**
- **[Pydantic Documentation](https://docs.pydantic.dev/)**

---

**Navigation:** [← Zurück zu Data Processing](03-Data-Processing.md) | [Weiter zu Monitoring →](05-Monitoring.md)
