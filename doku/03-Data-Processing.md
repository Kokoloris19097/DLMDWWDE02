# 03 - Data Processing

## Übersicht

Die Data Processing Layer verarbeitet Sensor-Daten in Echtzeit mit **Apache Spark Structured Streaming** und speichert aggregierte Ergebnisse in **PostgreSQL**. Kafka Connect synchronisiert kontinuierlich Daten zwischen Kafka und der Datenbank.

**Namespace:** `data`

![Datenfluss](Datenfluss.png)

## Apache Spark

### Spark Structured Streaming

**Image:** Custom-built `localhost/spark:latest`

**Base:** `apache/spark:3.5.7-scala2.12-java11-python3-r-ubuntu`

**Dockerfile:** [`spark/Dockerfile.spark`](../spark/Dockerfile.spark)

**Dependencies:**
```dockerfile
# PySpark Kafka Integration
pyspark==3.5.7

# Kafka Client Library
kafka-python==2.0.2

# JAR Dependencies (im Image)
/opt/spark/jars/
  ├── spark-sql-kafka-0-10_2.12-3.5.7.jar
  ├── kafka-clients-3.4.0.jar
  ├── spark-token-provider-kafka-0-10_2.12-3.5.7.jar
  └── commons-pool2-2.11.1.jar
```

### Streaming-Logik

**Quellcode:** [`spark/main.py`](../spark/main.py)

#### 1. Topic-Erstellung

```python
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
)

topics = [
    NewTopic(name="sensor-data", num_partitions=1, replication_factor=2),
    NewTopic(name="analytics-data", num_partitions=1, replication_factor=2)
]

# Erstelle Topics falls nicht vorhanden
existing_topics = admin_client.list_topics()
for topic in topics:
    if topic.name not in existing_topics:
        admin_client.create_topics([topic])
```

#### 2. Kafka Stream Lesen

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("SensorDataProcessor") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

# Schema für eingehende Daten
sensor_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("humidity", DoubleType(), False)
])

# Kafka Stream
raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# JSON Parsing
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), sensor_schema).alias("data")) \
    .select("data.*")
```

**Wichtige Parameter:**
- **`startingOffsets: latest`** - Nur neue Nachrichten (nicht alle vorhandenen)
- **Checkpoint Location:** `/tmp/spark-checkpoint` (für Offset-Tracking bei Neustarts)

#### 3. Tumbling Window Aggregation

```python
from pyspark.sql.functions import window, avg, to_timestamp

# Timestamp String → Timestamp Type
timestamped = parsed_stream.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)

# Tumbling Window: 30 Sekunden
aggregated = timestamped \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), WINDOW_DURATION),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
```

**Aggregation-Details:**

```mermaid
gantt
    title Tumbling Window (30 Sekunden)
    dateFormat HH:mm:ss
    axisFormat %H:%M:%S

    section Window 1
    10:00:00 - 10:00:30 :w1, 10:00:00, 30s

    section Window 2
    10:00:30 - 10:01:00 :w2, 10:00:30, 30s

    section Window 3
    10:01:00 - 10:01:30 :w3, 10:01:00, 30s
```

**Parameter:**
- **Window Duration:** `30 seconds` (konfigurierbar via `WINDOW_DURATION`)
- **Watermark:** `1 minute` (erlaubt verspätete Nachrichten bis 1 Min)
- **Aggregation:** `avg(temperature)`, `avg(humidity)`
- **Grouping:** `sensor_id`, `window`

#### 4. Kafka Output

```python
from pyspark.sql.functions import struct, to_json

# Bereite Output vor (flatten window struct)
output = aggregated \
    .select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity")
    ) \
    .select(to_json(struct("*")).alias("value"))

# Schreibe zu Kafka
query = output \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", TARGET_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start()

query.awaitTermination()
```

**Output-Format (JSON):**
```json
{
  "sensor_id": "SENSOR-001",
  "window_start": "2025-12-14T10:00:00",
  "window_end": "2025-12-14T10:00:30",
  "avg_temperature": 22.4,
  "avg_humidity": 64.8
}
```

**Trigger Interval:** `10 seconds` - Spark checkt alle 10s für neue Batches

### Konfiguration (values.yaml)

```yaml
spark:
  namespace: data
  image: localhost/spark:latest
  pullPolicy: Never

  env:
    KAFKA_BOOTSTRAP_SERVERS: |
      kafka-broker-0.kafka-broker.messaging.svc.cluster.local:9092,
      kafka-broker-1.kafka-broker.messaging.svc.cluster.local:9092
    SOURCE_TOPIC: "sensor-data"
    TARGET_TOPIC: "analytics-data"
    WINDOW_DURATION: "30 seconds"

  streaming:
    checkpointLocation: "/tmp/spark-checkpoint"
    triggerInterval: "10 seconds"
    watermarkDelay: "1 minute"

  resources:
    requests:
      cpu: "1000m"
      memory: "2Gi"
    limits:
      cpu: "2000m"
      memory: "4Gi"
```

### Deployment (Pod)

**Template:** [`helm-charts/system-cluster/templates/spark.yaml`](../helm-charts/system-cluster/templates/spark.yaml)

**Warum Pod statt Deployment?**
- Spark Streaming ist stateful (Checkpoint-Location)
- Einfache Neustarts für Testing
- Keine Load-Balancing erforderlich (nur 1 Instanz)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: spark
  namespace: {{ .Values.spark.namespace }}
  labels:
    app: spark
spec:
  containers:
  - name: spark
    image: {{ .Values.spark.image }}
    imagePullPolicy: {{ .Values.spark.pullPolicy }}
    command: ["python3", "/app/main.py"]
    env:
    {{- range $key, $value := .Values.spark.env }}
    - name: {{ $key }}
      value: {{ $value | quote }}
    {{- end }}
    resources:
      {{- toYaml .Values.spark.resources | nindent 6 }}
```

### Monitoring

**Spark UI (Port 4040):**
```bash
# Port-Forwarding
kubectl port-forward -n data pod/spark 4040:4040

# Zugriff: http://localhost:4040
```

**Spark UI Features:**
- Streaming Query Statistics (Input Rate, Processing Rate)
- Batch Details (Trigger Times, Processing Times)
- DAG Visualization
- Executor Metrics

**Logs:**
```bash
kubectl logs -n data pod/spark -f
```

**Wichtige Log-Patterns:**
```
Batch: <batch-id>
Input rows: <count>
Processed rows: <count>
Trigger time: <ms>
```

## PostgreSQL

### Database Schema

**Image:** `postgres:16-alpine`

**Database:** `sensordata`

**Init-Script:** ConfigMap in [`helm-charts/system-cluster/templates/postgresql.yaml`](../helm-charts/system-cluster/templates/postgresql.yaml)

```sql
-- Datenbank erstellen
CREATE DATABASE sensordata;

-- Zum sensordata DB wechseln
\c sensordata;

-- Analytics-Tabelle
CREATE TABLE IF NOT EXISTS analytics_data (
    sensor_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sensor_id, timestamp)
);

-- Index für Time-Range Queries
CREATE INDEX IF NOT EXISTS idx_analytics_timestamp
    ON analytics_data (timestamp DESC);

-- App-User mit Berechtigungen
CREATE USER appuser WITH PASSWORD 'appuser-secure-pw';
GRANT ALL PRIVILEGES ON DATABASE sensordata TO appuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO appuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO appuser;
```

**Tabellen-Schema:**

| Spalte | Typ | Constraints | Beschreibung |
|--------|-----|-------------|--------------|
| `sensor_id` | VARCHAR(50) | NOT NULL, PK | Sensor-Identifikator |
| `timestamp` | TIMESTAMP | NOT NULL, PK | Aggregations-Zeitpunkt (Window Start) |
| `temperature` | DOUBLE PRECISION | - | Durchschnittstemperatur (°C) |
| `humidity` | DOUBLE PRECISION | - | Durchschnitt Luftfeuchtigkeit (%) |
| `created_at` | TIMESTAMP | DEFAULT NOW() | Einfügezeitpunkt |

**Composite Primary Key:** `(sensor_id, timestamp)`
- Verhindert Duplikate für gleiche Sensor + Zeitfenster
- Ermöglicht Kafka Connect Upserts

### Benutzer & Berechtigungen

**Admin-User (für Maintenance):**
- **Username:** `postgres`
- **Password:** `postgres-admin-pw`
- **Berechtigungen:** SUPERUSER

**App-User (für FastAPI/Kafka Connect):**
- **Username:** `appuser`
- **Password:** `appuser-secure-pw`
- **Berechtigungen:** ALL auf `sensordata` Database

### StatefulSet Konfiguration

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgresql
  namespace: {{ .Values.postgresql.namespace }}
spec:
  serviceName: postgresql-headless
  replicas: 1
  selector:
    matchLabels:
      app: postgresql
  template:
    metadata:
      labels:
        app: postgresql
    spec:
      containers:
      - name: postgresql
        image: {{ .Values.postgresql.image }}
        env:
        - name: POSTGRES_DB
          value: {{ .Values.postgresql.database }}
        - name: POSTGRES_USER
          value: {{ .Values.postgresql.env.POSTGRES_USER }}
        - name: POSTGRES_PASSWORD
          value: {{ .Values.postgresql.env.POSTGRES_PASSWORD }}
        - name: PGDATA
          value: /var/lib/postgresql/data/pgdata
        volumeMounts:
        - name: postgresql-data
          mountPath: /var/lib/postgresql/data
        - name: init-script
          mountPath: /docker-entrypoint-initdb.d
        ports:
        - containerPort: 5432
          name: postgresql
        livenessProbe:
          exec:
            command: ["pg_isready", "-U", "postgres"]
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "postgres"]
          initialDelaySeconds: 10
          periodSeconds: 5
      volumes:
      - name: init-script
        configMap:
          name: postgresql-init-script
  volumeClaimTemplates:
  - metadata:
      name: postgresql-data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: standard
      resources:
        requests:
          storage: 5Gi
```

### Services

**Headless Service (für StatefulSet):**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: postgresql-headless
  namespace: data
spec:
  clusterIP: None
  selector:
    app: postgresql
  ports:
    - port: 5432
      targetPort: 5432
```

**Client-facing Service:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: postgresql
  namespace: data
spec:
  type: ClusterIP
  selector:
    app: postgresql
  ports:
    - port: 5432
      targetPort: 5432
```

**Connection String:**
```
postgresql://appuser:appuser-secure-pw@postgresql.data.svc.cluster.local:5432/sensordata
```

### Database-Management

**psql Console:**
```bash
# Via kubectl exec
kubectl exec -it -n data postgresql-0 -- psql -U postgres -d sensordata

# Queries
sensordata=# SELECT COUNT(*) FROM analytics_data;
sensordata=# SELECT * FROM analytics_data ORDER BY timestamp DESC LIMIT 10;
sensordata=# \dt  -- Liste Tabellen
sensordata=# \d analytics_data  -- Tabellen-Schema
```

**Backup (manuell):**
```bash
# Dump erstellen
kubectl exec -n data postgresql-0 -- pg_dump -U postgres sensordata > backup.sql

# Restore
kubectl exec -i -n data postgresql-0 -- psql -U postgres sensordata < backup.sql
```

**Performance-Metriken:**
```sql
-- Tabellengröße
SELECT pg_size_pretty(pg_total_relation_size('analytics_data'));

-- Index-Nutzung
SELECT
    schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename = 'analytics_data';

-- Aktive Verbindungen
SELECT * FROM pg_stat_activity WHERE datname = 'sensordata';
```

## Datenfluss-Details

### End-to-End Pipeline

```mermaid
sequenceDiagram
    participant Sim as Sensor Simulator
    participant FA as FastAPI
    participant K1 as Kafka Topic<br/>'sensor-data'
    participant SP as Spark Streaming
    participant K2 as Kafka Topic<br/>'analytics-data'
    participant KC as Kafka Connect
    participant PG as PostgreSQL

    Sim->>FA: POST /ingest<br/>{sensor_id, timestamp, temp, hum}
    FA->>K1: Produce (JSON)
    Note over K1: Retention: 7 Tage<br/>RF: 2, Partitions: 1

    loop Every 10 seconds (Trigger)
        SP->>K1: Read Batch (micro-batch)
        Note over SP: Tumbling Window (30s)<br/>Group: sensor_id, window<br/>Agg: avg(temp), avg(hum)
        SP->>K2: Produce (JSON)
    end

    Note over K2: Retention: 7 Tage<br/>RF: 2, Partitions: 1

    loop Continuously
        KC->>K2: Consume (JDBC Sink)
        Note over KC: Transform:<br/>ISO 8601 → SQL Timestamp
        KC->>PG: INSERT ON CONFLICT UPDATE<br/>(sensor_id, timestamp)
        PG-->>KC: Rows Affected
        KC->>K2: Commit Offset
    end
```

### Daten-Transformationen

**1. FastAPI → Kafka (sensor-data):**
```json
{
  "sensor_id": "SENSOR-001",
  "timestamp": "2025-12-14T10:00:15",
  "temperature": 22.5,
  "humidity": 65.0
}
```

**2. Spark Aggregation (30s Fenster):**
```
Window: 10:00:00 - 10:00:30
Input:  10 Messages (SENSOR-001)
  → avg(temperature) = 22.4°C
  → avg(humidity) = 64.8%
```

**3. Spark → Kafka (analytics-data):**
```json
{
  "sensor_id": "SENSOR-001",
  "window_start": "2025-12-14T10:00:00",
  "window_end": "2025-12-14T10:00:30",
  "avg_temperature": 22.4,
  "avg_humidity": 64.8
}
```

**4. Kafka Connect Transformation:**
```sql
-- TimestampConverter Transform
"2025-12-14T10:00:00" → TIMESTAMP '2025-12-14 10:00:00'
```

**5. PostgreSQL Row:**
```sql
INSERT INTO analytics_data (sensor_id, timestamp, temperature, humidity)
VALUES ('SENSOR-001', '2025-12-14 10:00:00', 22.4, 64.8)
ON CONFLICT (sensor_id, timestamp)
DO UPDATE SET temperature = EXCLUDED.temperature, humidity = EXCLUDED.humidity;
```

### Latenz-Analyse

**Gesamtlatenz: ~40-60 Sekunden**

| Schritt | Latenz | Beschreibung |
|---------|--------|--------------|
| FastAPI → Kafka | ~10ms | Producer Ack |
| Kafka Retention | ~0s | Stream-Processing |
| Spark Window Wait | 0-30s | Tumbling Window (worst case) |
| Spark Processing | ~10s | Trigger Interval |
| Kafka → Connect | ~5s | Consumer Poll Interval |
| Connect → PostgreSQL | ~1s | JDBC Batch Insert |

**Optimierungsmöglichkeiten:**
- Kleinere Spark Windows (z.B. 10s statt 30s) → geringere Latenz
- Kürzerer Trigger Interval (z.B. 5s statt 10s) → schnellere Verarbeitung
- Kafka Connect `batch.size` erhöhen → weniger Roundtrips

## Performance & Skalierung

### Spark Tuning

**Executor-Konfiguration:**
```python
spark = SparkSession.builder \
    .appName("SensorDataProcessor") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.default.parallelism", "4") \
    .getOrCreate()
```

**Streaming-Optimierung:**
```python
# Kafka Consumer Cache
spark.conf.set("spark.sql.streaming.kafka.consumer.cache.enabled", "true")

# Max Rate per Partition (Backpressure)
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")

# Shuffle Partitions (Default: 200 → zu viel für lokales Setup)
spark.conf.set("spark.sql.shuffle.partitions", "4")
```

### PostgreSQL Tuning

**postgresql.conf (via ConfigMap geplant):**
```ini
# Connection Pooling
max_connections = 100

# Memory
shared_buffers = 256MB
effective_cache_size = 1GB

# Write Performance
wal_buffers = 16MB
checkpoint_completion_target = 0.9

# Query Performance
work_mem = 4MB
maintenance_work_mem = 64MB
```

**Index-Optimierung:**
```sql
-- Multi-Column Index für Time-Range Queries mit Sensor-Filter
CREATE INDEX idx_analytics_sensor_timestamp
    ON analytics_data (sensor_id, timestamp DESC);

-- Partial Index für Recent Data (letzte 7 Tage)
CREATE INDEX idx_analytics_recent
    ON analytics_data (timestamp DESC)
    WHERE timestamp > NOW() - INTERVAL '7 days';
```

### Skalierungs-Strategie

**Horizontale Skalierung:**

1. **Kafka Partitions erhöhen:**
   ```bash
   kafka-topics.sh --bootstrap-server localhost:9092 \
     --alter --topic sensor-data --partitions 4
   ```

2. **Spark Parallelism:**
   ```python
   # Mehr Kafka Partitions = mehr Spark Tasks
   spark.conf.set("spark.default.parallelism", num_partitions * 2)
   ```

3. **PostgreSQL Read Replicas:**
   ```yaml
   # StatefulSet Replicas erhöhen + Streaming Replication
   replicas: 3  # 1 Primary + 2 Replicas
   ```

4. **Kafka Connect Scaling:**
   ```yaml
   # Deployment Replicas erhöhen
   replicas: 3
   # Tasks.max in Connector Config
   "tasks.max": "3"
   ```

## Troubleshooting

### Spark-Probleme

**Problem: Spark Job hängt**
```bash
# Spark UI prüfen
kubectl port-forward -n data pod/spark 4040:4040

# Logs
kubectl logs -n data pod/spark --tail=100

# Checkpoint löschen (bei Corruption)
kubectl exec -n data pod/spark -- rm -rf /tmp/spark-checkpoint/*
kubectl delete pod -n data spark  # Neustart
```

**Problem: Kafka Connection Failed**
```bash
# DNS-Auflösung testen
kubectl exec -n data pod/spark -- \
  nslookup kafka-broker-0.kafka-broker.messaging.svc.cluster.local

# Network Connectivity
kubectl exec -n data pod/spark -- \
  nc -zv kafka.messaging.svc.cluster.local 9092
```

**Problem: Out of Memory**
```yaml
# Mehr Memory allocieren (values.yaml)
spark:
  resources:
    limits:
      memory: "8Gi"  # Erhöhen
```

### PostgreSQL-Probleme

**Problem: Connection Refused**
```bash
# Pod Status
kubectl get pod -n data postgresql-0

# Logs
kubectl logs -n data postgresql-0 --tail=50

# pg_isready
kubectl exec -n data postgresql-0 -- pg_isready -U postgres
```

**Problem: Disk Full**
```bash
# PVC Status
kubectl get pvc -n data

# Disk Usage im Pod
kubectl exec -n data postgresql-0 -- df -h /var/lib/postgresql/data

# Größte Tabellen
kubectl exec -n data postgresql-0 -- psql -U postgres -d sensordata -c \
  "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
   FROM pg_tables ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC LIMIT 10;"
```

**Problem: Slow Queries**
```sql
-- Enable Query Logging
ALTER SYSTEM SET log_min_duration_statement = 1000;  -- Queries > 1s
SELECT pg_reload_conf();

-- pg_stat_statements Extension
CREATE EXTENSION pg_stat_statements;
SELECT query, calls, mean_exec_time FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;
```

### Kafka Connect-Probleme

**Problem: Connector nicht registered**
```bash
# Connector Status
kubectl exec -n messaging deployment/kafka-connect -- \
  curl -s http://localhost:8083/connectors

# Manuell registrieren
kubectl exec -n messaging deployment/kafka-connect -- \
  curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/connector-config.json
```

**Problem: Tasks failed**
```bash
# Task Status
kubectl exec -n messaging deployment/kafka-connect -- \
  curl -s http://localhost:8083/connectors/postgresql-sink/tasks/0/status

# Logs für Errors
kubectl logs -n messaging deployment/kafka-connect | grep -i error
```

## Weiterführende Dokumentation

- **[02 - Kafka Messaging](02-Kafka-Messaging.md)** - Kafka Topics & Connect
- **[04 - API Layer](04-API-Layer.md)** - FastAPI Query-Endpoints für PostgreSQL
- **[08 - Troubleshooting](08-Troubleshooting.md)** - Debug-Dataflow-Skript
- **[Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)**
- **[PostgreSQL Documentation](https://www.postgresql.org/docs/)**

---

**Navigation:** [← Zurück zu Kafka Messaging](02-Kafka-Messaging.md) | [Weiter zu API Layer →](04-API-Layer.md)
