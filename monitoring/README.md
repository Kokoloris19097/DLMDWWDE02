# Prometheus Monitoring Setup

## Übersicht

Dieses Monitoring-System sammelt Metriken von allen Komponenten des Data Engineering Systems:

### Überwachte Komponenten

1. **Kafka Broker & Controller**
   - JMX Exporter auf Port 7071
   - JVM Metriken (Memory, GC, Threads)
   - Kafka-spezifische Metriken (Topics, Partitions, Requests)

2. **Kafka Connect**
   - JMX Metriken auf Port 7071
   - REST API Metriken auf `/metrics` Endpoint

3. **PostgreSQL**
   - Postgres Exporter (Port 9187)
   - Standard DB Metriken (Connections, Transactions, Locks)
   - Custom Queries für `analytics_data` Tabelle

4. **FastAPI**
   - Prometheus Instrumentierung
   - HTTP Request Metriken (Latenz, Status Codes)
   - Custom Application Metriken (Ingestion Counter, Kafka Publish)

5. **Kubernetes**
   - Pod Health Status
   - Node Metriken
   - Container Resource Usage

## Architektur

```
┌─────────────────┐
│  Kafka Broker   │──┐
│  (JMX: 7071)    │  │
└─────────────────┘  │
                     │
┌─────────────────┐  │
│ Kafka Controller│──┤
│  (JMX: 7071)    │  │
└─────────────────┘  │
                     │    ┌──────────────────┐
┌─────────────────┐  │    │   Prometheus     │
│ Kafka Connect   │──┼───▶│   (Port 9090)    │──▶ Grafana (optional)
│  (JMX: 7071)    │  │    │                  │
└─────────────────┘  │    │ - TSDB Storage   │
                     │    │ - PromQL Engine  │
┌─────────────────┐  │    │ - Alert Manager  │
│   PostgreSQL    │──┤    └──────────────────┘
│ (Exporter: 9187)│  │
└─────────────────┘  │
                     │
┌─────────────────┐  │
│    FastAPI      │──┘
│  (/metrics)     │
└─────────────────┘
```

## Komponenten

### 1. Prometheus Server

**Deployment**: `templates/prometheus.yaml`

- **Storage**: 10Gi PersistentVolume
- **Retention**: 15 Tage
- **Scrape Interval**: 15 Sekunden
- **Resources**:
  - Requests: 500m CPU, 1Gi RAM
  - Limits: 2000m CPU, 4Gi RAM

**ConfigMap**: Enthält `prometheus.yml` mit allen Scrape-Targets

**RBAC**: ClusterRole für Kubernetes API Discovery

### 2. JMX Exporter (Kafka)

**ConfigMap**: `templates/jmx-exporter-config.yaml`

- Extrahiert JVM und Kafka-Metriken aus JMX MBeans
- Konvertiert zu Prometheus-Format
- Läuft als Java Agent im Kafka-Prozess

**Integration**:
- InitContainer lädt `jmx_prometheus_javaagent.jar` herunter
- Mounted als Volume in Kafka Pods
- KAFKA_OPTS: `-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/config.yaml`

**Verfügbare Metriken**:
```promql
# JVM Memory
jvm_memory_heap_used_bytes
jvm_memory_heap_max_bytes
jvm_gc_collection_count_total

# Kafka Broker
kafka_server_brokertopicmetrics_messagesinpersec_total
kafka_server_brokertopicmetrics_bytesinpersec_total
kafka_network_requestmetrics_requests_total

# Kafka Controller
kafka_controller_kafkacontroller_activecontrollercount
kafka_controller_controllerstats_leaderelectionrateandtimems_total
```

### 3. PostgreSQL Exporter

**Deployment**: `templates/monitoring-exporters.yaml`

- Image: `prometheuscommunity/postgres-exporter:v0.15.0`
- Port: 9187
- Connection String: Via ENV Variable

**Custom Queries** (ConfigMap: `postgres-exporter-queries`):

1. `pg_analytics_data_total_rows` - Gesamtanzahl Zeilen
2. `pg_analytics_data_rows_by_sensor` - Zeilen pro Sensor
3. `pg_analytics_data_latest_timestamp` - Letzter Timestamp pro Sensor
4. `pg_analytics_data_avg_temperature` - Durchschnittstemperatur (1h)
5. `pg_analytics_data_avg_humidity` - Durchschnittsfeuchtigkeit (1h)
6. `pg_database_size` - Datenbankgröße in Bytes
7. `pg_table_size` - Tabellengröße in Bytes

**Beispiel-Abfrage**:
```promql
pg_analytics_data_rows_by_sensor{sensor_id="SENSOR-001"}
```

### 4. FastAPI Metriken

**Instrumentierung**: `prometheus-fastapi-instrumentator`

**Standard-Metriken**:
- `http_requests_total` - Anzahl HTTP Requests
- `http_request_duration_seconds` - Request Latenz
- `http_requests_in_progress` - Aktive Requests

**Custom Metriken** (in `fastapi/main.py`):

```python
sensor_data_ingestion_total{sensor_id, status}  # Counter
sensor_data_ingestion_duration_seconds{sensor_id}  # Histogram
kafka_messages_published_total{topic, status}  # Counter
database_queries_total{query_type, status}  # Counter
database_query_duration_seconds{query_type}  # Histogram
kafka_producer_active_connections  # Gauge
```

## Deployment

### Voraussetzungen

```powershell
# 1. Namespace erstellen (automatisch via Helm)
kubectl create namespace monitoring

# 2. FastAPI mit Prometheus-Dependencies neu bauen
cd fastapi
podman build -f Dockerfile.fastapi -t localhost/fastapi:latest .
podman save localhost/fastapi:latest -o fastapi.tar
kind load image-archive fastapi.tar --name system-cluster
```

### Installation

```powershell
# Helm Chart deployen (inkludiert Prometheus)
cd helm-charts\system-cluster
helm upgrade --install system-cluster . --namespace default --wait
```

### Zugriff auf Prometheus UI

```powershell
# Port-Forward zu Prometheus
kubectl port-forward -n monitoring svc/prometheus 9090:9090

# Browser öffnen
Start-Process "http://localhost:9090"
```

## Monitoring Best Practices

### 1. Wichtige Queries

**Kafka Health**:
```promql
# Aktive Controller (sollte immer 1 sein)
kafka_controller_kafkacontroller_activecontrollercount

# Message Ingestion Rate
rate(kafka_server_brokertopicmetrics_messagesinpersec_total[5m])

# Under-Replicated Partitions (sollte 0 sein)
kafka_server_replicamanager_underreplicatedpartitions
```

**FastAPI Performance**:
```promql
# Request Rate
rate(http_requests_total[5m])

# 95th Percentile Latency
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Error Rate
rate(http_requests_total{status=~"5.."}[5m])
```

**PostgreSQL Health**:
```promql
# Datenbankgröße (GB)
pg_database_size / 1024 / 1024 / 1024

# Aktive Connections
pg_stat_activity_count

# Durchschnittstemperatur letzte Stunde
pg_analytics_data_avg_temperature
```

**Kubernetes Resources**:
```promql
# Pod Memory Usage
container_memory_usage_bytes{namespace="api", pod=~"fastapi.*"}

# CPU Usage
rate(container_cpu_usage_seconds_total{namespace="messaging"}[5m])
```

### 2. Alerting Rules (Optional)

Erstelle `prometheus-alerts.yaml`:

```yaml
groups:
- name: kafka
  rules:
  - alert: KafkaControllerDown
    expr: kafka_controller_kafkacontroller_activecontrollercount == 0
    for: 1m
    annotations:
      summary: "Kein aktiver Kafka Controller"

- name: fastapi
  rules:
  - alert: HighErrorRate
    expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.05
    for: 2m
    annotations:
      summary: "FastAPI Error Rate > 5%"
```

### 3. Retention & Storage

**Aktuelle Konfiguration**:
- Retention: 15 Tage
- Storage: 10Gi PVC
- Scrape Interval: 15s

**Storage-Berechnung**:
```
Metriken pro Scrape: ~5000
Scrapes pro Tag: 5760 (alle 15s)
Bytes pro Sample: ~2 Bytes
Täglicher Bedarf: 5000 * 5760 * 2 = ~57 MB/Tag
15 Tage: ~855 MB

Mit Overhead: ~2-3 GB für 15 Tage
```

## Troubleshooting

### Prometheus kann Kafka nicht scrapen

**Symptom**: Targets im Prometheus UI zeigen "Connection refused"

**Lösung**:
```powershell
# 1. Prüfe ob JMX Exporter Port offen ist
kubectl exec -n messaging kafka-broker-0 -- netstat -tuln | grep 7071

# 2. Prüfe Kafka Logs
kubectl logs -n messaging kafka-broker-0 | grep jmx

# 3. Prüfe ob ConfigMap mounted ist
kubectl exec -n messaging kafka-broker-0 -- ls -la /opt/jmx-exporter/
```

### PostgreSQL Exporter zeigt "connection refused"

**Lösung**:
```powershell
# 1. Prüfe Exporter Logs
kubectl logs -n data deployment/postgres-exporter

# 2. Teste DB Connection
kubectl exec -n data postgresql-0 -- psql -U appuser -d sensordata -c "SELECT 1"

# 3. Prüfe DATA_SOURCE_NAME ENV
kubectl get deployment -n data postgres-exporter -o yaml | grep DATA_SOURCE_NAME
```

### FastAPI /metrics Endpoint nicht erreichbar

**Lösung**:
```powershell
# 1. Prüfe ob prometheus-client installiert ist
kubectl exec -n api deployment/fastapi -- pip list | grep prometheus

# 2. Teste Endpoint direkt
kubectl exec -n api deployment/fastapi -- curl http://localhost:8000/metrics

# 3. FastAPI neu bauen mit Dependencies
cd fastapi
podman build -f Dockerfile.fastapi -t localhost/fastapi:latest .
```

## Erweiterungen

### Grafana Integration

```powershell
# Grafana deployen
kubectl create deployment grafana --image=grafana/grafana:main-ubuntu -n monitoring
kubectl expose deployment grafana --port=3000 --type=ClusterIP -n monitoring

# Port-Forward
kubectl port-forward -n monitoring svc/grafana 3000:3000

# Login: admin / admin
# Datasource hinzufügen: http://prometheus.monitoring.svc.cluster.local:9090
```

### Alertmanager

Ergänze in `prometheus.yaml`:
```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager.monitoring.svc.cluster.local:9093']
```

## Metriken-Übersicht

| Komponente | Exporter | Port | Metriken |
|------------|----------|------|----------|
| Kafka Broker | JMX | 7071 | JVM, Broker, Topics |
| Kafka Controller | JMX | 7071 | JVM, Controller |
| Kafka Connect | JMX | 7071 | JVM, Connectors |
| PostgreSQL | postgres-exporter | 9187 | DB Stats, Custom Queries |
| FastAPI | prometheus-client | 8000 | HTTP, Custom App Metrics |
| Kubernetes | kubelet | 10250 | Pods, Nodes, Containers |

## Performance-Tipps

1. **Scrape Interval anpassen**: Für weniger Last 30s statt 15s
2. **Metric Relabeling**: Ungenutzte Labels droppen
3. **Recording Rules**: Aggregierte Queries vorberechnen
4. **Remote Write**: Für langfristige Speicherung externe TSDB nutzen

## Nächste Schritte

1. ✅ Prometheus Server deployen
2. ✅ JMX Exporter zu Kafka hinzufügen
3. ✅ PostgreSQL Exporter deployen
4. ✅ FastAPI instrumentieren
5. ⏳ Grafana Dashboards erstellen
6. ⏳ Alerting Rules definieren
7. ⏳ Kafka Connect Metriken aktivieren
