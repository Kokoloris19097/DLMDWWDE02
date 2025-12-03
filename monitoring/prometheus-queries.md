# Wichtige PromQL Queries für Monitoring

## System Health

### Kafka Cluster Health
```promql
# Aktiver Controller (sollte genau 1 sein)
kafka_controller_kafkacontroller_activecontrollercount

# Anzahl Broker im Cluster
count(up{job="kafka-broker"} == 1)

# Under-Replicated Partitions (sollte 0 sein)
kafka_server_replicamanager_underreplicatedpartitions
```

### PostgreSQL Health
```promql
# Datenbankgröße in GB
pg_database_size / 1024 / 1024 / 1024

# Aktive Connections
pg_stat_activity_count

# Anzahl Rows in analytics_data
pg_analytics_data_total_rows
```

### FastAPI Health
```promql
# FastAPI ist erreichbar
up{job="fastapi"} == 1

# Aktive HTTP Requests
http_requests_in_progress
```

## Performance Metriken

### Kafka Throughput
```promql
# Messages pro Sekunde (alle Topics)
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total[5m]))

# Bytes In Rate (MB/s)
sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_total[5m])) / 1024 / 1024

# Bytes Out Rate (MB/s)
sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[5m])) / 1024 / 1024

# Messages pro Topic
sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesinpersec_total{topic!=""}[5m]))
```

### FastAPI Performance
```promql
# Request Rate gesamt
sum(rate(http_requests_total[5m]))

# Request Rate nach Endpoint
sum by (handler) (rate(http_requests_total[5m]))

# 95th Percentile Latency
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# 99th Percentile Latency
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))

# Error Rate (5xx Responses)
sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))

# Ingestion Counter (Custom Metric)
sum(rate(sensor_data_ingestion_total[5m])) by (sensor_id, status)
```

### Database Performance
```promql
# Query Latency (95th Percentile)
histogram_quantile(0.95, rate(database_query_duration_seconds_bucket[5m]))

# Queries pro Sekunde nach Typ
sum by (query_type) (rate(database_queries_total[5m]))

# Durchschnittliche Temperatur (letzte Stunde)
pg_analytics_data_avg_temperature

# Durchschnittliche Luftfeuchtigkeit (letzte Stunde)
pg_analytics_data_avg_humidity
```

## Ressourcen-Monitoring

### Kafka Memory
```promql
# Heap Memory Usage (%)
(jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes) * 100

# Non-Heap Memory
jvm_memory_nonheap_used_bytes

# Heap Memory Usage nach Broker
jvm_memory_heap_used_bytes{job="kafka-broker"} / 1024 / 1024 / 1024
```

### Kafka GC Activity
```promql
# GC Collections pro Sekunde
sum(rate(jvm_gc_collection_count_total[5m])) by (gc)

# GC Time pro Sekunde (ms)
sum(rate(jvm_gc_collection_time_ms_total[5m])) by (gc)

# GC Overhead (% der Zeit in GC)
(sum(rate(jvm_gc_collection_time_ms_total[5m])) / 1000) / sum(rate(jvm_runtime_uptime_seconds[5m])) * 100
```

### Kubernetes Resources
```promql
# Pod Memory Usage (MB)
sum(container_memory_usage_bytes{namespace=~"messaging|api|data"}) by (pod) / 1024 / 1024

# Pod CPU Usage (cores)
sum(rate(container_cpu_usage_seconds_total{namespace=~"messaging|api|data"}[5m])) by (pod)

# Pod Restart Count
sum(kube_pod_container_status_restarts_total{namespace=~"messaging|api|data"}) by (pod)
```

## Data Pipeline Metriken

### End-to-End Data Flow
```promql
# Kafka Publish Success Rate
sum(rate(kafka_messages_published_total{status="success"}[5m]))

# Kafka Publish Error Rate
sum(rate(kafka_messages_published_total{status="error"}[5m]))

# Sensor Readings pro Sensor (letzte 5 Min)
sum by (sensor_id) (rate(sensor_data_ingestion_total{status="success"}[5m]))

# Data Latency (Ingestion)
histogram_quantile(0.95, rate(sensor_data_ingestion_duration_seconds_bucket[5m]))
```

### PostgreSQL Data Growth
```promql
# Table Size Growth (MB/hour)
rate(pg_table_size[1h]) / 1024 / 1024

# New Rows per Hour
rate(pg_analytics_data_total_rows[1h]) * 3600

# Latest Timestamp pro Sensor (seconds ago)
time() - pg_analytics_data_latest_timestamp
```

## Alerting Queries

### Critical Alerts
```promql
# Kein aktiver Kafka Controller
kafka_controller_kafkacontroller_activecontrollercount == 0

# Kafka Broker Down
count(up{job="kafka-broker"} == 1) < 2

# PostgreSQL Down
up{job="postgresql"} == 0

# FastAPI Down
up{job="fastapi"} == 0
```

### Warning Alerts
```promql
# High Error Rate (> 5%)
(sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))) > 0.05

# High GC Time (> 10% der Zeit)
(sum(rate(jvm_gc_collection_time_ms_total[5m])) / 1000) / sum(rate(jvm_runtime_uptime_seconds[5m])) * 100 > 10

# High Heap Usage (> 85%)
(jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes) * 100 > 85

# Under-Replicated Partitions
kafka_server_replicamanager_underreplicatedpartitions > 0

# Stale Data (keine neuen Daten seit 5 Min)
time() - pg_analytics_data_latest_timestamp > 300
```

## Dashboard Panels

### Panel 1: Kafka Message Rate
```promql
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total[5m])) by (topic)
```
**Typ**: Graph
**Interval**: 5m
**Legende**: {{topic}}

### Panel 2: FastAPI Request Latency
```promql
histogram_quantile(0.50, rate(http_request_duration_seconds_bucket[5m]))
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
```
**Typ**: Graph
**Legende**: p50, p95, p99

### Panel 3: System Health Overview
```promql
up{job=~"kafka-broker|fastapi|postgresql"}
```
**Typ**: Stat Panel
**Thresholds**: 0 (Red), 1 (Green)

### Panel 4: Sensor Data Growth
```promql
pg_analytics_data_total_rows
```
**Typ**: Graph
**Interval**: 1m

### Panel 5: Error Rate
```promql
sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))
```
**Typ**: Gauge
**Unit**: Percent
**Thresholds**: 0-1% (Green), 1-5% (Yellow), >5% (Red)

## Nützliche Aggregationen

### Top 10 Sensoren nach Datenmenge
```promql
topk(10, pg_analytics_data_rows_by_sensor)
```

### Durchschnittliche Request-Latenz nach Endpoint (letzte Stunde)
```promql
avg_over_time((histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])))[1h:5m])
```

### Kafka Message Lag Trend
```promql
deriv(kafka_server_brokertopicmetrics_messagesinpersec_total[10m])
```

### PostgreSQL Connection Utilization
```promql
pg_stat_activity_count / pg_settings_max_connections * 100
```
