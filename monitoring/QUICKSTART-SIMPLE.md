# Prometheus Monitoring - Quick Start

## Überblick

Prometheus sammelt **Ressourcen-Metriken** aller Pods im Cluster:
- **CPU-Auslastung** pro Container/Pod
- **Memory-Auslastung** pro Container/Pod
- **Network I/O** (RX/TX Bytes)
- **Disk I/O** (Read/Write Bytes)
- **Pod-Status** (Running, Pending, Failed)
- **Kubelet-Performance**

## Schneller Start

### 1. Port-Forward
```powershell
kubectl port-forward -n monitoring svc/prometheus 9090:9090
```

### 2. Im Browser öffnen
**http://localhost:9090**

### 3. Targets prüfen
Gehe zu **Status → Targets** und prüfe ob diese UP sind:
- ✅ `prometheus` (Self-Monitoring)
- ✅ `kubernetes-apiservers` (Cluster-Metriken)
- ✅ `kubernetes-cadvisor` (Container-Ressourcen)
- ✅ `kubernetes-kubelet` (Pod-Status & Kubelet)

## Häufige Queries

### Container CPU-Auslastung
```promql
# CPU pro Pod (Rate über 5 Minuten)
sum(rate(container_cpu_usage_seconds_total[5m])) by (pod, namespace)

# CPU pro Namespace
sum(rate(container_cpu_usage_seconds_total[5m])) by (namespace)

# Top 5 CPU-intensive Pods
topk(5, sum(rate(container_cpu_usage_seconds_total[5m])) by (pod))
```

### Container Memory-Auslastung
```promql
# Memory pro Pod (in MB)
sum(container_memory_usage_bytes) by (pod, namespace) / 1024 / 1024

# Memory pro Namespace
sum(container_memory_usage_bytes) by (namespace) / 1024 / 1024 / 1024

# Working Set Memory (aktiv genutzter Memory)
sum(container_memory_working_set_bytes) by (pod, namespace) / 1024 / 1024
```

### Kafka Ressourcen
```promql
# Kafka CPU
sum(rate(container_cpu_usage_seconds_total{namespace="messaging", pod=~"kafka.*"}[5m])) by (pod)

# Kafka Memory (MB)
sum(container_memory_usage_bytes{namespace="messaging", pod=~"kafka.*"}) by (pod) / 1024 / 1024

# Kafka Network Empfangen (MB/s)
sum(rate(container_network_receive_bytes_total{namespace="messaging", pod=~"kafka.*"}[5m])) / 1024 / 1024
```

### FastAPI Ressourcen
```promql
# FastAPI CPU
sum(rate(container_cpu_usage_seconds_total{namespace="api", pod=~"fastapi.*"}[5m])) by (pod)

# FastAPI Memory (MB)
sum(container_memory_usage_bytes{namespace="api", pod=~"fastapi.*"}) by (pod) / 1024 / 1024
```

### PostgreSQL Ressourcen
```promql
# PostgreSQL CPU
sum(rate(container_cpu_usage_seconds_total{namespace="default", pod=~"postgresql.*"}[5m])) by (pod)

# PostgreSQL Memory (MB)
sum(container_memory_usage_bytes{namespace="default", pod=~"postgresql.*"}) by (pod) / 1024 / 1024

# PostgreSQL Disk Read (MB/s)
sum(rate(container_fs_reads_bytes_total{namespace="default", pod=~"postgresql.*"}[5m])) / 1024 / 1024
```

### Network I/O
```promql
# Network Empfangen pro Pod (MB/s)
sum(rate(container_network_receive_bytes_total[5m])) by (pod) / 1024 / 1024

# Network Senden pro Pod (MB/s)
sum(rate(container_network_transmit_bytes_total[5m])) by (pod) / 1024 / 1024

# Gesamtes Network Traffic (MB/s)
sum(rate(container_network_receive_bytes_total[5m]) + rate(container_network_transmit_bytes_total[5m])) / 1024 / 1024
```

### Disk I/O
```promql
# Disk Read pro Pod (MB/s)
sum(rate(container_fs_reads_bytes_total[5m])) by (pod) / 1024 / 1024

# Disk Write pro Pod (MB/s)
sum(rate(container_fs_writes_bytes_total[5m])) by (pod) / 1024 / 1024
```

## Konfiguration

- **Scrape Interval**: 15 Sekunden
- **Retention**: 15 Tage
- **Storage**: 10 GB

Änderungen in: `helm-charts/system-cluster/values.yaml` (Section `prometheus`)

## Weitere Informationen

Siehe `prometheus-queries.md` für erweiterte Queries und `README.md` für vollständige Dokumentation.
