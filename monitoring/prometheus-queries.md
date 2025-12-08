# Erweiterte PromQL Queries - Ressourcen-Monitoring

## Wichtige Konzepte

### Container-Metriken
Alle Metriken stammen von Kubernetes cAdvisor und Kubelet:
- `container_cpu_usage_seconds_total` - CPU-Zeit (counter, cumulative)
- `container_memory_usage_bytes` - Aktueller Memory in Bytes
- `container_memory_working_set_bytes` - Genutzter Memory (ohne Cache)
- `container_network_receive_bytes_total` - Netzwerk empfangen
- `container_fs_reads_bytes_total` - Disk gelesen

### Rate vs Counter
- **Counter** (steigt nur): `container_cpu_usage_seconds_total` → mit `rate()` abfragen
- **Gauge** (kann sich ändern): `container_memory_usage_bytes` → direkt abfragen

---

## CPU Monitoring

### CPU pro Pod (prozentual)
```promql
# CPU-Auslastung in Prozent (relativ zu requested CPU)
(sum(rate(container_cpu_usage_seconds_total[5m])) by (pod, namespace) /
 sum(kube_pod_container_resource_requests{resource="cpu"}) by (pod, namespace)) * 100
```

### CPU-intensivste Pods
```promql
# Top 10 CPU-Consumer
topk(10, sum(rate(container_cpu_usage_seconds_total[5m])) by (pod, namespace))

# Pods über 1 CPU
sum(rate(container_cpu_usage_seconds_total[5m])) by (pod, namespace) > 1
```

### CPU-Trends
```promql
# CPU-Anstieg in den letzten 5 Minuten
rate(container_cpu_usage_seconds_total[5m]) - rate(container_cpu_usage_seconds_total[10m])

# 1-Stunden-Durchschnitt vs 5-Minuten-Durchschnitt
rate(container_cpu_usage_seconds_total[1h]) vs rate(container_cpu_usage_seconds_total[5m])
```

---

## Memory Monitoring

### Memory pro Pod
```promql
# Memory in MB
sum(container_memory_usage_bytes) by (pod, namespace) / 1024 / 1024

# Working Set Memory (echter Speicherverbrauch)
sum(container_memory_working_set_bytes) by (pod, namespace) / 1024 / 1024

# Cache Memory (kann freigegeben werden)
sum(container_memory_usage_bytes - container_memory_working_set_bytes) by (pod) / 1024 / 1024
```

### Memory-Limits vs Nutzung
```promql
# Memory-Auslastung in Prozent (vs Limit)
(sum(container_memory_usage_bytes) by (pod) /
 sum(kube_pod_container_resource_limits{resource="memory"}) by (pod)) * 100

# Pods über 80% Memory-Limit
sum(container_memory_usage_bytes) by (pod) /
sum(kube_pod_container_resource_limits{resource="memory"}) by (pod) > 0.8
```

### Memory-Lecks erkennen
```promql
# Steigende Memory ohne Nutzung-Steigerung (mögliches Leak)
rate(container_memory_usage_bytes[1h]) > 0 and
rate(container_network_transmit_bytes_total[1h]) == 0
```

---

## Network Monitoring

### Network Bandbreite
```promql
# Empfangene Daten pro Pod (MB/s)
sum(rate(container_network_receive_bytes_total[5m])) by (pod) / 1024 / 1024

# Gesendete Daten pro Pod (MB/s)
sum(rate(container_network_transmit_bytes_total[5m])) by (pod) / 1024 / 1024

# Gesamte Cluster-Bandbreite (MB/s)
sum(rate(container_network_receive_bytes_total[5m]) + rate(container_network_transmit_bytes_total[5m])) / 1024 / 1024
```

### Top Network-Consumer
```promql
# Pods mit meistem Traffic
topk(5, sum(rate(container_network_receive_bytes_total[5m]) +
             rate(container_network_transmit_bytes_total[5m])) by (pod)) / 1024 / 1024

# Kafka-Network-Traffic
sum(rate(container_network_receive_bytes_total{pod=~"kafka.*"}[5m])) / 1024 / 1024
```

---

## Disk I/O Monitoring

### Disk Read/Write Rate
```promql
# Disk Read in MB/s
sum(rate(container_fs_reads_bytes_total[5m])) by (pod) / 1024 / 1024

# Disk Write in MB/s
sum(rate(container_fs_writes_bytes_total[5m])) by (pod) / 1024 / 1024

# Gesamter Disk I/O
sum(rate(container_fs_reads_bytes_total[5m]) + rate(container_fs_writes_bytes_total[5m])) / 1024 / 1024
```

### Top Disk-Consumer
```promql
# Pods mit meistem Disk I/O
topk(5, sum(rate(container_fs_reads_bytes_total[5m]) +
             rate(container_fs_writes_bytes_total[5m])) by (pod)) / 1024 / 1024

# PostgreSQL-Disk-Activity
sum(rate(container_fs_reads_bytes_total{pod=~"postgresql.*"}[5m]) +
    rate(container_fs_writes_bytes_total{pod=~"postgresql.*"}[5m])) / 1024 / 1024
```

---

## Cluster-weites Monitoring

### Gesamt-Ressourcennutzung
```promql
# Gesamte CPU (alle Pods)
sum(rate(container_cpu_usage_seconds_total[5m]))

# Gesamter Memory (alle Pods)
sum(container_memory_usage_bytes) / 1024 / 1024 / 1024

# Gesamte Network-Bandbreite
sum(rate(container_network_receive_bytes_total[5m]) +
    rate(container_network_transmit_bytes_total[5m])) / 1024 / 1024
```

### Namespace-Aufteilung
```promql
# CPU pro Namespace
sum(rate(container_cpu_usage_seconds_total[5m])) by (namespace)

# Memory pro Namespace
sum(container_memory_usage_bytes) by (namespace) / 1024 / 1024 / 1024

# Pod-Count pro Namespace
count(container_last_seen) by (namespace)
```

### Anomalie-Erkennung
```promql
# Pods mit ungewöhnlich hohem CPU-Verbrauch (>2 Std. Durchschnitt)
rate(container_cpu_usage_seconds_total[5m]) > 2 * rate(container_cpu_usage_seconds_total[2h])

# Plötzliche Memory-Spitzen
container_memory_usage_bytes / avg_over_time(container_memory_usage_bytes[1h]) > 2
```

---

## Alerts definieren

### Empfohlene Alert-Rules
```yaml
groups:
- name: resource_alerts
  rules:
  # CPU Alert
  - alert: HighCPUUsage
    expr: sum(rate(container_cpu_usage_seconds_total[5m])) by (pod) > 2
    for: 5m
    annotations:
      summary: "Pod {{ $labels.pod }} has high CPU usage"

  # Memory Alert
  - alert: HighMemoryUsage
    expr: sum(container_memory_usage_bytes) by (pod) / 1024 / 1024 / 1024 > 4
    for: 5m
    annotations:
      summary: "Pod {{ $labels.pod }} has high memory usage"

  # Target Down
  - alert: PrometheusTargetDown
    expr: up == 0
    for: 1m
    annotations:
      summary: "Prometheus target {{ $labels.job }} is down"
```
