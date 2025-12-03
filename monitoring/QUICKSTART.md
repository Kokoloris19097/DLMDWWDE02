# Prometheus Monitoring - Quick Start Guide

## Installation (5 Minuten)

### Schritt 1: Monitoring deployen

```powershell
cd monitoring
.\deploy-monitoring.ps1
```

Das Skript führt automatisch aus:
1. ✅ FastAPI neu bauen mit Prometheus Dependencies
2. ✅ Image in Kind Cluster laden
3. ✅ Helm Chart mit Prometheus deployen
4. ✅ Alle Komponenten verifizieren

### Schritt 2: Prometheus UI öffnen

```powershell
kubectl port-forward -n monitoring svc/prometheus 9090:9090
```

Browser: **http://localhost:9090**

---

## Sofort verwendbare Queries

### 1. System Health Check

```promql
up{job=~"kafka-broker|fastapi|postgresql"}
```

**Ergebnis**: `1` = Healthy, `0` = Down

### 2. Kafka Message Rate

```promql
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total[5m]))
```

**Ergebnis**: Messages/Sekunde durch alle Broker

### 3. FastAPI Request Rate

```promql
sum(rate(http_requests_total[5m]))
```

**Ergebnis**: HTTP Requests/Sekunde

### 4. Sensor Data in Database

```promql
pg_analytics_data_total_rows
```

**Ergebnis**: Anzahl gespeicherter Sensor-Readings

### 5. API Latenz (95th Percentile)

```promql
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
```

**Ergebnis**: 95% der Requests sind schneller als dieser Wert

---

## Troubleshooting

### Prometheus kann Targets nicht erreichen

1. **Prüfe Target-Status**: http://localhost:9090/targets
2. **Alle Targets sollten "UP" sein**

**Wenn Kafka Targets DOWN**:
```powershell
kubectl logs -n messaging kafka-broker-0 | grep -i jmx
kubectl exec -n messaging kafka-broker-0 -- netstat -tuln | grep 7071
```

**Wenn PostgreSQL Exporter DOWN**:
```powershell
kubectl logs -n data deployment/postgres-exporter
```

**Wenn FastAPI Metriken fehlen**:
```powershell
kubectl exec -n api deployment/fastapi -- curl http://localhost:8000/metrics
```

### FastAPI Image Build fehlgeschlagen

```powershell
cd fastapi
pip install -r requirements.txt  # Prüfe ob alle Dependencies installierbar
podman build -f Dockerfile.fastapi -t localhost/fastapi:latest .
```

---

## Wichtige Endpoints

| Service | URL | Beschreibung |
|---------|-----|--------------|
| Prometheus UI | http://localhost:9090 | Haupt-Monitoring UI |
| Prometheus Targets | http://localhost:9090/targets | Scrape Target Status |
| Prometheus Config | http://localhost:9090/config | Aktive Konfiguration |
| Prometheus Alerts | http://localhost:9090/alerts | Alert Status (wenn konfiguriert) |
| FastAPI Metriken | http://localhost:8000/metrics | FastAPI Prometheus Endpoint |
| PostgreSQL Exporter | http://localhost:9187/metrics | PostgreSQL Metriken |

*Benötigt Port-Forwards für lokalen Zugriff*

---

## Monitoring-Architektur

```
┌─────────────────────────────────────────────────────────────┐
│                    Prometheus Server                        │
│                      (Port 9090)                            │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Scrape Configuration (alle 15s)                     │  │
│  │  - Kafka Brokers/Controllers (JMX: 7071)            │  │
│  │  - PostgreSQL (Exporter: 9187)                       │  │
│  │  - FastAPI (/metrics: 8000)                          │  │
│  │  - Kubernetes Pods/Nodes                             │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Time-Series Database                                │  │
│  │  - Retention: 15 Tage                                │  │
│  │  - Storage: 10Gi PVC                                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Query Engine (PromQL)                               │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
              ┌──────────────────────┐
              │  Grafana (Optional)  │
              │    Dashboards        │
              └──────────────────────┘
```

---

## Nächste Schritte

### 1. Grafana hinzufügen (Optional)

```powershell
kubectl create deployment grafana --image=grafana/grafana:main-ubuntu -n monitoring
kubectl expose deployment grafana --port=3000 --type=ClusterIP -n monitoring
kubectl port-forward -n monitoring svc/grafana 3000:3000
```

**Login**: admin / admin
**Datasource**: http://prometheus.monitoring.svc.cluster.local:9090

### 2. Alerting konfigurieren

Siehe: `monitoring/README.md` - Abschnitt "Alerting Rules"

### 3. Custom Queries lernen

Siehe: `monitoring/prometheus-queries.md` - Umfassende Query-Sammlung

---

## Support

**Probleme mit Deployment**: Siehe `monitoring/README.md` - Abschnitt "Troubleshooting"

**Fragen zu Queries**: Siehe `monitoring/prometheus-queries.md`

**Helm Chart Anpassungen**: `helm-charts/system-cluster/values.yaml` - Abschnitt `prometheus:`
