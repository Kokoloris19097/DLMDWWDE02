# 08 - Troubleshooting

## Übersicht

Dieses Modul beschreibt systematische Debug-Strategien, häufige Probleme und deren Lösungen. Das zentrale Debug-Tool ist `debug-dataflow.ps1`, das die gesamte Daten-Pipeline analysiert.

## Debug-Dataflow Skript

**Skript:** [`debug-dataflow.ps1`](../debug-dataflow.ps1)

**Zweck:** 6-stufige Pipeline-Analyse von Sensor-Daten → PostgreSQL

### Ablauf

```mermaid
flowchart TD
    Start([debug-dataflow.ps1]) --> S1[Step 1: Kafka 'sensor-data']
    S1 --> CountSD[kafka-console-consumer<br/>--from-beginning --timeout 5s]
    CountSD --> S2[Step 2: Kafka 'analytics-data']

    S2 --> CountAD[kafka-console-consumer<br/>--from-beginning --timeout 5s]
    CountAD --> S3[Step 3: Spark Status]

    S3 --> SparkPod[kubectl get pod spark -n data]
    SparkPod --> SparkLogs[kubectl logs spark --tail=20]
    SparkLogs --> S4[Step 4: Kafka Connect Status]

    S4 --> ConnectPod[kubectl get pod kafka-connect]
    ConnectPod --> ConnectLogs[kubectl logs kafka-connect --tail=20]
    ConnectLogs --> S5[Step 5: PostgreSQL Check]

    S5 --> PGCount[psql -c 'SELECT COUNT(*)'<br/>FROM analytics_data]
    PGCount --> PGRecent[psql -c 'SELECT * LIMIT 5'<br/>ORDER BY timestamp DESC]
    PGRecent --> S6[Step 6: Diagnose]

    S6 --> Compare[Vergleiche Counts:<br/>sensor-data → analytics-data → PostgreSQL]
    Compare --> Identify[Identifiziere Engpass]
    Identify --> End([Report])

    style Start fill:#e1f5ff
    style S1 fill:#ffe1e1
    style S2 fill:#ffe1e1
    style S3 fill:#fff4e1
    style S4 fill:#fff4e1
    style S5 fill:#e1ffe1
    style S6 fill:#e1f5ff
    style End fill:#e1ffe1
```

### Skript-Logik

```powershell
Write-Host "=== Data Pipeline Debug ===" -ForegroundColor Cyan

# Step 1: Kafka sensor-data Topic
Write-Host "`n[1/6] Kafka Topic 'sensor-data' prüfen..." -ForegroundColor Yellow
$sensorDataCount = kubectl exec -n messaging kafka-broker-0 -- `
  kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic sensor-data `
  --from-beginning `
  --timeout-ms 5000 2>&1 | Measure-Object -Line | Select-Object -ExpandProperty Lines

Write-Host "  Messages in sensor-data: $sensorDataCount" -ForegroundColor Green

# Step 2: Kafka analytics-data Topic
Write-Host "`n[2/6] Kafka Topic 'analytics-data' prüfen..." -ForegroundColor Yellow
$analyticsDataCount = kubectl exec -n messaging kafka-broker-0 -- `
  kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic analytics-data `
  --from-beginning `
  --timeout-ms 5000 2>&1 | Measure-Object -Line | Select-Object -ExpandProperty Lines

Write-Host "  Messages in analytics-data: $analyticsDataCount" -ForegroundColor Green

# Step 3: Spark Pod Status
Write-Host "`n[3/6] Spark Pod Status & Logs..." -ForegroundColor Yellow
kubectl get pod spark -n data
kubectl logs spark -n data --tail=20

# Step 4: Kafka Connect Status
Write-Host "`n[4/6] Kafka Connect Status & Logs..." -ForegroundColor Yellow
kubectl get pod -n messaging -l app=kafka-connect
kubectl logs -n messaging -l app=kafka-connect --tail=20

# Step 5: PostgreSQL Daten
Write-Host "`n[5/6] PostgreSQL Tabelle prüfen..." -ForegroundColor Yellow
$pgCount = kubectl exec -n data postgresql-0 -- `
  psql -U appuser -d sensordata -t -c "SELECT COUNT(*) FROM analytics_data" | `
  Select-String -Pattern '\d+' | ForEach-Object { $_.Matches.Value }

Write-Host "  Rows in PostgreSQL: $pgCount" -ForegroundColor Green

kubectl exec -n data postgresql-0 -- `
  psql -U appuser -d sensordata -c `
  "SELECT sensor_id, timestamp, temperature, humidity FROM analytics_data ORDER BY timestamp DESC LIMIT 5"

# Step 6: Diagnose
Write-Host "`n[6/6] Zusammenfassung & Diagnose" -ForegroundColor Cyan
Write-Host "  sensor-data Messages:      $sensorDataCount" -ForegroundColor White
Write-Host "  analytics-data Messages:   $analyticsDataCount" -ForegroundColor White
Write-Host "  PostgreSQL Rows:           $pgCount" -ForegroundColor White

# Engpass-Analyse
if ($sensorDataCount -eq 0) {
    Write-Host "`n⚠ PROBLEM: Keine Daten im sensor-data Topic!" -ForegroundColor Red
    Write-Host "  → Prüfe: Sensor Simulator läuft? FastAPI erreichbar?" -ForegroundColor Yellow
}
elseif ($analyticsDataCount -eq 0) {
    Write-Host "`n⚠ PROBLEM: Spark verarbeitet keine Daten!" -ForegroundColor Red
    Write-Host "  → Prüfe: Spark Pod Status & Logs oben" -ForegroundColor Yellow
}
elseif ([int]$pgCount -eq 0) {
    Write-Host "`n⚠ PROBLEM: Kafka Connect schreibt nicht nach PostgreSQL!" -ForegroundColor Red
    Write-Host "  → Prüfe: Kafka Connect Status & Connector Config" -ForegroundColor Yellow
}
else {
    Write-Host "`n✓ Pipeline funktioniert!" -ForegroundColor Green
}
```

### Beispiel-Output

```
=== Data Pipeline Debug ===

[1/6] Kafka Topic 'sensor-data' prüfen...
  Messages in sensor-data: 1523

[2/6] Kafka Topic 'analytics-data' prüfen...
  Messages in analytics-data: 45

[3/6] Spark Pod Status & Logs...
NAME    READY   STATUS    RESTARTS   AGE
spark   1/1     Running   0          15m

[Spark Logs]
Batch: 12
Input rows: 150
Processed rows: 5
Trigger time: 10234 ms

[4/6] Kafka Connect Status & Logs...
NAME                     READY   STATUS    RESTARTS   AGE
kafka-connect-xyz123     1/1     Running   0          15m

[Kafka Connect Logs]
Connector postgresql-sink: Running
Task 0: Running

[5/6] PostgreSQL Tabelle prüfen...
  Rows in PostgreSQL: 45

 sensor_id  |      timestamp      | temperature | humidity
------------+---------------------+-------------+----------
 SENSOR-001 | 2025-12-14 12:30:00 |        22.4 |     64.8
 SENSOR-002 | 2025-12-14 12:30:00 |        21.8 |     66.2
 ...

[6/6] Zusammenfassung & Diagnose
  sensor-data Messages:      1523
  analytics-data Messages:   45
  PostgreSQL Rows:           45

✓ Pipeline funktioniert!
```

## Häufige Probleme & Lösungen

### 1. Pods starten nicht

#### Problem: ImagePullBackOff

**Symptom:**
```bash
kubectl get pods -A
# OUTPUT:
NAME           READY   STATUS             RESTARTS   AGE
fastapi-xyz    0/1     ImagePullBackOff   0          2m
```

**Ursache:** Lokales Image nicht in Kind geladen oder `imagePullPolicy` falsch

**Lösung:**
```powershell
# 1. Image in Kind vorhanden?
podman exec system-cluster-control-plane crictl images | grep fastapi

# 2. Image neu laden
cd fastapi
.\deploy-fastapi.ps1 -noHelm

# 3. Pod neu starten
kubectl delete pod -n api -l app=fastapi

# 4. Prüfe imagePullPolicy in values.yaml
fastapi:
  pullPolicy: Never  # MUSS "Never" sein für lokale Images
```

#### Problem: CrashLoopBackOff

**Symptom:**
```bash
kubectl get pods -n data
# OUTPUT:
NAME    READY   STATUS             RESTARTS   AGE
spark   0/1     CrashLoopBackOff   5          5m
```

**Ursache:** Application-Fehler beim Start

**Lösung:**
```powershell
# 1. Logs anschauen
kubectl logs -n data pod/spark --previous  # Letzter Crash

# 2. Häufige Fehler:
# - Python Import Error → requirements.txt fehlt Dependency
# - Kafka Connection Failed → Kafka Service DNS falsch
# - Environment Variable Missing → values.yaml prüfen

# 3. Describe für Events
kubectl describe pod -n data spark

# 4. Interaktive Debug-Session
kubectl run -it --rm debug --image=localhost/spark:latest --restart=Never -- /bin/bash
# Manuell testen: python3 /app/main.py
```

### 2. Kafka-Probleme

#### Problem: Broker nicht erreichbar

**Symptom:**
```
Error: Connection to node -1 failed
```

**Ursache:** Quorum Voters falsch konfiguriert oder DNS-Auflösung fehlgeschlagen

**Lösung:**
```powershell
# 1. Controller Logs prüfen
kubectl logs -n messaging kafka-controller-0 | grep -i quorum

# 2. DNS-Auflösung testen
kubectl exec -n messaging kafka-broker-0 -- \
  nslookup kafka-controller-0.kafka-controller.messaging.svc.cluster.local

# 3. CLUSTER_ID identisch?
kubectl exec -n messaging kafka-controller-0 -- env | grep CLUSTER_ID
kubectl exec -n messaging kafka-broker-0 -- env | grep CLUSTER_ID
# MUSS gleich sein!

# 4. Neustart (als letzter Ausweg)
kubectl delete pod -n messaging kafka-controller-0 kafka-controller-1
kubectl delete pod -n messaging kafka-broker-0 kafka-broker-1
```

#### Problem: Topic nicht gefunden

**Symptom:**
```
Error: Topic 'sensor-data' not found
```

**Lösung:**
```powershell
# 1. Topic manuell erstellen
kubectl exec -n messaging kafka-broker-0 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic sensor-data \
  --partitions 1 --replication-factor 2

# 2. Auto-Create aktiviert?
kubectl exec -n messaging kafka-broker-0 -- env | grep AUTO_CREATE_TOPICS
# Sollte: KAFKA_AUTO_CREATE_TOPICS_ENABLE=true

# 3. Spark/FastAPI neu starten (erstellen Topic automatisch)
kubectl delete pod -n data spark
kubectl delete pod -n api -l app=fastapi
```

#### Problem: Consumer Lag

**Symptom:** Daten kommen langsam in PostgreSQL

**Diagnose:**
```powershell
# Consumer Group Lag prüfen
kubectl exec -n messaging kafka-broker-0 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group connect-postgresql-sink

# OUTPUT:
GROUP                    TOPIC           PARTITION  CURRENT-OFFSET  LAG
connect-postgresql-sink  analytics-data  0          1234            567  # ← Hoher Lag!
```

**Lösung:**
```powershell
# 1. Kafka Connect Tasks erhöhen (connector-config.json)
"tasks.max": "2"  # Statt 1

# 2. Kafka Connect Replicas erhöhen (values.yaml)
kafka-connect:
  replicaCount: 2

# 3. PostgreSQL Performance prüfen (siehe unten)
```

### 3. PostgreSQL-Probleme

#### Problem: Connection Refused

**Symptom:**
```
psycopg2.OperationalError: could not connect to server
```

**Lösung:**
```powershell
# 1. Pod läuft?
kubectl get pod -n data postgresql-0

# 2. Service existiert?
kubectl get svc -n data postgresql

# 3. pg_isready
kubectl exec -n data postgresql-0 -- pg_isready -U postgres

# 4. Logs
kubectl logs -n data postgresql-0 --tail=50

# 5. Neustart
kubectl delete pod -n data postgresql-0
# StatefulSet erstellt automatisch neuen Pod
```

#### Problem: Disk Full

**Symptom:**
```
ERROR: could not extend file: No space left on device
```

**Lösung:**
```powershell
# 1. PVC Size prüfen
kubectl get pvc -n data

# 2. Disk Usage
kubectl exec -n data postgresql-0 -- df -h /var/lib/postgresql/data

# 3. Größte Tabellen
kubectl exec -n data postgresql-0 -- \
  psql -U postgres -d sensordata -c \
  "SELECT pg_size_pretty(pg_total_relation_size('analytics_data'))"

# 4. Alte Daten löschen
kubectl exec -n data postgresql-0 -- \
  psql -U appuser -d sensordata -c \
  "DELETE FROM analytics_data WHERE timestamp < NOW() - INTERVAL '7 days'"

# 5. VACUUM
kubectl exec -n data postgresql-0 -- \
  psql -U postgres -d sensordata -c "VACUUM FULL analytics_data"

# 6. PVC erweitern (values.yaml)
postgresql:
  storage:
    size: 10Gi  # Statt 5Gi
# → helm upgrade system-cluster ...
```

#### Problem: Slow Queries

**Symptom:** FastAPI Query-Endpoints langsam (>5s)

**Diagnose:**
```sql
-- Slow Query Log aktivieren
ALTER SYSTEM SET log_min_duration_statement = 1000;  -- 1s
SELECT pg_reload_conf();

-- Logs prüfen
kubectl logs -n data postgresql-0 | grep "duration:"
```

**Lösung:**
```sql
-- 1. Fehlende Indizes?
SELECT * FROM pg_stat_user_indexes WHERE idx_scan = 0;

-- 2. Index erstellen
CREATE INDEX idx_analytics_sensor_timestamp
  ON analytics_data (sensor_id, timestamp DESC);

-- 3. ANALYZE
ANALYZE analytics_data;

-- 4. Query Plan prüfen
EXPLAIN ANALYZE
  SELECT * FROM analytics_data
  WHERE sensor_id = 'SENSOR-001'
  ORDER BY timestamp DESC LIMIT 100;
```

### 4. Spark-Probleme

#### Problem: Spark Job hängt

**Symptom:** Keine Logs, keine Verarbeitung

**Lösung:**
```powershell
# 1. Spark UI prüfen (Port 4040)
kubectl port-forward -n data pod/spark 4040:4040
# Browser: http://localhost:4040/jobs

# 2. Streaming Query Status
# In Spark UI: Structured Streaming Tab
# → Prüfe: Input Rate, Processing Rate

# 3. Checkpoint Corruption?
kubectl exec -n data pod/spark -- ls -la /tmp/spark-checkpoint

# 4. Checkpoint löschen & neu starten
kubectl exec -n data pod/spark -- rm -rf /tmp/spark-checkpoint/*
kubectl delete pod -n data spark
```

#### Problem: Out of Memory

**Symptom:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Lösung:**
```yaml
# values.yaml
spark:
  resources:
    limits:
      memory: "8Gi"  # Erhöhen von 4Gi

  env:
    SPARK_EXECUTOR_MEMORY: "6g"  # Neu hinzufügen
```

### 5. Netzwerk-Probleme

#### Problem: DNS Resolution Failed

**Symptom:**
```
Name or service not known: kafka.messaging.svc.cluster.local
```

**Lösung:**
```powershell
# 1. CoreDNS läuft?
kubectl get pods -n kube-system -l k8s-app=kube-dns

# 2. DNS-Test von Pod aus
kubectl run -it --rm debug --image=busybox --restart=Never -- \
  nslookup kafka.messaging.svc.cluster.local

# 3. Service existiert?
kubectl get svc -A | grep kafka

# 4. CoreDNS Logs
kubectl logs -n kube-system -l k8s-app=kube-dns --tail=50

# 5. CoreDNS neu starten
kubectl rollout restart deployment -n kube-system coredns
```

#### Problem: Network Policy Blockierung

**Symptom:** Verbindungen zwischen Namespaces schlagen fehl

**Lösung:**
```powershell
# 1. Network Policies auflisten
kubectl get networkpolicies -A

# 2. Temporär löschen (zum Testen)
kubectl delete networkpolicy -n messaging <policy-name>

# 3. Connectivity testen
kubectl exec -n api deployment/fastapi -- \
  nc -zv kafka.messaging.svc.cluster.local 9092
```

## Debugging-Tools

### kubectl Cheat Sheet

```powershell
# Pod Status
kubectl get pods -A
kubectl get pods -n <namespace> -o wide  # Mit Node-Info

# Pod Details
kubectl describe pod <pod-name> -n <namespace>

# Logs
kubectl logs <pod-name> -n <namespace>
kubectl logs <pod-name> -n <namespace> --previous  # Vorheriger Container
kubectl logs <pod-name> -n <namespace> -f  # Follow (Stream)
kubectl logs <pod-name> -n <namespace> --tail=100  # Letzte 100 Zeilen

# Interaktive Shell
kubectl exec -it <pod-name> -n <namespace> -- /bin/bash

# Port-Forwarding
kubectl port-forward -n <namespace> svc/<service-name> <local-port>:<remote-port>

# Ressourcen-Verbrauch
kubectl top nodes
kubectl top pods -A
kubectl top pods -n <namespace>

# Events
kubectl get events -n <namespace> --sort-by=.metadata.creationTimestamp

# Service Endpoints
kubectl get endpoints -A
kubectl describe svc <service-name> -n <namespace>
```

### Podman Debugging

```powershell
# Podman Machine Status
podman machine list

# Container in Kind-Cluster
podman ps -a | Select-String "system-cluster"

# Container Logs
podman logs system-cluster-control-plane --tail=100

# Exec in Container
podman exec -it system-cluster-control-plane /bin/bash

# Container neu starten
podman stop system-cluster-control-plane
podman start system-cluster-control-plane
```

### Helm Debugging

```powershell
# Release Status
helm list -A

# Release Details
helm get values system-cluster -n default
helm get manifest system-cluster -n default

# Dry-Run (ohne Installation)
helm install system-cluster . --dry-run --debug

# Template Rendering
helm template system-cluster . > rendered.yaml

# Rollback
helm rollback system-cluster 0 -n default  # Zu vorheriger Version
```

## Performance-Probleme

### CPU/Memory Bottlenecks

**Diagnose:**
```powershell
# Node Ressourcen
kubectl top nodes

# Pod Ressourcen
kubectl top pods -A --sort-by=cpu
kubectl top pods -A --sort-by=memory

# Limits vs. Requests
kubectl describe node | grep -A 5 "Allocated resources"
```

**Lösung:**
```yaml
# values.yaml - Ressourcen anpassen
kafka:
  broker:
    resources:
      limits:
        cpu: "4000m"    # Erhöhen
        memory: "8Gi"   # Erhöhen
```

### Disk I/O Bottlenecks

**Diagnose:**
```powershell
# PVC Status
kubectl get pvc -A

# Disk Usage in Pod
kubectl exec -n data postgresql-0 -- df -h

# I/O Stats (wenn iotop installiert)
kubectl exec -n data postgresql-0 -- iotop -b -n 1
```

**Lösung:**
- Größere PVCs (values.yaml)
- SSD statt HDD für Podman Machine
- Kafka Log Retention reduzieren

## Logs & Monitoring

### Zentralisiertes Logging (geplant)

**EFK Stack (Elasticsearch, Fluentd, Kibana):**
```yaml
# Helm Install (Beispiel)
helm repo add elastic https://helm.elastic.co
helm install elasticsearch elastic/elasticsearch -n logging
helm install kibana elastic/kibana -n logging
helm install fluentd fluent/fluentd -n logging
```

### Prometheus Alerts

**Alert bei Pod Down:**
```yaml
# prometheus-alerts.yaml
groups:
  - name: kubernetes
    rules:
      - alert: PodDown
        expr: kube_pod_status_phase{phase="Failed"} > 0
        for: 5m
        annotations:
          summary: "Pod {{ $labels.pod }} failed"
```

## Disaster Recovery

### Backup-Strategie

**PostgreSQL Backup:**
```powershell
# Dump erstellen
kubectl exec -n data postgresql-0 -- \
  pg_dump -U postgres sensordata > backup_$(Get-Date -Format "yyyyMMdd_HHmmss").sql

# Restore
kubectl exec -i -n data postgresql-0 -- \
  psql -U postgres sensordata < backup_20251214_120000.sql
```

**Kafka Topic Backup:**
```powershell
# Mirror Maker (für Produktion)
# Oder: Export zu S3/MinIO via Kafka Connect S3 Sink
```

### Cluster-Neuaufbau

```powershell
# 1. Alte Daten sichern (siehe oben)

# 2. Cluster löschen
kind delete cluster --name system-cluster

# 3. Podman aufräumen
podman system prune -a -f

# 4. Neuinstallation
.\init.ps1

# 5. Daten wiederherstellen
```

## Weiterführende Dokumentation

- **[06 - Deployment](06-Deployment.md)** - Deployment-Fehler beheben
- **[07 - Testing](07-Testing.md)** - Test-Failures debuggen
- **[Kubernetes Troubleshooting](https://kubernetes.io/docs/tasks/debug/)**
- **[Kafka Troubleshooting](https://kafka.apache.org/documentation/#troubleshooting)**

---

**Navigation:** [← Zurück zu Testing](07-Testing.md) | [Zurück zur Übersicht](README.md)
