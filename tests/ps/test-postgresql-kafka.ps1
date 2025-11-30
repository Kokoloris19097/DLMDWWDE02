# Kafka → PostgreSQL Pipeline Test
# Schreibt Testdaten in analytics-data Topic und prüft ob sie in PostgreSQL ankommen

Write-Host "=== Kafka → PostgreSQL Pipeline Test ===" -ForegroundColor Green

$namespace = "messaging"
$dbNamespace = "data"
$bootstrapServer = "kafka-broker-0.kafka-broker.messaging.svc.cluster.local:9092"
$topic = "analytics-data"
$testId = "test-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
$timeoutSeconds = 10

try {
   # 1. Testdaten vorbereiten (mit Schema für JDBC Sink)
    Write-Host "`n[1/6] Bereite Testdaten vor..." -ForegroundColor Yellow

    $timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss'Z'")
    $temperature = [math]::Round((Get-Random -Minimum 15.0 -Maximum 30.0), 2)
    $humidity = [math]::Round((Get-Random -Minimum 30.0 -Maximum 80.0), 2)

    $schema = '{"type":"struct","fields":[{"field":"sensor_id","type":"string"},{"field":"timestamp","type":"string"},{"field":"temperature","type":"double"},{"field":"humidity","type":"double"}]}'
    $payload = "{`"sensor_id`":`"$testId`",`"timestamp`":`"$timestamp`",`"temperature`":$temperature,`"humidity`":$humidity}"
    $testData = "{`"schema`":$schema,`"payload`":$payload}"

    Write-Host "  Sensor-ID:   $testId" -ForegroundColor Gray
    Write-Host "  Timestamp:   $timestamp" -ForegroundColor Gray
    Write-Host "  Temperature: $temperature" -ForegroundColor Gray
    Write-Host "  Humidity:    $humidity" -ForegroundColor Gray
    Write-Host "Testdaten vorbereitet" -ForegroundColor Green

    # 2. Prüfe Kafka-Broker
    Write-Host "`n[2/6] Prüfe Kafka-Broker..." -ForegroundColor Yellow
    $brokerCheck = kubectl exec kafka-broker-0 -n $namespace -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrapServer --list 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka-Broker nicht erreichbar!"
    }
    Write-Host "Kafka-Broker erreichbar" -ForegroundColor Green

    # 3. Prüfe/Erstelle Topic
    Write-Host "`n[3/6] Prüfe Topic '$topic'..." -ForegroundColor Yellow
    $topicExists = $brokerCheck | Select-String -Pattern "^$topic$"

    if (-not $topicExists) {
        Write-Host "  Topic existiert nicht, erstelle..." -ForegroundColor Gray
        kubectl exec kafka-broker-0 -n $namespace -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrapServer --create --topic $topic --partitions 1 --replication-factor 2
        if ($LASTEXITCODE -ne 0) {
            throw "Topic konnte nicht erstellt werden!"
        }
    }
    Write-Host "Topic '$topic' bereit" -ForegroundColor Green

    # 4. Schreibe Daten in Kafka
    Write-Host "`n[4/6] Schreibe Daten in Kafka Topic..." -ForegroundColor Yellow
    Write-Host "  Sende: $testData" -ForegroundColor Gray

    $testData | kubectl exec -i kafka-broker-0 -n $namespace -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $bootstrapServer --topic $topic

    if ($LASTEXITCODE -ne 0) {
        throw "Fehler beim Schreiben in Kafka!"
    }
    Write-Host "Daten erfolgreich in Kafka geschrieben" -ForegroundColor Green

    # 5. Prüfe Connector Status
    Write-Host "`n[5/6] Prüfe Kafka Connector..." -ForegroundColor Yellow
    $connectorStatus = kubectl exec deployment/kafka-connect -n $namespace -- curl -s http://localhost:8083/connectors 2>$null

    if ($connectorStatus -match "postgresql-sink") {
        Write-Host "  Connector 'postgresql-sink' vorhanden" -ForegroundColor Gray

        # Detailstatus abrufen
        $sinkStatus = kubectl exec deployment/kafka-connect -n $namespace -- curl -s http://localhost:8083/connectors/postgresql-sink/status 2>$null
        $statusJson = $sinkStatus | ConvertFrom-Json -ErrorAction SilentlyContinue

        if ($statusJson.connector.state -eq "RUNNING" -and $statusJson.tasks[0].state -eq "RUNNING") {
            Write-Host "  Connector und Task laufen" -ForegroundColor Green
        } elseif ($statusJson.tasks[0].state -eq "FAILED") {
            Write-Host "  WARNUNG: Task ist FAILED!" -ForegroundColor Red
            Write-Host "  Starte Task neu..." -ForegroundColor Yellow
            kubectl exec deployment/kafka-connect -n $namespace -- curl -s -X POST http://localhost:8083/connectors/postgresql-sink/tasks/0/restart 2>$null
            Start-Sleep -Seconds 3
        } else {
            Write-Host "  Connector Status: $($statusJson.connector.state), Task: $($statusJson.tasks[0].state)" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  Verfügbare Connectors: $connectorStatus" -ForegroundColor Yellow
        throw "postgresql-sink Connector nicht gefunden!"
    }

    # 6. Warte und prüfe PostgreSQL
    Write-Host "`n[6/6] Warte auf Daten in PostgreSQL..." -ForegroundColor Yellow

    $found = $false
    $elapsed = 0
    $checkInterval = 2

    while (-not $found -and $elapsed -lt $timeoutSeconds) {
        Start-Sleep -Seconds $checkInterval
        $elapsed += $checkInterval

        Write-Host "  Prüfe PostgreSQL... ($elapsed/$timeoutSeconds s)" -ForegroundColor Gray

        $query = "SELECT COUNT(*) FROM sensor_readings WHERE sensor_id = '$testId';"
        $result = kubectl exec postgresql-0 -n $dbNamespace -- psql -U postgres -d sensordata -t -c $query 2>$null
        $result = "$result".Trim()

        if ($result -match '^\d+$' -and [int]$result -gt 0) {
            $found = $true
            Write-Host "  Gefunden!" -ForegroundColor Green
        }
    }

    # Ergebnis
    Write-Host ""
    if ($found) {
        Write-Host "=== TEST ERFOLGREICH ===" -ForegroundColor Green
        Write-Host "Daten sind in PostgreSQL angekommen!" -ForegroundColor Cyan

        # Zeige die Daten
        Write-Host "`nGespeicherte Daten:" -ForegroundColor Yellow
        $selectQuery = "SELECT * FROM sensor_readings WHERE sensor_id = '$testId';"
        kubectl exec postgresql-0 -n $dbNamespace -- psql -U postgres -d sensordata -c $selectQuery
    } else {
        Write-Host "=== TEST FEHLGESCHLAGEN ===" -ForegroundColor Red
        Write-Host "Timeout nach $timeoutSeconds Sekunden - Daten nicht in PostgreSQL gefunden" -ForegroundColor Red

        # Debug-Infos
        Write-Host "`nDebug-Informationen:" -ForegroundColor Yellow

        Write-Host "`n  Connector Status:" -ForegroundColor Gray
        $debugStatus = kubectl exec deployment/kafka-connect -n $namespace -- curl -s http://localhost:8083/connectors/postgresql-sink/status 2>$null
        $debugJson = $debugStatus | ConvertFrom-Json -ErrorAction SilentlyContinue
        Write-Host "    Connector: $($debugJson.connector.state)"
        Write-Host "    Task: $($debugJson.tasks[0].state)"
        if ($debugJson.tasks[0].trace) {
            $traceLen = [Math]::Min(500, $debugJson.tasks[0].trace.Length)
            Write-Host "    Error: $($debugJson.tasks[0].trace.Substring(0, $traceLen))..." -ForegroundColor Red
        }

        Write-Host "`n  Letzte Kafka-Nachrichten:" -ForegroundColor Gray
        kubectl exec kafka-broker-0 -n $namespace -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $bootstrapServer --topic $topic --from-beginning --max-messages 3 --timeout-ms 5000 2>$null

        Write-Host "`n  PostgreSQL Tabelle:" -ForegroundColor Gray
        kubectl exec postgresql-0 -n $dbNamespace -- psql -U postgres -d sensordata -c "SELECT COUNT(*) FROM sensor_readings;"

        throw "Daten nicht in PostgreSQL angekommen"
    }
}
catch {
    Write-Host "`nFehler beim Test: $_" -ForegroundColor Red
    exit 1
}
finally {
    Write-Host "`nTest abgeschlossen." -ForegroundColor Gray
}
