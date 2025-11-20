# FastAPI → Kafka Integration Test
# DLMDWWDE02 Master Project
# Testet ob Nachrichten von FastAPI in Kafka landen

Write-Host "`n=== FastAPI → Kafka Integration Test ===" -ForegroundColor Cyan

$NAMESPACE_API = "api"
$NAMESPACE_MESSAGING = "messaging"
$TEST_TIMEOUT = 30

try {
    # 1. Port-Forward zu FastAPI starten
    Write-Host "`n[1/5] Starte Port-Forward zu FastAPI..." -ForegroundColor Yellow
    $portForwardJob = Start-Job -ScriptBlock {
        kubectl port-forward -n api svc/fastapi 8000:8000
    }

    Start-Sleep -Seconds 3
    Write-Host "  Port-Forward aktiv (Job ID: $($portForwardJob.Id))" -ForegroundColor Green

    # 2. FastAPI Health Check
    Write-Host "`n[2/5] Prüfe FastAPI Health..." -ForegroundColor Yellow
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:8000/health" -TimeoutSec 5
        Write-Host "  Status: $($health.status)" -ForegroundColor Green
    }
    catch {
        Write-Error "FastAPI nicht erreichbar: $_"
        exit 1
    }

    # 3. Test-Daten an FastAPI senden
    Write-Host "`n[3/5] Sende Test-Nachricht an FastAPI..." -ForegroundColor Yellow

    # Timestamp: 1 Minute in der Vergangenheit, um Validierung zu bestehen
    $timestamp = (Get-Date).AddMinutes(-1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $sensorId = "TEST-$(Get-Random -Minimum 1000 -Maximum 9999)"

    $testData = @{
        sensor_id = $sensorId
        timestamp = $timestamp
        temperature = 22.5
        humidity = 65.0
    } | ConvertTo-Json

    Write-Host "  Sensor ID: $sensorId" -ForegroundColor Cyan
    Write-Host "  Timestamp: $timestamp" -ForegroundColor Cyan

    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/ingest" `
            -Method POST `
            -Body $testData `
            -ContentType "application/json" `
            -TimeoutSec 10

        Write-Host "  Nachricht gesendet" -ForegroundColor Green
        Write-Host "  Kafka Partition: $($response.kafka_partition)" -ForegroundColor White
        Write-Host "  Kafka Offset: $($response.kafka_offset)" -ForegroundColor White
    }
    catch {
        Write-Error "Fehler beim Senden: $_"
        exit 1
    }

    # 4. Warte kurz, damit Nachricht in Kafka landet
    Write-Host "`n[4/5] Warte auf Kafka-Verarbeitung..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5

    # 5. Prüfe ob Nachricht in Kafka ist
    Write-Host "`n[5/5] Prüfe Kafka Topic 'sensor-data'..." -ForegroundColor Yellow

    # Alle Partitionen des Topics mit dem Test-Pod abfragen
    $topicDetails = kubectl exec -n $NAMESPACE_MESSAGING kafka-test -- /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka-broker.messaging.svc.cluster.local:9092 `
    --describe --topic sensor-data
    $topicDetails[1].trim()  -match 'Partition: (?<partid>\d[,\d]*)'
    $partitions = ($Matches.partid -split ',') | ForEach-Object { [int]($_.Trim()) }

    $result = @()
    foreach ($partition in $partitions) {
        $output = kubectl exec -n $NAMESPACE_MESSAGING kafka-test -- `
            /opt/kafka/bin/kafka-console-consumer.sh `
            --bootstrap-server kafka-broker.messaging.svc.cluster.local:9092 `
            --topic sensor-data `
            --partition $partition `
            --from-beginning `
            --timeout-ms 10000 2>$null | Select-String -Pattern $sensorId
        if ($output) { $result += $output }
    }

    if ($result) {
        Write-Host "`nTEST ERFOLGREICH!" -ForegroundColor Green
        Write-Host "`nNachricht in Kafka gefunden:" -ForegroundColor Cyan
        Write-Host $result -ForegroundColor White

        # Versuche JSON zu parsen
        try {
            $kafkaMessage = $result | ConvertFrom-Json
            Write-Host "`nVerifizierung:" -ForegroundColor Yellow
            Write-Host " $($kafkaMessage)" -ForegroundColor White

            if ($kafkaMessage.sensor_id -eq $sensorId) {
                Write-Host "`nSensor ID stimmt überein!" -ForegroundColor Green
            }
        }
        catch {
            Write-Warning "JSON-Parsing fehlgeschlagen, aber Nachricht gefunden"
        }

        $exitCode = 0
    }
    else {
        Write-Host "`n TEST FEHLGESCHLAGEN!" -ForegroundColor Red
        throw
    }

}
catch {
    Write-Error "Fehler beim Test: $_"
    $exitCode = 1
}
finally {
    # Cleanup: Port-Forward beenden
    if ($portForwardJob) {
        Write-Host "`nBeende Port-Forward..." -ForegroundColor Yellow
        Stop-Job $portForwardJob -ErrorAction SilentlyContinue
        Remove-Job $portForwardJob -Force -ErrorAction SilentlyContinue
        Write-Host "  Port-Forward gestoppt" -ForegroundColor Green
    }
}

Write-Host "`n=== Test abgeschlossen ===" -ForegroundColor Cyan
exit $exitCode
