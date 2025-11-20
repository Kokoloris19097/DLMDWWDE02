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

    # Kafka Console Consumer als Job starten
    $kafkaConsumerJob = Start-Job -ScriptBlock {
        param($sensorId, $namespace)

        kubectl exec -n $namespace kafka-broker-0 -- `
            /opt/kafka/bin/kafka-console-consumer.sh `
            --bootstrap-server localhost:9092 `
            --topic sensor-data `
            --from-beginning `
            --timeout-ms 10000 2>$null | Select-String -Pattern $sensorId

    } -ArgumentList $sensorId, $NAMESPACE_MESSAGING

    # Warte auf Ergebnis
    $result = Wait-Job $kafkaConsumerJob -Timeout $TEST_TIMEOUT | Receive-Job
    Remove-Job $kafkaConsumerJob -Force

    if ($result) {
        Write-Host "`nTEST ERFOLGREICH!" -ForegroundColor Green
        Write-Host "`nNachricht in Kafka gefunden:" -ForegroundColor Cyan
        Write-Host $result -ForegroundColor White

        # Versuche JSON zu parsen
        try {
            $kafkaMessage = $result | ConvertFrom-Json
            Write-Host "`nVerifizierung:" -ForegroundColor Yellow
            Write-Host "  Sensor ID: $($kafkaMessage.sensor_id)" -ForegroundColor White
            Write-Host "  Timestamp: $($kafkaMessage.timestamp)" -ForegroundColor White
            Write-Host "  Temperature: $($kafkaMessage.temperature)°C" -ForegroundColor White
            Write-Host "  Humidity: $($kafkaMessage.humidity)%" -ForegroundColor White
            Write-Host "  Location: $($kafkaMessage.location)" -ForegroundColor White

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
        Write-Host "`nTEST FEHLGESCHLAGEN!" -ForegroundColor Red
        Write-Host "Nachricht wurde nicht in Kafka gefunden." -ForegroundColor Yellow
        Write-Host "`nMögliche Ursachen:" -ForegroundColor Yellow
        Write-Host "  - Kafka Producer Fehler in FastAPI" -ForegroundColor White
        Write-Host "  - Kafka Topic 'sensor-data' existiert nicht" -ForegroundColor White
        Write-Host "  - Kafka Broker nicht erreichbar" -ForegroundColor White

        $exitCode = 1
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
