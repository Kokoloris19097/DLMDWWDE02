# Kafka Funktionstest
Write-Host "=== Kafka Funktionstest ===" -ForegroundColor Green

$podName = "kafka-test"
$namespace = "messaging"
$bootstrapServer = "kafka.messaging.svc.cluster.local:9092"
$topic = "test-topic"

try {
    # 1. Test-Pod erstellen
    Write-Host "`n[1/8] Erstelle Test-Pod..." -ForegroundColor Yellow
    kubectl run $podName --image=apache/kafka:4.1.0 --namespace=$namespace --restart=Never -- sleep 3600

    # Warte bis Pod ready ist
    Write-Host "Warte auf Pod-Start..." -ForegroundColor Gray
    kubectl wait --for=condition=Ready pod/$podName -n $namespace --timeout=60s
    if ($LASTEXITCODE -ne 0) {
        throw "Pod konnte nicht gestartet werden"
    }
    Write-Host "Pod bereit" -ForegroundColor Green

    # 2. Topics auflisten
    Write-Host "`n[2/8] Liste Topics auf..." -ForegroundColor Yellow
    kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrapServer --list
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Topics konnten nicht aufgelistet werden" -ForegroundColor Red
        exit 1
    }else {
        Write-Host "Topics aufgelistet" -ForegroundColor Green
    }

    # 3. Test-Topic erstellen
    Write-Host "`n[3/8] Erstelle Test-Topic..." -ForegroundColor Yellow
    kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrapServer `
        --create --topic $topic `
        --partitions 3 `
        --replication-factor 2 `
        --if-not-exists
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Topic konnte nicht erstellt werden" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Topic erstellt" -ForegroundColor Green
    }

    # 4. Topic-Details anzeigen
    Write-Host "`n[4/8] Topic-Details:" -ForegroundColor Yellow
    kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrapServer `
        --describe --topic $topic
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Topic-Details konnten nicht abgerufen werden" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Topic-Details abgerufen" -ForegroundColor Green
    }

    # 5. Nachrichten senden
    Write-Host "`n[5/8] Sende Test-Nachrichten..." -ForegroundColor Yellow
    $messages = @("Nachricht 1 $(Get-Date -Format 'HH:mm:ss')", "Nachricht 2 $(Get-Date -Format 'HH:mm:ss')", "Nachricht 3 $(Get-Date -Format 'HH:mm:ss')")

    # Sende Nachrichten einzeln mit Bestätigung
    foreach ($msg in $messages) {
        Write-Host "  Sende: $msg" -ForegroundColor Gray
        echo $msg | kubectl exec -i -n $namespace $podName -- /opt/kafka/bin/kafka-console-producer.sh `
            --bootstrap-server $bootstrapServer `
            --topic $topic `
            --request-required-acks 1 `
            --request-timeout-ms 5000

        if ($LASTEXITCODE -ne 0) {
            Write-Host "Nachricht konnte nicht gesendet werden" -ForegroundColor Red
            exit 1
        }
    }
    Write-Host "$($messages.Count) Nachrichten erfolgreich gesendet" -ForegroundColor Green

    # Warte auf Replikation
    Write-Host "Warte auf Replikation..." -ForegroundColor Gray
    Start-Sleep -Seconds 3

    # 6. Nachrichten lesen
    Write-Host "`n[6/8] Lese Nachrichten..." -ForegroundColor Yellow

    # Prüfe zuerst ob Nachrichten im Topic sind
    Write-Host "Pruefe Topic-Offsets..." -ForegroundColor Gray
    $offsetOutput = kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-get-offsets.sh `
        --bootstrap-server $bootstrapServer `
        --topic $topic 2>&1

    Write-Host $offsetOutput

    # Jetzt lesen
    Write-Host "Starte Consumer..." -ForegroundColor Gray
    $consumerOutput = kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-console-consumer.sh `
        --bootstrap-server $bootstrapServer `
        --topic $topic `
        --from-beginning `
        --max-messages 3 `
        --timeout-ms 10000  2>&1

    $exitCode = $LASTEXITCODE
    Write-Host $consumerOutput

    # Prüfe ob Nachrichten empfangen wurden
    if ($consumerOutput -match "Processed a total of 0 messages") {
        Write-Host "FEHLER: Keine Nachrichten im Topic gefunden!" -ForegroundColor Red
        exit 1
    } elseif ($exitCode -ne 0) {
        Write-Host "Consumer-Fehler (Exit-Code: $exitCode)" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Nachrichten erfolgreich empfangen" -ForegroundColor Green
    }

    # 7. Test-Topic löschen
    Write-Host "`n[7/8] Lösche Test-Topic..." -ForegroundColor Yellow
    kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrapServer `
        --delete --topic $topic
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Topic konnte nicht gelöscht werden" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Topic gelöscht" -ForegroundColor Green
    }

    # 8. Broker-Info
    Write-Host "`n[8/8] Broker-Informationen:" -ForegroundColor Yellow
    kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-broker-api-versions.sh `
        --bootstrap-server $bootstrapServer 2>$null | Select-Object -First 5
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Broker-Info konnte nicht abgerufen werden" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Broker-Info abgerufen" -ForegroundColor Green
    }

    Write-Host "`n=== Alle Tests erfolgreich! ===" -ForegroundColor Green
    Write-Host "Kafka Cluster ist voll funktionsfähig." -ForegroundColor Cyan
}
catch {
    Write-Host "`nFehler beim Test: $_" -ForegroundColor Red
     # Test-Topic löschen falls noch vorhanden
    $topicExists = kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrapServer `
        --list 2>$null | Select-String -Pattern "^$topic$"

    if ($topicExists) {
        Write-Host "Lösche verbleibender Test-Topic..." -ForegroundColor Yellow
        kubectl exec -n $namespace $podName -- /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server $bootstrapServer `
            --delete --topic $topic 2>$null | Out-Null
    }
    exit 1
}
finally {
    # Cleanup: Pod und Topic löschen
    Write-Host "`nRäume Pod auf.." -ForegroundColor Yellow

    # Pod löschen
    kubectl delete pod $podName -n $namespace --ignore-not-found=true 2>$null | Out-Null
    Write-Host "Cleanup abgeschlossen" -ForegroundColor Green
}
