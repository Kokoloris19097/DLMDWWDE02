# FastAPI - Test Script
# DLMDWWDE02 Master Project
# Testet Deployment, Konfiguration und Funktionalität der FastAPI

Write-Host "`n=== FastAPI Test Suite ===" -ForegroundColor Cyan

# Wechsle ins Skript-Verzeichnis
try {
    Push-Location $PSScriptRoot

    $testsPassed = 0
    $testsFailed = 0

    function Test-Step {
        param(
            [string]$Name,
            [scriptblock]$Test
        )

        Write-Host "`n[TEST] $Name" -ForegroundColor Yellow
        try {
            & $Test
            Write-Host "  PASSED" -ForegroundColor Green
            $script:testsPassed++
            return $true
        }
        catch {
            Write-Host "  FAILED: $_" -ForegroundColor Red
            $script:testsFailed++
            return $false
        }
    }

    # Test 1: Kubernetes Kontext prüfen
    Test-Step "Kubernetes Kontext" {
        $context = kubectl config current-context
        if ($context -ne "kind-system-cluster") {
            throw "Falscher Kontext: $context (erwartet: kind-system-cluster)"
        }
        Write-Host "  Kontext: $context" -ForegroundColor White
    }

    # Test 2: Namespace existiert
    Test-Step "API Namespace existiert" {
        $ns = kubectl get namespace api --no-headers 2>$null
        if (-not $ns) {
            throw "Namespace 'api' existiert nicht"
        }
        Write-Host "  Namespace 'api' gefunden" -ForegroundColor White
    }

    # Test 3: Deployment existiert
    Test-Step "FastAPI Deployment existiert" {
        $deploy = kubectl get deployment -n api fastapi --no-headers 2>$null
        if (-not $deploy) {
            throw "Deployment 'fastapi' existiert nicht"
        }
        Write-Host "  Deployment vorhanden" -ForegroundColor White
    }

    # Test 4: Pod läuft
    Test-Step "FastAPI Pod läuft" {
        $pods = kubectl get pods -n api -l app=fastapi --no-headers 2>$null
        if (-not $pods) {
            throw "Keine Pods gefunden"
        }

        $podStatus = ($pods -split '\s+')[2]
        if ($podStatus -ne "Running") {
            throw "Pod Status: $podStatus (erwartet: Running)"
        }

        $podName = ($pods -split '\s+')[0]
        Write-Host "  Pod: $podName - Status: $podStatus" -ForegroundColor White
    }

    # Test 5: Pod ist Ready
    Test-Step "FastAPI Pod ist Ready" {
        $pods = kubectl get pods -n api -l app=fastapi --no-headers 2>$null
        $ready = ($pods -split '\s+')[1]

        if ($ready -notmatch "1/1") {
            throw "Pod nicht ready: $ready"
        }
        Write-Host "  Ready: $ready" -ForegroundColor White
    }

    # Test 7: Umgebungsvariablen gesetzt
    Test-Step "Umgebungsvariablen konfiguriert" {
        $envVars = kubectl get pods -n api -l app=fastapi -o json 2>$null | ConvertFrom-Json
        $container = $envVars.items[0].spec.containers[0]

        $kafkaServers = ($container.env | Where-Object { $_.name -eq "KAFKA_BOOTSTRAP_SERVERS" }).value
        $kafkaTopic = ($container.env | Where-Object { $_.name -eq "KAFKA_TOPIC" }).value

        if (-not $kafkaServers) {
            throw "KAFKA_BOOTSTRAP_SERVERS nicht gesetzt"
        }
        if (-not $kafkaTopic) {
            throw "KAFKA_TOPIC nicht gesetzt"
        }

        Write-Host "  KAFKA_BOOTSTRAP_SERVERS: $kafkaServers" -ForegroundColor White
        Write-Host "  KAFKA_TOPIC: $kafkaTopic" -ForegroundColor White
    }

    # Test 8: Service existiert
    Test-Step "FastAPI Service existiert" {
        $svc = kubectl get service -n api fastapi --no-headers 2>$null
        if (-not $svc) {
            throw "Service 'fastapi' existiert nicht"
        }

        $port = kubectl get service -n api fastapi -o jsonpath='{.spec.ports[0].port}' 2>$null
        if ($port -ne "8000") {
            throw "Falscher Port: $port (erwartet: 8000)"
        }

        Write-Host "  Service Port: $port" -ForegroundColor White
    }

    # Test 9: Liveness Probe konfiguriert
    Test-Step "Liveness Probe konfiguriert" {
        $probe = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].spec.containers[0].livenessProbe.httpGet.path}' 2>$null

        if ($probe -ne "/health") {
            throw "Liveness Probe nicht oder falsch konfiguriert: $probe"
        }
        Write-Host "  Liveness Probe: $probe" -ForegroundColor White
    }

    # Test 10: Readiness Probe konfiguriert
    Test-Step "Readiness Probe konfiguriert" {
        $probe = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].spec.containers[0].readinessProbe.httpGet.path}' 2>$null

        if ($probe -ne "/ready") {
            throw "Readiness Probe nicht oder falsch konfiguriert: $probe"
        }
        Write-Host "  Readiness Probe: $probe" -ForegroundColor White
    }

    # Test 11: Pod Logs (keine Fehler)
    Test-Step "Pod Logs (keine kritischen Fehler)" {
        $podName = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].metadata.name}' 2>$null
        $logs = kubectl logs -n api $podName --tail=50 2>$null

        if ($logs -match "ERROR|CRITICAL|Traceback|Exception") {
            Write-Host "  Warnung: Fehler in Logs gefunden" -ForegroundColor Yellow
            Write-Host "  Letzte Zeilen:" -ForegroundColor Yellow
            $logs -split "`n" | Select-Object -Last 5 | ForEach-Object { Write-Host "    $_" -ForegroundColor Yellow }
        }
        else {
            Write-Host "  Keine kritischen Fehler in Logs" -ForegroundColor White
        }
    }

    # Test 12: Port-Forward und Health Check
    $portForwardJob = $null
    Test-Step "HTTP Health Check" {
        Write-Host "  Starte Port-Forward..." -ForegroundColor White

        # Port-Forward im Hintergrund
        $podName = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].metadata.name}' 2>$null
        $script:portForwardJob = Start-Job -ScriptBlock {
            kubectl port-forward -n api $args[0] 8000:8000
        } -ArgumentList $podName

        Start-Sleep -Seconds 5

        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -TimeoutSec 10

            if ($response.StatusCode -ne 200) {
                throw "Health Check fehlgeschlagen: HTTP $($response.StatusCode)"
            }

            $content = $response.Content | ConvertFrom-Json
            Write-Host "  Status: $($content.status)" -ForegroundColor White
            Write-Host "  Service: $($content.service)" -ForegroundColor White
        }
        finally {
            if ($script:portForwardJob) {
                Stop-Job $script:portForwardJob
                Remove-Job $script:portForwardJob
            }
        }
    }

    # Test 13: Readiness Check
    Test-Step "HTTP Readiness Check" {
        Write-Host "  Starte Port-Forward..." -ForegroundColor White

        $podName = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].metadata.name}' 2>$null
        $script:portForwardJob = Start-Job -ScriptBlock {
            kubectl port-forward -n api $args[0] 8000:8000
        } -ArgumentList $podName

        Start-Sleep -Seconds 5

        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8000/ready" -UseBasicParsing -TimeoutSec 10

            if ($response.StatusCode -ne 200) {
                throw "Readiness Check fehlgeschlagen: HTTP $($response.StatusCode)"
            }

            $content = $response.Content | ConvertFrom-Json
            Write-Host "  Status: $($content.status)" -ForegroundColor White
            Write-Host "  Kafka Connected: $($content.kafka_connected)" -ForegroundColor White
        }
        finally {
            if ($script:portForwardJob) {
                Stop-Job $script:portForwardJob
                Remove-Job $script:portForwardJob
            }
        }
    }

    # Test 14: API Dokumentation erreichbar
    Test-Step "API Dokumentation (Swagger UI)" {
        Write-Host "  Starte Port-Forward..." -ForegroundColor White

        $podName = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].metadata.name}' 2>$null
        $script:portForwardJob = Start-Job -ScriptBlock {
            kubectl port-forward -n api $args[0] 8000:8000
        } -ArgumentList $podName

        Start-Sleep -Seconds 5

        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 10

            if ($response.StatusCode -ne 200) {
                throw "Swagger UI nicht erreichbar: HTTP $($response.StatusCode)"
            }

            Write-Host "  Swagger UI erreichbar" -ForegroundColor White
        }
        finally {
            if ($script:portForwardJob) {
                Stop-Job $script:portForwardJob
                Remove-Job $script:portForwardJob
            }
        }
    }

    # Test 15: Test Ingest Endpoint
    Test-Step "POST /ingest Endpoint" {
        Write-Host "  Starte Port-Forward..." -ForegroundColor White

        $podName = kubectl get pods -n api -l app=fastapi -o jsonpath='{.items[0].metadata.name}' 2>$null
        $script:portForwardJob = Start-Job -ScriptBlock {
            kubectl port-forward -n api $args[0] 8000:8000
        } -ArgumentList $podName

        Start-Sleep -Seconds 5

        try {
            $testData = @{
                sensor_id = "TEST-SENSOR-001"
                timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
                temperature = 22.5
                humidity = 65.0
                location = "Test-Lab"
            } | ConvertTo-Json

            $response = Invoke-WebRequest -Uri "http://localhost:8000/ingest" `
                -Method POST `
                -Body $testData `
                -ContentType "application/json" `
                -UseBasicParsing `
                -TimeoutSec 10

            if ($response.StatusCode -ne 201) {
                throw "Ingest fehlgeschlagen: HTTP $($response.StatusCode)"
            }

            $content = $response.Content | ConvertFrom-Json
            Write-Host "  Status: $($content.status)" -ForegroundColor White
            Write-Host "  Sensor ID: $($content.sensor_id)" -ForegroundColor White
            Write-Host "  Kafka Partition: $($content.kafka_partition)" -ForegroundColor White
            Write-Host "  Kafka Offset: $($content.kafka_offset)" -ForegroundColor White
        }
        finally {
            if ($script:portForwardJob) {
                Stop-Job $script:portForwardJob
                Remove-Job $script:portForwardJob
            }
        }
    }

    # Zusammenfassung
    Write-Host "`n=== Test Zusammenfassung ===" -ForegroundColor Cyan
    Write-Host "Tests bestanden: $testsPassed" -ForegroundColor Green
    Write-Host "Tests fehlgeschlagen: $testsFailed" -ForegroundColor $(if ($testsFailed -eq 0) { "Green" } else { "Red" })

    $totalTests = $testsPassed + $testsFailed
    $successRate = [math]::Round(($testsPassed / $totalTests) * 100, 2)
    Write-Host "Erfolgsrate: $successRate%" -ForegroundColor $(if ($successRate -eq 100) { "Green" } elseif ($successRate -ge 80) { "Yellow" } else { "Red" })

    if ($testsFailed -eq 0) {
        Write-Host "`nAlle Tests erfolgreich!" -ForegroundColor Green
        exit 0
    }
    else {
        Write-Host "`nEinige Tests sind fehlgeschlagen. Bitte Logs pruefen." -ForegroundColor Red
        Write-Host "Logs anzeigen: kubectl logs -n api -l app=fastapi --tail=100" -ForegroundColor Yellow
        exit 1
    }
}finally {
    Pop-Location
}
