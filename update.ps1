# Update Script für Helm Chart
$releaseName = "system-cluster"
$chartPath = "helm-charts\$releaseName"
$global:update_starttime = Get-Date
$scriptRoot = $PSScriptRoot
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"

Write-Host "Helm Chart Update" -ForegroundColor Green
Write-Host "Welche Images möchten Sie aktualisieren? (J/N)" -ForegroundColor Cyan
$updateFastAPI = $(Read-Host -Prompt "  [1] FastAPI") -eq "J"
$updatePostgresConnector = $(Read-Host -Prompt "  [2] Postgres Connector") -eq "J"
$updateSpark = $(Read-Host -Prompt "  [3] Spark") -eq "J"

try {
    Set-Location $scriptRoot  # Starte immer vom Script-Verzeichnis

    # 1. Kontext prüfen
    Write-Host "[1/4] Prüfe Kubernetes Kontext..." -ForegroundColor Yellow
    $currentContext = kubectl config current-context
    Write-Host "  Aktueller Kontext: $currentContext" -ForegroundColor Cyan

    if ($currentContext -ne "kind-$releaseName") {
        Write-Warning "Kontext ist nicht 'kind-$releaseName'. Fortfahren? (J/N)"
        $response = Read-Host
        if ($response -ne "J" -and $response -ne "j") {
            Write-Host "Abgebrochen." -ForegroundColor Red
            exit 0
        }
    }

    # 2. Chart validieren
    Write-Host "[2/4] Validiere Helm Chart..." -ForegroundColor Yellow
    $absoluteChartPath = Join-Path $scriptRoot $chartPath
    Set-Location $absoluteChartPath

    # Dependencies aktualisieren
    Write-Host "Lade Chart Dependencies..." -ForegroundColor Cyan
    helm dependency update
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Dependency Update fehlgeschlagen!"
        exit 1
    }

    helm lint .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Chart hat Fehler! Bitte korrigieren."
        exit 1
    }
    Write-Host "Chart valide" -ForegroundColor Green

    # 2.1 FastAPI Deployment
    if (-not $updateFastAPI) {
        Write-Host "[2.1/4] Überspringe FastAPI Update." -ForegroundColor Yellow
    } else {
        Write-Host "[2.1/4] Deploye FastAPI..." -ForegroundColor Yellow
        Set-Location $scriptRoot  # Zurück zum Root
        $deployScript = Join-Path $scriptRoot "fastapi\deploy-fastapi.ps1"
        if (Test-Path $deployScript) {
            & $deployScript
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "FastAPI Deployment fehlgeschlagen. Fahre ohne FastAPI-Update fort."
            }
        } else {
            Write-Warning "$deployScript nicht gefunden."
        }
    }

    # 2.2 Postgres Connector Deployment
    if (-not $updatePostgresConnector) {
        Write-Host "[2.2/4] Überspringe Postgres Connector Update." -ForegroundColor Yellow
    } else {
        Write-Host "[2.2/4] Deploye Postgres Connector..." -ForegroundColor Yellow
        Set-Location $scriptRoot  # Zurück zum Root

        $deployScript = Join-Path $scriptRoot "postgresql-connector\deploy-postgres-connector.ps1"
        if (Test-Path $deployScript) {
            & $deployScript
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Postgres Connector Deployment fehlgeschlagen. Fahre ohne Postgres Connector-Update fort."
            }
        } else {
            Write-Warning "$deployScript nicht gefunden."
        }
    }

    if (-not $updateSpark) {
        Write-Host "[2.3/4] Überspringe Spark Update." -ForegroundColor Yellow
    } else {
        Write-Host "[2.3/4] Baue Spark Image..." -ForegroundColor Yellow
        Set-Location $scriptRoot  # Zurück zum Root

        $deployScript = Join-Path $scriptRoot "spark\deploy-spark.ps1"
        if (Test-Path $deployScript) {
            & $deployScript -noHelm
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Spark Deployment fehlgeschlagen!" -ForegroundColor Red
                exit 1
            }
        } else {
            Write-Warning "$deployScript nicht gefunden."
        }

        Push-Location $chartPath  # Zurück zum Chart-Verzeichnis
    }

    # Zurück zum Chart-Verzeichnis
    Set-Location $absoluteChartPath

    # 3. Release prüfen
    Write-Host "[3/4] Prüfe Release Status..." -ForegroundColor Yellow
    $releaseExists = helm list -n default -q | Select-String -Pattern "^$releaseName$"

    if (-not $releaseExists) {
        Write-Host "Release '$releaseName' existiert nicht. Verwende init.ps1 für Installation." -ForegroundColor Red
        exit 1
    }

    $releaseStatus = helm list -n default -o json | ConvertFrom-Json | Where-Object { $_.name -eq $releaseName }
    Write-Host "  Status: $($releaseStatus.status)" -ForegroundColor Cyan
    Write-Host "  Revision: $($releaseStatus.revision)" -ForegroundColor Cyan

    # 4. Upgrade durchführen
    Write-Host "[4/4] Führe Upgrade durch..." -ForegroundColor Yellow
    Write-Host "  Warte auf Rollout (max. 5 Minuten)..." -ForegroundColor Cyan
    helm upgrade $releaseName . --namespace default --wait --timeout=300s

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Upgrade fehlgeschlagen!"
        Write-Host "`nRollback verfügbar mit:" -ForegroundColor Yellow
        Write-Host "  helm rollback $releaseName 0 --namespace default" -ForegroundColor Cyan
        exit 1
    }

    Write-Host "Running pytest..." -ForegroundColor Yellow
    Push-Location ./tests
    pytest test_1_health.py -v --tb=short

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Tests fehlgeschlagen!"
        exit 1
    }


    # Prüfe auf nicht-running Pods und gebe deren Logs aus
    try {
        $pods = kubectl get pods --all-namespaces -o json | ConvertFrom-Json
        $badPods = $pods.items | Where-Object {$_.status.phase -ne 'Running' -or
            ($_.status.containerStatuses | Where-Object { $_.state.waiting -and $_.state.waiting.reason -eq 'CrashLoopBackOff' })
        }
        foreach ($pod in $badPods) {
            $podName = $pod.metadata.name
            $podNs = $pod.metadata.namespace
            Write-Host "ERROR: Pod $podName (Namespace: $podNs) ist nicht Running (Status: $($pod.status.phase)). Logs:" -ForegroundColor Red
            kubectl logs $podName -n $podNs | ForEach-Object { Write-Host $_ }
        }
        if ($badPods.Count -gt 0) {
            Write-Error "=== Upgrade fehlgeschlagen ==="
        } else {
            Write-Host "=== Upgrade erfolgreich ===" -ForegroundColor Green
        }
    } catch {
        Write-Host "ERROR: Fehler beim Auslesen der Pod-Logs: $_" -ForegroundColor Red
    }

    # 5. Status anzeigen
    Write-Host "`n=== Pod Status ===" -ForegroundColor Yellow
    kubectl get pods -A

    Write-Host "`n=== Service Status ===" -ForegroundColor Yellow
    kubectl get svc -A

    Write-Host "`nUpdate abgeschlossen!" -ForegroundColor Green
}
catch {
    Write-Error "Fehler beim Update: $_"
    exit 1
}
finally {
    Set-Location $scriptRoot  # Immer zurück zum Ausgangspunkt
    $delay = (Get-Date) - $update_starttime
    Write-Host ("Dauer des Updates: {0}h {1}m {2}s" -f ([int]$delay.TotalHours), ([int]$delay.Minutes), ([int]$delay.Seconds)) -ForegroundColor Cyan
}
