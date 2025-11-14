# Update Script für Helm Chart
$releaseName = "system-cluster"
$chartPath = "helm-charts\$releaseName"

Write-Host "Helm Chart Update" -ForegroundColor Green

try {
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
    Push-Location $chartPath
    helm lint .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Chart hat Fehler! Bitte korrigieren."
        exit 1
    }
    Write-Host "Chart valide" -ForegroundColor Green

    # 3. Release prüfen
    Write-Host "[3/4] Prüfe Release Status..." -ForegroundColor Yellow
    $releaseExists = helm list -q | Select-String -Pattern "^$releaseName$"

    if (-not $releaseExists) {
        Write-Host "Release '$releaseName' existiert nicht. Verwende init.ps1 für Installation." -ForegroundColor Red
        exit 1
    }

    $releaseStatus = helm list -o json | ConvertFrom-Json | Where-Object { $_.name -eq $releaseName }
    Write-Host "  Status: $($releaseStatus.status)" -ForegroundColor Cyan
    Write-Host "  Revision: $($releaseStatus.revision)" -ForegroundColor Cyan

    # 4. Upgrade durchführen
    Write-Host "[4/4] Führe Upgrade durch..." -ForegroundColor Yellow
    Write-Host "  Warte auf Pod-Rollout (max. 5 Minuten)..." -ForegroundColor Cyan

    helm upgrade $releaseName . --wait --timeout=300s

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Upgrade fehlgeschlagen!"
        Write-Host "`nRollback verfügbar mit:" -ForegroundColor Yellow
        Write-Host "  helm rollback $releaseName 0" -ForegroundColor Cyan
        exit 1
    }

    Write-Host "Upgrade erfolgreich" -ForegroundColor Green

    # 5. Status anzeigen
    Write-Host "`n=== Pod Status ===" -ForegroundColor Yellow
    kubectl get pods -n messaging

    Write-Host "`n=== Service Status ===" -ForegroundColor Yellow
    kubectl get svc -n messaging

    Write-Host "`nUpdate abgeschlossen!" -ForegroundColor Green
}
catch {
    Write-Error "Fehler beim Update: $_"
    exit 1
}
finally {
    Pop-Location
}
