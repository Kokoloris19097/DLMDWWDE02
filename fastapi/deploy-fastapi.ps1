# FastAPI - Build & Deploy Script
# DLMDWWDE02 Master Project

Write-Host "`n=== FastAPI Build & Deploy ===" -ForegroundColor Cyan

# Konfiguration
$IMAGE_NAME = "fastapi"
$IMAGE_TAG = "latest"
$LOCAL_IMAGE = "localhost/${IMAGE_NAME}:${IMAGE_TAG}"
$CLUSTER_NAME = "system-cluster"
$HELM_RELEASE = "system-cluster"

try {
    Push-Location $PSScriptRoot
    # 1. Prüfe ob Podman läuft
    Write-Host "`n[1/6] Pruefe Podman..." -ForegroundColor Yellow
    if (-not (Get-Command podman -ErrorAction SilentlyContinue)) {
        Write-Host "  Podman nicht gefunden, versuche Installation..." -ForegroundColor Cyan
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install RedHat.Podman
            if ($LASTEXITCODE -ne 0) {
                Write-Host "  Podman konnte nicht installiert werden. Bitte manuell installieren." -ForegroundColor Red
                exit 1
            }
            Write-Host "  Podman installiert" -ForegroundColor Green
        } else {
            Write-Host "  Podman konnte nicht installiert werden. Bitte manuell installieren." -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "  Podman vorhanden" -ForegroundColor Green
    }

    # 2. Prüfe ob Dateien existieren
    Write-Host "`n[2/6] Pruefe Dateien..." -ForegroundColor Yellow
    $requiredFiles = @("Dockerfile.fastapi", "main.py", "requirements.txt")
    foreach ($file in $requiredFiles) {
        if (-Not (Test-Path $file)) {
            Write-Host "FEHLER: Datei $file nicht gefunden!" -ForegroundColor Red
            exit 1
        }
    }
    Write-Host "Alle Dateien vorhanden" -ForegroundColor Green

    # 3. Podman Image bauen
    Write-Host "`n[3/6] Baue Podman Image..." -ForegroundColor Yellow
    podman build -f Dockerfile.fastapi -t ${IMAGE_NAME}:${IMAGE_TAG} -t ${LOCAL_IMAGE} .
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FEHLER: Podman Build fehlgeschlagen!" -ForegroundColor Red
        exit 1
    }
    Write-Host "Image erfolgreich gebaut: ${LOCAL_IMAGE}" -ForegroundColor Green

    # 4. Image in Kind Cluster laden
    Write-Host "`n[4/6] Lade Image in Kind Cluster..." -ForegroundColor Yellow
    # Exportiere als Tar für Kind's Podman Provider
    podman save ${LOCAL_IMAGE} -o fastapi-image.tar
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FEHLER: Podman Save fehlgeschlagen!" -ForegroundColor Red
        exit 1
    }

    # Lade in Kind Cluster
    kind load image-archive fastapi-image.tar --name ${CLUSTER_NAME}
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FEHLER: Kind Load fehlgeschlagen!" -ForegroundColor Red
        Remove-Item fastapi-image.tar -ErrorAction SilentlyContinue
        exit 1
    }

    # Bereinige Tar-Datei
    Remove-Item fastapi-image.tar -ErrorAction SilentlyContinue
    Write-Host "Image in Cluster geladen: ${LOCAL_IMAGE}" -ForegroundColor Green

    # 5. Helm Upgrade/Install
    Write-Host "`n[5/6] Deploye mit Helm..." -ForegroundColor Yellow
    Push-Location ..\helm-charts\system-cluster


    # Prüfe ob Release existiert
    $releaseExists = helm list -q --namespace default | Select-String "^${HELM_RELEASE}$"
    if ($releaseExists) {
        Write-Host "  Upgrade existierendes Release..." -ForegroundColor Cyan
        helm upgrade ${HELM_RELEASE} . --namespace default --wait --timeout=180s
    } else {
        Write-Host "  Installiere neues Release..." -ForegroundColor Cyan
        helm install ${HELM_RELEASE} . --namespace default --create-namespace --wait --timeout=180s
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Host "FEHLER: Helm Deployment fehlgeschlagen!" -ForegroundColor Red
        exit 1
    }
    Write-Host "Helm Deployment erfolgreich" -ForegroundColor Green

    Write-Host "  Pod-Neustart mit neuem Image..." -ForegroundColor Cyan
    kubectl delete pod -n api -l app=fastapi --ignore-not-found=true 2>$null
    Write-Host "  Pod wird mit neuem Image neu erstellt" -ForegroundColor Green


}catch {
    Write-Host "FEHLER: $_" -ForegroundColor Red
    # Bereinige Tar-Datei
    if (Test-Path fastapi-image.tar) {
        Remove-Item fastapi-image.tar -ErrorAction SilentlyContinue
        Write-Host "Lokales Image fastapi-image.tar entfernt" -ForegroundColor Green
    }
    exit 1
}
finally {
    Pop-Location
}

# 6. Status prüfen
Write-Host "`n[6/6] Pruefe Deployment-Status..." -ForegroundColor Yellow
Start-Sleep -Seconds 10
kubectl get pods -n api
kubectl get svc -n api
kubectl get ingress -n api 2>$null

Write-Host "`n=== Deployment abgeschlossen ===" -ForegroundColor Green
Write-Host "`nNaechste Schritte:" -ForegroundColor Cyan
Write-Host "  1. Logs pruefen: kubectl logs -n api -l app=fastapi --tail=50" -ForegroundColor White
Write-Host "  2. Port Forward: kubectl port-forward -n api svc/fastapi 8000:8000" -ForegroundColor White
Write-Host "  3. API Docs: http://localhost:8000/docs" -ForegroundColor White
Write-Host "  4. Health Check: curl http://localhost:8000/health" -ForegroundColor White
Write-Host "  5. Test Ingest: curl -X POST http://localhost:8000/ingest -H 'Content-Type: application/json' -d '{...}'" -ForegroundColor White
