# Installation Script
Write-Host "System Cluster Installation" -ForegroundColor Green

# 1. Kind Cluster erstellen/prüfen
Write-Host "[1/5] Prüfe Kind Cluster..." -ForegroundColor Yellow
$clusterName = "system-cluster"
$clusterExists = kind get clusters 2>$null | Select-String -Pattern "^$clusterName$"

if (-not $clusterExists) {
    Write-Host "Erstelle neuen Kind Cluster '$clusterName'..." -ForegroundColor Cyan
    kind create cluster --name $clusterName
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Kind Cluster-Erstellung fehlgeschlagen!"
        exit 1
    }
    Write-Host "✓ Kind Cluster erstellt" -ForegroundColor Green
} else {
    Write-Host "✓ Kind Cluster '$clusterName' existiert bereits" -ForegroundColor Green
}

# 2. Cluster Check
Write-Host "[2/5] Prüfe Kubernetes Cluster..." -ForegroundColor Yellow
kubectl cluster-info --context kind-$clusterName | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Kubernetes Cluster nicht erreichbar!"
    exit 1
}
Write-Host "✓ Cluster bereit" -ForegroundColor Green

# 3. Chart Lint
Write-Host "[3/5] Validiere Helm Chart..." -ForegroundColor Yellow
cd helm-charts\system
helm lint .
if ($LASTEXITCODE -ne 0) {
    Write-Error "Helm Chart hat Fehler!"
    exit 1
}
Write-Host "✓ Chart valide" -ForegroundColor Green

# 4. Installation
Write-Host "[4/5] Installiere Kafka Cluster..." -ForegroundColor Yellow
helm install kafka-cluster . --create-namespace --wait --timeout=300s
if ($LASTEXITCODE -ne 0) {
    Write-Error "Installation fehlgeschlagen!"
    exit 1
}
Write-Host "✓ Installation erfolgreich" -ForegroundColor Green

# 5. Status
Write-Host "[5/5] Status:" -ForegroundColor Yellow
Write-Host ""
kubectl get pods -n messaging
Write-Host ""
kubectl get svc -n messaging
Write-Host ""
Write-Host "✓ Kafka Cluster läuft!" -ForegroundColor Green
Write-Host ""
Write-Host "Connection String: kafka.messaging.svc.cluster.local:9092" -ForegroundColor Cyan
