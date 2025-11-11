# Installation Script
$clusterName = "system-cluster"
$chartPath = "helm-charts\$clusterName"
Write-Host "System Cluster Installation" -ForegroundColor Green
try {
    # 0. Prerequisites prüfen
    Write-Host "[0/5] Prüfe Prerequisites..." -ForegroundColor Yellow
    $missingTools = @()

    # Kind prüfen
    if (-not (Get-Command kind -ErrorAction SilentlyContinue)) {
        Write-Host "  Kind nicht gefunden, versuche Installation..." -ForegroundColor Cyan
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Kubernetes.kind --silent --accept-source-agreements --accept-package-agreements
            if ($LASTEXITCODE -ne 0) { $missingTools += "kind" }
            else { Write-Host "  ✓ Kind installiert" -ForegroundColor Green }
        } else {
            $missingTools += "kind"
        }
    } else {
        Write-Host "  ✓ Kind vorhanden" -ForegroundColor Green
    }

    # Kubectl prüfen
    if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
        Write-Host "  Kubectl nicht gefunden, versuche Installation..." -ForegroundColor Cyan
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Kubernetes.kubectl --silent --accept-source-agreements --accept-package-agreements
            if ($LASTEXITCODE -ne 0) { $missingTools += "kubectl" }
            else { Write-Host "  ✓ Kubectl installiert" -ForegroundColor Green }
        } else {
            $missingTools += "kubectl"
        }
    } else {
        Write-Host "  ✓ Kubectl vorhanden" -ForegroundColor Green
    }

    if ($missingTools.Count -gt 0) {
        Write-Error @"
Folgende Tools konnten nicht installiert werden: $($missingTools -join ', ')

Bitte manuell installieren:
  winget install Kubernetes.kind
  winget install Kubernetes.kubectl
"@
        exit 1
    }

    # 1. Cluster erstellen/prüfen
    Write-Host "[1/5] Prüfe Cluster..." -ForegroundColor Yellow
    $clusterExists = kind get clusters 2>$null | Select-String -Pattern "^$clusterName$"

    if (-not $clusterExists) {
        Write-Host "Erstelle neuen Cluster '$clusterName'..." -ForegroundColor Cyan
        kind create cluster --name $clusterName
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Cluster-Erstellung fehlgeschlagen!"
            exit 1
        }
        Write-Host "✓ Cluster erstellt" -ForegroundColor Green
    } else {
        Write-Host "✓ Cluster vorhanden" -ForegroundColor Green
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
    Push-Location $chartPath
    helm lint .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Helm Chart hat Fehler!"
        exit 1
    }
    Write-Host "✓ Chart valide" -ForegroundColor Green

    # 4. Installation
    Write-Host "[4/5] Installiere Kafka Cluster..." -ForegroundColor Yellow
    helm install $clusterName . --create-namespace --wait --timeout=600s
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
}
finally {
    Pop-Location
}
