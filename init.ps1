# Installation Script
$clusterName = "system-cluster"
$chartPath = "helm-charts/$clusterName"
Write-Host "System Cluster Installation" -ForegroundColor Green
try {
    #region  0. Prerequisites prüfen
    Write-Host "[0/5] Prüfe Prerequisites..." -ForegroundColor Yellow
    $missingTools = @()

    # Kind prüfen
    if (-not (Get-Command kind -ErrorAction SilentlyContinue)) {
        Write-Host "  Kind nicht gefunden, versuche Installation..." -ForegroundColor Cyan
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Kubernetes.kind
            if ($LASTEXITCODE -ne 0) { $missingTools += "kind" }
            else { Write-Host "  Kind installiert" -ForegroundColor Green }
        } else {
            $missingTools += "kind"
        }
    } else {
        Write-Host "  Kind vorhanden" -ForegroundColor Green
    }

    # Kubectl prüfen
    if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
        Write-Host "  Kubectl nicht gefunden, versuche Installation..." -ForegroundColor Cyan
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Kubernetes.kubectl
            if ($LASTEXITCODE -ne 0) { $missingTools += "kubectl" }
            else { Write-Host "  Kubectl installiert" -ForegroundColor Green }
        } else {
            $missingTools += "kubectl"
        }
    } else {
        Write-Host "  Kubectl vorhanden" -ForegroundColor Green
    }

    # Helm prüfen
    if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
        Write-Host "  Helm nicht gefunden, versuche Installation..." -ForegroundColor Cyan
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Helm.Helm
            if ($LASTEXITCODE -ne 0) { $missingTools += "helm" }
            else { Write-Host "  Helm installiert" -ForegroundColor Green }
        } else {
            $missingTools += "helm"
        }
    } else {
        Write-Host "  Helm vorhanden" -ForegroundColor Green
    }

    if ($missingTools.Count -gt 0) {
        Write-Error @"
Folgende Tools konnten nicht installiert werden: $($missingTools -join ', ')

Bitte manuell installieren:
  winget install Kubernetes.kind
  winget install Kubernetes.kubectl
  winget install Helm.Helm
"@
        exit 1
    }
    #endregion Prerequisites

    #region 1. Cluster erstellen/prüfen
    Write-Host "[1/5] Prüfe Cluster..." -ForegroundColor Yellow
    $clusterExists = kind get clusters 2>$null | Select-String -Pattern "^$clusterName$"

    if (-not $clusterExists) {
        Write-Host "Erstelle neuen Cluster '$clusterName'..." -ForegroundColor Cyan

        # Prüfe ob kind-config.yaml existiert
        $kindConfig = Join-Path $PSScriptRoot "kind-config.yaml"
        if (Test-Path $kindConfig) {
            Write-Host "  Nutze Kind-Konfiguration..." -ForegroundColor Cyan
            kind create cluster --config $kindConfig --wait 120s
        } else {
            Write-Warning "kind-config.yaml nicht gefunden. Erstelle Cluster ohne."
            kind create cluster --name $clusterName --wait 120s
        }

        if ($LASTEXITCODE -ne 0) {
            Write-Error "Cluster-Erstellung fehlgeschlagen!"
            exit 1
        }
        Write-Host "Cluster erstellt" -ForegroundColor Green
    } else {
        Write-Host "Cluster vorhanden" -ForegroundColor Green
    }
    #endregion Cluster erstellen/prüfen

    #region 2. Cluster Check
    Write-Host "[2/5] Prüfe Kubernetes Cluster..." -ForegroundColor Yellow
    kubectl cluster-info --context kind-$clusterName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Kubernetes Cluster nicht erreichbar!"
        exit 1
    }
    Write-Host "Cluster bereit" -ForegroundColor Green
    #endregion Cluster Check

    #region 3. Chart Lint
    Write-Host "[3/5] Validiere Helm Chart..." -ForegroundColor Yellow
    Push-Location $chartPath

    # Dependencies aktualisieren
    Write-Host "Lade Chart Dependencies..." -ForegroundColor Cyan
    helm dependency update
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Dependency Update fehlgeschlagen!"
        exit 1
    }

    helm lint .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Helm Chart hat Fehler!"
        exit 1
    }
    Write-Host "Chart valide" -ForegroundColor Green
    #endregion 3. Chart Lint

    #region 3.5. FastAPI Image bauen
    Write-Host "[3.5/5] Baue FastAPI Image..." -ForegroundColor Yellow
    Pop-Location  # Zurück zum Root-Verzeichnis

    $deployScript = Join-Path $PSScriptRoot "fastapi\deploy-fastapi.ps1"
    if (Test-Path $deployScript) {
        & $deployScript
        if ($LASTEXITCODE -ne 0) {
            Write-Error "FastAPI Deployment fehlgeschlagen!"
            exit 1
        }
    } else {
        Write-Warning "fastapi\deploy-fastapi.ps1 nicht gefunden."
        exit 1
    }

    Push-Location $chartPath  # Zurück zum Chart-Verzeichnis
    #endregion 3.5. FastAPI Image bauen

    #region 4. Installation
    Write-Host "[4/5] Installiere System Cluster..." -ForegroundColor Yellow

    # Prüfen ob Release bereits existiert (im default Namespace!)
    $releases = helm list -n default -o json 2>$null | ConvertFrom-Json
    $releaseExists = $releases | Where-Object { $_.name -eq $clusterName }
    $releasePending = $releases | Where-Object { $_.name -eq $clusterName -and $_.status -eq "pending-install" }

    if ($releasePending) {
        Write-Host "Hängende Operation gefunden. Bereinige..." -ForegroundColor Cyan
        helm rollback $clusterName 0 -n default 2>$null
        if ($LASTEXITCODE -ne 0) {
            helm uninstall $clusterName -n default --wait
        }
        Start-Sleep -Seconds 5
        $releaseExists = $null  # Nach Bereinigung neu prüfen
    }

    if ($releaseExists) {
        Write-Host "Bestehende Installation gefunden (Revision: $($releaseExists.revision))..." -ForegroundColor Cyan
        helm upgrade $clusterName . -n default --wait --timeout=300s
        $action = "Upgrade"
    } else {
        Write-Host "Neue Installation..." -ForegroundColor Cyan
        helm install $clusterName . -n default --create-namespace --wait --timeout=300s
        $action = "Installation"
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Error "$action fehlgeschlagen!"
        exit 1
    }
    Write-Host "$action erfolgreich" -ForegroundColor Green
    #endregion 4. Installation

    #region 5. Status
    Write-Host "[5/5] Status:" -ForegroundColor Yellow
    Write-Host ""
    kubectl get pods -n messaging
    Write-Host ""
    kubectl get svc -n messaging
    Write-Host ""
    Write-Host "Cluster läuft!" -ForegroundColor Green
    #endregion Status
}
finally {
    Pop-Location
}
