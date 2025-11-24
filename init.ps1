# Installation Script
$clusterName = "system-cluster"
$chartPath = "helm-charts/$clusterName"

$global:init_starttime = Get-Date
function Write-Log {
    param (
        [string]$LEVEL = "INFO",
        [string]$Message
    )
    if ($LEVEL -eq "INFO") {
        $color = "Yellow"
    }elseif ($LEVEL -eq "DEBUG") {
        $color = "Cyan"
    } elseif ($LEVEL -eq "SUCCESS") {
        $color = "Green"
    } elseif ($LEVEL -eq "WARN") {
        $color = "Orange"
    } elseif ($LEVEL -eq "ERROR") {
        $color = "Red"
    } else {
        $color = "White"
    }
    $delay = $((Get-Date) - $init_starttime).TotalSeconds.ToString("F2")
    Write-Host "[init $delay s] $Message" -ForegroundColor $color
}
Push-Location $PSScriptRoot
Write-Log "INFO" "System Cluster Installation"
try {
    #region  0. Prerequisites prüfen
    Write-Log "INFO" "[0/5] Prüfe Prerequisites..."
    $missingTools = @()

    # Kind prüfen
    if (-not (Get-Command kind -ErrorAction SilentlyContinue)) {
        Write-Log "DEBUG" "  Kind nicht gefunden, versuche Installation..."
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Kubernetes.kind
            if ($LASTEXITCODE -ne 0) { $missingTools += "kind" }
            else { Write-Log "SUCCESS" "  Kind installiert" }
        } else {
            $missingTools += "kind"
        }
    } else {
        Write-Log "SUCCESS" "  Kind vorhanden"
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
        Write-Log "SUCCESS" "  Kubectl vorhanden"
    }

    # Helm prüfen
    if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
        Write-Log "DEBUG" "  Helm nicht gefunden, versuche Installation..."
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install Helm.Helm
            if ($LASTEXITCODE -ne 0) { $missingTools += "helm" }
            else { Write-Log "SUCCESS" "  Helm installiert" }
        } else {
            $missingTools += "helm"
        }
    } else {
        Write-Log "SUCCESS" "  Helm vorhanden"
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
    Write-Log "INFO" "[1/5] Prüfe Cluster..."
    $clusterExists = kind get clusters 2>$null | Select-String -Pattern "^$clusterName$"

    if (-not $clusterExists) {
        Write-Log "DEBUG" "Erstelle neuen Cluster '$clusterName'..."
        # Prüfe ob kind-config.yaml existiert
        $kindConfig = Join-Path $PSScriptRoot "kind-config.yaml"
        if (Test-Path $kindConfig) {
            Write-Log "DEBUG" "  Nutze Kind-Konfiguration..."
            kind create cluster --config $kindConfig --wait 120s
        } else {
            Write-Log "WARN" "kind-config.yaml nicht gefunden. Erstelle Cluster ohne."
            kind create cluster --name $clusterName --wait 120s
        }

        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Cluster-Erstellung fehlgeschlagen!"
            exit 1
        }
        Write-Log "SUCCESS" "Cluster erstellt"
    } else {
        Write-Log "SUCCESS" "Cluster vorhanden"
    }
    #endregion Cluster erstellen/prüfen

    #region 2. Cluster Check
    Write-Log "INFO" "[2/5] Prüfe Kubernetes Cluster..."
    kubectl cluster-info --context kind-$clusterName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Kubernetes Cluster nicht erreichbar!"
        exit 1
    }
    Write-Log "SUCCESS" "Cluster bereit"
    #endregion Cluster Check

    #region 3. Chart Lint
    Write-Log "INFO" "[3/5] Validiere Helm Chart..."
    Push-Location $chartPath

    # Dependencies aktualisieren
    Write-Log "INFO" "Lade Chart Dependencies..."
    helm dependency update
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Dependency Update fehlgeschlagen!"
        exit 1
    }

    helm lint .
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Helm Chart hat Fehler!"
        exit 1
    }
    Write-Log "SUCCESS" "Chart valide"
    #endregion 3. Chart Lint

    #region 3.5. FastAPI Image bauen
    Write-Log "INFO" "[3.A/5] Baue FastAPI Image..."
    Pop-Location  # Zurück zum Root-Verzeichnis

    $deployScript = Join-Path $PSScriptRoot "fastapi\deploy-fastapi.ps1"
    if (Test-Path $deployScript) {
        & $deployScript -noHelm
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "FastAPI Deployment fehlgeschlagen!"
            exit 1
        }
    } else {
        Write-Log "WARN" "fastapi\deploy-fastapi.ps1 nicht gefunden."
        exit 1
    }

    Push-Location $chartPath  # Zurück zum Chart-Verzeichnis
    #endregion 3.5. FastAPI Image bauen

        #region 3.5. FastAPI Image bauen
    Write-Log "INFO" "[3.B/5] Baue Spark Image..."
    Pop-Location  # Zurück zum Root-Verzeichnis

    $deployScript = Join-Path $PSScriptRoot "spark\deploy-spark.ps1"
    if (Test-Path $deployScript) {
        & $deployScript -noHelm
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Spark Deployment fehlgeschlagen!"
            exit 1
        }
    } else {
        Write-Log "WARN" "spark\deploy-spark.ps1 nicht gefunden."
        exit 1
    }

    Push-Location $chartPath  # Zurück zum Chart-Verzeichnis
    #endregion 3.5. FastAPI Image bauen


    #region 4. Installation
    Write-Log "INFO" "[4/5] Installiere System Cluster..."

    # Prüfen ob Release bereits existiert (im default Namespace!)
    $releases = helm list -n default -o json 2>$null | ConvertFrom-Json
    $releaseExists = $releases | Where-Object { $_.name -eq $clusterName }
    $releasePending = $releases | Where-Object { $_.name -eq $clusterName -and $_.status -eq "pending-install" }

    if ($releasePending) {
        Write-Log "DEBUG" "Hängende Operation gefunden. Bereinige..."
        helm rollback $clusterName 0 -n default 2>$null
        if ($LASTEXITCODE -ne 0) {
            helm uninstall $clusterName -n default --wait
        }
        Start-Sleep -Seconds 5
        $releaseExists = $null  # Nach Bereinigung neu prüfen
    }

    if ($releaseExists) {
        Write-Log "DEBUG" "Bestehende Installation gefunden (Revision: $($releaseExists.revision))..."
        helm upgrade $clusterName . -n default --wait --timeout=300s
        $action = "Upgrade"
    } else {
        Write-Log "DEBUG" "Neue Installation..."
        helm install $clusterName . -n default --create-namespace --wait --timeout=300s
        $action = "Installation"
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "$action fehlgeschlagen!"
        exit 1
    }
    Write-Log "SUCCESS" "$action erfolgreich"
    #endregion 4. Installation

    #region 5. Status
    Write-Log "INFO" "[5/5] Status:"
    kubectl get pods -n messaging
    Write-Host "---"
    kubectl get svc -n messaging
    Write-Host "---"
    kubectl get pods -A -n default
    Write-Log "SUCCESS" "=== Cluster läuft! ==="
    #endregion Status
}
finally {
    Pop-Location
    $delay = (Get-Date) - $init_starttime
    Write-Host ("Dauer der Installation: {0}h {1}m {2}s" -f ([int]$delay.TotalHours), ([int]$delay.Minutes), ([int]$delay.Seconds)) -ForegroundColor Cyan
}
Pop-Location
