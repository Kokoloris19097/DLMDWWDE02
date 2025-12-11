# Installation Script
$clusterName = "system-cluster"
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"
$initScriptRoot = $PSScriptRoot
$chartPath = Join-Path $initScriptRoot "helm-charts/$clusterName"

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

    #region 1. Podman-VM prüfen und starten
    # Prüfe ob Podman installiert ist
    if (-not (Get-Command podman -ErrorAction SilentlyContinue)) {
        Write-Log "ERROR" "Podman nicht gefunden! Bitte installiere Podman (https://podman.io/) und versuche es erneut."
        exit 1
    }

    # Prüfe ob eine Podman-VM existiert
    $podmanMachines = podman machine list --format json | ConvertFrom-Json
    if (-not $podmanMachines -or $podmanMachines.Count -eq 0) {
        Write-Log "INFO" "Erstelle neue Podman-VM..."
        podman machine init
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Podman-VM konnte nicht erstellt werden!"
            exit 1
        }
    }

    # Prüfe ob Podman-VM läuft
    $podmanState = ($podmanMachines | Where-Object { $_.Name -eq "podman-machine-default" })
    if (-not $podmanState) {
        # Fallback: Nimm erste VM
        $podmanState = $podmanMachines[0]
    }
    if ($podmanState.Running -ne $true) {
        Write-Log "INFO" "Starte Podman-VM..."
        podman machine start $podmanState.Name
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Podman-VM konnte nicht gestartet werden!"
            exit 1
        }
    } else {
        Write-Log "SUCCESS" "Podman-VM läuft bereits."
    }
    #endregion 1. Podman-VM prüfen und starten

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
    Set-Location $chartPath

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

    #region 3.A FastAPI Image bauen
    Write-Log "INFO" "[3A/5] Baue FastAPI Image..."
    Set-Location $initScriptRoot  # Zurück zum Root

    $deployScript = Join-Path $initScriptRoot "fastapi\deploy-fastapi.ps1"
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

    Set-Location $chartPath  # Zurück zum Chart-Verzeichnis
    #endregion 3.A FastAPI Image bauen

    #region 3.B Postgres Connector Image bauen
    Write-Log "INFO" "[3B/5] Baue Postgres Connector Image..."
    Set-Location $initScriptRoot  # Zurück zum Root

    $deployScript = Join-Path $initScriptRoot "postgresql-connector\deploy-postgres-connector.ps1"
    if (Test-Path $deployScript) {
        & $deployScript -noHelm
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Postgres Connector Deployment fehlgeschlagen. Fahre ohne Postgres Connector-Update fort."
        }
    } else {
        Write-Warning "$deployScript nicht gefunden."
    }
    #endregion 3.B Postgres Connector Image bauen

    #region 3.C Spark Image bauen
    Write-Log "INFO" "[3C/5] Baue Spark Image..."
    Set-Location $initScriptRoot  # Zurück zum Root-Verzeichnis

    $deployScript = Join-Path $initScriptRoot "spark\deploy-spark.ps1"
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

    Set-Location $chartPath  # Zurück zum Chart-Verzeichnis
    #endregion 3.C Spark Image bauen


    #region 4. Installation
    Write-Log "INFO" "[4/5] Installiere System Cluster..."

    # Warte kurz, bis Cluster vollständig bereit ist
    Write-Log "DEBUG" "  Warte auf Kubernetes API..."
    $maxRetries = 10
    $retryCount = 0
    while ($retryCount -lt $maxRetries) {
        kubectl cluster-info 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Log "SUCCESS" "  Kubernetes API bereit"
            break
        }
        $retryCount++
        Write-Log "DEBUG" "  Warte auf Cluster (Versuch $retryCount/$maxRetries)..."
        Start-Sleep -Seconds 5
    }

    # Warte bis kube-apiserver antwortet
    Write-Log "DEBUG" "  Warte auf API-Server..."
    $maxRetries = 15
    $retryCount = 0
    $apiReady = $false
    while ($retryCount -lt $maxRetries -and -not $apiReady) {
        try {
            $testPod = kubectl api-resources 2>&1
            if ($LASTEXITCODE -eq 0) {
                $apiReady = $true
                Write-Log "SUCCESS" "  API-Server reagiert"
            }
        } catch {}
        if (-not $apiReady) {
            $retryCount++
            if ($retryCount -lt $maxRetries) {
                Write-Log "DEBUG" "  API-Server nicht bereit (Versuch $retryCount/$maxRetries)..."
                Start-Sleep -Seconds 3
            }
        }
    }

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
        Write-Log "INFO" "Pods mit Status ungleich 'Running':"
        $pods = (kubectl get pods -A -o json | ConvertFrom-Json).items

        # Filtere Pods, bei denen mindestens ein Container nicht im Status 'running' ist
        $nonRunning = @()
        foreach ($pod in $pods) {
            if ($pod.status -and $pod.status.containerStatuses) {
                foreach ($container in $pod.status.containerStatuses) {
                    $stateName = $container.state.PSObject.Properties.Name
                    if ($stateName -ne 'running') {
                        $reason = $null
                        if ($container.state.$stateName -and $container.state.$stateName.reason) {
                            $reason = $container.state.$stateName.reason
                        } elseif ($container.state.$stateName -and $container.state.$stateName.message) {
                            $reason = $container.state.$stateName.message
                        } else {
                            $reason = $stateName
                        }
                        $nonRunning += [PSCustomObject]@{
                            Namespace = $pod.metadata.namespace
                            Name      = $pod.metadata.name
                            Phase     = $pod.status.phase
                            Status    = $stateName
                            Reason    = $reason
                        }
                    }
                }
            }
        }
        if ($nonRunningPods) {
            $nonRunning | Format-Table -AutoSize
        } else {
            Write-Log "SUCCESS" "Alle Pods sind im Status 'Running'."
        }
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

    #region 6 Tests ausführen
    Write-Log "INFO" "[5/5] Führe Tests aus..."
    start-sleep -Seconds 10 # Warten bis Pods bereit sind
    Write-Log "DEBUG" "  Running pytest..."
    Set-Location "$scriptRoot/tests"
    pytest test_1_health.py -v --tb=short
    pytest test_2_connectivity.py  -v --tb=short
    pytest test_3_functional.py  -v --tb=short
    #endregion 6 Tests ausführen

    #region 6. Port-Forwarding Skript starten
    Write-Log "INFO" "Starte Port-Forwarding Skript..."
    $portForwardScript = Join-Path $initScriptRoot "port-forward.ps1"
    if (Test-Path $portForwardScript) {
        Start-Process -FilePath "cmd.exe" -ArgumentList "/k pwsh -File `"$portForwardScript`"" -WindowStyle Minimized
        Write-Log "SUCCESS" "Port-Forwarding in neuem Prozess gestartet"
    } else {
        Write-Log "WARN" "port-forward.ps1 nicht gefunden."
    }
    #endregion 6. Port-Forwarding Skript starten

    #region 7. Sensor Simulator starten
    Write-Log "INFO" "Starte Sensor Simulator..."
    $simulatorScript = Join-Path $initScriptRoot "simulator\sensor_simulator.py"
    if (Test-Path $simulatorScript) {
        # Warte kurz, damit Port-Forwarding Zeit hat zu starten
        Write-Log "DEBUG" "  Warte 5 Sekunden auf Port-Forwarding..."
        Start-Sleep -Seconds 5

        # Starte Simulator in neuem PowerShell-Fenster mit venv-Aktivierung
        $venvPath = Join-Path $initScriptRoot "venv"
        $activateCmd = "$venvPath\Scripts\Activate.ps1"
        $simulatorCmd = "python `"$simulatorScript`" --interval 1.0 --sensors 15"
        $fullCmd = "& '$activateCmd'; python '$simulatorScript' --interval 1.0 --sensors 15"
        Start-Process -FilePath "pwsh.exe" -ArgumentList "-NoExit", "-Command", $fullCmd -WindowStyle Normal
        Write-Log "SUCCESS" "Sensor Simulator gestartet (15 Sensoren, 1.0s Intervall, venv aktiviert, PowerShell)"
    } else {
        Write-Log "WARN" "simulator\sensor_simulator.py nicht gefunden."
    }
    #endregion 7. Sensor Simulator starten
}
finally {
    Set-Location $initScriptRoot
    $delay = (Get-Date) - $init_starttime
    Write-Host ("Dauer der Installation: {0}h {1}m {2}s" -f ([int]$delay.TotalHours), ([int]$delay.Minutes), ([int]$delay.Seconds)) -ForegroundColor Cyan
}
