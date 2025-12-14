# Update Script für Helm Chart
$releaseName = "system-cluster"
$chartPath = "helm-charts\$releaseName"
$global:update_starttime = Get-Date
$scriptRoot = $PSScriptRoot
$env:KIND_EXPERIMENTAL_PROVIDER = "podman"

function Write-Log {
    param ([string]$Level = "INFO", [string]$Message)
    $colors = @{ "INFO" = "Yellow"; "DEBUG" = "Cyan"; "SUCCESS" = "Green"; "WARN" = "Magenta"; "ERROR" = "Red" }
    $delay = ((Get-Date) - $update_starttime).TotalSeconds.ToString("F2")
    Write-Host "[update $delay s] $Message" -ForegroundColor $colors[$Level]
}

function Get-UnrunningPods {
    # Prüfe auf nicht-running Pods und gebe deren Logs aus
    try {
        $pods = kubectl get pods --all-namespaces -o json | ConvertFrom-Json
        $badPods = $pods.items | Where-Object {$_.status.phase -ne 'Running' -or
            ($_.status.containerStatuses | Where-Object { $_.state.waiting -and $_.state.waiting.reason -eq 'CrashLoopBackOff' })
        }
        foreach ($pod in $badPods) {
            $podName = $pod.metadata.name
            $podNs = $pod.metadata.namespace
            Write-Log "ERROR" "Pod $podName (Namespace: $podNs) ist nicht Running (Status: $($pod.status.phase)). Logs:"
            kubectl logs $podName -n $podNs | ForEach-Object { Write-Host $_ }
        }
        return $badPods
    } catch {
        Write-Log "ERROR" "Fehler beim Auslesen der Pod-Logs: $_"
    }
}

Write-Log "INFO" "Helm Chart Update"
Write-Host "Welche Images möchten Sie aktualisieren? (J/N)" -ForegroundColor Cyan
$updateFastAPI = $(Read-Host -Prompt "  [1] FastAPI") -eq "J"
$updatePostgresConnector = $(Read-Host -Prompt "  [2] Postgres Connector") -eq "J"
$updateSpark = $(Read-Host -Prompt "  [3] Spark") -eq "J"

try {
    Set-Location $scriptRoot  # Starte immer vom Script-Verzeichnis

    # 0. Podman und Cluster prüfen/starten
    Write-Log "INFO" "[0/5] Prüfe Podman und Cluster..."

    # 0.1 Podman Machine prüfen
    Write-Log "DEBUG" "  Prüfe Podman Machine Status..."
    $podmanMachineRunning = $false
    try {
        $machineList = podman machine list --format json 2>$null | ConvertFrom-Json
        if ($machineList) {
            $runningMachine = $machineList | Where-Object { $_.Running -eq $true }
            if ($runningMachine) {
                $podmanMachineRunning = $true
                Write-Log "SUCCESS" "  Podman Machine läuft bereits ($($runningMachine.Name))"
            } else {
                Write-Log "WARN" "  Podman Machine existiert, läuft aber nicht"
                $defaultMachine = $machineList | Select-Object -First 1
                if ($defaultMachine) {
                    Write-Log "DEBUG" "  Starte Podman Machine '$($defaultMachine.Name)'..."
                    podman machine start $defaultMachine.Name
                    if ($LASTEXITCODE -eq 0) {
                        Write-Log "SUCCESS" "  Podman Machine gestartet"
                        $podmanMachineRunning = $true
                        Start-Sleep -Seconds 5  # Warte kurz, bis Machine vollständig hochgefahren ist
                    } else {
                        Write-Log "ERROR" "  Podman Machine konnte nicht gestartet werden!"
                        exit 1
                    }
                }
            }
        } else {
            Write-Log "WARN" "  Keine Podman Machine gefunden"
        }
    } catch {
        Write-Log "WARN" "  Fehler beim Prüfen von Podman: $_"
    }

    if (-not $podmanMachineRunning) {
        Write-Log "ERROR" "Podman Machine läuft nicht und konnte nicht gestartet werden!"
        exit 1
    }

    # 0.2 Kind Cluster prüfen
    Write-Log "DEBUG" "  Prüfe Kind Cluster Status..."
    $clusterExists = kind get clusters 2>$null | Select-String -Pattern "^$releaseName$"

    if (-not $clusterExists) {
        Write-Log "ERROR" "Cluster '$releaseName' existiert nicht!"
        exit 1
    }

    Write-Log "SUCCESS" "  Cluster '$releaseName' existiert"

    # 0.3 Cluster Erreichbarkeit prüfen
    Write-Log "DEBUG" "  Prüfe Cluster Erreichbarkeit..."
    kubectl cluster-info --context kind-$releaseName 2>$null | Out-Null

    if ($LASTEXITCODE -ne 0) {
        Write-Log "WARN" "  Cluster nicht erreichbar, versuche Container zu starten..."

        # Prüfe ob der Cluster-Container existiert und starte ihn
        $clusterContainerName = "$releaseName-control-plane"
        $containerStatus = podman ps -a --filter "name=^${clusterContainerName}$" --format "{{.Status}}"

        if ($containerStatus) {
            Write-Log "DEBUG" "  Container Status: $containerStatus"

            # Prüfe ob Container bereits läuft
            if ($containerStatus -match "^Up") {
                Write-Log "DEBUG" "  Container läuft bereits, prüfe Kubernetes API..."

                # Warte bis Kubernetes API verfügbar ist (max 30 Sekunden)
                $maxRetries = 6
                $retryCount = 0
                $clusterReady = $false

                while ($retryCount -lt $maxRetries -and -not $clusterReady) {
                    Start-Sleep -Seconds 5
                    $retryCount++
                    Write-Log "DEBUG" "  Warte auf Kubernetes API (Versuch $retryCount/$maxRetries)..."

                    kubectl cluster-info --context kind-$releaseName 2>$null | Out-Null
                    if ($LASTEXITCODE -eq 0) {
                        $clusterReady = $true
                        Write-Log "SUCCESS" "  Kubernetes API ist bereit"
                    }
                }

                if (-not $clusterReady) {
                    Write-Log "ERROR" "  Kubernetes API reagiert nicht nach $($maxRetries * 5) Sekunden!"
                    Write-Log "WARN" "  Versuche Container Neustart..."

                    podman restart $clusterContainerName | Out-Null
                    Start-Sleep -Seconds 15

                    kubectl cluster-info --context kind-$releaseName 2>$null | Out-Null
                    if ($LASTEXITCODE -ne 0) {
                        Write-Log "ERROR" "  Cluster nicht verfügbar nach Neustart!"
                        Write-Log "WARN" "  Bitte init.ps1 ausführen oder manuell prüfen."
                        exit 1
                    }
                    Write-Log "SUCCESS" "  Cluster nach Neustart erreichbar"
                }
            } else {
                # Container ist gestoppt, starte ihn
                Write-Log "DEBUG" "  Starte gestoppten Container '$clusterContainerName'..."
                podman start $clusterContainerName | Out-Null

                if ($LASTEXITCODE -ne 0) {
                    Write-Log "WARN" "  Container konnte nicht gestartet werden! Versuche Neustart..."
                    podman restart $clusterContainerName | Out-Null
                    if ($LASTEXITCODE -ne 0) {
                        Write-Log "ERROR" "  Neustart des Containers fehlgeschlagen!"
                        Write-Log "WARN" "  Prüfe Podman Logs mit: podman logs $clusterContainerName"
                        Write-Log "WARN" "  Möglicherweise ist der Container beschädigt. Entferne ihn ggf. manuell mit: podman rm $clusterContainerName"
                        Write-Log "WARN" "  Danach führe init.ps1 aus, um das Cluster neu zu erstellen."
                        exit 1
                    } else {
                        Write-Log "SUCCESS" "  Container erfolgreich neugestartet, warte auf Kubernetes API..."
                    }
                } else {
                    Write-Log "SUCCESS" "  Container gestartet, warte auf Kubernetes API..."
                }

                # Warte mit Retry-Logik bis Cluster vollständig hochgefahren ist
                $maxRetries = 12
                $retryCount = 0
                $clusterReady = $false

                while ($retryCount -lt $maxRetries -and -not $clusterReady) {
                    Start-Sleep -Seconds 5
                    $retryCount++
                    Write-Log "DEBUG" "  Warte auf Cluster (Versuch $retryCount/$maxRetries)..."

                    kubectl cluster-info --context kind-$releaseName 2>$null | Out-Null
                    if ($LASTEXITCODE -eq 0) {
                        $clusterReady = $true
                        Write-Log "SUCCESS" "  Cluster erfolgreich gestartet und erreichbar"

                        # Zusätzliche Prüfung: Warte bis Core-Pods ready sind
                        Write-Log "DEBUG" "  Prüfe Core-System Pods..."
                        Start-Sleep -Seconds 5
                        $corePods = kubectl get pods -n kube-system -o json 2>$null | ConvertFrom-Json
                        if ($corePods.items) {
                            $notReadyPods = $corePods.items | Where-Object {
                                $_.status.phase -ne 'Running' -or
                                ($_.status.containerStatuses | Where-Object { $_.ready -eq $false })
                            }
                            if ($notReadyPods.Count -gt 0) {
                                Write-Log "WARN" "  Einige System-Pods sind noch nicht ready, warte weitere 10 Sekunden..."
                                Start-Sleep -Seconds 10
                            }
                        }
                    }
                }

                if (-not $clusterReady) {
                    Write-Log "ERROR" "  Cluster ist nach $($maxRetries * 5) Sekunden nicht bereit!"
                    Write-Log "WARN" "  Prüfe Container Logs: podman logs $clusterContainerName"
                    Write-Log "WARN" "  Oder führe init.ps1 aus um Cluster neu zu erstellen."
                    exit 1
                }
            }
        } else {
            Write-Log "ERROR" "  Cluster Container '$clusterContainerName' nicht gefunden!"
            Write-Log "WARN" "  Bitte init.ps1 ausführen um den Cluster neu zu erstellen."
            exit 1
        }
    } else {
        Write-Log "SUCCESS" "  Cluster ist erreichbar"
    }

    # 1. Kontext prüfen
    Write-Log "INFO" "[1/5] Prüfe Kubernetes Kontext..."
    $currentContext = kubectl config current-context
    Write-Log "DEBUG" "  Aktueller Kontext: $currentContext"

    if ($currentContext -ne "kind-$releaseName") {
        Write-Log "WARN" "Kontext ist nicht 'kind-$releaseName'. Fortfahren? (J/N)"
        $response = Read-Host
        if ($response -ne "J" -and $response -ne "j") {
            Write-Log "ERROR" "Abgebrochen."
            exit 0
        }
    }

    # 2. Chart validieren
    Write-Log "INFO" "[2/5] Validiere Helm Chart..."
    $absoluteChartPath = Join-Path $scriptRoot $chartPath
    Set-Location $absoluteChartPath

    # Dependencies aktualisieren
    Write-Log "DEBUG" "  Lade Chart Dependencies..."
    helm dependency update
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Dependency Update fehlgeschlagen!"
        exit 1
    }

    helm lint .
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Chart hat Fehler! Bitte korrigieren."
        exit 1
    }
    Write-Log "SUCCESS" "Chart valide"

    # 2.1 FastAPI Deployment
    if (-not $updateFastAPI) {
        Write-Log "INFO" "[2.1/5] Überspringe FastAPI Update."
    } else {
        Write-Log "INFO" "[2.1/5] Deploye FastAPI..."
        Set-Location $scriptRoot  # Zurück zum Root
        $deployScript = Join-Path $scriptRoot "fastapi\deploy-fastapi.ps1"
        if (Test-Path $deployScript) {
            & $deployScript -noHelm
            if ($LASTEXITCODE -ne 0) {
                Write-Log "WARN" "FastAPI Deployment fehlgeschlagen. Fahre ohne FastAPI-Update fort."
            }
        } else {
            Write-Log "WARN" "$deployScript nicht gefunden."
        }
    }

    # 2.2 Postgres Connector Deployment
    if (-not $updatePostgresConnector) {
        Write-Log "INFO" "[2.2/5] Überspringe Postgres Connector Update."
    } else {
        Write-Log "INFO" "[2.2/5] Deploye Postgres Connector..."
        Set-Location $scriptRoot  # Zurück zum Root

        $deployScript = Join-Path $scriptRoot "postgresql-connector\deploy-postgres-connector.ps1"
        if (Test-Path $deployScript) {
            & $deployScript -noHelm
            if ($LASTEXITCODE -ne 0) {
                Write-Log "WARN" "Postgres Connector Deployment fehlgeschlagen. Fahre ohne Postgres Connector-Update fort."
            }
        } else {
            Write-Log "WARN" "$deployScript nicht gefunden."
        }
    }

    if (-not $updateSpark) {
        Write-Log "INFO" "[2.3/5] Überspringe Spark Update."
    } else {
        Write-Log "INFO" "[2.3/5] Baue Spark Image..."
        Set-Location $scriptRoot  # Zurück zum Root

        $deployScript = Join-Path $scriptRoot "spark\deploy-spark.ps1"
        if (Test-Path $deployScript) {
            & $deployScript -noHelm
            if ($LASTEXITCODE -ne 0) {
                Write-Log "ERROR" "Spark Deployment fehlgeschlagen!"
                exit 1
            }
        } else {
            Write-Log "WARN" "$deployScript nicht gefunden."
        }

        Push-Location $chartPath  # Zurück zum Chart-Verzeichnis
    }

    # Zurück zum Chart-Verzeichnis
    Set-Location $absoluteChartPath

    # 4. Upgrade durchführen
        Write-Log "INFO" "[4/5] Prüfe Release und führe Upgrade durch..."
    $releaseExists = helm list -n default -q | Select-String -Pattern "^$releaseName$"
    function Start-HelmReinstall {
        Write-Log "INFO" "Starte Neuinstallation des Releases..."
        helm uninstall $releaseName --namespace default
        helm install $releaseName . --namespace default --wait --timeout=300s
        if ($LASTEXITCODE -eq 0) {
            Write-Log "SUCCESS" "Neuinstallation erfolgreich."
            exit 0
        } else {
            Write-Log "ERROR" "Neuinstallation des Releases fehlgeschlagen!"
            exit 1
        }
    }

    if (-not $releaseExists) {
        Write-Log "INFO" "Es existiert kein Release für '$releaseName'."
        $HelmStatus = helm status $releaseName --namespace default | Select-String -Pattern "^STATUS:\s*(\w+)" | ForEach-Object {if ($_ -match "^STATUS:\s*(\w+)") { $matches[1] }}
        if ($HelmStatus -eq "pending-install" -or $HelmStatus -eq "pending-upgrade") {
            Write-Log "WARN" "Release konnte nicht vollständig installiert werden. Vermutlich laufen einige Pods nicht korrekt."
            $badPods = Get-UnrunningPods
            if ($badPods.Count -gt 0) {
                Write-Log "ERROR" "Behebe die Probleme mit den Pods."
                exit 1
            }
            # Prüfe, ob eine Revision existiert
            $history = helm history $releaseName --namespace default -o json | ConvertFrom-Json
            if ($history -and $history.Count -gt 0) {
                $lastRevision = $history | Where-Object { $_.status -eq "deployed" } | Select-Object -Last 1
                if ($lastRevision) {
                    Write-Log "INFO" "Versuche Rollback auf Revision $($lastRevision.revision) ..."
                    helm rollback $releaseName $($lastRevision.revision) --namespace default --wait --timeout=300s
                    Write-Log "DEBUG" "  Warte auf Rollout (max. 5 Minuten)..."
                    helm upgrade $releaseName . --namespace default --wait --timeout=300s
                    if ($LASTEXITCODE -eq 0) {
                        Write-Log "SUCCESS" "Rollback erfolgreich."
                        exit 0
                    } else {
                        Write-Log "WARN" "Rollback fehlgeschlagen"
                    }
                } else {
                    Write-Log "WARN" "Keine erfolgreiche Revision gefunden, versuche Neuinstallation ..."
                    Start-HelmReinstall
                }
            } else {
                Write-Log "WARN" "Keine Release-Historie gefunden, versuche Neuinstallation ..."
                Start-HelmReinstall
            }

            if ($LASTEXITCODE -eq 0) {
                Write-Log "SUCCESS" "Neuinstallation erfolgreich."
                exit 0
            } else {
                Write-Log "ERROR" "Installation des Releases trotz Fix fehlgeschlagen!"
                exit 1
            }
        }
    }else {
        $releaseStatus = helm list -n default -o json | ConvertFrom-Json | Where-Object { $_.name -eq $releaseName }
        Write-Log "DEBUG" "  Status: $($releaseStatus.status)"
        Write-Log "DEBUG" "  Revision: $($releaseStatus.revision)"
        Write-Log "DEBUG" "  Warte auf Rollout (max. 5 Minuten)..."
        helm upgrade $releaseName . --namespace default --wait --timeout=300s
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Upgrade fehlgeschlagen!"
        Write-Log "WARN" "Rollback verfügbar mit:"
        Write-Log "DEBUG" "  helm rollback $releaseName 0 --namespace default"
        Write-Log "INFO" "Pods mit Status ungleich 'Running':"
        # Filtere Pods, bei denen mindestens ein Container nicht im Status 'running' ist
        $pods = (kubectl get pods -A -o json | ConvertFrom-Json).items
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

    # Restart der Pods erzwingen, um sicherzustellen, dass alle Änderungen übernommen werden
    Write-Log "DEBUG" "  Erzwinge Pod-Restarts..."
    if ($updateFastAPI) {
        kubectl delete pod -n api -l app=fastapi
    }
    if ($updatePostgresConnector) {
        kubectl delete pod -n messaging -l app=kafka-connect
    }
    if ($updateSpark) {
        kubectl delete pod -n data -l app=spark
    }

    Write-Log "INFO" "[5/5] Führe Tests aus..."
    start-sleep -Seconds 10 # Warten bis Pods bereit sind
    Write-Log "DEBUG" "  Running pytest..."
    Set-Location "$scriptRoot/tests"
    pytest test_1_health.py -v --tb=short

    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Tests fehlgeschlagen!"
        exit 1
    }

    $badPods = Get-UnrunningPods
    if ($badPods.Count -gt 0) {
        Write-Log "ERROR" "=== Upgrade fehlgeschlagen ==="
    } else {
        Write-Log "SUCCESS" "=== Upgrade erfolgreich ==="
    }

    # 6. Status anzeigen
    Write-Host "`n=== Pod Status ===" -ForegroundColor Yellow
    kubectl get pods -A

    Write-Host "`n=== Service Status ===" -ForegroundColor Yellow
    kubectl get svc -A

    Write-Log "SUCCESS" "Update abgeschlossen!"
}
catch {
    Write-Log "ERROR" "Fehler beim Update: $_"
    exit 1
}
finally {
    Set-Location $scriptRoot  # Immer zurück zum Ausgangspunkt
    $delay = (Get-Date) - $update_starttime
    Write-Log "DEBUG" ("Dauer des Updates: {0}h {1}m {2}s" -f ([int]$delay.TotalHours), ([int]$delay.Minutes), ([int]$delay.Seconds))
}
