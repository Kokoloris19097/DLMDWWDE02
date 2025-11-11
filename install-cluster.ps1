# Vollständige Installation des Data Engineering Clusters
# Alle Services werden als eigene Deployments installiert (keine Helm Dependencies)
<#



#>

Write-Host "Data Engineering Cluster Installation gestartet..." -ForegroundColor Green
# Aktiviere virtuelle Umgebung
Write-Host "Aktiviere virtuelle Umgebung..." -ForegroundColor Cyan
.\.venv\Scripts\Activate.ps1

if ($LASTEXITCODE -ne 0 -and -not $?) {
    Write-Error "Virtuelle Umgebung konnte nicht aktiviert werden!"
    exit 1
}

Write-Host "OK - Virtuelle Umgebung aktiviert" -ForegroundColor Green

try {
    # 1. Kubernetes Cluster prüfen
    Write-Host "[1/5] Prüfe Kubernetes Cluster..." -ForegroundColor Yellow
    try {
        kubectl cluster-info | Out-Null
        Write-Host "OK - Kubernetes Cluster ist bereit" -ForegroundColor Green
    } catch {
        Write-Error "Kubernetes Cluster nicht gefunden! Bitte erst Cluster erstellen (Docker Desktop K8s oder kind create cluster)"
        exit 1
    }

    # 2. NGINX Ingress Controller installieren
    Write-Host "[2/5] Installiere NGINX Ingress Controller..." -ForegroundColor Yellow

    # Prüfe ob NGINX bereits installiert ist
    $nginxExists = kubectl get namespace ingress-nginx 2>$null
    if ($nginxExists) {
        Write-Host "INFO - NGINX Ingress Controller bereits installiert" -ForegroundColor Cyan
    } else {
        helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
        helm repo update

        helm install ingress-nginx ingress-nginx/ingress-nginx `
            --create-namespace `
            --namespace ingress-nginx `
            --set controller.service.type=NodePort `
            --wait --timeout=300s

        if ($LASTEXITCODE -eq 0) {
            Write-Host "OK - NGINX Ingress Controller installiert" -ForegroundColor Green
        } else {
            Write-Error "NGINX Installation fehlgeschlagen"
            exit 1
        }
    }

    # 3. Wechsel ins Chart-Verzeichnis
    Write-Host "[3/6] Wechsle ins Chart-Verzeichnis..." -ForegroundColor Yellow
    Push-Location .\helm-charts\data-engineering-platform
    Write-Host "OK - Im Chart-Verzeichnis" -ForegroundColor Green

    # 4. Helm Chart Lint-Prüfung
    Write-Host "[4/7] Prüfe Helm Chart mit Lint..." -ForegroundColor Yellow
    helm lint . -f values-custom.yaml

    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK - Helm Chart ist valide" -ForegroundColor Green
    } else {
        Write-Error "Helm Lint hat Fehler gefunden - bitte Chart korrigieren"
        exit 1
    }

    # 5. Prüfe auf bestehende Installation
    Write-Host "[5/7] Prüfe auf bestehende Installation..." -ForegroundColor Yellow
    $existingRelease = helm list -q | Select-String "data-platform"

    if ($existingRelease) {
        Write-Host "WARNUNG - Bestehende Installation 'data-platform' gefunden" -ForegroundColor Red
        $response = Read-Host "Möchten Sie die bestehende Installation deinstallieren? (j/n)"

        if ($response -eq "j" -or $response -eq "J") {
            Write-Host "Deinstalliere bestehende Installation..." -ForegroundColor Yellow
            helm uninstall data-platform 2>$null

            # Lösche alle Namespaces explizit
            Write-Host "Lösche Namespaces..." -ForegroundColor Yellow
            $namespaces = @("api", "messaging", "data", "monitoring")

            foreach ($ns in $namespaces) {
                $nsExists = kubectl get namespace $ns 2>$null
                if ($nsExists) {
                    Write-Host "  Lösche Namespace '$ns'..." -ForegroundColor Cyan
                    $(kubectl delete namespace $ns --timeout=90s) 2>$null
                }
            }

            # Warte auf vollständige Bereinigung der Namespaces
            Write-Host "Warte auf vollständige Bereinigung der Namespaces..." -ForegroundColor Yellow
            $maxWaitTime = 180  # Maximal 3 Minuten warten (erhöht wegen PVC)
            $interval = 5

            foreach ($ns in $namespaces) {
                $elapsed = 0
                while ($elapsed -lt $maxWaitTime) {
                    $nsStatus = kubectl get namespace $ns 2>$null
                    if (-not $nsStatus) {
                        # Namespace existiert nicht mehr
                        Write-Host "  Namespace '$ns' bereinigt" -ForegroundColor Green
                        break
                    }

                    Write-Host "  Warte auf Namespace '$ns' (${elapsed}s / ${maxWaitTime}s)..." -ForegroundColor Cyan
                    Start-Sleep -Seconds $interval
                    $elapsed += $interval
                }

                if ($elapsed -ge $maxWaitTime) {
                    Write-Host "  WARNUNG - Namespace '$ns' wurde nicht rechtzeitig bereinigt" -ForegroundColor Yellow
                }
            }

            Write-Host "OK - Bestehende Installation entfernt" -ForegroundColor Green
        } else {
            Write-Error "Installation abgebrochen - bitte zuerst manuell deinstallieren: helm uninstall data-platform"
            exit 1
        }
    } else {
        Write-Host "OK - Keine bestehende Installation gefunden" -ForegroundColor Green
    }

    # 6. Data Engineering Platform installieren
    Write-Host "[6/7] Installiere Data Engineering Platform..." -ForegroundColor Yellow
    Write-Host "INFO - Alle Services werden als Deployments erstellt (Kafka, Redis, MinIO, ClickHouse, Prometheus, Grafana)" -ForegroundColor Cyan

    helm install data-platform . -f values-custom.yaml --wait --timeout=600s

    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK - Data Engineering Platform installiert" -ForegroundColor Green
    } else {
        Write-Error "Platform Installation fehlgeschlagen"
        exit 1
    }

    # 7. Status anzeigen
    Write-Host "[7/7] System Status:" -ForegroundColor Yellow
    Write-Host ""
    helm status data-platform
    Write-Host ""
    Write-Host "Pods in allen Namespaces:" -ForegroundColor Cyan
    kubectl get pods --all-namespaces
    Write-Host ""
    Write-Host "Services in allen Namespaces:" -ForegroundColor Cyan
    kubectl get services --all-namespaces

    Write-Host ""
    Write-Host "Installation erfolgreich abgeschlossen!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Zugriff auf Services testen:" -ForegroundColor Cyan
    Write-Host "  kubectl port-forward -n api service/ingestion-api-service 8000:8000" -ForegroundColor White
    Write-Host "  kubectl port-forward -n api service/reporting-api-service 8001:8001" -ForegroundColor White
    Write-Host "  kubectl port-forward -n monitoring service/grafana 3000:3000" -ForegroundColor White
    Write-Host ""
    Write-Host "Grafana Login:" -ForegroundColor Cyan
    Write-Host "  URL: http://localhost:3000" -ForegroundColor White
    Write-Host "  User: admin" -ForegroundColor White
    Write-Host "  Password: grafana-admin" -ForegroundColor White
}
catch {
    Write-Error "Installation fehlgeschlagen: $_"
    exit 1
}
finally {
    Pop-Location
}
