#Requires -Version 7.0
<#
.SYNOPSIS
    Deployt Prometheus Monitoring Stack und aktualisiert FastAPI

.DESCRIPTION
    Dieses Skript:
    1. Rebuilt FastAPI Image mit Prometheus Dependencies
    2. Lädt Image in Kind Cluster
    3. Upgraded Helm Release mit Prometheus
    4. Verifiziert Deployment
    5. Startet Port-Forward zu Prometheus UI

.EXAMPLE
    .\deploy-monitoring.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# =============================================================================
# KONFIGURATION
# =============================================================================

$CLUSTER_NAME = "system-cluster"
$NAMESPACE = "monitoring"
$HELM_RELEASE = "system-cluster"
$FASTAPI_IMAGE = "localhost/fastapi:latest"

# =============================================================================
# FUNKTIONEN
# =============================================================================

function Write-Step {
    param([string]$Message)
    Write-Host "`n[SCHRITT]" -ForegroundColor Cyan -NoNewline
    Write-Host " $Message" -ForegroundColor White
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK]" -ForegroundColor Green -NoNewline
    Write-Host " $Message" -ForegroundColor White
}

function Write-Warning {
    param([string]$Message)
    Write-Host "[WARNUNG]" -ForegroundColor Yellow -NoNewline
    Write-Host " $Message" -ForegroundColor White
}

function Write-Error {
    param([string]$Message)
    Write-Host "[FEHLER]" -ForegroundColor Red -NoNewline
    Write-Host " $Message" -ForegroundColor White
}

function Test-Command {
    param([string]$Command)
    return $null -ne (Get-Command $Command -ErrorAction SilentlyContinue)
}

function Wait-ForPods {
    param(
        [string]$Namespace,
        [string]$LabelSelector,
        [int]$TimeoutSeconds = 120
    )

    Write-Host "Warte auf Pods mit Label '$LabelSelector' in Namespace '$Namespace'..." -ForegroundColor Gray

    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        $pods = kubectl get pods -n $Namespace -l $LabelSelector -o json | ConvertFrom-Json

        if ($pods.items.Count -gt 0) {
            $allReady = $true
            foreach ($pod in $pods.items) {
                $ready = $pod.status.conditions | Where-Object { $_.type -eq "Ready" -and $_.status -eq "True" }
                if (-not $ready) {
                    $allReady = $false
                    break
                }
            }

            if ($allReady) {
                Write-Success "Alle Pods sind bereit"
                return $true
            }
        }

        Start-Sleep -Seconds 5
        $elapsed += 5
        Write-Host "." -NoNewline -ForegroundColor Gray
    }

    Write-Host ""
    Write-Error "Timeout beim Warten auf Pods"
    return $false
}

# =============================================================================
# HAUPTSKRIPT
# =============================================================================

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " Prometheus Monitoring Deployment" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# -----------------------------------------------------------------------------
# 1. Voraussetzungen prüfen
# -----------------------------------------------------------------------------

Write-Step "Prüfe Voraussetzungen"

$requiredCommands = @("kubectl", "helm", "podman", "kind")
$missingCommands = @()

foreach ($cmd in $requiredCommands) {
    if (Test-Command $cmd) {
        Write-Success "$cmd ist installiert"
    } else {
        Write-Error "$cmd nicht gefunden"
        $missingCommands += $cmd
    }
}

if ($missingCommands.Count -gt 0) {
    Write-Error "Fehlende Kommandos: $($missingCommands -join ', ')"
    exit 1
}

# -----------------------------------------------------------------------------
# 2. Cluster-Erreichbarkeit prüfen
# -----------------------------------------------------------------------------

Write-Step "Prüfe Kubernetes Cluster"

try {
    $clusterInfo = kubectl cluster-info 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Cluster nicht erreichbar"
    }
    Write-Success "Cluster ist erreichbar"
} catch {
    Write-Error "Kubernetes Cluster nicht erreichbar. Starte Kind Cluster?"
    Write-Host "  kind create cluster --name $CLUSTER_NAME" -ForegroundColor Yellow
    exit 1
}

# -----------------------------------------------------------------------------
# 3. FastAPI Image mit Prometheus Dependencies neu bauen
# -----------------------------------------------------------------------------

Write-Step "Baue FastAPI Image mit Prometheus Support"

Push-Location "$PSScriptRoot\..\fastapi"

try {
    Write-Host "Baue Image mit Podman..." -ForegroundColor Gray
    podman build -f Dockerfile.fastapi -t $FASTAPI_IMAGE .

    if ($LASTEXITCODE -ne 0) {
        throw "Podman Build fehlgeschlagen"
    }

    Write-Success "FastAPI Image erfolgreich gebaut"
} catch {
    Write-Error "Fehler beim Bauen des Images: $_"
    Pop-Location
    exit 1
} finally {
    Pop-Location
}

# -----------------------------------------------------------------------------
# 4. Image in Kind Cluster laden
# -----------------------------------------------------------------------------

Write-Step "Lade FastAPI Image in Kind Cluster"

Push-Location "$PSScriptRoot\..\fastapi"

try {
    Write-Host "Exportiere Image als TAR..." -ForegroundColor Gray
    podman save $FASTAPI_IMAGE -o fastapi.tar

    Write-Host "Lade Image in Kind..." -ForegroundColor Gray
    kind load image-archive fastapi.tar --name $CLUSTER_NAME

    if ($LASTEXITCODE -ne 0) {
        throw "Kind Load fehlgeschlagen"
    }

    # Cleanup
    Remove-Item fastapi.tar -Force

    Write-Success "Image erfolgreich in Cluster geladen"
} catch {
    Write-Error "Fehler beim Laden des Images: $_"
    Pop-Location
    exit 1
} finally {
    Pop-Location
}

# -----------------------------------------------------------------------------
# 5. Namespace erstellen
# -----------------------------------------------------------------------------

Write-Step "Erstelle Monitoring Namespace"

$namespaceExists = kubectl get namespace $NAMESPACE 2>$null
if ($LASTEXITCODE -ne 0) {
    kubectl create namespace $NAMESPACE
    Write-Success "Namespace '$NAMESPACE' erstellt"
} else {
    Write-Success "Namespace '$NAMESPACE' existiert bereits"
}

# -----------------------------------------------------------------------------
# 6. Helm Chart deployen/upgraden
# -----------------------------------------------------------------------------

Write-Step "Deploy Helm Chart mit Prometheus"

Push-Location "$PSScriptRoot\..\helm-charts\system-cluster"

try {
    Write-Host "Validiere Helm Chart..." -ForegroundColor Gray
    helm lint .

    if ($LASTEXITCODE -ne 0) {
        throw "Helm Lint fehlgeschlagen"
    }

    Write-Host "Upgrade/Install Helm Release..." -ForegroundColor Gray
    helm upgrade --install $HELM_RELEASE . `
        --namespace default `
        --wait `
        --timeout 600s `
        --create-namespace

    if ($LASTEXITCODE -ne 0) {
        throw "Helm Upgrade fehlgeschlagen"
    }

    Write-Success "Helm Chart erfolgreich deployed"
} catch {
    Write-Error "Fehler beim Helm Deployment: $_"
    Pop-Location
    exit 1
} finally {
    Pop-Location
}

# -----------------------------------------------------------------------------
# 7. Warte auf Pod-Readiness
# -----------------------------------------------------------------------------

Write-Step "Warte auf Pod-Readiness"

$components = @(
    @{ Namespace = "monitoring"; Label = "app=prometheus" },
    @{ Namespace = "data"; Label = "app=postgres-exporter" },
    @{ Namespace = "api"; Label = "app=fastapi" },
    @{ Namespace = "messaging"; Label = "app=kafka,component=broker" }
)

foreach ($component in $components) {
    $success = Wait-ForPods -Namespace $component.Namespace -LabelSelector $component.Label
    if (-not $success) {
        Write-Warning "Timeout bei $($component.Label) - fahre fort"
    }
}

# -----------------------------------------------------------------------------
# 8. Verifiziere Metriken-Endpoints
# -----------------------------------------------------------------------------

Write-Step "Verifiziere Metriken-Endpoints"

Write-Host "Prüfe Prometheus Health..." -ForegroundColor Gray
$prometheusPod = kubectl get pod -n monitoring -l app=prometheus -o jsonpath='{.items[0].metadata.name}' 2>$null

if ($prometheusPod) {
    $healthCheck = kubectl exec -n monitoring $prometheusPod -- wget -q -O- http://localhost:9090/-/healthy 2>$null
    if ($healthCheck -eq "Prometheus Server is Healthy.") {
        Write-Success "Prometheus ist healthy"
    } else {
        Write-Warning "Prometheus Health Check fehlgeschlagen"
    }
} else {
    Write-Warning "Prometheus Pod nicht gefunden"
}

Write-Host "`nPrüfe PostgreSQL Exporter..." -ForegroundColor Gray
$pgExporterPod = kubectl get pod -n data -l app=postgres-exporter -o jsonpath='{.items[0].metadata.name}' 2>$null

if ($pgExporterPod) {
    $pgMetrics = kubectl exec -n data $pgExporterPod -- wget -q -O- http://localhost:9187/metrics 2>$null
    if ($pgMetrics -match "pg_up") {
        Write-Success "PostgreSQL Exporter ist aktiv"
    } else {
        Write-Warning "PostgreSQL Exporter Metriken nicht verfügbar"
    }
} else {
    Write-Warning "PostgreSQL Exporter Pod nicht gefunden"
}

Write-Host "`nPrüfe FastAPI Metriken..." -ForegroundColor Gray
$fastapiPod = kubectl get pod -n api -l app=fastapi -o jsonpath='{.items[0].metadata.name}' 2>$null

if ($fastapiPod) {
    $fastapiMetrics = kubectl exec -n api $fastapiPod -- curl -s http://localhost:8000/metrics 2>$null
    if ($fastapiMetrics -match "http_requests_total") {
        Write-Success "FastAPI Metriken sind aktiv"
    } else {
        Write-Warning "FastAPI Metriken nicht verfügbar"
    }
} else {
    Write-Warning "FastAPI Pod nicht gefunden"
}

# -----------------------------------------------------------------------------
# 9. Zeige Deployment-Status
# -----------------------------------------------------------------------------

Write-Step "Deployment Status"

Write-Host "`nPrometheus:" -ForegroundColor Cyan
kubectl get pods -n monitoring -l app=prometheus

Write-Host "`nPostgreSQL Exporter:" -ForegroundColor Cyan
kubectl get pods -n data -l app=postgres-exporter

Write-Host "`nKafka Pods (mit JMX Exporter):" -ForegroundColor Cyan
kubectl get pods -n messaging -l app=kafka

Write-Host "`nFastAPI:" -ForegroundColor Cyan
kubectl get pods -n api -l app=fastapi

# -----------------------------------------------------------------------------
# 10. Port-Forward Anleitung
# -----------------------------------------------------------------------------

Write-Host "`n=============================================" -ForegroundColor Cyan
Write-Host " Deployment erfolgreich abgeschlossen" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Cyan

Write-Host "`nZugriff auf Prometheus UI:" -ForegroundColor Yellow
Write-Host "  kubectl port-forward -n monitoring svc/prometheus 9090:9090" -ForegroundColor White
Write-Host "  http://localhost:9090" -ForegroundColor White

Write-Host "`nNützliche Kommandos:" -ForegroundColor Yellow
Write-Host "  # Prometheus Logs" -ForegroundColor Gray
Write-Host "  kubectl logs -n monitoring -l app=prometheus -f" -ForegroundColor White

Write-Host "`n  # Targets prüfen" -ForegroundColor Gray
Write-Host "  kubectl exec -n monitoring <prometheus-pod> -- wget -q -O- http://localhost:9090/api/v1/targets | jq" -ForegroundColor White

Write-Host "`n  # PostgreSQL Exporter Metriken" -ForegroundColor Gray
Write-Host "  kubectl port-forward -n data svc/postgres-exporter 9187:9187" -ForegroundColor White
Write-Host "  http://localhost:9187/metrics" -ForegroundColor White

Write-Host "`n  # FastAPI Metriken" -ForegroundColor Gray
Write-Host "  kubectl port-forward -n api svc/fastapi 8000:8000" -ForegroundColor White
Write-Host "  http://localhost:8000/metrics" -ForegroundColor White

Write-Host "`nDokumentation:" -ForegroundColor Yellow
Write-Host "  monitoring\README.md" -ForegroundColor White

# -----------------------------------------------------------------------------
# 11. Optional: Port-Forward starten
# -----------------------------------------------------------------------------

$startPortForward = Read-Host "`nMöchtest du Port-Forward zu Prometheus jetzt starten? (j/n)"

if ($startPortForward -eq "j" -or $startPortForward -eq "J") {
    Write-Host "`nStarte Port-Forward..." -ForegroundColor Cyan
    Write-Host "Drücke Ctrl+C zum Beenden" -ForegroundColor Yellow
    kubectl port-forward -n monitoring svc/prometheus 9090:9090
}

Write-Host "`nDeployment abgeschlossen!" -ForegroundColor Green
