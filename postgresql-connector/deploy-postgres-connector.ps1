# PostgreSQL Kafka Connector - Build & Deploy Script
param (
    [switch]$noHelm
)

$IMAGE_NAME = "postgres-kafka-connector"
$IMAGE_TAG = "latest"
$TAR_NAME = "postgres-connector-image.tar"
$CLUSTER_NAME = "system-cluster"
$global:starttime = Get-Date
$scriptRoot = $PSScriptRoot

function Write-Log {
    param ([string]$Level = "INFO", [string]$Message)
    $colors = @{ "INFO" = "Yellow"; "DEBUG" = "Cyan"; "SUCCESS" = "Green"; "ERROR" = "Red" }
    $delay = ((Get-Date) - $starttime).TotalSeconds.ToString("F2")
    Write-Host "[postgres-connector $delay s] $Message" -ForegroundColor $colors[$Level]
}

Write-Log "INFO" "=== PostgreSQL Kafka Connector Build & Deploy ==="

try {
    Set-Location $scriptRoot

    # 1. Prüfe Dateien
    Write-Log "INFO" "[1/4] Prüfe Dateien..."
    $requiredFiles = @("Dockerfile.postgres-connector", "start.sh", "connector-config.json")
    foreach ($file in $requiredFiles) {
        if (-Not (Test-Path $file)) {
            Write-Log "ERROR" "Datei $file nicht gefunden!"
            exit 1
        }
    }
    Write-Log "SUCCESS" "Alle Dateien vorhanden"

    # 2. Image bauen
    Write-Log "INFO" "[2/4] Baue Podman Image..."
    podman build -f Dockerfile.postgres-connector -t ${IMAGE_NAME}:${IMAGE_TAG} .
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Podman Build fehlgeschlagen!"
        exit 1
    }
    Write-Log "SUCCESS" "Image gebaut: ${IMAGE_NAME}:${IMAGE_TAG}"

    # 3. Image in Kind laden
    Write-Log "INFO" "[3/4] Lade Image in Kind Cluster..."
    podman save ${IMAGE_NAME}:${IMAGE_TAG} -o $TAR_NAME
    kind load image-archive $TAR_NAME --name $CLUSTER_NAME
    Remove-Item $TAR_NAME -ErrorAction SilentlyContinue
    Write-Log "SUCCESS" "Image in Cluster geladen"

    # 4. Helm Upgrade (optional)
    if (-not $noHelm) {
        Write-Log "INFO" "[4/4] Deploye mit Helm..."
        $helmChartPath = Join-Path $scriptRoot "..\helm-charts\system-cluster"
        Set-Location $helmChartPath
        helm upgrade system-cluster . --namespace default --wait --timeout=180s
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Helm Upgrade fehlgeschlagen!"
            exit 1
        }
        Write-Log "SUCCESS" "Helm Deployment erfolgreich"
    } else {
        Write-Log "INFO" "[4/4] Helm übersprungen (-noHelm)"
    }

    Write-Log "SUCCESS" "=== PostgreSQL Connector Deployment abgeschlossen ==="
}
catch {
    Write-Log "ERROR" "Fehler: $_"
    exit 1
}
finally {
    Set-Location $scriptRoot
    if (Test-Path $TAR_NAME) { Remove-Item $TAR_NAME -ErrorAction SilentlyContinue }
}
