# FastAPI - Build & Deploy Script
# DLMDWWDE02 Master Project
param (
    [switch]$noHelm
)
# Konfiguration
$IMAGE_NAME = "clickhouse-connect"
$IMAGE_TAG = "latest"
$tar_name = "clickhouse-connect-image.tar"
$LOCAL_IMAGE = "localhost/${IMAGE_NAME}:${IMAGE_TAG}"
$CLUSTER_NAME = "system-cluster"
$HELM_RELEASE = "system-cluster"
$global:starttime = Get-Date
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
    $delay = $((Get-Date) - $starttime).TotalSeconds.ToString("F2")
    Write-Host "[deploy kafka-connector $delay s] $Message" -ForegroundColor $color
}
Write-Log "INFO" "`n=== Kafka Connector Build & Deploy ==="
try {
    Push-Location $PSScriptRoot
    # 1. Prüfe ob Podman läuft
    Write-Log "INFO" "[1/6] Pruefe Podman..."
    if (-not (Get-Command podman -ErrorAction SilentlyContinue)) {
        Write-Log "DEBUG" "Podman nicht gefunden, versuche Installation..."
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install RedHat.Podman
            if ($LASTEXITCODE -ne 0) {
                Write-Log "ERROR" "Podman konnte nicht installiert werden. Bitte manuell installieren."
                exit 1
            }
            Write-Log "SUCCESS" "Podman installiert"
        } else {
            Write-Log "ERROR" "Podman konnte nicht installiert werden. Bitte manuell installieren."
            exit 1
        }
    } else {
        Write-Log "SUCCESS" "Podman vorhanden"
    }

    # 2. Prüfe ob Dateien existieren
    Write-Log "INFO" "[2/6] Pruefe Dateien..."
    $requiredFiles = @("Dockerfile.clickhouse.connect", "start.sh", "config.json", "clickhouse-kafka-connect-v1.3.4\lib\clickhouse-kafka-connect-v1.3.4-confluent.jar")
    foreach ($file in $requiredFiles) {
        if (-Not (Test-Path $file)) {
            Write-Log "ERROR" "Datei $file nicht gefunden!"
            exit 1
        }
    }
    Write-Log "SUCCESS" "Alle Dateien vorhanden"
    # 3. Podman Image bauen
    Write-Log "INFO" "[3/6] Baue Podman Image..."
    podman build -f Dockerfile.clickhouse.connect -t ${IMAGE_NAME}:${IMAGE_TAG} -t ${LOCAL_IMAGE} .
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Podman Build fehlgeschlagen!"
        exit 1
    }
    Write-Log "SUCCESS" "Image erfolgreich gebaut: ${LOCAL_IMAGE}"

    # 4. Image in Kind Cluster laden
    Write-Log "INFO" "[4/6] Lade Image in Kind Cluster..."
    # Exportiere als Tar für Kind's Podman Provider
    podman save ${LOCAL_IMAGE} -o $tar_name
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Podman Save fehlgeschlagen!"
        exit 1
    }

    # Lade in Kind Cluster
    kind load image-archive $tar_name --name ${CLUSTER_NAME}
    if ($LASTEXITCODE -ne 0) {
        Write-Log "ERROR" "Kind Load fehlgeschlagen!"
        Remove-Item $tar_name -ErrorAction SilentlyContinue
        exit 1
    }

    # Bereinige Tar-Datei
    Remove-Item $tar_name -ErrorAction SilentlyContinue
    Write-Log "SUCCESS" "Image in Cluster geladen: ${LOCAL_IMAGE}"

    if ($noHelm) {
        Write-Log "INFO" "[5/6] Überspringe Helm..."
        Write-Log "INFO" "[6/6] Überspringe status Prüfung..."
    }else {
        # 5. Helm Upgrade/Install
        Write-Log "INFO" "[5/6] Deploye mit Helm..."
        Push-Location ..\helm-charts\system-cluster


        # Prüfe ob Release existiert
        $releaseExists = helm list -q --namespace default | Select-String "^${HELM_RELEASE}$"
        if ($releaseExists) {
            Write-Log "INFO" "[5/6] Upgrade existierendes Release..."
            helm upgrade ${HELM_RELEASE} . --namespace default --wait --timeout=180s
        } else {
            Write-Log "INFO" "[5/6] Installiere neues Release..."
            helm install ${HELM_RELEASE} . --namespace default --create-namespace --wait --timeout=180s
        }

        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Helm Deployment fehlgeschlagen!"
            exit 1
        }
        Write-Log "SUCCESS" "Helm Deployment erfolgreich"

        Write-Log "INFO" "[5/6] Pod-Neustart mit neuem Image..."
        kubectl delete pod -n api -l app=fastapi --ignore-not-found=true 2>$null
        Write-Log "SUCCESS" "[5/6] Pod wird mit neuem Image neu erstellt"

        # 6. Status prüfen
        Write-Log "INFO" "[6/6] Pruefe Deployment-Status..."
        Start-Sleep -Seconds 10
        kubectl get pods -n api
        kubectl get svc -n api
        kubectl get ingress -n api 2>$null
    }
    Write-Log "SUCCESS" "`n=== Clickhouse Connector Deployment abgeschlossen ==="
}catch {
    Write-Log "ERROR" "FEHLER: $_"
    # Bereinige Tar-Datei
    if (Test-Path $tar_name) {
        Remove-Item $tar_name -ErrorAction SilentlyContinue
        Write-Log "SUCCESS" "Lokales Image $tar_name entfernt"
    }
    # Prüfe auf nicht-running fastapi-Pods und gebe deren Logs aus
    try {
        $pods = kubectl get pods -n messaging -l app=clickhouse-connector -o json | ConvertFrom-Json
        $badPods = $pods.items | Where-Object { $_.status.phase -ne 'Running' }
        foreach ($pod in $badPods) {
            $podName = $pod.metadata.name
            Write-Log "ERROR" "Pod $podName ist nicht Running (Status: $($pod.status.phase)). Logs:"
            kubectl logs $podName -n messaging | ForEach-Object { Write-Host $_ }
        }
    } catch {
        Write-Log "ERROR" "Fehler beim Auslesen der Pod-Logs: $_"
    }
    exit 1
}
finally {
    Pop-Location
}
