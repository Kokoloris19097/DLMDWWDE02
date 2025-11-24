# Apache Spark - Build & Deploy Script
param (
    [switch]$noHelm
)

Write-Host "`n=== Spark Build & Deploy ===" -ForegroundColor Cyan

# Konfiguration
$IMAGE_NAME = "spark-app"
$IMAGE_TAG = "latest"
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
    Write-Host "[deploy spark $delay s] $Message" -ForegroundColor $color
}
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
	$requiredFiles = @("Dockerfile.spark", "main.py", "requirements.txt")
	foreach ($file in $requiredFiles) {
		if (-Not (Test-Path $file)) {
			Write-Log "ERROR" "Datei $file nicht gefunden!"
			exit 1
		}
	}
	Write-Log "SUCCESS" "Alle Dateien vorhanden"
	# 3. Podman Image bauen
	Write-Log "INFO" "[3/6] Baue Podman Image..."
	podman build -f Dockerfile.spark -t ${IMAGE_NAME}:${IMAGE_TAG} -t ${LOCAL_IMAGE} .
	if ($LASTEXITCODE -ne 0) {
		Write-Log "ERROR" "Podman Build fehlgeschlagen!"
		exit 1
	}
	Write-Log "SUCCESS" "Image erfolgreich gebaut: ${LOCAL_IMAGE}"

	# 4. Image in Kind Cluster laden
	Write-Log "INFO" "[4/6] Lade Image in Kind Cluster..."
	# Exportiere als Tar für Kind's Podman Provider
	podman save ${LOCAL_IMAGE} -o spark-image.tar
	if ($LASTEXITCODE -ne 0) {
		Write-Log "ERROR" "Podman Save fehlgeschlagen!"
		exit 1;
	}

	# Lade in Kind Cluster
	kind load image-archive spark-image.tar --name ${CLUSTER_NAME}
	if ($LASTEXITCODE -ne 0) {
		Write-Log "ERROR" "Kind Load fehlgeschlagen!"
		Remove-Item spark-image.tar -ErrorAction SilentlyContinue
		exit 1
	}

	# Bereinige Tar-Datei
	Remove-Item spark-image.tar -ErrorAction SilentlyContinue
	Write-Log "SUCCESS" "Image in Cluster geladen: ${LOCAL_IMAGE}"

	if (-not $noHelm) {
        Write-Log "INFO" "[5/6] Deploye mit Helm..."
        Push-Location ..\helm-charts\system-cluster

        # Prüfe ob Release existiert
        $releaseExists = helm list -q --namespace default | Select-String "^${HELM_RELEASE}$"
        if ($releaseExists) {
            Write-Log "INFO" "  Upgrade existierendes Release..."
            helm upgrade ${HELM_RELEASE} . --namespace default --wait --timeout=180s
        } else {
            Write-Log "INFO" "  Installiere neues Release..."
            helm install ${HELM_RELEASE} . --namespace default --create-namespace --wait --timeout=180s
        }

        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR" "Helm Deployment fehlgeschlagen!"
            exit 1
        }
        Write-Log "SUCCESS" "Helm Deployment erfolgreich"

        Write-Log "INFO" "  Pod-Neustart mit neuem Image..."
        kubectl delete pod -n api -l app=spark --ignore-not-found=true 2>$null
        Write-Log "INFO" "  Pod wird mit neuem Image neu erstellt"

        # 6. Status prüfen
        Write-Log "INFO" "[6/6] Pruefe Deployment-Status..."
        Start-Sleep -Seconds 10
        kubectl get pods -n api
        kubectl get svc -n api
        kubectl get ingress -n api 2>$null
    } else {
        Write-Log "INFO" "[5/6] Überspringe Helm..."
        Write-Log "INFO" "[6/6] Überspringe status Prüfung..."
    }
    Write-Log "SUCCESS" "=== Deployment abgeschlossen ==="
    Write-Log "INFO" "Naechste Schritte:"
    Write-Log "INFO" "  1. Logs pruefen: kubectl logs -n api -l app=spark --tail=50"
    Write-Log "INFO" "  2. Pod-Status: kubectl get pods -n api"
    Write-Log "INFO" "  3. Health Check: (falls implementiert)"
}catch {
	Write-Log "ERROR" "FEHLER: $_"
	# Bereinige Tar-Datei
	if (Test-Path spark-image.tar) {
		Remove-Item spark-image.tar -ErrorAction SilentlyContinue
		Write-Log "SUCCESS" "Lokales Image spark-image.tar entfernt"
	}
	exit 1
}
finally {
	Pop-Location
}
