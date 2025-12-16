# Kafka Connect PostgreSQL Connector zurücksetzen
# Löscht und registriert den Connector neu

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$msg, [string]$color = "Yellow")
    Write-Host $msg -ForegroundColor $color
}

Write-Log "[1/4] Suche Kafka Connect Pod..."
$connectPod = (kubectl get pods -n messaging -l app=kafka-connect -o jsonpath='{.items[0].metadata.name}')
if (-not $connectPod) {
    Write-Log "Kafka Connect Pod nicht gefunden!" "Red"
    exit 1
}
Write-Log "Kafka Connect Pod: $connectPod" "Cyan"

Write-Log "[2/4] Lösche bestehenden Connector (postgresql-sink)..."
$resp = kubectl exec -n messaging $connectPod -- curl -s -X DELETE http://localhost:8083/connectors/postgresql-sink
Write-Log "Antwort von Kafka Connect: $resp" "Gray"
Start-Sleep -Seconds 2

Write-Log "[3/4] Registriere Connector neu..."
$cfgPath = Join-Path $PSScriptRoot "..\\postgresql-connector\\connector-config.json"
if (-not (Test-Path $cfgPath)) {
    Write-Log "Connector-Konfigurationsdatei nicht gefunden: $cfgPath" "Red"
    exit 1
}

# PowerShell: Pipe den Inhalt direkt an kubectl exec -i ...
$cfgJson = Get-Content $cfgPath -Raw
$resp = $cfgJson | kubectl exec -i -n messaging $connectPod -- sh -c "cat | curl -s -X POST -H 'Content-Type: application/json' --data-binary @- http://localhost:8083/connectors"
Write-Log "Antwort von Kafka Connect: $resp" "Gray"

Write-Log "[4/4] Connector-Reset abgeschlossen." "Green"
