
# Port Forward Script - Expose Grafana and FastAPI locally
# Macht Grafana und FastAPI über localhost verfügbar

function Test-PortFree {
    param([int]$Port)
    $tcp = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    return -not $tcp
}

Write-Host "Starte Port Forwarding fuer Grafana und FastAPI..." -ForegroundColor Green

# Ports prüfen
if (-not (Test-PortFree 3000)) {
    Write-Host "FEHLER: Port 3000 ist bereits belegt! Bitte Prozess beenden." -ForegroundColor Red
    exit 1
}
if (-not (Test-PortFree 8000)) {
    Write-Host "FEHLER: Port 8000 ist bereits belegt! Bitte Prozess beenden." -ForegroundColor Red
    exit 1
}

# Grafana Port Forward (localhost:3000 -> Grafana Pod)
Write-Host "`nStarte Grafana Port Forward auf http://localhost:3000" -ForegroundColor Cyan
$grafanaJob = Start-Job -ScriptBlock {
    kubectl port-forward -n monitoring svc/grafana 3000:3000 2>&1
} -Name "GrafanaPortForward"

# Kurze Pause zwischen den Starts
Start-Sleep -Seconds 1

# FastAPI Port Forward (localhost:8000 -> FastAPI Pod)
Write-Host "Starte FastAPI Port Forward auf http://localhost:8000" -ForegroundColor Cyan
$fastapiJob = Start-Job -ScriptBlock {
    kubectl port-forward -n api svc/fastapi 8000:8000 2>&1
} -Name "FastAPIPortForward"

# Warte kurz und prüfe, ob Jobs laufen
Start-Sleep -Seconds 2
$grafanaState = Get-Job -Name "GrafanaPortForward"
$fastapiState = Get-Job -Name "FastAPIPortForward"

if ($grafanaState.State -ne 'Running') {
    Write-Host "FEHLER: Grafana Port-Forward konnte nicht gestartet werden!" -ForegroundColor Red
    Receive-Job -Name "GrafanaPortForward" | Write-Host -ForegroundColor Red
    Stop-Job -Name "GrafanaPortForward" -ErrorAction SilentlyContinue
    Stop-Job -Name "FastAPIPortForward" -ErrorAction SilentlyContinue
    exit 1
}
if ($fastapiState.State -ne 'Running') {
    Write-Host "FEHLER: FastAPI Port-Forward konnte nicht gestartet werden!" -ForegroundColor Red
    Receive-Job -Name "FastAPIPortForward" | Write-Host -ForegroundColor Red
    Stop-Job -Name "GrafanaPortForward" -ErrorAction SilentlyContinue
    Stop-Job -Name "FastAPIPortForward" -ErrorAction SilentlyContinue
    exit 1
}

Write-Host "`nPort Forwarding gestartet!" -ForegroundColor Green
Write-Host "Grafana:  http://localhost:3000 (admin/admin)" -ForegroundColor White
Write-Host "FastAPI:  http://localhost:8000" -ForegroundColor White
Write-Host "API Docs: http://localhost:8000/docs" -ForegroundColor White
Write-Host "`nZum Beenden: Schliesse die Port-Forward-Fenster oder drücke Enter" -ForegroundColor Yellow
Read-Host -Prompt "Druecke Enter zum Beenden dieses Skripts und des Port-Forwards"

# Beende Jobs sauber
if (Get-Job -Name "GrafanaPortForward" -ErrorAction SilentlyContinue) {
    Stop-Job -Name "GrafanaPortForward" -Force
    Remove-Job -Name "GrafanaPortForward"
}
if (Get-Job -Name "FastAPIPortForward" -ErrorAction SilentlyContinue) {
    Stop-Job -Name "FastAPIPortForward" -Force
    Remove-Job -Name "FastAPIPortForward"
}
