# Port Forward Script - Expose Grafana and FastAPI locally
# Macht Grafana und FastAPI über localhost verfügbar

Write-Host "Starte Port Forwarding fuer Grafana und FastAPI..." -ForegroundColor Green

# Grafana Port Forward (localhost:3000 -> Grafana Pod)
Write-Host "`nStarte Grafana Port Forward auf http://localhost:3000" -ForegroundColor Cyan
$grafanaJob = Start-Job -ScriptBlock {
    kubectl port-forward -n monitoring svc/grafana 3000:3000
} -Name "GrafanaPortForward"

# Kurze Pause zwischen den Starts
Start-Sleep -Seconds 1

# FastAPI Port Forward (localhost:8000 -> FastAPI Pod)
Write-Host "Starte FastAPI Port Forward auf http://localhost:8000" -ForegroundColor Cyan
$fastapiJob = Start-Job -ScriptBlock {
    kubectl port-forward -n api svc/fastapi 8000:8000
} -Name "FastAPIPortForward"

Write-Host "`nPort Forwarding gestartet!" -ForegroundColor Green
Write-Host "Grafana:  http://localhost:3000 (admin/admin)" -ForegroundColor White
Write-Host "FastAPI:  http://localhost:8000" -ForegroundColor White
Write-Host "API Docs: http://localhost:8000/docs" -ForegroundColor White
Write-Host "`nZum Beenden: Schliesse die Port-Forward-Fenster" -ForegroundColor Yellow
Read-Host -Prompt "Druecke Enter zum Beenden dieses Skripts und des Port-Forwards"

Stop-Job -Name "GrafanaPortForward"
Stop-Job -Name "FastAPIPortForward"
