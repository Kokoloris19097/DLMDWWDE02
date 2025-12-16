
# Port Forward Script - Expose Grafana and FastAPI locally
# Macht Grafana und FastAPI über localhost verfügbar
$grafanaJobName = "GrafanaPortForward"
$fastapiJobName = "FastAPIPortForward"

function Test-PortFree {
    param([int]$Port)
    $tcp = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    return -not $tcp
}
function Show-PortBlocker {
    # Ports prüfen und blockierenden Prozess anzeigen
    param([int]$Port)
    $conns = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    if ($conns) {
        foreach ($conn in $conns) {
            $processID = $conn.OwningProcess
            $proc = Get-Process -Id $processID -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host ("  Port $Port belegt von PID $($proc.Id) ($($proc.ProcessName))") -ForegroundColor Red
            } else {
                Write-Host ("  Port $Port belegt von PID $processID (Prozess nicht gefunden)") -ForegroundColor Red
            }
        }
    }
}

try {
    Write-Host "Starte Port Forwarding fuer Grafana und FastAPI..." -ForegroundColor Green
    # prüfe, ob Jobs laufen
    $grafanaJob = Get-Job -Name $grafanaJobName -ErrorAction SilentlyContinue
    $fastapiJob = Get-Job -Name $fastapiJobName -ErrorAction SilentlyContinue
    if ($grafanaJob){
        Stop-Job -Name $grafanaJobName
        Remove-Job -Name $grafanaJobName
        Write-Host "Alter Grafana Port Forward beendet." -ForegroundColor Magenta
    }
    if ($fastapiJob){
        Stop-Job -Name $fastapiJobName
        Remove-Job -Name $fastapiJobName
        Write-Host "Alter FastAPI Port Forward beendet." -ForegroundColor Magenta
    }

    # prüfe, ob Ports frei sind
    if (-not (Test-PortFree 3000)) {
        Write-Host "FEHLER: Port 3000 ist bereits belegt! Bitte Prozess beenden." -ForegroundColor Red
        Show-PortBlocker 3000
        exit 1
    }
    if (-not (Test-PortFree 8000)) {
        Write-Host "FEHLER: Port 8000 ist bereits belegt! Bitte Prozess beenden." -ForegroundColor Red
        Show-PortBlocker 8000
        exit 1
    }

    # Grafana Port Forward (localhost:3000 -> Grafana Pod)
    Write-Host "`nStarte Grafana Port Forward auf http://localhost:3000 (Job: $grafanaJobName)" -ForegroundColor Cyan
    $grafanaJob = Start-Job -ScriptBlock {
        kubectl port-forward -n monitoring svc/grafana 3000:3000
    } -Name $grafanaJobName

    # Kurze Pause zwischen den Starts
    Start-Sleep -Seconds 1

    # FastAPI Port Forward (localhost:8000 -> FastAPI Pod)
    Write-Host "Starte FastAPI Port Forward auf http://localhost:8000 (Job: $fastapiJobName)" -ForegroundColor Cyan
    $fastapiJob = Start-Job -ScriptBlock {
        kubectl port-forward -n api svc/fastapi 8000:8000
    } -Name $fastapiJobName

    # Warte kurz und aktualisiere die Jobs
    Start-Sleep -Seconds 2

    if ($grafanaJob.State.GetType()-eq [System.Array]) {
        $grafanaState = $grafanaJob.State[-1]
    }elseif($grafanaJob.State.GetType()-eq [String]){
        $grafanaState = $grafanaJob.State
    }
    if ($fastapiJob.State.GetType()-eq [System.Array]) {
        $fastapiState = $fastapiJob.State[-1]
    }elseif($fastapiJob.State.GetType()-eq [String]){
        $fastapiState = $fastapiJob.State
    }

    if ($grafanaState -ne 'Running') {
        Write-Host "FEHLER: Grafana Port-Forward konnte nicht gestartet werden!" -ForegroundColor Red
        Receive-Job -Name $grafanaJob.Name | Write-Host -ForegroundColor Red
        exit 1
    }
    if ($fastapiState -ne 'Running') {
        Write-Host "FEHLER: FastAPI Port-Forward konnte nicht gestartet werden!" -ForegroundColor Red
        Receive-Job -Name $fastapiJob.Name | Write-Host -ForegroundColor Red
        exit 1
    }

    Write-Host "`nPort Forwarding gestartet!" -ForegroundColor Green
    Write-Host "Grafana:  http://localhost:3000 (admin/admin)" -ForegroundColor White
    Write-Host "FastAPI:  http://localhost:8000" -ForegroundColor White
    Write-Host "API Docs: http://localhost:8000/docs" -ForegroundColor White
    Write-Host "`nZum Beenden: Schliesse die Port-Forward-Fenster oder drücke Enter" -ForegroundColor Yellow
    Read-Host -Prompt "Druecke Enter zum Beenden dieses Skripts und des Port-Forwards"
}
finally {
    if (Get-Job -Name $grafanaJobName -ErrorAction SilentlyContinue) {
        Stop-Job -Name $grafanaJobName
        Remove-Job -Name $grafanaJobName
        Write-Host "Grafana Port Forward beendet." -ForegroundColor Green
    }
    if (Get-Job -Name $fastapiJobName -ErrorAction SilentlyContinue) {
        Stop-Job -Name $fastapiJobName
        Remove-Job -Name $fastapiJobName
        Write-Host "FastAPI Port Forward beendet." -ForegroundColor Green
    }
}
