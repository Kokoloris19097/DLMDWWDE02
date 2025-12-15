# Zeigt die letzten 50 Zeilen der Logs aller nicht laufenden Pods (Status != Running oder Container nicht ready)


$pods = (kubectl get pods -A -o json | ConvertFrom-Json).items
$nonRunning = @()
foreach ($pod in $pods) {
    if ($pod.status -and $pod.status.containerStatuses) {
        foreach ($container in $pod.status.containerStatuses) {
            $ready = $container.ready
            $stateName = $container.state.PSObject.Properties.Name
            if (-not $ready -or $stateName -ne 'running') {
                $nonRunning += [PSCustomObject]@{
                    Namespace = $pod.metadata.namespace
                    Name      = $pod.metadata.name
                    Container = $container.name
                    Phase     = $pod.status.phase
                    Status    = $stateName
                    Reason    = if ($container.state.$stateName -and $container.state.$stateName.reason) {
                        $container.state.$stateName.reason
                    } elseif ($container.state.$stateName -and $container.state.$stateName.message) {
                        $container.state.$stateName.message
                    } else {
                        $stateName
                    }
                }
            }
        }
    }
}

if ($nonRunning.Count -eq 0) {
    Write-Host "Alle Pods/Container sind im Status 'Running'." -ForegroundColor Green
    exit 0
}

$nonRunning | Format-Table -AutoSize

$antwort = Read-Host "`nLogs der nicht laufenden Pods anzeigen? (j/n)"
if ($antwort -ne "j") {
    Write-Host "Abgebrochen."
    exit 0
}

foreach ($entry in $nonRunning) {
    Write-Host "\n=== $($entry.Namespace)/$($entry.Name) ===" -ForegroundColor Yellow
    Write-Host "--- Container: $($entry.Container) ---" -ForegroundColor Cyan
    kubectl logs $entry.Name -n $entry.Namespace -c $entry.Container --tail=50
}
