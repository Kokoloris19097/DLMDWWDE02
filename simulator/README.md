# Sensor Data Simulator

Python-Skript zur Simulation von 5 verschiedenen Sensoren, die kontinuierlich Temperatur- und Feuchtigkeitsdaten an die FastAPI senden.

## Features

- **n verschiedene Sensoren** mit realistischen Charakteristiken:
    - Unterschiedliche Basiswerte

- **Realistische Datengenerierung**:
  - Natürliche Werteschwankungen mit Drift
  - Temperaturbereich: -10-40°C
  - Luftfeuchtigkeitsbereich: 30-99%
  - Zeitstempel in Millisekunden (Unix-Epoch)

- **Robuste Fehlerbehandlung**:
  - Automatische Wiederholungsversuche
  - Health Check vor Start
  - Timeout-Handling
  - Verbindungsfehler-Behandlung

- **Monitoring & Statistiken**:
  - Logging aller gesendeten Werte
  - Erfolgs-/Fehlerstatistiken
  - Durchsatzberechnung

## Installation

```powershell
# Dependencies installieren
cd simulator
pip install -r requirements.txt
```

## Verwendung

### Standard
```powershell
python sensor_simulator.py
```

### Mit benutzerdefinierten Parametern
```powershell
python sensor_simulator.py --url <NEW URL[str]> --interval <NEW INTERVAL[double]> --sensors <NUMBER OF SENSORS[int]>
```

## Parameter

| Parameter | Standard | Beschreibung |
|-----------|----------|--------------|
| `--url` | `http://localhost:8000` | FastAPI Base-URL |
| `--interval` | `2.0` | Intervall zwischen Messungen in Sekunden |
| `--sensors` | 5 | Anzahl Sensoren die Simuliert werden sollen |

## Beenden

Mit `Ctrl+C` wird der Simulator sauber beendet und zeigt finale Statistiken an:

## Troubleshooting

**"FastAPI nicht erreichbar"**:
- Prüfe ob FastAPI läuft: `kubectl get pods -n api`
- Prüfe Port-Forwarding: `kubectl get svc -n api`
- Verwende die korrekte URL mit Port-Forwarding Port

**"Connection refused"**:
- Stelle sicher, dass `port-forward.ps1` läuft
- Prüfe ob der Port nicht bereits belegt ist

**Zu viele Fehler**:
- Erhöhe das Intervall: `--interval 5.0`
- Prüfe Kafka-Verfügbarkeit: `kubectl get pods -n messaging`
