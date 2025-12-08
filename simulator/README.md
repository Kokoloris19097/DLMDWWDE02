# Sensor Data Simulator

Python-Skript zur Simulation von 5 verschiedenen Sensoren, die kontinuierlich Temperatur- und Feuchtigkeitsdaten an die FastAPI senden.

## Features

- **5 verschiedene Sensoren** mit realistischen Charakteristiken:
  - `sensor-datacenter-rack-01` (Datacenter Rack)
  - `sensor-datacenter-rack-02` (Datacenter Rack)
  - `sensor-server-room-01` (Server Room)
  - `sensor-cooling-unit-01` (Cooling Unit)
  - `sensor-storage-area-01` (Storage Area)

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

### Standard (FastAPI unter localhost:8000)
```powershell
python sensor_simulator.py
```

### Mit benutzerdefinierten Parametern
```powershell
# Andere URL
python sensor_simulator.py --url http://localhost:30080

# Anderes Intervall (z.B. alle 5 Sekunden)
python sensor_simulator.py --interval 5.0

# Beides kombinieren
python sensor_simulator.py --url http://localhost:30080 --interval 1.0
```

### Mit Port-Forwarding
Wenn du das `port-forward.ps1` Skript verwendest:
```powershell
python sensor_simulator.py --url http://localhost:30080
```

## Parameter

| Parameter | Standard | Beschreibung |
|-----------|----------|--------------|
| `--url` | `http://localhost:8000` | FastAPI Base-URL |
| `--interval` | `2.0` | Intervall zwischen Messungen in Sekunden |

## Beenden

Mit `Ctrl+C` wird der Simulator sauber beendet und zeigt finale Statistiken an:

```
============================================================
Statistiken:
  Laufzeit: 0:05:30
  Erfolgreich gesendet: 825
  Fehlgeschlagen: 0
  Rate: 2.50 Nachrichten/Sekunde
============================================================
```

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
