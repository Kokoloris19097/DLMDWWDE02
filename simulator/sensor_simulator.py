"""
Sensor Data Simulator
Simuliert 5 verschiedene Sensoren und sendet kontinuierlich Daten an die FastAPI
"""
import time
import random
import requests
import json
from datetime import datetime
from typing import Dict, List
import logging
import sys

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class SensorSimulator:
    """Simuliert einen einzelnen Sensor mit realistischen Werten"""

    def __init__(self, sensor_id: str, sensor_type: str, base_temp: float, base_humidity: float):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.base_temp = base_temp
        self.base_humidity = base_humidity
        self.temp_drift = 0.0
        self.humidity_drift = 0.0

    def generate_reading(self) -> Dict:
        """Generiert einen realistischen Sensorwert mit natürlicher Variation"""
        # Kleine zufällige Änderungen für realistische Schwankungen
        self.temp_drift += random.uniform(-0.3, 0.3)
        self.temp_drift = max(-2.0, min(2.0, self.temp_drift))  # Drift begrenzen

        self.humidity_drift += random.uniform(-1.5, 1.5)
        self.humidity_drift = max(-8.0, min(8.0, self.humidity_drift))  # Drift begrenzen

        # Aktuelle Werte berechnen
        temperature = round(self.base_temp + self.temp_drift + random.uniform(-0.2, 0.2), 2)
        humidity = round(self.base_humidity + self.humidity_drift + random.uniform(-1.0, 1.0), 2)

        # Werte in realistischen Bereichen halten
        temperature = max(-10.0, min(40.0, temperature))
        humidity = max(30.0, min(99.0, humidity))

        return {
            "sensor_id": self.sensor_id,
            "temperature": temperature,
            "humidity": humidity,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }


class DataSimulator:
    """Hauptklasse für die Simulation mehrerer Sensoren"""

    def __init__(self, fastapi_url: str = "http://localhost:8000", interval: float = 2.0, num_sensors: int = 5):
        self.fastapi_url = fastapi_url.rstrip('/')
        self.interval = interval
        self.num_sensors = num_sensors
        self.sensors: List[SensorSimulator] = []
        self.running = False
        self.stats = {
            "total_sent": 0,
            "total_failed": 0,
            "start_time": None
        }

    def initialize_sensors(self):
        """Initialisiert dynamisch die gewünschte Anzahl an Sensoren"""
        # Basis-Konfigurationen für verschiedene Sensor-Typen
        sensor_types = [
            {"type": "Datacenter Rack", "base_temp": 22.0, "base_humidity": 45.0},
            {"type": "Server Room", "base_temp": 21.0, "base_humidity": 42.0},
            {"type": "Cooling Unit", "base_temp": 19.5, "base_humidity": 55.0},
            {"type": "Storage Area", "base_temp": 24.0, "base_humidity": 48.0},
            {"type": "Network Equipment", "base_temp": 23.0, "base_humidity": 47.0}
        ]

        for i in range(self.num_sensors):
            # Wähle Sensor-Typ rotierend aus
            sensor_type_config = sensor_types[i % len(sensor_types)]

            # Füge kleine Variationen hinzu für unterschiedliche Sensoren des gleichen Typs
            temp_variation = random.uniform(-1.0, 1.0)
            humidity_variation = random.uniform(-3.0, 3.0)

            sensor = SensorSimulator(
                sensor_id=f"S{i+1}",
                sensor_type=sensor_type_config["type"],
                base_temp=sensor_type_config["base_temp"] + temp_variation,
                base_humidity=sensor_type_config["base_humidity"] + humidity_variation
            )
            self.sensors.append(sensor)
            logger.info(f"Sensor initialisiert: S{i+1} ({sensor_type_config['type']})")

    def send_reading(self, reading: Dict) -> bool:
        """Sendet einen Messwert an die FastAPI"""
        try:
            response = requests.post(
                f"{self.fastapi_url}/ingest",
                json=reading,
                timeout=5
            )

            if response.status_code in [200, 201]:
                self.stats["total_sent"] += 1
                return True
            else:
                logger.warning(
                    f"API antwortete mit Status {response.status_code} für {reading['sensor_id']}: {response.text}"
                )
                self.stats["total_failed"] += 1
                return False

        except requests.exceptions.ConnectionError:
            logger.error(f"Verbindung zu {self.fastapi_url} fehlgeschlagen")
            self.stats["total_failed"] += 1
            return False
        except requests.exceptions.Timeout:
            logger.error(f"Timeout bei Anfrage an {self.fastapi_url}")
            self.stats["total_failed"] += 1
            return False
        except Exception as e:
            logger.error(f"Fehler beim Senden: {e}")
            self.stats["total_failed"] += 1
            return False

    def check_fastapi_health(self) -> bool:
        """Prüft ob die FastAPI erreichbar ist"""
        try:
            response = requests.get(f"{self.fastapi_url}/health", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"FastAPI Health Check fehlgeschlagen: {e}")
            return False

    def print_statistics(self):
        """Gibt Statistiken aus"""
        if self.stats["start_time"]:
            runtime = datetime.now() - self.stats["start_time"]
            runtime_seconds = runtime.total_seconds()
            rate = self.stats["total_sent"] / runtime_seconds if runtime_seconds > 0 else 0

            # Cyan ANSI Color Code: \033[96m, Reset: \033[0m
            print("\033[96m" + "=" * 60)
            print(f"Statistiken:")
            print(f"  Laufzeit: {runtime}")
            print(f"  Erfolgreich gesendet: {self.stats['total_sent']}")
            print(f"  Fehlgeschlagen: {self.stats['total_failed']}")
            print(f"  Rate: {rate:.2f} Nachrichten/Sekunde")
            print("=" * 60 + "\033[0m")

    def run(self):
        """Hauptschleife für die kontinuierliche Simulation"""
        logger.info("=" * 60)
        logger.info("Sensor Simulator gestartet")
        logger.info(f"FastAPI URL: {self.fastapi_url}")
        logger.info(f"Intervall: {self.interval}s")
        logger.info(f"Anzahl Sensoren: {len(self.sensors)}")
        logger.info("=" * 60)

        # Warte auf FastAPI
        logger.info("Warte auf FastAPI Verfügbarkeit...")
        max_retries = 30
        for attempt in range(max_retries):
            if self.check_fastapi_health():
                logger.info("FastAPI ist bereit!")
                break
            if attempt < max_retries - 1:
                logger.info(f"Versuch {attempt + 1}/{max_retries} - warte 2 Sekunden...")
                time.sleep(2)
        else:
            logger.error(f"FastAPI nicht erreichbar nach {max_retries} Versuchen")
            logger.error(f"Stelle sicher, dass FastAPI unter {self.fastapi_url} läuft")
            sys.exit(1)

        self.running = True
        self.stats["start_time"] = datetime.now()

        try:
            iteration = 0
            while self.running:
                iteration += 1
                logger.info(f"\n--- Iteration {iteration} ---")

                # Generiere und sende Daten für jeden Sensor
                for sensor in self.sensors:
                    reading = sensor.generate_reading()
                    success = self.send_reading(reading)

                    status = "OK" if success else "FEHLER"
                    logger.info(
                        f"{status} | {sensor.sensor_id}: "
                        f"Temp={reading['temperature']:.1f}°C, "
                        f"Hum={reading['humidity']:.1f}%"
                    )

                # Statistiken alle 10 Iterationen
                if iteration % 10 == 0:
                    self.print_statistics()

                # Warte bis zum nächsten Intervall
                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("\n\nSimulator wird beendet...")
            self.running = False
            self.print_statistics()
            logger.info("Simulator gestoppt")
        except Exception as e:
            logger.error(f"Unerwarteter Fehler: {e}")
            self.running = False
            raise


def main():
    """Hauptfunktion"""
    import argparse

    parser = argparse.ArgumentParser(description="Sensor Data Simulator")
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="FastAPI URL (Standard: http://localhost:8000)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Intervall zwischen Messungen in Sekunden (Standard: 2.0)"
    )
    parser.add_argument(
        "--sensors",
        type=int,
        default=5,
        help="Anzahl der zu simulierenden Sensoren (Standard: 5)"
    )

    args = parser.parse_args()

    # Simulator initialisieren
    simulator = DataSimulator(
        fastapi_url=args.url,
        interval=args.interval,
        num_sensors=args.sensors
    )

    # Sensoren initialisieren
    simulator.initialize_sensors()

    # Simulation starten
    simulator.run()


if __name__ == "__main__":
    main()
