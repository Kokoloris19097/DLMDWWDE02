"""
Pytest Configuration and Fixtures
Test Suite for Kafka-PostgreSQL Pipeline
"""

import pytest
import subprocess
import json
from datetime import datetime, timezone


class Config:
    """Test configuration"""
    MESSAGING_NAMESPACE = "messaging"
    DATA_NAMESPACE = "data"
    KAFKA_BOOTSTRAP = "kafka-broker-0.kafka-broker.messaging.svc.cluster.local:9092"
    KAFKA_TOPIC = "analytics-data"
    POSTGRESQL_HOST = "postgresql.data.svc.cluster.local"
    POSTGRESQL_PORT = 5432
    POSTGRESQL_DB = "sensordata"
    POSTGRESQL_TABLE = "sensor_readings"
    POSTGRESQL_USER = "appuser"
    POSTGRESQL_PASSWORD = "appuser-secure-pw"
    TIMEOUT_SHORT = 10
    TIMEOUT_MEDIUM = 30
    TIMEOUT_LONG = 60


@pytest.fixture(scope="session")
def config():
    """Provide test configuration"""
    return Config()


def run_command(cmd: list, timeout: int = 30) -> tuple:
    """Execute a command and return (success, output)"""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        output = result.stdout.strip() or result.stderr.strip()
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


@pytest.fixture(scope="session")
def kubectl(config):
    """Provide kubectl command executor"""
    def run(command: str, namespace: str = None, timeout: int = 30) -> tuple:
        cmd = ["kubectl"]
        if namespace:
            cmd.extend(["-n", namespace])

        # Baue Befehl als Liste, um Probleme mit Quotes zu vermeiden
        parts = command.split()
        cmd.extend(parts)

        return run_command(cmd, timeout)

    return run


@pytest.fixture(scope="session")
def kafka_exec(config):
    """Execute commands in Kafka broker pod"""
    def run(command: str, timeout: int = 30) -> tuple:
        cmd = [
            "kubectl", "-n", config.MESSAGING_NAMESPACE,
            "exec", "kafka-broker-0", "--",
            "/bin/bash", "-c", command
        ]
        return run_command(cmd, timeout)

    return run


@pytest.fixture(scope="session")
def connect_exec(config):
    """Execute commands in Kafka Connect pod"""
    def run(command: str, timeout: int = 30) -> tuple:
        cmd = [
            "kubectl", "-n", config.MESSAGING_NAMESPACE,
            "exec", "deployment/kafka-connect", "--",
            "/bin/bash", "-c", command
        ]
        return run_command(cmd, timeout)

    return run


@pytest.fixture(scope="session")
def postgres_exec(config):
    """Execute commands in PostgreSQL pod"""
    def run(command: str, timeout: int = 30) -> tuple:
        cmd = [
            "kubectl", "-n", config.DATA_NAMESPACE,
            "exec", "postgresql-0", "--",
            "/bin/bash", "-c", command
        ]
        return run_command(cmd, timeout)

    return run


@pytest.fixture
def test_message():
    """Generate a test message with schema"""
    test_id = f"pytest-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    schema = {
        "type": "struct",
        "fields": [
            {"field": "sensor_id", "type": "string"},
            {"field": "timestamp", "type": "string"},
            {"field": "temperature", "type": "double"},
            {"field": "humidity", "type": "double"}
        ]
    }

    payload = {
        "sensor_id": test_id,
        "timestamp": timestamp,
        "temperature": 22.5,
        "humidity": 55.0
    }

    return {
        "id": test_id,
        "timestamp": timestamp,
        "message": json.dumps({"schema": schema, "payload": payload})
    }
