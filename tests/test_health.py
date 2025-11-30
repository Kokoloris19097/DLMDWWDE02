"""
Health Check Tests
Verify all components are running and healthy

Test Dependency Hierarchy (Layer 1 - Foundation):
    Pod Health Tests are the foundation for all other tests.
    If pods are not running, connectivity and functional tests will be skipped.
"""

import pytest
import json


# =============================================================================
# LAYER 1A: KAFKA BROKER & CONTROLLER HEALTH (Foundation)
# =============================================================================

class TestKafkaHealth:
    """Kafka cluster health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_broker_0_running")
    def test_kafka_broker_0_running(self, kafka_exec):
        """Verify kafka-broker-0 pod is running"""
        success, output = kafka_exec("echo OK")
        assert success, f"kafka-broker-0 is not reachable: {output}"
        assert "OK" in output, f"Unexpected output: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_broker_1_running")
    def test_kafka_broker_1_running(self, config):
        """Verify kafka-broker-1 pod is running"""
        cmd = [
            "kubectl", "-n", config.MESSAGING_NAMESPACE,
            "exec", "kafka-broker-1", "--",
            "echo", "OK"
        ]
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, f"kafka-broker-1 is not reachable: {result.stderr}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_controller_0_running")
    def test_kafka_controller_0_running(self, config):
        """Verify kafka-controller-0 pod is running"""
        cmd = [
            "kubectl", "-n", config.MESSAGING_NAMESPACE,
            "get", "pod", "kafka-controller-0",
            "-o", "jsonpath={.status.phase}"
        ]
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, f"Failed to get pod status: {result.stderr}"
        assert result.stdout.strip() == "Running", f"kafka-controller-0 is not running: {result.stdout}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_controller_1_running")
    def test_kafka_controller_1_running(self, config):
        """Verify kafka-controller-1 pod is running"""
        cmd = [
            "kubectl", "-n", config.MESSAGING_NAMESPACE,
            "get", "pod", "kafka-controller-1",
            "-o", "jsonpath={.status.phase}"
        ]
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, f"Failed to get pod status: {result.stderr}"
        assert result.stdout.strip() == "Running", f"kafka-controller-1 is not running: {result.stdout}"

    @pytest.mark.health
    @pytest.mark.dependency(
        name="kafka_topics_accessible",
        depends=["kafka_broker_0_running"]
    )
    def test_kafka_topics_accessible(self, kafka_exec):
        """Verify Kafka topics can be listed"""
        success, output = kafka_exec(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        )
        assert success, f"Failed to list topics: {output}"


# =============================================================================
# LAYER 1B: KAFKA CONNECT HEALTH (Depends on Kafka Brokers)
# =============================================================================

class TestKafkaConnectHealth:
    """Kafka Connect health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(
        name="kafka_connect_running",
        depends=["kafka_broker_0_running", "kafka_broker_1_running"]
    )
    def test_kafka_connect_running(self, config):
        """Verify Kafka Connect deployment is running"""
        cmd = [
            "kubectl", "-n", config.MESSAGING_NAMESPACE,
            "get", "deployment", "kafka-connect",
            "-o", "jsonpath={.status.readyReplicas}"
        ]
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, f"Failed to get deployment status: {result.stderr}"
        assert result.stdout.strip() == "1", f"Kafka Connect not ready: {result.stdout}"

    @pytest.mark.health
    @pytest.mark.dependency(
        name="kafka_connect_api_available",
        depends=["kafka_connect_running"]
    )
    def test_kafka_connect_api_available(self, connect_exec):
        """Verify Kafka Connect REST API is responding"""
        success, output = connect_exec("curl -s http://localhost:8083/")
        assert success, f"Failed to reach Connect API: {output}"
        assert "version" in output, f"Invalid API response: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(
        name="postgresql_sink_connector_exists",
        depends=["kafka_connect_api_available"]
    )
    def test_postgresql_sink_connector_exists(self, connect_exec):
        """Verify postgresql-sink connector is registered"""
        success, output = connect_exec("curl -s http://localhost:8083/connectors")
        assert success, f"Failed to list connectors: {output}"
        assert "postgresql-sink" in output, f"Connector not found: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(
        name="postgresql_sink_connector_running",
        depends=["postgresql_sink_connector_exists"]
    )
    def test_postgresql_sink_connector_running(self, connect_exec):
        """Verify postgresql-sink connector and task are running"""
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get connector status: {output}"

        try:
            status = json.loads(output)
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON response: {output}")

        if "error_code" in status:
            pytest.fail(f"Connector not found: {status.get('message', output)}")

        assert status["connector"]["state"] == "RUNNING", \
            f"Connector not running: {status['connector']['state']}"
        assert len(status["tasks"]) > 0, "No tasks found"
        assert status["tasks"][0]["state"] == "RUNNING", \
            f"Task not running: {status['tasks'][0]['state']}"


# =============================================================================
# LAYER 1C: POSTGRESQL HEALTH (Independent Foundation)
# =============================================================================

class TestPostgreSQLHealth:
    """PostgreSQL health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(name="postgresql_running")
    def test_postgresql_running(self, config):
        """Verify PostgreSQL pod is running"""
        cmd = [
            "kubectl", "-n", config.DATA_NAMESPACE,
            "get", "pod", "postgresql-0",
            "-o", "jsonpath={.status.phase}"
        ]
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, f"Failed to get pod status: {result.stderr}"
        assert result.stdout.strip() == "Running", f"PostgreSQL is not running: {result.stdout}"

    @pytest.mark.health
    @pytest.mark.dependency(
        name="postgresql_accepting_connections",
        depends=["postgresql_running"]
    )
    def test_postgresql_accepting_connections(self, postgres_exec):
        """Verify PostgreSQL accepts connections"""
        success, output = postgres_exec("psql -U postgres -d sensordata -c 'SELECT 1'")
        assert success, f"Failed to connect to PostgreSQL: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(
        name="sensor_readings_table_exists",
        depends=["postgresql_accepting_connections"]
    )
    def test_sensor_readings_table_exists(self, postgres_exec):
        """Verify sensor_readings table exists"""
        success, output = postgres_exec(
            "psql -U postgres -d sensordata -t -c \"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='sensor_readings'\""
        )
        assert success, f"Failed to query tables: {output}"
        assert "1" in output, f"sensor_readings table not found: {output}"
