"""
Health Check Tests
Verify all components are running and healthy
"""

import pytest
import json


class TestKafkaHealth:
    """Kafka cluster health checks"""

    @pytest.mark.health
    def test_kafka_broker_0_running(self, kubectl, config):
        """Verify kafka-broker-0 pod is running"""
        success, output = kubectl(
            "get pod kafka-broker-0 -o jsonpath={.status.phase}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get pod status: {output}"
        assert output == "Running", f"kafka-broker-0 is not running: {output}"

    @pytest.mark.health
    def test_kafka_broker_1_running(self, kubectl, config):
        """Verify kafka-broker-1 pod is running"""
        success, output = kubectl(
            "get pod kafka-broker-1 -o jsonpath={.status.phase}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get pod status: {output}"
        assert output == "Running", f"kafka-broker-1 is not running: {output}"

    @pytest.mark.health
    def test_kafka_controller_0_running(self, kubectl, config):
        """Verify kafka-controller-0 pod is running"""
        success, output = kubectl(
            "get pod kafka-controller-0 -o jsonpath={.status.phase}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get pod status: {output}"
        assert output == "Running", f"kafka-controller-0 is not running: {output}"

    @pytest.mark.health
    def test_kafka_controller_1_running(self, kubectl, config):
        """Verify kafka-controller-1 pod is running"""
        success, output = kubectl(
            "get pod kafka-controller-1 -o jsonpath={.status.phase}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get pod status: {output}"
        assert output == "Running", f"kafka-controller-1 is not running: {output}"

    @pytest.mark.health
    def test_kafka_topics_accessible(self, kafka_exec):
        """Verify Kafka topics can be listed"""
        success, output = kafka_exec(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        )
        assert success, f"Failed to list topics: {output}"


class TestKafkaConnectHealth:
    """Kafka Connect health checks"""

    @pytest.mark.health
    def test_kafka_connect_running(self, kubectl, config):
        """Verify Kafka Connect deployment is running"""
        success, output = kubectl(
            "get deployment kafka-connect -o jsonpath={.status.readyReplicas}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get deployment status: {output}"
        assert output == "1", f"Kafka Connect not ready: {output}"

    @pytest.mark.health
    def test_kafka_connect_api_available(self, connect_exec):
        """Verify Kafka Connect REST API is responding"""
        success, output = connect_exec("curl -s http://localhost:8083/")
        assert success, f"Failed to reach Connect API: {output}"
        assert "version" in output, f"Invalid API response: {output}"

    @pytest.mark.health
    def test_postgresql_sink_connector_exists(self, connect_exec):
        """Verify postgresql-sink connector is registered"""
        success, output = connect_exec("curl -s http://localhost:8083/connectors")
        assert success, f"Failed to list connectors: {output}"
        assert "postgresql-sink" in output, f"Connector not found: {output}"

    @pytest.mark.health
    def test_postgresql_sink_connector_running(self, connect_exec):
        """Verify postgresql-sink connector and task are running"""
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get connector status: {output}"

        status = json.loads(output)
        assert status["connector"]["state"] == "RUNNING", \
            f"Connector not running: {status['connector']['state']}"
        assert len(status["tasks"]) > 0, "No tasks found"
        assert status["tasks"][0]["state"] == "RUNNING", \
            f"Task not running: {status['tasks'][0]['state']}"


class TestPostgreSQLHealth:
    """PostgreSQL health checks"""

    @pytest.mark.health
    def test_postgresql_running(self, kubectl, config):
        """Verify PostgreSQL pod is running"""
        success, output = kubectl(
            "get pod postgresql-0 -o jsonpath={.status.phase}",
            namespace=config.DATA_NAMESPACE
        )
        assert success, f"Failed to get pod status: {output}"
        assert output == "Running", f"PostgreSQL is not running: {output}"

    @pytest.mark.health
    def test_postgresql_accepting_connections(self, postgres_exec):
        """Verify PostgreSQL accepts connections"""
        success, output = postgres_exec(
            "psql -U postgres -d sensordata -c 'SELECT 1'"
        )
        assert success, f"Failed to connect to PostgreSQL: {output}"

    @pytest.mark.health
    def test_sensor_readings_table_exists(self, postgres_exec):
        """Verify sensor_readings table exists"""
        success, output = postgres_exec(
            "psql -U postgres -d sensordata -t -c \"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='sensor_readings'\""
        )
        assert success, f"Failed to query tables: {output}"
        assert "1" in output, f"sensor_readings table not found: {output}"
