"""
Functional Pipeline Tests
End-to-end tests for Kafka to PostgreSQL pipeline

Test Dependency Hierarchy (Layer 3 - Functional):
    Functional tests depend on both health and connectivity tests.
    If infrastructure is not ready, functional tests will be skipped.
"""

import pytest
import json
import time
import subprocess


# =============================================================================
# LAYER 3A: KAFKA TOPIC TESTS (Depends on Kafka Health)
# =============================================================================

class TestKafkaTopics:
    """Kafka topic configuration tests"""

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="topic_exists",
        depends=["kafka_topics_accessible"], scope="session"
    )
    def test_topic_exists(self, kafka_exec, config):
        """Verify analytics-data topic exists"""
        success, output = kafka_exec(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        )
        assert success, f"Failed to list topics: {output}"
        assert config.KAFKA_TOPIC in output, f"Topic {config.KAFKA_TOPIC} not found"

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="topic_configuration",
        depends=["topic_exists"], scope="session"
    )
    def test_topic_configuration(self, kafka_exec, config):
        """Verify topic has correct configuration"""
        success, output = kafka_exec(
            f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "
            f"--describe --topic {config.KAFKA_TOPIC}"
        )
        assert success, f"Failed to describe topic: {output}"

        # Check replication factor (handle both formats)
        has_replication = (
            "ReplicationFactor: 2" in output or
            "ReplicationFactor:2" in output
        )
        assert has_replication, f"Unexpected replication factor: {output}"


# =============================================================================
# LAYER 3B: CONNECTOR CONFIGURATION TESTS
# =============================================================================

class TestConnectorConfiguration:
    """Kafka Connect connector configuration tests"""

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="connector_config_valid",
        depends=["kafka_connect_api_available"], scope="session"
    )
    def test_connector_config_valid(self, connect_exec):
        """Verify connector configuration is correct"""
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/config"
        )
        assert success, f"Failed to get config: {output}"

        cfg = json.loads(output)

        # Validate critical configuration
        assert cfg.get("value.converter.schemas.enable") == "true", \
            "schemas.enable should be true"
        assert cfg.get("pk.mode") == "none", \
            f"Unexpected pk.mode: {cfg.get('pk.mode')}"
        assert "TimestampConverter" in cfg.get("transforms", ""), \
            "TimestampConverter transform missing"


# =============================================================================
# LAYER 3C: END-TO-END PIPELINE TESTS
# =============================================================================

class TestPipelineEndToEnd:
    """End-to-end pipeline tests"""

    @pytest.mark.functional
    @pytest.mark.slow
    @pytest.mark.dependency(
        name="message_to_postgresql",
        depends=[
            "topic_exists",
            "postgresql_sink_connector_running",
            "connect_to_kafka_broker",
            "connect_to_postgresql",
            "sensor_readings_table_exists"
        ], scope="session"
    )
    def test_message_to_postgresql(
        self, kafka_exec, postgres_exec, connect_exec, test_message, config
    ):
        """Test complete pipeline: Kafka message -> PostgreSQL"""
        # 1. Verify connector is running
        self._verify_connector_running(connect_exec)

        # 2. Send message to Kafka
        test_id = test_message["id"]
        self._send_kafka_message(test_message["message"], config)

        # 3. Wait for message to appear in PostgreSQL
        self._wait_for_message(postgres_exec, test_id, config)

        # 4. Verify data integrity
        self._verify_data_integrity(postgres_exec, test_id, config)

    def _verify_connector_running(self, connect_exec):
        """Verify the sink connector is in RUNNING state"""
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get connector status: {output}"

        status = json.loads(output)
        task_state = status["tasks"][0]["state"]
        assert task_state == "RUNNING", \
            f"Connector task not running: {status['tasks'][0].get('trace', 'No trace')}"

    def _send_kafka_message(self, message: str, config):
        """Send a message to Kafka topic"""
        cmd = [
            "kubectl", "exec", "-i", "kafka-broker-0",
            "-n", config.MESSAGING_NAMESPACE,
            "--", "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", "localhost:9092",
            "--topic", config.KAFKA_TOPIC
        ]

        result = subprocess.run(
            cmd,
            input=message,
            capture_output=True,
            text=True,
            timeout=config.COMMAND_TIMEOUT
        )
        assert result.returncode == 0, f"Failed to send message: {result.stderr}"

    def _wait_for_message(self, postgres_exec, test_id: str, config, max_wait: int = 15):
        """Wait for message to appear in PostgreSQL"""
        query = (
            f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c "
            f"\"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} "
            f"WHERE sensor_id = '{test_id}'\""
        )

        for _ in range(max_wait):
            time.sleep(1)
            success, output = postgres_exec(query)

            if success and output.strip().isdigit() and int(output.strip()) > 0:
                return

        pytest.fail(f"Message not found in PostgreSQL after {max_wait} seconds")

    def _verify_data_integrity(self, postgres_exec, test_id: str, config):
        """Verify the data was correctly stored"""
        query = (
            f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c "
            f"\"SELECT temperature, humidity FROM {config.POSTGRESQL_TABLE} "
            f"WHERE sensor_id = '{test_id}'\""
        )

        success, output = postgres_exec(query)
        assert success, f"Failed to query data: {output}"
        assert "22.5" in output, f"Temperature mismatch: {output}"
        assert "55" in output, f"Humidity mismatch: {output}"
