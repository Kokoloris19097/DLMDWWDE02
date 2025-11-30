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

class TestKafkaPipeline:
    """End-to-end pipeline tests"""

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="topic_exists",
        depends=["kafka_topics_accessible"]
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
        depends=["topic_exists"]
    )
    def test_topic_configuration(self, kafka_exec, config):
        """Verify topic has correct configuration"""
        success, output = kafka_exec(
            f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic {config.KAFKA_TOPIC}"
        )
        assert success, f"Failed to describe topic: {output}"
        assert "ReplicationFactor: 2" in output or "ReplicationFactor:2" in output, \
            f"Unexpected replication factor: {output}"

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
        ]
    )
    def test_message_to_postgresql(self, kafka_exec, postgres_exec, connect_exec, test_message, config):
        """Test complete pipeline: Kafka message -> PostgreSQL"""

        # 1. Verify connector is running
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get connector status: {output}"
        status = json.loads(output)
        assert status["tasks"][0]["state"] == "RUNNING", \
            f"Connector task not running: {status['tasks'][0].get('trace', 'No trace')}"

        # 2. Send message to Kafka
        message = test_message["message"]
        test_id = test_message["id"]

        cmd = [
            "kubectl", "exec", "-i", "kafka-broker-0", "-n", config.MESSAGING_NAMESPACE,
            "--", "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", "localhost:9092",
            "--topic", config.KAFKA_TOPIC
        ]

        result = subprocess.run(
            cmd,
            input=message,
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0, f"Failed to send message: {result.stderr}"

        # 3. Wait for message to be processed
        max_wait = 15
        found = False

        for i in range(max_wait):
            time.sleep(1)
            success, output = postgres_exec(
                f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c \"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = '{test_id}'\""
            )

            if success and output.strip().isdigit() and int(output.strip()) > 0:
                found = True
                break

        assert found, f"Message not found in PostgreSQL after {max_wait} seconds"

        # 4. Verify data integrity
        success, output = postgres_exec(
            f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c \"SELECT temperature, humidity FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = '{test_id}'\""
        )
        assert success, f"Failed to query data: {output}"
        assert "22.5" in output, f"Temperature mismatch: {output}"
        assert "55" in output, f"Humidity mismatch: {output}"

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="connector_config_valid",
        depends=["kafka_connect_api_available"]
    )
    def test_connector_config_valid(self, connect_exec):
        """Verify connector configuration is correct"""
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/config"
        )
        assert success, f"Failed to get config: {output}"

        cfg = json.loads(output)

        assert cfg.get("value.converter.schemas.enable") == "true", \
            "schemas.enable should be true"
        assert cfg.get("pk.mode") == "none", \
            f"Unexpected pk.mode: {cfg.get('pk.mode')}"
        assert "TimestampConverter" in cfg.get("transforms", ""), \
            "TimestampConverter transform missing"
