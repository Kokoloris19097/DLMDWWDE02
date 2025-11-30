"""
Functional Pipeline Tests
End-to-end tests for Kafka to PostgreSQL pipeline
"""

import pytest
import json
import time
import subprocess


class TestKafkaPipeline:
    """End-to-end pipeline tests"""

    @pytest.mark.functional
    def test_topic_exists(self, kafka_exec, config):
        """Verify analytics-data topic exists"""
        success, output = kafka_exec(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        )
        assert success, f"Failed to list topics: {output}"
        assert config.KAFKA_TOPIC in output, f"Topic {config.KAFKA_TOPIC} not found"

    @pytest.mark.functional
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


class TestDataValidation:
    """Data validation tests"""

    @pytest.mark.functional
    def test_postgresql_table_schema(self, postgres_exec):
        """Verify sensor_readings table has correct schema"""
        success, output = postgres_exec(
            "psql -U postgres -d sensordata -c '\\d sensor_readings'"
        )
        assert success, f"Failed to describe table: {output}"

        assert "sensor_id" in output, "sensor_id column missing"
        assert "timestamp" in output, "timestamp column missing"
        assert "temperature" in output, "temperature column missing"
        assert "humidity" in output, "humidity column missing"

    @pytest.mark.functional
    def test_postgresql_indexes(self, postgres_exec):
        """Verify required indexes exist"""
        success, output = postgres_exec(
            "psql -U postgres -d sensordata -t -c \"SELECT indexname FROM pg_indexes WHERE tablename = 'sensor_readings'\""
        )
        assert success, f"Failed to query indexes: {output}"
        assert "sensor_readings_pkey" in output, "Primary key index missing"


class TestErrorHandling:
    """Error handling tests"""

    @pytest.mark.functional
    def test_connector_restart_recovery(self, connect_exec):
        """Verify connector can be restarted and recovers"""
        # Get current status
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get status: {output}"

        status = json.loads(output)
        assert status["connector"]["state"] == "RUNNING", "Connector not running initially"

        # Restart task
        success, _ = connect_exec(
            "curl -s -X POST http://localhost:8083/connectors/postgresql-sink/tasks/0/restart"
        )
        assert success, "Failed to restart task"

        # Wait and verify recovery
        time.sleep(3)

        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get status after restart: {output}"

        status = json.loads(output)
        assert status["connector"]["state"] == "RUNNING", \
            f"Connector not running after restart: {status['connector']['state']}"
        assert status["tasks"][0]["state"] == "RUNNING", \
            f"Task not running after restart: {status['tasks'][0]['state']}"

    @pytest.mark.functional
    def test_connector_pause_resume(self, connect_exec):
        """Verify connector can be paused and resumed"""
        # Pause connector
        success, _ = connect_exec(
            "curl -s -X PUT http://localhost:8083/connectors/postgresql-sink/pause"
        )
        assert success, "Failed to pause connector"

        time.sleep(2)

        # Verify paused
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get status: {output}"
        status = json.loads(output)
        assert status["connector"]["state"] == "PAUSED", "Connector not paused"

        # Resume connector
        success, _ = connect_exec(
            "curl -s -X PUT http://localhost:8083/connectors/postgresql-sink/resume"
        )
        assert success, "Failed to resume connector"

        time.sleep(3)

        # Verify running again
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get status: {output}"
        status = json.loads(output)
        assert status["connector"]["state"] == "RUNNING", "Connector not running after resume"


class TestMultipleMessages:
    """Test handling of multiple messages"""

    @pytest.mark.functional
    @pytest.mark.slow
    def test_batch_messages(self, postgres_exec, connect_exec, config):
        """Test sending multiple messages in batch"""
        import random
        from datetime import datetime, timezone

        # Verify connector is running
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get connector status: {output}"
        status = json.loads(output)
        assert status["tasks"][0]["state"] == "RUNNING", "Connector task not running"

        # Generate batch of messages
        batch_id = f"batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        num_messages = 5
        messages = []

        schema = {
            "type": "struct",
            "fields": [
                {"field": "sensor_id", "type": "string"},
                {"field": "timestamp", "type": "string"},
                {"field": "temperature", "type": "double"},
                {"field": "humidity", "type": "double"}
            ]
        }

        for i in range(num_messages):
            test_id = f"{batch_id}-{i}"
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            payload = {
                "sensor_id": test_id,
                "timestamp": timestamp,
                "temperature": round(random.uniform(18.0, 28.0), 2),
                "humidity": round(random.uniform(40.0, 70.0), 2)
            }
            messages.append(json.dumps({"schema": schema, "payload": payload}))

        # Send all messages
        all_messages = "\n".join(messages)
        cmd = [
            "kubectl", "exec", "-i", "kafka-broker-0", "-n", config.MESSAGING_NAMESPACE,
            "--", "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", "localhost:9092",
            "--topic", config.KAFKA_TOPIC
        ]

        result = subprocess.run(
            cmd,
            input=all_messages,
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0, f"Failed to send messages: {result.stderr}"

        # Wait and verify all messages arrived
        max_wait = 20
        found_count = 0

        for i in range(max_wait):
            time.sleep(1)
            success, output = postgres_exec(
                f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c \"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} WHERE sensor_id LIKE '{batch_id}%'\""
            )

            if success and output.strip().isdigit():
                found_count = int(output.strip())
                if found_count >= num_messages:
                    break

        assert found_count == num_messages, \
            f"Expected {num_messages} messages, found {found_count} after {max_wait} seconds"
