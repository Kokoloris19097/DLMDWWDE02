"""
Functional Pipeline Tests
End-to-end tests for complete data pipeline

Test Dependency Hierarchy (Layer 3 - Functional):
    Functional tests depend on both health and connectivity tests.
    Tests are ordered from basic functionality to complex end-to-end flows:
    1. Kafka Topic Tests
    2. Connector Configuration Tests
    3. Kafka → PostgreSQL Pipeline Tests
    4. FastAPI Query Endpoint Tests
    5. Complete End-to-End Workflow Tests
"""

import pytest
import json
import time
import subprocess
from datetime import datetime, timedelta


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


# =============================================================================
# LAYER 3D: FASTAPI QUERY ENDPOINT TESTS (Depends on FastAPI Health & Data)
# =============================================================================

class TestFastAPIQueryEndpoints:
    """Test FastAPI query endpoints for data retrieval"""

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="list_sensors",
        depends=["fastapi_ready_endpoint", "sensor_readings_table_exists"],
        scope="session"
    )
    def test_list_sensors(self, fastapi_exec):
        """Verify /sensors returns list of sensors"""
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to call /sensors: {output}"

        try:
            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"

            # If data exists, validate structure
            if len(data) > 0:
                sensor = data[0]
                assert "sensor_id" in sensor, "Missing sensor_id field"
                assert "first_reading" in sensor, "Missing first_reading field"
                assert "latest_reading" in sensor, "Missing latest_reading field"
                assert "reading_count" in sensor, "Missing reading_count field"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["fastapi_ready_endpoint"])
    def test_list_sensors_with_limit(self, fastapi_exec):
        """Verify /sensors respects limit parameter"""
        success, output = fastapi_exec('curl -s "http://localhost:8000/sensors?limit=5"')
        assert success, f"Failed to call /sensors with limit: {output}"

        try:
            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"
            assert len(data) <= 5, f"Expected max 5 results, got {len(data)}"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="get_sensor_data",
        depends=["list_sensors"],
        scope="session"
    )
    def test_get_sensor_data(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data returns time-series data"""
        # First get a sensor_id
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Get data for this sensor
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/data"'
            )
            assert success, f"Failed to get sensor data: {output}"

            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"

            # Validate structure if data exists
            if len(data) > 0:
                reading = data[0]
                assert "sensor_id" in reading, "Missing sensor_id field"
                assert "timestamp" in reading, "Missing timestamp field"
                assert "temperature" in reading, "Missing temperature field"
                assert "humidity" in reading, "Missing humidity field"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["list_sensors"])
    def test_get_sensor_data_with_time_range(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data respects time range"""
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Query with time range (last 24 hours)
            end = datetime.utcnow()
            start = end - timedelta(days=1)
            start_iso = start.isoformat()
            end_iso = end.isoformat()

            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/data?start={start_iso}&end={end_iso}"'
            )
            assert success, f"Failed to get sensor data with time range: {output}"

            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["list_sensors"])
    def test_get_sensor_data_with_limit(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data respects limit parameter"""
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/data?limit=10"'
            )
            assert success, f"Failed to get sensor data with limit: {output}"

            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"
            assert len(data) <= 10, f"Expected max 10 results, got {len(data)}"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["fastapi_ready_endpoint"])
    def test_get_sensor_data_nonexistent_sensor(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data returns empty list for nonexistent sensor"""
        success, output = fastapi_exec(
            'curl -s "http://localhost:8000/sensors/NONEXISTENT-999/data"'
        )
        assert success, f"Failed to query nonexistent sensor: {output}"

        try:
            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"
            assert len(data) == 0, f"Expected empty list, got {len(data)} items"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(
        name="get_sensor_stats",
        depends=["list_sensors"],
        scope="session"
    )
    def test_get_sensor_stats(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/stats returns aggregated statistics"""
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Get stats
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/stats"'
            )
            assert success, f"Failed to get sensor stats: {output}"

            data = json.loads(output)
            assert isinstance(data, dict), f"Expected dict, got {type(data)}"

            # Validate required fields
            required_fields = [
                "sensor_id", "period_start", "period_end",
                "temperature_min", "temperature_max", "temperature_avg",
                "humidity_min", "humidity_max", "humidity_avg",
                "reading_count"
            ]
            for field in required_fields:
                assert field in data, f"Missing field: {field}"

            # Validate ranges
            assert data["temperature_min"] <= data["temperature_max"]
            assert data["humidity_min"] <= data["humidity_max"]
            assert data["temperature_min"] <= data["temperature_avg"] <= data["temperature_max"]
            assert data["humidity_min"] <= data["humidity_avg"] <= data["humidity_max"]
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["list_sensors"])
    def test_get_sensor_stats_with_time_range(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/stats respects time range"""
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Query with time range (last 24 hours)
            end = datetime.utcnow()
            start = end - timedelta(days=1)
            start_iso = start.isoformat()
            end_iso = end.isoformat()

            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/stats?start={start_iso}&end={end_iso}"'
            )
            assert success, f"Failed to get sensor stats with time range: {output}"

            data = json.loads(output)
            assert isinstance(data, dict), f"Expected dict, got {type(data)}"
            assert "period_start" in data and "period_end" in data
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["fastapi_ready_endpoint"])
    def test_get_sensor_stats_nonexistent_sensor(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/stats returns 404 for nonexistent sensor"""
        success, output = fastapi_exec(
            'curl -s -w "\\n%{http_code}" "http://localhost:8000/sensors/NONEXISTENT-999/stats"'
        )
        assert success, f"Failed to query nonexistent sensor: {output}"

        lines = output.strip().split('\n')
        http_code = lines[-1] if lines else ""
        assert "404" in http_code, f"Expected 404, got {http_code}"


# =============================================================================
# LAYER 3E: COMPLETE END-TO-END WORKFLOW
# =============================================================================

class TestCompleteEndToEndWorkflow:
    """Test complete data pipeline from Kafka to FastAPI queries"""

    @pytest.mark.functional
    @pytest.mark.slow
    @pytest.mark.dependency(
        depends=[
            "message_to_postgresql",
            "list_sensors",
            "get_sensor_data",
            "get_sensor_stats"
        ]
    )
    def test_complete_workflow(self, fastapi_exec):
        """
        Complete workflow test:
        1. List all sensors (verify data exists)
        2. Get data for first sensor
        3. Get stats for first sensor
        4. Verify data consistency between endpoints
        """
        # Step 1: List sensors
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to list sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for end-to-end test")

            sensor_id = sensors[0]["sensor_id"]
            list_reading_count = sensors[0]["reading_count"]

            # Step 2: Get data
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/data?limit=1000"'
            )
            assert success, f"Failed to get sensor data: {output}"

            data = json.loads(output)
            assert isinstance(data, list), "Data should be a list"
            data_count = len(data)

            # Step 3: Get stats
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/stats"'
            )
            assert success, f"Failed to get sensor stats: {output}"

            stats = json.loads(output)
            stats_count = stats["reading_count"]

            # Step 4: Verify consistency
            assert stats_count >= data_count, \
                f"Stats count ({stats_count}) should be >= data count ({data_count})"

            # Verify all data points are within stats ranges
            if len(data) > 0:
                for reading in data:
                    temp = reading["temperature"]
                    humidity = reading["humidity"]

                    assert stats["temperature_min"] <= temp <= stats["temperature_max"], \
                        f"Temperature {temp} outside range [{stats['temperature_min']}, {stats['temperature_max']}]"
                    assert stats["humidity_min"] <= humidity <= stats["humidity_max"], \
                        f"Humidity {humidity} outside range [{stats['humidity_min']}, {stats['humidity_max']}]"

        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")
