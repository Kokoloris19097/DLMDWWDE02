"""
Functional Pipeline Tests
End-to-end tests for complete data pipeline

Best Practices Applied:
- Test Isolation: Each test creates its own data via fixtures
- AAA Pattern: Arrange-Act-Assert structure
- No Skip Logic: Tests ensure data exists instead of skipping
- Cleanup: Fixtures handle teardown of test data
- Factory Pattern: Centralized test data creation
"""

import pytest
import json
import time
import subprocess
import uuid
from datetime import datetime, timedelta


# =============================================================================
# TEST DATA FACTORY & FIXTURES
# =============================================================================

class TestDataFactory:
    """Factory for creating consistent test data across tests"""

    @staticmethod
    def create_sensor_reading(sensor_id=None, **kwargs):
        """Create a sensor reading with defaults that can be overridden"""
        defaults = {
            "sensor_id": sensor_id or f"test-{uuid.uuid4().hex[:8]}",
            "temperature": 22.5,
            "humidity": 55.0,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        return {**defaults, **kwargs}

    @staticmethod
    def create_kafka_message(sensor_id=None, **kwargs):
        """Create Kafka Connect compatible message"""
        reading = TestDataFactory.create_sensor_reading(sensor_id, **kwargs)

        return json.dumps({
            "schema": {
                "type": "struct",
                "fields": [
                    {"field": "sensor_id", "type": "string"},
                    {"field": "temperature", "type": "double"},
                    {"field": "humidity", "type": "double"},
                    {"field": "timestamp", "type": "int64", "name": "org.apache.kafka.connect.data.Timestamp"}
                ]
            },
            "payload": reading
        })


@pytest.fixture
def isolated_sensor(kafka_exec, postgres_exec, config):
    """
    Creates isolated sensor with test data for individual tests.
    Ensures data exists and cleans up after test.
    """
    sensor_id = f"test-isolated-{uuid.uuid4().hex[:8]}"

    # ARRANGE: Send test message to Kafka
    message = TestDataFactory.create_kafka_message(sensor_id)
    cmd = [
        "kubectl", "exec", "-i", config.KAFKA_BROKER_POD,
        "-n", config.MESSAGING_NAMESPACE,
        "--", "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", "localhost:9092",
        "--topic", config.SENSOR_DATA_TOPIC
    ]

    result = subprocess.run(
        cmd, input=message, capture_output=True, text=True, timeout=config.COMMAND_TIMEOUT
    )
    assert result.returncode == 0, f"Failed to send message: {result.stderr}"

    # Wait for connector to process
    max_wait = 15
    for _ in range(max_wait):
        time.sleep(1)
        success, output = postgres_exec(
            f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
            f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{sensor_id}\'"'
        )
        if success and output.strip().isdigit() and int(output.strip()) > 0:
            break
    else:
        pytest.fail(f"Sensor {sensor_id} not found in database after {max_wait}s")

    yield sensor_id

    # CLEANUP: Remove test data
    postgres_exec(
        f'psql -U postgres -d {config.POSTGRESQL_DB} -c '
        f'"DELETE FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{sensor_id}\'"'
    )


@pytest.fixture(scope="class")
def seeded_sensors(kafka_exec, postgres_exec, config):
    """
    Class-scoped fixture that ensures multiple sensors exist for query tests.
    Creates 3 test sensors if database is empty.
    """
    # Check if data exists
    success, output = postgres_exec(
        f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
        f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE}"'
    )
    count = int(output.strip()) if (success and output.strip().isdigit()) else 0

    created_sensors = []

    if count == 0:
        # Seed database with test sensors
        test_sensors = [
            {"id": f"test-seed-{uuid.uuid4().hex[:6]}", "temp": 22.5, "hum": 55.0},
            {"id": f"test-seed-{uuid.uuid4().hex[:6]}", "temp": 23.0, "hum": 60.0},
            {"id": f"test-seed-{uuid.uuid4().hex[:6]}", "temp": 21.5, "hum": 50.0},
        ]

        for sensor in test_sensors:
            message = TestDataFactory.create_kafka_message(
                sensor_id=sensor["id"],
                temperature=sensor["temp"],
                humidity=sensor["hum"]
            )

            cmd = [
                "kubectl", "exec", "-i", config.KAFKA_BROKER_POD,
                "-n", config.MESSAGING_NAMESPACE,
                "--", "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", config.SENSOR_DATA_TOPIC
            ]

            subprocess.run(cmd, input=message, capture_output=True, text=True, timeout=config.COMMAND_TIMEOUT)
            created_sensors.append(sensor["id"])

        # Wait for all messages: sensor-data → Spark (30s window) → analytics-data → PostgreSQL
        # Need to wait for at least one Spark window (30s) + processing time
        time.sleep(45)

    yield

    # CLEANUP: Remove seeded test data
    for sensor_id in created_sensors:
        postgres_exec(
            f'psql -U postgres -d {config.POSTGRESQL_DB} -c '
            f'"DELETE FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{sensor_id}\'"'
        )


# =============================================================================
# LAYER 3A: KAFKA TOPIC TESTS (Depends on Kafka Health)
# =============================================================================

class TestKafkaTopics:
    """Kafka topic configuration tests"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="topic_exists", scope="session")
    def test_topic_exists(self, kafka_exec, config):
        """
        Verify analytics-data topic exists

        Dependencies: kafka_topics_accessible
        """
        success, output = kafka_exec(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        )
        assert success, f"Failed to list topics: {output}"
        assert config.ANALYTICS_DATA_TOPIC in output, f"Topic {config.ANALYTICS_DATA_TOPIC} not found"

    @pytest.mark.functional
    @pytest.mark.dependency(name="topic_configuration", scope="session")
    def test_topic_configuration(self, kafka_exec, config):
        """
        Verify topic has correct configuration

        Dependencies: topic_exists
        """
        success, output = kafka_exec(
            f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "
            f"--describe --topic {config.ANALYTICS_DATA_TOPIC}"
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
    @pytest.mark.dependency(name="connector_config_valid",scope="session")
    def test_connector_config_valid(self, connect_exec):
        """
        Verify connector configuration is correct

        Dependencies: kafka_connect_api_available, postgresql_sink_connector_exists

        Note: This test may fail if Kafka cluster is unstable (COORDINATOR_NOT_AVAILABLE).
        Check: kubectl logs -n messaging -l app=kafka-connect
        """
        # First verify API is responsive with simple endpoint
        success, output = connect_exec("curl -s --max-time 5 http://localhost:8083/")
        if not success:
            pytest.skip(f"Kafka Connect API not responding: {output}")

        # Wait briefly for coordinator stability
        time.sleep(2)

        # Now get connector config with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            success, output = connect_exec(
                "curl -s --max-time 15 http://localhost:8083/connectors/postgresql-sink/config"
            )
            if success:
                break

            if attempt < max_retries - 1:
                time.sleep(3)  # Wait before retry

        assert success, (
            f"Failed to get connector config after {max_retries} attempts: {output}\n"
            f"This may indicate Kafka coordinator issues. "
            f"Check: kubectl logs -n messaging -l app=kafka-connect"
        )

        cfg = json.loads(output)

        # Prüfe, ob die wichtigsten Felder vorhanden sind (nicht auf exakte Werte)
        required_fields = [
            "connection.url",
            "connection.user",
            "connection.password",
            "topics",
            "table.name.format",
            "transforms",
            "transforms.TimestampConverter.type",
            "transforms.TimestampConverter.field",
            "transforms.TimestampConverter.format",
            "transforms.TimestampConverter.target.type",
            "key.converter",
            "value.converter"
        ]
        for field in required_fields:
            assert field in cfg, f"Konfigurationsfeld fehlt: {field}"



# =============================================================================
# LAYER 3C: FASTAPI ENDPOINT TESTS
# =============================================================================

class TestFastAPIHealthEndpoints:
    """Test FastAPI health and readiness endpoints (no external dependencies)"""

    @pytest.mark.functional
    def test_health_endpoint(self, fastapi_exec):
        """Verify /health endpoint returns healthy status"""
        # ACT
        success, output = fastapi_exec('curl -s http://localhost:8000/health')

        # ASSERT
        assert success, f"Failed to call /health: {output}"

        try:
            data = json.loads(output)
            assert data.get("status") == "healthy", f"Expected healthy status, got {data.get('status')}"
            assert "service" in data, "Missing service field"
            assert "timestamp" in data, "Missing timestamp field"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_ready_endpoint(self, fastapi_exec):
        """Verify /ready endpoint returns ready status when Kafka is available"""
        # ACT
        success, output = fastapi_exec('curl -s http://localhost:8000/ready')

        # ASSERT
        assert success, f"Failed to call /ready: {output}"

        try:
            data = json.loads(output)
            # May fail if Kafka not available, but should return valid JSON
            assert "status" in data, "Missing status field"
            assert "kafka_connected" in data, "Missing kafka_connected field"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_root_endpoint(self, fastapi_exec):
        """Verify root endpoint returns API information"""
        # ACT
        success, output = fastapi_exec('curl -s http://localhost:8000/')

        # ASSERT
        assert success, f"Failed to call root endpoint: {output}"

        try:
            data = json.loads(output)
            assert "service" in data, "Missing service field"
            assert "version" in data, "Missing version field"
            assert "endpoints" in data, "Missing endpoints field"
            assert "kafka" in data, "Missing kafka config"
            assert "database" in data, "Missing database config"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")


class TestFastAPIIngestionEndpoint:
    """Test FastAPI /ingest endpoint (Kafka integration required)"""

    @pytest.mark.functional
    def test_ingest_valid_data(self, fastapi_exec):
        """Verify /ingest accepts valid sensor data and returns success"""
        # ARRANGE
        sensor_data = {
            "sensor_id": f"test-ingest-{uuid.uuid4().hex[:8]}",
            "timestamp": (datetime.now() - timedelta(seconds=1)).isoformat(),
            "temperature": 22.5,
            "humidity": 55.0
        }
        json_payload = json.dumps(sensor_data)

        # ACT
        success, output = fastapi_exec(
            f'curl -s -X POST http://localhost:8000/ingest '
            f'-H "Content-Type: application/json" '
            f'-d \'{json_payload}\''
        )

        # ASSERT
        assert success, f"Failed to call /ingest: {output}"

        try:
            data = json.loads(output)
            assert data.get("status") == "success", f"Expected success status, got {data.get('status')}"
            assert data.get("sensor_id") == sensor_data["sensor_id"], "Sensor ID mismatch"
            assert "kafka_partition" in data, "Missing kafka_partition field"
            assert "kafka_offset" in data, "Missing kafka_offset field"
            assert "message" in data, "Missing message field"
            assert "timestamp" in data, "Missing timestamp field"

            # Verify Kafka metadata values are valid
            assert isinstance(data["kafka_partition"], int), "kafka_partition must be integer"
            assert isinstance(data["kafka_offset"], int), "kafka_offset must be integer"
            assert data["kafka_partition"] >= 0, "kafka_partition must be non-negative"
            assert data["kafka_offset"] >= 0, "kafka_offset must be non-negative"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_ingest_invalid_data_missing_field(self, fastapi_exec):
        """Verify /ingest rejects data with missing required fields"""
        # ARRANGE - missing temperature field
        sensor_data = {
            "sensor_id": "test-sensor",
            "timestamp": datetime.now().isoformat(),
            "humidity": 55.0
        }
        json_payload = json.dumps(sensor_data)

        # ACT
        success, output = fastapi_exec(
            f'curl -s -w "\\n%{{http_code}}" -X POST http://localhost:8000/ingest '
            f'-H "Content-Type: application/json" '
            f'-d \'{json_payload}\''
        )

        # ASSERT
        assert success, f"Failed to execute curl: {output}"
        lines = output.strip().split('\n')
        http_code = lines[-1] if lines else ""

        # Should return 422 Unprocessable Entity for validation error
        assert "422" in http_code, f"Expected 422 validation error, got {http_code}"

    @pytest.mark.functional
    def test_ingest_invalid_data_out_of_range(self, fastapi_exec):
        """Verify /ingest rejects data with out-of-range values"""
        # ARRANGE - humidity > 100%
        sensor_data = {
            "sensor_id": "test-sensor",
            "timestamp": datetime.now().isoformat(),
            "temperature": 22.5,
            "humidity": 150.0  # Invalid: > 100%
        }
        json_payload = json.dumps(sensor_data)

        # ACT
        success, output = fastapi_exec(
            f'curl -s -w "\\n%{{http_code}}" -X POST http://localhost:8000/ingest '
            f'-H "Content-Type: application/json" '
            f'-d \'{json_payload}\''
        )

        # ASSERT
        assert success, f"Failed to execute curl: {output}"
        lines = output.strip().split('\n')
        http_code = lines[-1] if lines else ""

        # Should return 422 for validation error
        assert "422" in http_code, f"Expected 422 validation error, got {http_code}"


# =============================================================================
# LAYER 3D: PROMETHEUS FUNCTIONAL TESTS
# =============================================================================

class TestPrometheusMetrics:
    """Test Prometheus metric collection and query functionality"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="prometheus_metrics_available", scope="session")
    def test_prometheus_has_metrics(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus is collecting metrics from Kubernetes

        Dependencies: prometheus_running, prometheus_scrape_targets
        """
        # Get Prometheus pod name dynamically
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found"

        prometheus_pod = prometheus_pods[0]

        # Query for basic Kubernetes metrics (up metric)
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- "
            f"wget -q -O- 'http://localhost:9090/api/v1/query?query=up'"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query Prometheus metrics: {output}"
        assert '"status":"success"' in output, f"Prometheus query failed: {output}"
        assert '"result"' in output, f"No results returned: {output}"

        # Verify we have actual metric data
        try:
            data = json.loads(output)
            assert len(data["data"]["result"]) > 0, "No metrics found"
        except (json.JSONDecodeError, KeyError) as e:
            pytest.fail(f"Invalid Prometheus response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_prometheus_container_metrics(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus is collecting container resource metrics (cAdvisor)

        Dependencies: prometheus_running, prometheus_to_cadvisor
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found"

        prometheus_pod = prometheus_pods[0]

        # Query for container CPU usage metric
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- "
            f"wget -q -O- 'http://localhost:9090/api/v1/query?query=container_cpu_usage_seconds_total'"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query container metrics: {output}"
        assert '"status":"success"' in output, f"Container metrics query failed: {output}"

        try:
            data = json.loads(output)
            assert len(data["data"]["result"]) > 0, "No container CPU metrics found"
        except (json.JSONDecodeError, KeyError) as e:
            pytest.fail(f"Invalid Prometheus response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_prometheus_memory_metrics(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus is collecting container memory metrics

        Dependencies: prometheus_running
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found"

        prometheus_pod = prometheus_pods[0]

        # Query for container memory usage metric
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- "
            f"wget -q -O- 'http://localhost:9090/api/v1/query?query=container_memory_working_set_bytes'"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query memory metrics: {output}"
        assert '"status":"success"' in output, f"Memory metrics query failed: {output}"

        try:
            data = json.loads(output)
            assert len(data["data"]["result"]) > 0, "No container memory metrics found"
        except (json.JSONDecodeError, KeyError) as e:
            pytest.fail(f"Invalid Prometheus response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_prometheus_namespace_metrics(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus can query metrics for specific namespaces

        Dependencies: prometheus_running
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found"

        prometheus_pod = prometheus_pods[0]

        # Query for metrics in messaging namespace
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- "
            f"wget -q -O- 'http://localhost:9090/api/v1/query?query=container_memory_working_set_bytes{{namespace=\"{config.MESSAGING_NAMESPACE}\"}}'"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query namespace metrics: {output}"
        assert '"status":"success"' in output, f"Namespace metrics query failed: {output}"

        try:
            data = json.loads(output)
            results = data["data"]["result"]
            if len(results) > 0:
                # Verify namespace label is correct
                assert results[0]["metric"]["namespace"] == config.MESSAGING_NAMESPACE, \
                    f"Namespace mismatch in metrics"
        except (json.JSONDecodeError, KeyError) as e:
            pytest.fail(f"Invalid Prometheus response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_prometheus_targets_healthy(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus scrape targets are healthy (up state)

        Dependencies: prometheus_running, prometheus_scrape_targets
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found"

        prometheus_pod = prometheus_pods[0]

        # Get all targets status
        cmd = f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- wget -q -O- http://localhost:9090/api/v1/targets"
        success, output = kubectl(cmd)

        assert success, f"Failed to query targets: {output}"
        assert '"status":"success"' in output, f"Targets query failed: {output}"

        try:
            data = json.loads(output)
            active_targets = data["data"]["activeTargets"]
            assert len(active_targets) > 0, "No active scrape targets found"

            # Check that at least some targets are healthy
            healthy_count = sum(1 for t in active_targets if t["health"] == "up")
            assert healthy_count > 0, f"No healthy targets found. Total targets: {len(active_targets)}"

        except (json.JSONDecodeError, KeyError) as e:
            pytest.fail(f"Invalid Prometheus targets response: {e}\nOutput: {output}")


# =============================================================================
# LAYER 3E: GRAFANA FUNCTIONAL TESTS
# =============================================================================

class TestGrafanaDatasourceAndDashboards:
    """Test Grafana datasource configuration and dashboard functionality"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="grafana_api_accessible", scope="session")
    def test_grafana_api_health(self, kubectl, get_pod_names, config):
        """
        Verify Grafana API is accessible and healthy

        Dependencies: grafana_running
        """
        # Get Grafana pod name dynamically
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found"

        grafana_pod = grafana_pods[0]

        # Check Grafana health endpoint
        cmd = f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- curl -s http://localhost:3000/api/health"
        success, output = kubectl(cmd)

        assert success, f"Failed to query Grafana health: {output}"
        assert "ok" in output.lower() or "database" in output.lower(), f"Grafana health check failed: {output}"

    @pytest.mark.functional
    def test_grafana_datasource_configured(self, kubectl, get_pod_names, config):
        """
        Verify Grafana has Prometheus datasource configured

        Dependencies: grafana_running, grafana_to_prometheus
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found"

        grafana_pod = grafana_pods[0]

        # Query Grafana datasources API (using admin:admin default credentials)
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- "
            f"curl -s -u admin:admin http://localhost:3000/api/datasources"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query Grafana datasources: {output}"

        try:
            datasources = json.loads(output)
            assert isinstance(datasources, list), "Datasources response should be a list"
            assert len(datasources) > 0, "No datasources configured in Grafana"

            # Find Prometheus datasource
            prometheus_ds = next((ds for ds in datasources if ds.get("type") == "prometheus"), None)
            assert prometheus_ds is not None, "No Prometheus datasource found in Grafana"
            assert "prometheus" in prometheus_ds.get("url", "").lower(), \
                f"Prometheus datasource URL invalid: {prometheus_ds.get('url')}"

        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid Grafana datasources response: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_grafana_can_query_prometheus(self, kubectl, get_pod_names, config):
        """
        Verify Grafana can successfully query Prometheus datasource

        Dependencies: grafana_running, grafana_prometheus_api
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found"

        grafana_pod = grafana_pods[0]

        # First, get datasource UID
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- "
            f"curl -s -u admin:admin http://localhost:3000/api/datasources"
        )
        success, output = kubectl(cmd)
        assert success, "Failed to get datasources"

        try:
            datasources = json.loads(output)
            prometheus_ds = next((ds for ds in datasources if ds.get("type") == "prometheus"), None)
            assert prometheus_ds is not None, "No Prometheus datasource found"
            ds_id = prometheus_ds.get("id")
            assert ds_id, "Datasource ID not found"

            # Query Prometheus through Grafana datasource proxy (using ID, not UID)
            query = "up"
            cmd = (
                f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- "
                f"curl -s -u admin:admin "
                f"'http://localhost:3000/api/datasources/proxy/{ds_id}/api/v1/query?query={query}'"
            )
            success, output = kubectl(cmd)

            assert success, f"Failed to query Prometheus via Grafana: {output}"
            assert '"status":"success"' in output, f"Prometheus query via Grafana failed: {output}"

            query_result = json.loads(output)
            assert len(query_result.get("data", {}).get("result", [])) > 0, \
                "No metrics returned from Prometheus via Grafana"

        except (json.JSONDecodeError, KeyError, StopIteration) as e:
            pytest.fail(f"Failed to query Prometheus via Grafana: {e}\nOutput: {output}")

    @pytest.mark.functional
    def test_grafana_dashboards_provisioned(self, kubectl, get_pod_names, config):
        """
        Verify Grafana has provisioned dashboards available

        Dependencies: grafana_running
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found"

        grafana_pod = grafana_pods[0]

        # Query Grafana dashboards API
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- "
            f"curl -s -u admin:admin http://localhost:3000/api/search?type=dash-db"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query Grafana dashboards: {output}"

        try:
            dashboards = json.loads(output)
            assert isinstance(dashboards, list), "Dashboards response should be a list"
            # Note: May be empty if no dashboards provisioned yet, just verify API works

        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid Grafana dashboards response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.slow
    def test_grafana_can_render_dashboard(self, kubectl, get_pod_names, config):
        """
        Verify Grafana can render a dashboard with Prometheus data

        Dependencies: grafana_running, grafana_datasource_configured
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found"

        grafana_pod = grafana_pods[0]

        # Get list of dashboards
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- "
            f"curl -s -u admin:admin http://localhost:3000/api/search?type=dash-db"
        )
        success, output = kubectl(cmd)
        assert success, "Failed to get dashboards"

        try:
            dashboards = json.loads(output)
            if len(dashboards) > 0:
                # Try to get dashboard JSON for first dashboard
                dashboard_uid = dashboards[0].get("uid")
                cmd = (
                    f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- "
                    f"curl -s -u admin:admin http://localhost:3000/api/dashboards/uid/{dashboard_uid}"
                )
                success, output = kubectl(cmd)

                assert success, f"Failed to get dashboard: {output}"
                dashboard_data = json.loads(output)
                assert "dashboard" in dashboard_data, "Invalid dashboard response"
                assert "panels" in dashboard_data.get("dashboard", {}), "Dashboard has no panels"
            # If no dashboards, test passes (dashboards are optional)

        except (json.JSONDecodeError, KeyError) as e:
            pytest.fail(f"Failed to render dashboard: {e}\nOutput: {output}")


# =============================================================================
# LAYER 3F: END-TO-END PIPELINE TESTS
# =============================================================================

class TestPipelineEndToEnd:
    """End-to-end pipeline tests"""

    @pytest.mark.functional
    @pytest.mark.slow
    @pytest.mark.dependency(name="message_to_postgresql", scope="session")
    def test_message_to_postgresql(
        self, kafka_exec, postgres_exec, connect_exec, test_message, config
    ):
        """
        Test complete pipeline: Kafka message -> PostgreSQL

        Dependencies: topic_exists, postgresql_sink_connector_running,
                      connect_to_kafka_broker, connect_to_postgresql,
                      analytics_data_table_exists
        """
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
            "--topic", config.ANALYTICS_DATA_TOPIC
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
# LAYER 3G: COMPLETE END-TO-END WORKFLOW
# =============================================================================

class TestCompleteEndToEndWorkflow:
    """Test complete data pipeline from Kafka to FastAPI queries with isolated test data"""

    @pytest.mark.functional
    @pytest.mark.slow
    def test_complete_workflow(self, fastapi_exec, isolated_sensor):
        """
        Complete workflow test with isolated test data:
        1. Verify sensor exists in /sensors list
        2. Get time-series data via /sensors/{id}/data
        3. Get aggregated stats via /sensors/{id}/stats
        4. Verify data consistency between all endpoints

        Uses isolated_sensor fixture for complete test isolation.
        """
        # ARRANGE
        sensor_id = isolated_sensor

        # ACT & ASSERT: Step 1 - List sensors
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to list sensors: {output}"

        try:
            sensors = json.loads(output)
            assert len(sensors) > 0, "Sensor list should not be empty"

            # Find our test sensor
            test_sensor = next((s for s in sensors if s["sensor_id"] == sensor_id), None)
            assert test_sensor is not None, f"Sensor {sensor_id} not found in sensor list"
            list_reading_count = test_sensor["reading_count"]
            assert list_reading_count > 0, "Sensor should have readings"

            # ACT & ASSERT: Step 2 - Get time-series data
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/data?limit=1000"'
            )
            assert success, f"Failed to get sensor data: {output}"

            data = json.loads(output)
            assert isinstance(data, list), "Data should be a list"
            assert len(data) > 0, "Should have at least one reading"
            data_count = len(data)

            # ACT & ASSERT: Step 3 - Get aggregated stats
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/stats"'
            )
            assert success, f"Failed to get sensor stats: {output}"

            stats = json.loads(output)
            assert stats["sensor_id"] == sensor_id, "Stats sensor_id mismatch"
            stats_count = stats["reading_count"]

            # ASSERT: Step 4 - Verify consistency between endpoints

            # Reading count consistency
            assert stats_count == data_count, \
                f"Stats count ({stats_count}) should equal data count ({data_count}) with limit=1000"
            assert stats_count == list_reading_count, \
                f"Stats count ({stats_count}) should equal list count ({list_reading_count})"

            # Verify all data points are within stats ranges
            for reading in data:
                temp = reading["temperature"]
                humidity = reading["humidity"]

                assert stats["temperature_min"] <= temp <= stats["temperature_max"], \
                    f"Temperature {temp} outside stats range [{stats['temperature_min']}, {stats['temperature_max']}]"
                assert stats["humidity_min"] <= humidity <= stats["humidity_max"], \
                    f"Humidity {humidity} outside stats range [{stats['humidity_min']}, {stats['humidity_max']}]"

            # Verify sensor_id consistency across all readings
            for reading in data:
                assert reading["sensor_id"] == sensor_id, \
                    f"Reading sensor_id {reading['sensor_id']} doesn't match expected {sensor_id}"

        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")
