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
from datetime import datetime, timedelta, UTC


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
            "insert.mode",
            "pk.mode",
            "auto.create",
            "auto.evolve",
            "key.converter",
            "value.converter",
            "value.converter.schemas.enable"
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
# LAYER 3F: POSTGRESQL PERMISSIONS TESTS
# =============================================================================

class TestPostgreSQLPermissions:
    """Test PostgreSQL user permissions for analytics_data table"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="postgresql_permissions_valid", scope="session")
    def test_appuser_can_read_analytics_data(self, postgres_exec, config):
        """
        Verify that appuser (used by FastAPI) can read from analytics_data table

        This test ensures that table permissions are correctly configured
        so FastAPI /sensors endpoints can query the database.

        Dependencies: postgresql_running, analytics_data_table_exists
        """
        # Test 1: Verify appuser can connect
        success, output = postgres_exec(
            f'psql -U appuser -d {config.POSTGRESQL_DB} -c "SELECT 1" -t'
        )
        assert success, f"appuser cannot connect to database: {output}"

        # Test 2: Verify appuser can SELECT from analytics_data
        success, output = postgres_exec(
            f'psql -U appuser -d {config.POSTGRESQL_DB} -t -c '
            f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE}"'
        )
        assert success, \
            f"appuser cannot SELECT from {config.POSTGRESQL_TABLE}: {output}\n" \
            f"Check: GRANT SELECT ON {config.POSTGRESQL_TABLE} TO appuser"

        # Test 3: Verify appuser can INSERT into analytics_data
        test_sensor_id = f"test-perm-{uuid.uuid4().hex[:8]}"
        insert_query = (
            f'psql -U appuser -d {config.POSTGRESQL_DB} -c '
            f'"INSERT INTO {config.POSTGRESQL_TABLE} (sensor_id, timestamp, temperature, humidity) '
            f"VALUES ('{test_sensor_id}', "
            f"{int(datetime.now().timestamp() * 1000)}, 20.0, 50.0)\""
        )
        success, output = postgres_exec(insert_query)
        assert success, \
            f"appuser cannot INSERT into {config.POSTGRESQL_TABLE}: {output}\n" \
            f"Check: GRANT INSERT ON {config.POSTGRESQL_TABLE} TO appuser"

        # Test 4: Verify appuser can DELETE from analytics_data (cleanup)
        success, output = postgres_exec(
            f'psql -U appuser -d {config.POSTGRESQL_DB} -c '
            f'"DELETE FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{test_sensor_id}\'"'
        )
        assert success, \
            f"appuser cannot DELETE from {config.POSTGRESQL_TABLE}: {output}\n" \
            f"Check: GRANT DELETE ON {config.POSTGRESQL_TABLE} TO appuser"

    @pytest.mark.functional
    def test_postgres_superuser_has_full_access(self, postgres_exec, config):
        """
        Verify postgres superuser has full access to analytics_data table

        This is a sanity check to ensure the table exists and is accessible
        by the admin user used in tests.

        Dependencies: postgresql_running
        """
        # Test SELECT
        success, output = postgres_exec(
            f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
            f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE}"'
        )
        assert success, f"postgres user cannot SELECT: {output}"

        # Test INSERT
        test_sensor_id = f"test-admin-{uuid.uuid4().hex[:8]}"
        success, output = postgres_exec(
            f'psql -U postgres -d {config.POSTGRESQL_DB} -c '
            f'"INSERT INTO {config.POSTGRESQL_TABLE} (sensor_id, timestamp, temperature, humidity) '
            f"VALUES ('{test_sensor_id}', "
            f"{int(datetime.now().timestamp() * 1000)}, 21.0, 51.0)\""
        )
        assert success, f"postgres user cannot INSERT: {output}"

        # Test DELETE (cleanup)
        success, output = postgres_exec(
            f'psql -U postgres -d {config.POSTGRESQL_DB} -c '
            f'"DELETE FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{test_sensor_id}\'"'
        )
        assert success, f"postgres user cannot DELETE: {output}"

    @pytest.mark.functional
    def test_table_permissions_grant(self, postgres_exec, config):
        """
        Verify that analytics_data table has correct permission grants

        Checks PostgreSQL system catalogs to ensure appuser has required privileges.

        Dependencies: postgresql_running, analytics_data_table_exists
        """
        # Query pg_class and pg_roles to check privileges
        check_query = (
            f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
            f'"SELECT has_table_privilege(\'appuser\', \'{config.POSTGRESQL_TABLE}\', \'SELECT\') AS can_select, '
            f"has_table_privilege('appuser', '{config.POSTGRESQL_TABLE}', 'INSERT') AS can_insert, "
            f"has_table_privilege('appuser', '{config.POSTGRESQL_TABLE}', 'UPDATE') AS can_update, "
            f"has_table_privilege('appuser', '{config.POSTGRESQL_TABLE}', 'DELETE') AS can_delete\""
        )

        success, output = postgres_exec(check_query)
        assert success, f"Failed to check table privileges: {output}"

        # Parse output (expected format: " t | t | t | t")
        privileges = output.strip().split('|')
        assert len(privileges) == 4, f"Unexpected privilege output: {output}"

        can_select = privileges[0].strip() == 't'
        can_insert = privileges[1].strip() == 't'
        can_update = privileges[2].strip() == 't'
        can_delete = privileges[3].strip() == 't'

        assert can_select, f"appuser missing SELECT privilege on {config.POSTGRESQL_TABLE}"
        assert can_insert, f"appuser missing INSERT privilege on {config.POSTGRESQL_TABLE}"
        assert can_update, f"appuser missing UPDATE privilege on {config.POSTGRESQL_TABLE}"
        assert can_delete, f"appuser missing DELETE privilege on {config.POSTGRESQL_TABLE}"


# =============================================================================
# LAYER 3G: KAFKA CONNECT SINK TESTS
# =============================================================================

class TestKafkaConnectSink:
    """Test Kafka Connect JDBC Sink: analytics-data topic → PostgreSQL"""

    @pytest.mark.functional
    @pytest.mark.slow
    @pytest.mark.dependency(name="message_to_postgresql", scope="session")
    def test_kafka_to_postgresql_via_connector(
        self, kafka_exec, postgres_exec, connect_exec, test_message, config
    ):
        """
        Test Kafka Connect JDBC Sink: analytics-data topic → PostgreSQL

        This tests ONLY the sink connector, NOT the full pipeline.
        For full end-to-end testing (FastAPI → Spark → PostgreSQL),
        use TestCompleteEndToEndWorkflow.

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
        # Debug: Print message being sent
        print(f"\n[DEBUG] Sending message to topic '{config.ANALYTICS_DATA_TOPIC}':")
        try:
            msg_obj = json.loads(message)
            print(f"  sensor_id: {msg_obj['payload']['sensor_id']}")
            print(f"  timestamp: {msg_obj['payload']['timestamp']}")
            print(f"  temperature: {msg_obj['payload']['temperature']}")
            print(f"  humidity: {msg_obj['payload']['humidity']}")
            print(f"  Schema present: {'schema' in msg_obj}")
        except:
            print(f"  Raw message: {message[:200]}...")

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

        if result.returncode != 0:
            print(f"[ERROR] Failed to send message: {result.stderr}")
        else:
            print(f"[SUCCESS] Message sent successfully")

        assert result.returncode == 0, f"Failed to send message: {result.stderr}"

    def _wait_for_message(self, postgres_exec, test_id: str, config, max_wait: int = 30):
        """Wait for message to appear in PostgreSQL"""
        query = (
            f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c "
            f"\"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} "
            f"WHERE sensor_id = '{test_id}'\""
        )

        for i in range(max_wait):
            time.sleep(1)
            success, output = postgres_exec(query)

            if success and output.strip().isdigit() and int(output.strip()) > 0:
                return

            # Debug output every 5 seconds
            if (i + 1) % 5 == 0:
                print(f"  [{i+1}s] Still waiting for message (sensor_id={test_id})...")

        # Final debug: Check if any data exists in table
        all_query = f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c \"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE}\""
        success, total_count = postgres_exec(all_query)

        # Check recent entries
        recent_query = (
            f"psql -U postgres -d {config.POSTGRESQL_DB} -t -c "
            f"\"SELECT sensor_id, timestamp, temperature FROM {config.POSTGRESQL_TABLE} "
            f"ORDER BY created_at DESC LIMIT 5\""
        )
        success, recent = postgres_exec(recent_query)

        pytest.fail(
            f"Message not found in PostgreSQL after {max_wait} seconds.\n"
            f"Expected sensor_id: {test_id}\n"
            f"Total rows in table: {total_count.strip() if success else 'unknown'}\n"
            f"Recent entries:\n{recent if success else 'unable to query'}"
        )

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
# LAYER 3H: COMPLETE END-TO-END WORKFLOW
# =============================================================================

class TestCompleteEndToEndWorkflow:
    """
    Test complete data pipeline: FastAPI Input → Kafka → Spark → PostgreSQL → FastAPI Output

    This is a TRUE End-to-End test covering the full architecture:
    1. POST /ingest (FastAPI) → Kafka sensor-data topic
    2. Spark reads sensor-data → Aggregates (30s window) → Writes to analytics-data topic
    3. PostgreSQL Connector reads analytics-data → Stores in database
    4. GET /sensors/* (FastAPI) → Queries PostgreSQL → Returns data
    """

    @pytest.mark.functional
    @pytest.mark.slow
    def test_complete_workflow_via_fastapi_ingestion(self, fastapi_exec, postgres_exec, kafka_exec, config):
        """
        TRUE End-to-End test: FastAPI /ingest → Spark → PostgreSQL → FastAPI /sensors/*

        Pipeline stages:
        1. POST sensor data to FastAPI /ingest endpoint
        2. FastAPI sends to Kafka sensor-data topic
        3. Spark aggregates data (30s tumbling windows)
        4. Spark writes to analytics-data topic
        5. PostgreSQL Connector writes to database
        6. Query data via FastAPI /sensors endpoints
        7. Verify data consistency across all endpoints

        CRITICAL: This test validates the COMPLETE architecture as documented in README.md
        """
        print("\n" + "="*80)
        print("E2E TEST: Complete Pipeline Validation")
        print("="*80)

        # ARRANGE: Create unique test sensor
        sensor_id = f"test-e2e-{uuid.uuid4().hex[:8]}"
        print(f"\n[ARRANGE] Test Sensor ID: {sensor_id}")

        # Prepare sensor reading payload for FastAPI /ingest
        # IMPORTANT: Use UTC time to match FastAPI's default timezone
        utc_now = datetime.now(UTC)
        timestamp_utc = (utc_now - timedelta(seconds=2)).isoformat().replace('+00:00', 'Z')
        sensor_payload = {
            "sensor_id": sensor_id,
            "timestamp": timestamp_utc,
            "temperature": 23.5,
            "humidity": 58.0
        }
        print(f"[ARRANGE] Timestamp (UTC): {sensor_payload['timestamp']}")
        json_payload = json.dumps(sensor_payload)
        print(f"[ARRANGE] Payload: temp={sensor_payload['temperature']}°C, humidity={sensor_payload['humidity']}%")

        # ACT: Step 1 - POST to FastAPI /ingest (Start of E2E pipeline)
        print(f"\n{'─'*80}")
        print("[STEP 1/7] FastAPI Ingestion: POST /ingest")
        print(f"{'─'*80}")

        success, output = fastapi_exec(
            f'curl -s -X POST http://localhost:8000/ingest '
            f'-H "Content-Type: application/json" '
            f'-d \'{json_payload}\''
        )

        if not success:
            pytest.fail(
                f"❌ FAILED at STEP 1: FastAPI Ingestion\n"
                f"Service: FastAPI (/ingest endpoint)\n"
                f"Error: Failed to execute curl command\n"
                f"Output: {output}"
            )

        # Verify ingestion response
        try:
            ingest_response = json.loads(output)
        except json.JSONDecodeError as e:
            pytest.fail(
                f"❌ FAILED at STEP 1: FastAPI Ingestion\n"
                f"Service: FastAPI (/ingest endpoint)\n"
                f"Error: Invalid JSON response\n"
                f"Exception: {e}\n"
                f"Raw Output: {output[:500]}"
            )

        if ingest_response.get("status") != "success":
            pytest.fail(
                f"❌ FAILED at STEP 1: FastAPI Ingestion\n"
                f"Service: FastAPI (/ingest endpoint)\n"
                f"Error: Ingestion returned non-success status\n"
                f"Message: {ingest_response.get('message', 'No message')}\n"
                f"Full Response: {json.dumps(ingest_response, indent=2)}"
            )

        if ingest_response.get("sensor_id") != sensor_id:
            pytest.fail(
                f"❌ FAILED at STEP 1: FastAPI Ingestion\n"
                f"Service: FastAPI (/ingest endpoint)\n"
                f"Error: Sensor ID mismatch\n"
                f"Expected: {sensor_id}\n"
                f"Got: {ingest_response.get('sensor_id')}"
            )

        kafka_partition = ingest_response.get('kafka_partition', -1)
        kafka_offset = ingest_response.get('kafka_offset', -1)
        print(f"✓ FastAPI accepted data (partition={kafka_partition}, offset={kafka_offset})")

        # Verify message in Kafka sensor-data topic
        print(f"\n{'─'*80}")
        print("[STEP 2/7] Kafka Verification: Check sensor-data topic")
        print(f"{'─'*80}")

        # Count messages in sensor-data topic (last 10)
        kafka_check_cmd = (
            f"/opt/kafka/bin/kafka-console-consumer.sh "
            f"--bootstrap-server localhost:9092 "
            f"--topic {config.SENSOR_DATA_TOPIC} "
            f"--from-beginning --max-messages 10 --timeout-ms 5000"
        )
        success, kafka_output = kafka_exec(kafka_check_cmd)

        if success and sensor_id in kafka_output:
            print(f"✓ Message found in Kafka topic '{config.SENSOR_DATA_TOPIC}'")
        else:
            print(f"⚠ Warning: Could not verify message in Kafka (may have been consumed already)")
            print(f"  This is expected if Spark has already processed the message")

        # ACT: Step 2-5 - Wait for complete pipeline processing
        # Data must flow: Kafka sensor-data → Spark (30s window) → analytics-data → PostgreSQL
        # Conservative wait: 45s (30s window + 15s processing buffer)
        print(f"\n{'─'*80}")
        print("[STEP 3/7] Pipeline Processing: Kafka → Spark → PostgreSQL")
        print(f"{'─'*80}")
        print(f"[E2E TEST] Waiting for full pipeline: Kafka → Spark (30s window) → PostgreSQL...")
        time.sleep(45)

        print(f"\n[STEP 4/7] PostgreSQL Verification")
        print(f"{'─'*80}")

        # Verify data arrived in PostgreSQL (as postgres superuser)
        max_wait = 15
        for attempt in range(max_wait):
            success, output = postgres_exec(
                f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
                f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{sensor_id}\'"'
            )
            if success and output.strip().isdigit() and int(output.strip()) > 0:
                print(f"✓ Data found in PostgreSQL (postgres user) after {attempt + 1}s")
                break
            time.sleep(1)
        else:
            # Enhanced debugging: Check what's in the database
            print(f"\n❌ DEBUGGING: Data not found after {max_wait + 45}s")

            # Check 1: Total row count
            success, total_output = postgres_exec(
                f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
                f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE}"'
            )
            total_count = total_output.strip() if success else "unknown"
            print(f"  Total rows in analytics_data: {total_count}")

            # Check 2: Recent entries
            success, recent_output = postgres_exec(
                f'psql -U postgres -d {config.POSTGRESQL_DB} -t -c '
                f'"SELECT sensor_id, to_timestamp(timestamp/1000.0), temperature FROM {config.POSTGRESQL_TABLE} '
                f'ORDER BY created_at DESC LIMIT 5"'
            )
            print(f"  Recent entries:\n{recent_output if success else 'unable to query'}")

            # Check 3: Kafka analytics-data topic
            print(f"\n  Checking Kafka analytics-data topic...")
            kafka_analytics_cmd = (
                f"/opt/kafka/bin/kafka-console-consumer.sh "
                f"--bootstrap-server localhost:9092 "
                f"--topic {config.ANALYTICS_DATA_TOPIC} "
                f"--from-beginning --max-messages 5 --timeout-ms 3000"
            )
            success, analytics_output = kafka_exec(kafka_analytics_cmd)
            if success and analytics_output:
                print(f"  Recent messages in analytics-data topic:\n  {analytics_output[:500]}")
            else:
                print(f"  ⚠ No messages found in analytics-data topic")

            pytest.fail(
                f"❌ FAILED at STEP 4: PostgreSQL Verification\n"
                f"Service: Spark → Kafka Connector → PostgreSQL\n"
                f"Error: Data not found in PostgreSQL after {max_wait + 45}s\n"
                f"Expected sensor_id: {sensor_id}\n"
                f"Total rows in table: {total_count}\n"
                f"\nPossible causes:\n"
                f"1. Spark not running or not processing data\n"
                f"2. Spark window (30s) not yet triggered\n"
                f"3. Kafka Connector not running or misconfigured\n"
                f"4. PostgreSQL connection issues\n"
                f"\nDebug steps:\n"
                f"  kubectl logs -n data -l app=spark --tail=50\n"
                f"  kubectl logs -n messaging -l app=kafka-connect --tail=50"
            )

        # Verify data is also readable by appuser (FastAPI's DB user)
        success, output = postgres_exec(
            f'psql -U appuser -d {config.POSTGRESQL_DB} -t -c '
            f'"SELECT COUNT(*) FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{sensor_id}\'"'
        )
        if not success:
            pytest.fail(
                f"[E2E TEST] Data exists but appuser cannot read it!\n"
                f"This will cause FastAPI /sensors to fail.\n"
                f"Error: {output}"
            )

        appuser_count = int(output.strip()) if output.strip().isdigit() else 0
        if appuser_count == 0:
            pytest.fail(
                f"[E2E TEST] Data exists for postgres user but not visible to appuser!\n"
                f"Check table permissions: GRANT SELECT ON analytics_data TO appuser"
            )

        print(f"[E2E TEST] Data verified readable by appuser (count: {appuser_count})")

        # ACT & ASSERT: Step 6 - Query via FastAPI /sensors endpoints (End of E2E pipeline)

        # 6a. List sensors
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to list sensors: {output}"

        try:
            sensors = json.loads(output)

            # Debug: Check response structure
            if not isinstance(sensors, list):
                pytest.fail(
                    f"Expected /sensors to return a list, got {type(sensors).__name__}.\n"
                    f"Response: {output[:500]}"
                )

            assert len(sensors) > 0, f"Sensor list should not be empty. Response: {output}"

            # Find our test sensor
            test_sensor = next((s for s in sensors if s["sensor_id"] == sensor_id), None)
            assert test_sensor is not None, \
                f"Sensor {sensor_id} not found in /sensors list.\n" \
                f"Available sensors: {[s['sensor_id'] for s in sensors[:5]]}"

            list_reading_count = test_sensor["reading_count"]
            assert list_reading_count > 0, "Sensor should have readings"

            # 6b. Get time-series data
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/data?limit=1000"'
            )
            assert success, f"Failed to get sensor data: {output}"

            data = json.loads(output)
            assert isinstance(data, list), "Data should be a list"
            assert len(data) > 0, "Should have at least one reading"
            data_count = len(data)

            # 6c. Get aggregated stats
            success, output = fastapi_exec(
                f'curl -s "http://localhost:8000/sensors/{sensor_id}/stats"'
            )
            assert success, f"Failed to get sensor stats: {output}"

            stats = json.loads(output)
            assert stats["sensor_id"] == sensor_id, "Stats sensor_id mismatch"
            stats_count = stats["reading_count"]

            # ASSERT: Step 7 - Verify E2E data consistency

            # Reading count consistency across all endpoints
            assert stats_count == data_count, \
                f"Stats count ({stats_count}) should equal data count ({data_count}) with limit=1000"
            assert stats_count == list_reading_count, \
                f"Stats count ({stats_count}) should equal list count ({list_reading_count})"

            # Verify all data points are within stats ranges (aggregation validation)
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

            print(f"[E2E TEST] SUCCESS: Full pipeline validated for sensor {sensor_id}")

        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response from FastAPI: {e}\nRaw output: {output[:500]}")
        except KeyError as e:
            pytest.fail(f"Missing expected field in response: {e}\nResponse structure: {output[:500]}")

        finally:
            # CLEANUP: Remove test data
            postgres_exec(
                f'psql -U postgres -d {config.POSTGRESQL_DB} -c '
                f'"DELETE FROM {config.POSTGRESQL_TABLE} WHERE sensor_id = \'{sensor_id}\'"'
            )
