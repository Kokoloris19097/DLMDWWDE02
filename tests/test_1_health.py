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
    @pytest.mark.dependency(name="kafka_broker_0_running", scope="session")
    def test_kafka_broker_0_running(self, kafka_exec):
        """Verify kafka-broker-0 pod is running"""
        success, output = kafka_exec("echo OK")
        assert success, f"kafka-broker-0 is not reachable: {output}"
        assert "OK" in output, f"Unexpected output: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_broker_1_running", scope="session")
    def test_kafka_broker_1_running(self, get_pod_phase, config):
        """Verify kafka-broker-1 pod is running"""
        success, phase = get_pod_phase("kafka-broker-1", config.MESSAGING_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"kafka-broker-1 is not running: {phase}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_controller_0_running", scope="session")
    def test_kafka_controller_0_running(self, get_pod_phase, config):
        """Verify kafka-controller-0 pod is running"""
        success, phase = get_pod_phase("kafka-controller-0", config.MESSAGING_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"kafka-controller-0 is not running: {phase}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_controller_1_running", scope="session")
    def test_kafka_controller_1_running(self, get_pod_phase, config):
        """Verify kafka-controller-1 pod is running"""
        success, phase = get_pod_phase("kafka-controller-1", config.MESSAGING_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"kafka-controller-1 is not running: {phase}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_topics_accessible", scope="session")
    def test_kafka_topics_accessible(self, kafka_exec):
        """
        Verify Kafka topics can be listed

        Dependencies: kafka_broker_0_running
        """
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
    @pytest.mark.dependency(name="kafka_connect_running", scope="session")
    def test_kafka_connect_running(self, get_deployment_ready_replicas, config):
        """
        Verify Kafka Connect deployment is running

        Dependencies: kafka_broker_0_running, kafka_broker_1_running
        """
        success, replicas = get_deployment_ready_replicas(
            "kafka-connect", config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get deployment status: {replicas}"
        assert replicas == "1", f"Kafka Connect not ready: {replicas}"

    @pytest.mark.health
    @pytest.mark.dependency(name="kafka_connect_api_available", scope="session")
    def test_kafka_connect_api_available(self, connect_exec):
        """
        Verify Kafka Connect REST API is responding

        Dependencies: kafka_connect_running
        """
        success, output = connect_exec("curl -s http://localhost:8083/")
        assert success, f"Failed to reach Connect API: {output}"
        assert "version" in output, f"Invalid API response: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="postgresql_sink_connector_exists", scope="session")
    def test_postgresql_sink_connector_exists(self, connect_exec):
        """
        Verify postgresql-sink connector is registered

        Dependencies: kafka_connect_api_available
        """
        success, output = connect_exec("curl -s http://localhost:8083/connectors")
        assert success, f"Failed to list connectors: {output}"
        assert "postgresql-sink" in output, f"Connector not found: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="postgresql_sink_connector_running", scope="session")
    def test_postgresql_sink_connector_running(self, connect_exec):
        """
        Verify postgresql-sink connector and task are running

        Dependencies: postgresql_sink_connector_exists
        """
        success, output = connect_exec(
            "curl -s http://localhost:8083/connectors/postgresql-sink/status"
        )
        assert success, f"Failed to get connector status: {output}"

        status = json.loads(output)

        if "error_code" in status:
            pytest.fail(f"Connector not found: {status.get('message', output)}")

        connector_state = status["connector"]["state"]
        assert connector_state == "RUNNING", f"Connector not running: {connector_state}"

        tasks = status["tasks"]
        assert len(tasks) > 0, "No tasks found"

        task_state = tasks[0]["state"]
        assert task_state == "RUNNING", f"Task not running: {task_state}"


# =============================================================================
# LAYER 1C: POSTGRESQL HEALTH (Independent Foundation)
# =============================================================================

class TestPostgreSQLHealth:
    """PostgreSQL health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(name="postgresql_running", scope="session")
    def test_postgresql_running(self, get_pod_phase, config):
        """Verify PostgreSQL pod is running"""
        success, phase = get_pod_phase("postgresql-0", config.DATA_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"PostgreSQL is not running: {phase}"

    @pytest.mark.health
    @pytest.mark.dependency(name="postgresql_accepting_connections", scope="session")
    def test_postgresql_accepting_connections(self, postgres_exec):
        """
        Verify PostgreSQL accepts connections

        Dependencies: postgresql_running
        """
        success, output = postgres_exec("psql -U postgres -d sensordata -c 'SELECT 1'")
        assert success, f"Failed to connect to PostgreSQL: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="analytics_data_table_exists", scope="session")
    def test_analytics_data_table_exists(self, postgres_exec):
        """
        Verify analytics_data table exists

        Dependencies: postgresql_accepting_connections
        """
        success, output = postgres_exec(
            "psql -U postgres -d sensordata -t -c "
            "\"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='analytics_data'\""
        )
        assert success, f"Failed to query tables: {output}"
        assert "1" in output, f"analytics_data table not found: {output}"


# =============================================================================
# LAYER 1D: FASTAPI HEALTH (Independent Foundation)
# =============================================================================

class TestFastAPIHealth:
    """FastAPI health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(name="fastapi_running", scope="session")
    def test_fastapi_running(self, fastapi_exec):
        """
        Verify FastAPI pod is running and accessible

        Dependencies: kafka_broker_0_running
        """
        success, output = fastapi_exec("echo OK")
        assert success, f"FastAPI pod is not reachable: {output}"
        assert "OK" in output, f"Unexpected output: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="fastapi_health_endpoint", scope="session")
    def test_fastapi_health_endpoint(self, fastapi_exec):
        """
        Verify FastAPI /health endpoint returns 200

        Dependencies: fastapi_running
        """
        success, output = fastapi_exec(
            'curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health'
        )
        assert success, f"Failed to reach health endpoint: {output}"
        assert "200" in output, f"Health endpoint returned non-200: {output}"

    @pytest.mark.health
    @pytest.mark.dependency(name="fastapi_ready_endpoint", scope="session")
    def test_fastapi_ready_endpoint(self, fastapi_exec):
        """
        Verify FastAPI /ready endpoint returns 200

        Dependencies: fastapi_health_endpoint
        """
        success, output = fastapi_exec(
            'curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/ready'
        )
        assert success, f"Failed to reach ready endpoint: {output}"
        assert "200" in output, f"Ready endpoint returned non-200: {output}"

    # =============================================================================
    # LAYER 1E: PROMETHEUS & GRAFANA HEALTH (Monitoring Foundation)
    # =============================================================================

class TestMonitoringHealth:
    """Prometheus & Grafana health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(name="prometheus_running", scope="session")
    def test_prometheus_running(self, get_pod_phase, get_pod_names, config):
        """
        Verify Prometheus pod is running (dynamic pod name)
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, f"No Prometheus pod found in {config.MONITORING_NAMESPACE} namespace"

        pod_name = prometheus_pods[0]
        success, phase = get_pod_phase(pod_name, config.MONITORING_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"Prometheus pod {pod_name} is not running: {phase}"

    @pytest.mark.health
    @pytest.mark.dependency(name="grafana_running", scope="session")
    def test_grafana_running(self, get_pod_phase, get_pod_names, config):
        """
        Verify Grafana pod is running (dynamic pod name)
        """
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, f"No Grafana pod found in {config.MONITORING_NAMESPACE} namespace"

        pod_name = grafana_pods[0]
        success, phase = get_pod_phase(pod_name, config.MONITORING_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"Grafana pod {pod_name} is not running: {phase}"


# =============================================================================
# LAYER 1F: SPARK HEALTH (Processing Foundation)
# =============================================================================

class TestSparkHealth:
    """Apache Spark health checks"""

    @pytest.mark.health
    @pytest.mark.dependency(name="spark_running", scope="session")
    def test_spark_running(self, get_pod_phase, get_pod_names, config):
        """
        Verify Spark pod is running (dynamic pod name)
        """
        pod_names = get_pod_names(config.DATA_NAMESPACE)
        spark_pods = [n for n in pod_names if n.startswith("spark-")]
        assert spark_pods, f"No Spark pod found in {config.DATA_NAMESPACE} namespace"

        pod_name = spark_pods[0]
        success, phase = get_pod_phase(pod_name, config.DATA_NAMESPACE)
        assert success, f"Failed to get pod status: {phase}"
        assert phase == "Running", f"Spark pod {pod_name} is not running: {phase}"
