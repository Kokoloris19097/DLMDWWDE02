"""
Connectivity Tests
Verify network communication between components

Test Dependency Hierarchy (Layer 2):
    Connectivity tests depend on the respective pods being healthy.
    If health checks fail, connectivity tests will be skipped.
"""

import pytest


# =============================================================================
# HELPER: TCP Connectivity Check Commands
# =============================================================================

def tcp_check_cmd(host: str, port: int) -> str:
    """Generate bash command for TCP connectivity check"""
    return f"/bin/bash -c 'echo > /dev/tcp/{host}/{port} && echo OK || echo FAIL'"


def tcp_check_with_nc_fallback(host: str, port: int) -> str:
    """Generate bash command with netcat fallback"""
    return (
        f"/bin/bash -c 'if command -v nc >/dev/null 2>&1; then "
        f"nc -z -w 3 {host} {port} && echo OK || echo FAIL; else "
        f"(echo > /dev/tcp/{host}/{port}) >/dev/null 2>&1 && echo OK || echo FAIL; fi'"
    )


# =============================================================================
# LAYER 2A: KAFKA INTERNAL CONNECTIVITY (Depends on Kafka Health)
# =============================================================================

class TestKafkaConnectivity:
    """Kafka network connectivity tests"""

    CONTROLLER_HOST = "kafka-controller-{}.kafka-controller.messaging.svc.cluster.local"
    BROKER_HOST = "kafka-broker-{}.kafka-broker.messaging.svc.cluster.local"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="broker_to_controller_0", scope="session")
    def test_broker_to_controller_0(self, kafka_exec):
        """
        Verify broker can reach controller-0

        Dependencies: kafka_broker_0_running, kafka_controller_0_running
        """
        host = self.CONTROLLER_HOST.format(0)
        success, output = kafka_exec(tcp_check_cmd(host, 9093))
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach controller-0: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="broker_to_controller_1", scope="session")
    def test_broker_to_controller_1(self, kafka_exec):
        """
        Verify broker can reach controller-1

        Dependencies: kafka_broker_0_running, kafka_controller_1_running
        """
        host = self.CONTROLLER_HOST.format(1)
        success, output = kafka_exec(tcp_check_cmd(host, 9093))
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach controller-1: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="broker_0_to_broker_1", scope="session")
    def test_broker_0_to_broker_1(self, kafka_exec):
        """
        Verify broker-0 can reach broker-1

        Dependencies: kafka_broker_0_running, kafka_broker_1_running
        """
        host = self.BROKER_HOST.format(1)
        success, output = kafka_exec(tcp_check_cmd(host, 9092))
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach broker-1: {output}"


# =============================================================================
# LAYER 2B: KAFKA CONNECT CONNECTIVITY (Depends on Connect Health)
# =============================================================================

class TestKafkaConnectConnectivity:
    """Kafka Connect network connectivity tests"""

    KAFKA_BROKER_HOST = "kafka.messaging.svc.cluster.local"
    POSTGRESQL_HOST = "postgresql.data.svc.cluster.local"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="connect_to_kafka_broker", scope="session")
    def test_connect_to_kafka_broker(self, connect_exec):
        """
        Verify Kafka Connect can reach Kafka broker

        Dependencies: kafka_connect_running, kafka_broker_0_running
        """
        success, output = connect_exec(
            tcp_check_cmd(self.KAFKA_BROKER_HOST, 9092)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach Kafka broker: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="connect_to_postgresql", scope="session")
    def test_connect_to_postgresql(self, connect_exec):
        """
        Verify Kafka Connect can reach PostgreSQL

        Dependencies: kafka_connect_running, postgresql_running
        """
        success, output = connect_exec(
            tcp_check_with_nc_fallback(self.POSTGRESQL_HOST, 5432)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach PostgreSQL: {output}"


# =============================================================================
# LAYER 2C: KUBERNETES ENDPOINTS (Depends on Pod Health)
# =============================================================================

class TestEndpointsAvailable:
    """Verify Kubernetes endpoints are properly configured"""

    def _assert_endpoints_exist(self, success: bool, output: str, service: str):
        """Common assertion for endpoint checks"""
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for {service}: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="kafka_broker_endpoints", scope="session")
    def test_kafka_broker_endpoints(self, get_endpoints, config):
        """
        Verify kafka-broker service has endpoints

        Dependencies: kafka_broker_0_running, kafka_broker_1_running
        """
        success, output = get_endpoints("kafka-broker", config.MESSAGING_NAMESPACE)
        self._assert_endpoints_exist(success, output, "kafka-broker")

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="kafka_controller_endpoints", scope="session")
    def test_kafka_controller_endpoints(self, get_endpoints, config):
        """
        Verify kafka-controller service has endpoints

        Dependencies: kafka_controller_0_running, kafka_controller_1_running
        """
        success, output = get_endpoints("kafka-controller", config.MESSAGING_NAMESPACE)
        self._assert_endpoints_exist(success, output, "kafka-controller")

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="postgresql_endpoints", scope="session")
    def test_postgresql_endpoints(self, get_endpoints, config):
        """
        Verify postgresql service has endpoints

        Dependencies: postgresql_running
        """
        success, output = get_endpoints("postgresql", config.DATA_NAMESPACE)
        self._assert_endpoints_exist(success, output, "postgresql")


# =============================================================================
# LAYER 2D: FASTAPI CONNECTIVITY (Depends on FastAPI and DB Health)
# =============================================================================

class TestFastAPIConnectivity:
    """FastAPI network connectivity tests"""

    KAFKA_BROKER_HOST = "kafka.messaging.svc.cluster.local"
    POSTGRESQL_HOST = "postgresql.data.svc.cluster.local"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="fastapi_to_kafka_broker", scope="session")
    def test_fastapi_to_kafka_broker(self, fastapi_exec):
        """
        Verify FastAPI can reach Kafka broker

        Dependencies: fastapi_running, kafka_broker_0_running
        """
        success, output = fastapi_exec(
            tcp_check_with_nc_fallback(self.KAFKA_BROKER_HOST, 9092)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"FastAPI cannot reach Kafka broker: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="fastapi_to_postgresql", scope="session")
    def test_fastapi_to_postgresql(self, fastapi_exec):
        """
        Verify FastAPI can reach PostgreSQL

        Dependencies: fastapi_running, postgresql_running
        """
        success, output = fastapi_exec(
            tcp_check_with_nc_fallback(self.POSTGRESQL_HOST, 5432)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"FastAPI cannot reach PostgreSQL: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="fastapi_db_connection", scope="session")
    def test_fastapi_db_connection(self, fastapi_exec):
        """
        Verify FastAPI can establish database connection using psycopg2

        Dependencies: fastapi_to_postgresql
        """
        # Use Python psycopg2 to test DB connectivity (available in FastAPI container)
        cmd = (
            "python -c \"import psycopg2; "
            "conn = psycopg2.connect("
            "host='postgresql.data.svc.cluster.local', "
            "port=5432, "
            "user='appuser', "
            "password='appuser-secure-pw', "
            "dbname='sensordata', "
            "connect_timeout=5); "
            "cur = conn.cursor(); "
            "cur.execute('SELECT 1'); "
            "result = cur.fetchone(); "
            "cur.close(); "
            "conn.close(); "
            "print('OK' if result[0] == 1 else 'FAIL')\""
        )
        success, output = fastapi_exec(cmd)

        assert success, f"Failed to execute psycopg2 test: {output}"
        assert "OK" in output, f"Database connection test failed: {output}"


# =============================================================================
# LAYER 2E: MONITORING CONNECTIVITY (Depends on Monitoring Health)
# =============================================================================

class TestMonitoringConnectivity:
    """Monitoring stack network connectivity tests"""

    PROMETHEUS_HOST = "prometheus.monitoring.svc.cluster.local"
    GRAFANA_HOST = "grafana.monitoring.svc.cluster.local"
    KUBERNETES_API = "kubernetes.default.svc"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="grafana_to_prometheus", scope="session")
    def test_grafana_to_prometheus(self, kubectl, get_pod_names, config):
        """
        Verify Grafana can reach Prometheus (datasource connection)

        Dependencies: grafana_running, prometheus_running
        """
        # Get Grafana pod name dynamically
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found for connectivity test"

        grafana_pod = grafana_pods[0]

        # Test TCP connectivity to Prometheus
        cmd = f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- {tcp_check_with_nc_fallback(self.PROMETHEUS_HOST, 9090)}"
        success, output = kubectl(cmd)

        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Grafana cannot reach Prometheus: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="grafana_prometheus_api", scope="session")
    def test_grafana_prometheus_api(self, kubectl, get_pod_names, config):
        """
        Verify Grafana can query Prometheus API

        Dependencies: grafana_to_prometheus
        """
        # Get Grafana pod name dynamically
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        grafana_pods = [n for n in pod_names if n.startswith("grafana-")]
        assert grafana_pods, "No Grafana pod found for API test"

        grafana_pod = grafana_pods[0]

        # Test Prometheus API health endpoint
        cmd = f"exec -n {config.MONITORING_NAMESPACE} {grafana_pod} -- curl -s -o /dev/null -w '%{{http_code}}' http://{self.PROMETHEUS_HOST}:9090/-/healthy"
        success, output = kubectl(cmd)

        assert success, f"Failed to query Prometheus API: {output}"
        assert "200" in output, f"Prometheus API not healthy: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="prometheus_to_kubernetes_api", scope="session")
    def test_prometheus_to_kubernetes_api(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus can query its own API and has service discovery configured
        (Validates that Prometheus is operational and can perform metric queries)

        Dependencies: prometheus_running
        """
        # Get Prometheus pod name dynamically
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found for API connectivity test"

        prometheus_pod = prometheus_pods[0]

        # Test 1: Verify Prometheus API is responding
        cmd = f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- wget -q -O- http://localhost:9090/api/v1/status/config"
        success, output = kubectl(cmd)

        assert success, f"Failed to query Prometheus configuration: {output}"
        assert '"status":"success"' in output, f"Prometheus API returned error: {output}"
        assert "kubernetes-" in output, f"No Kubernetes service discovery configured in Prometheus: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="prometheus_scrape_targets", scope="session")
    def test_prometheus_scrape_targets(self, kubectl, config):
        """
        Verify Prometheus has active scrape targets

        Dependencies: prometheus_to_kubernetes_api
        """
        # Query Prometheus API for active targets
        cmd = f"exec -n {config.MONITORING_NAMESPACE} deployment/prometheus -- wget -q -O- http://localhost:9090/api/v1/targets"
        success, output = kubectl(cmd)

        assert success, f"Failed to query Prometheus targets: {output}"
        assert '"status":"success"' in output, f"Prometheus API returned error: {output}"
        assert '"activeTargets"' in output, f"No active targets found: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="prometheus_to_cadvisor", scope="session")
    def test_prometheus_to_cadvisor(self, kubectl, get_pod_names, config):
        """
        Verify Prometheus can scrape cAdvisor metrics from Kubelet

        Dependencies: prometheus_running
        """
        # Get Prometheus pod name dynamically
        pod_names = get_pod_names(config.MONITORING_NAMESPACE)
        prometheus_pods = [n for n in pod_names if n.startswith("prometheus-")]
        assert prometheus_pods, "No Prometheus pod found for cAdvisor test"

        prometheus_pod = prometheus_pods[0]

        # Query for container_cpu_usage_seconds_total metric (provided by cAdvisor)
        cmd = (
            f"exec -n {config.MONITORING_NAMESPACE} {prometheus_pod} -- "
            f"wget -q -O- 'http://localhost:9090/api/v1/query?query=up{{job=\"kubernetes-cadvisor\"}}'"
        )
        success, output = kubectl(cmd)

        assert success, f"Failed to query cAdvisor metrics: {output}"
        assert '"status":"success"' in output, f"Query failed: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="monitoring_endpoints", scope="session")
    def test_monitoring_endpoints(self, get_endpoints, config):
        """
        Verify monitoring service endpoints are available

        Dependencies: prometheus_running, grafana_running
        """
        # Check Prometheus endpoints
        success, output = get_endpoints("prometheus", config.MONITORING_NAMESPACE)
        assert success, f"Failed to get Prometheus endpoints: {output}"
        assert output != "", f"No endpoints for Prometheus service: {output}"

        # Check Grafana endpoints
        success, output = get_endpoints("grafana", config.MONITORING_NAMESPACE)
        assert success, f"Failed to get Grafana endpoints: {output}"
        assert output != "", f"No endpoints for Grafana service: {output}"


# =============================================================================
# LAYER 2F: SPARK CONNECTIVITY (Depends on Spark Health)
# =============================================================================

class TestSparkConnectivity:
    """Apache Spark network connectivity tests"""

    KAFKA_BROKER_HOST = "kafka.messaging.svc.cluster.local"
    KAFKA_BROKER_0_HOST = "kafka-broker-0.kafka-broker.messaging.svc.cluster.local"
    KAFKA_BROKER_1_HOST = "kafka-broker-1.kafka-broker.messaging.svc.cluster.local"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="spark_to_kafka_broker", scope="session")
    def test_spark_to_kafka_broker(self, spark_exec):
        """
        Verify Spark can reach Kafka broker service

        Dependencies: spark_running, kafka_broker_0_running
        """
        success, output = spark_exec(
            tcp_check_cmd(self.KAFKA_BROKER_HOST, 9092)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Spark cannot reach Kafka broker: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="spark_to_kafka_broker_0", scope="session")
    def test_spark_to_kafka_broker_0(self, spark_exec):
        """
        Verify Spark can reach Kafka broker-0 directly

        Dependencies: spark_running, kafka_broker_0_running
        """
        success, output = spark_exec(
            tcp_check_cmd(self.KAFKA_BROKER_0_HOST, 9092)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Spark cannot reach Kafka broker-0: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="spark_to_kafka_broker_1", scope="session")
    def test_spark_to_kafka_broker_1(self, spark_exec):
        """
        Verify Spark can reach Kafka broker-1 directly

        Dependencies: spark_running, kafka_broker_1_running
        """
        success, output = spark_exec(
            tcp_check_cmd(self.KAFKA_BROKER_1_HOST, 9092)
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Spark cannot reach Kafka broker-1: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="spark_kafka_topics_list", scope="session")
    def test_spark_kafka_topics_list(self, spark_exec, config):
        """
        Verify Spark can list Kafka topics using kafka-python

        Dependencies: spark_to_kafka_broker
        """
        # Test with kafka-python library (python3 in Spark container)
        cmd = (
            "python3 -c \"from kafka import KafkaAdminClient; "
            f"admin = KafkaAdminClient(bootstrap_servers='{self.KAFKA_BROKER_HOST}:9092', request_timeout_ms=5000); "
            "topics = admin.list_topics(); "
            "print('OK' if topics else 'FAIL'); "
            "admin.close()\""
        )
        success, output = spark_exec(cmd)
        assert success, f"Failed to execute kafka-python test: {output}"
        assert "OK" in output, f"Kafka topics listing failed: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="spark_kafka_produce_test", scope="session")
    def test_spark_kafka_produce_test(self, spark_exec, config):
        """
        Verify Spark can produce messages to Kafka

        Dependencies: spark_kafka_topics_list
        """
        test_topic = "spark-connectivity-test"
        test_message = "connectivity-test-message"

        # Produce a test message to Kafka (python3)
        cmd = (
            "python3 -c \"from kafka import KafkaProducer; "
            f"producer = KafkaProducer(bootstrap_servers='{self.KAFKA_BROKER_HOST}:9092', request_timeout_ms=10000); "
            f"future = producer.send('{test_topic}', b'{test_message}'); "
            "result = future.get(timeout=10); "
            "producer.flush(); "
            "producer.close(); "
            "print('OK')\""
        )
        success, output = spark_exec(cmd)
        assert success, f"Failed to produce message: {output}"
        assert "OK" in output, f"Kafka produce test failed: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(name="spark_kafka_consume_test", scope="session")
    def test_spark_kafka_consume_test(self, spark_exec, config):
        """
        Verify Spark can consume messages from Kafka

        Dependencies: spark_kafka_produce_test
        """
        test_topic = "spark-connectivity-test"

        # Consume the test message from Kafka (python3)
        cmd = (
            "python3 -c \"from kafka import KafkaConsumer; "
            "import time; "
            f"consumer = KafkaConsumer('{test_topic}', "
            f"bootstrap_servers='{self.KAFKA_BROKER_HOST}:9092', "
            "auto_offset_reset='earliest', "
            "consumer_timeout_ms=10000, "
            "enable_auto_commit=False); "
            "messages = []; "
            "for msg in consumer: "
            "    messages.append(msg.value); "
            "    break; "
            "consumer.close(); "
            "print('OK' if messages else 'FAIL')\""
        )
        success, output = spark_exec(cmd)
        assert success, f"Failed to consume message: {output}"
        assert "OK" in output, f"Kafka consume test failed: {output}"
