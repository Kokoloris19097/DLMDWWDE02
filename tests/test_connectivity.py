"""
Connectivity Tests
Verify network communication between components

Test Dependency Hierarchy (Layer 2):
    Connectivity tests depend on the respective pods being healthy.
    If health checks fail, connectivity tests will be skipped.
"""

import pytest


# =============================================================================
# LAYER 2A: KAFKA INTERNAL CONNECTIVITY (Depends on Kafka Health)
# =============================================================================

class TestKafkaConnectivity:
    """Kafka network connectivity tests"""

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="broker_to_controller_0",
        depends=["kafka_broker_0_running", "kafka_controller_0_running"]
    )
    def test_broker_to_controller_0(self, kafka_exec):
        """Verify broker can reach controller-0"""
        success, output = kafka_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-controller-0.kafka-controller.messaging.svc.cluster.local/9093 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach controller-0: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="broker_to_controller_1",
        depends=["kafka_broker_0_running", "kafka_controller_1_running"]
    )
    def test_broker_to_controller_1(self, kafka_exec):
        """Verify broker can reach controller-1"""
        success, output = kafka_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-controller-1.kafka-controller.messaging.svc.cluster.local/9093 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach controller-1: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="broker_0_to_broker_1",
        depends=["kafka_broker_0_running", "kafka_broker_1_running"]
    )
    def test_broker_0_to_broker_1(self, kafka_exec):
        """Verify broker-0 can reach broker-1"""
        success, output = kafka_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-broker-1.kafka-broker.messaging.svc.cluster.local/9092 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach broker-1: {output}"


# =============================================================================
# LAYER 2B: KAFKA CONNECT CONNECTIVITY (Depends on Connect Health)
# =============================================================================

class TestKafkaConnectConnectivity:
    """Kafka Connect network connectivity tests"""

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="connect_to_kafka_broker",
        depends=["kafka_connect_running", "kafka_broker_0_running"]
    )
    def test_connect_to_kafka_broker(self, connect_exec):
        """Verify Kafka Connect can reach Kafka broker"""
        host = "kafka.messaging.svc.cluster.local"
        port = 9092

        cmd = f'/bin/bash -c "echo > /dev/tcp/{host}/{port} && echo OK || echo FAIL"'

        success, output = connect_exec(cmd)
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach Kafka broker: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="connect_to_postgresql",
        depends=["kafka_connect_running", "postgresql_running"]
    )
    def test_connect_to_postgresql(self, connect_exec):
        """Verify Kafka Connect can reach PostgreSQL"""
        host = "postgresql.data.svc.cluster.local"
        port = 5432
        cmd = (
            "/bin/bash -c 'if command -v nc >/dev/null 2>&1; then nc -z -w 3 "
            + f"{host} {port} && echo OK || echo FAIL; else (echo > /dev/tcp/{host}/{port}) >/dev/null 2>&1 && echo OK || echo FAIL; fi'"
        )

        success, output = connect_exec(cmd)
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach PostgreSQL: {output}"


# =============================================================================
# LAYER 2C: KUBERNETES ENDPOINTS (Depends on Pod Health)
# =============================================================================

class TestEndpointsAvailable:
    """Verify Kubernetes endpoints are properly configured"""

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="kafka_broker_endpoints",
        depends=["kafka_broker_0_running", "kafka_broker_1_running"]
    )
    def test_kafka_broker_endpoints(self, kubectl, config):
        """Verify kafka-broker service has endpoints"""
        success, output = kubectl(
            "get endpoints kafka-broker -o jsonpath={.subsets[0].addresses[*].ip}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for kafka-broker: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="kafka_controller_endpoints",
        depends=["kafka_controller_0_running", "kafka_controller_1_running"]
    )
    def test_kafka_controller_endpoints(self, kubectl, config):
        """Verify kafka-controller service has endpoints"""
        success, output = kubectl(
            "get endpoints kafka-controller -o jsonpath={.subsets[0].addresses[*].ip}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for kafka-controller: {output}"

    @pytest.mark.connectivity
    @pytest.mark.dependency(
        name="postgresql_endpoints",
        depends=["postgresql_running"]
    )
    def test_postgresql_endpoints(self, kubectl, config):
        """Verify postgresql service has endpoints"""
        success, output = kubectl(
            "get endpoints postgresql -o jsonpath={.subsets[0].addresses[*].ip}",
            namespace=config.DATA_NAMESPACE
        )
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for postgresql: {output}"
