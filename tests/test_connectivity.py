"""
Connectivity Tests
Verify network communication between components
"""

import pytest


class TestKafkaConnectivity:
    """Kafka network connectivity tests"""

    @pytest.mark.connectivity
    def test_broker_to_controller_0(self, kafka_exec):
        """Verify broker can reach controller-0"""
        success, output = kafka_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-controller-0.kafka-controller.messaging.svc.cluster.local/9093 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach controller-0: {output}"

    @pytest.mark.connectivity
    def test_broker_to_controller_1(self, kafka_exec):
        """Verify broker can reach controller-1"""
        success, output = kafka_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-controller-1.kafka-controller.messaging.svc.cluster.local/9093 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach controller-1: {output}"

    @pytest.mark.connectivity
    def test_broker_0_to_broker_1(self, kafka_exec):
        """Verify broker-0 can reach broker-1"""
        success, output = kafka_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-broker-1.kafka-broker.messaging.svc.cluster.local/9092 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach broker-1: {output}"


class TestKafkaConnectConnectivity:
    """Kafka Connect network connectivity tests"""

    @pytest.mark.connectivity
    def test_connect_to_kafka_broker(self, connect_exec):
        """Verify Kafka Connect can reach Kafka broker"""
        success, output = connect_exec(
            "/bin/bash -c 'echo > /dev/tcp/kafka-broker-0.kafka-broker.messaging.svc.cluster.local/9092 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach Kafka broker: {output}"

    @pytest.mark.connectivity
    def test_connect_to_postgresql(self, connect_exec):
        """Verify Kafka Connect can reach PostgreSQL"""
        success, output = connect_exec(
            "/bin/bash -c 'echo > /dev/tcp/postgresql.data.svc.cluster.local/5432 && echo OK || echo FAIL'"
        )
        assert success, f"Command failed: {output}"
        assert "OK" in output, f"Cannot reach PostgreSQL: {output}"


class TestEndpointsAvailable:
    """Verify Kubernetes endpoints are properly configured"""

    @pytest.mark.connectivity
    def test_kafka_broker_endpoints(self, kubectl, config):
        """Verify kafka-broker service has endpoints"""
        success, output = kubectl(
            "get endpoints kafka-broker -o jsonpath={.subsets[0].addresses[*].ip}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for kafka-broker: {output}"

    @pytest.mark.connectivity
    def test_kafka_controller_endpoints(self, kubectl, config):
        """Verify kafka-controller service has endpoints"""
        success, output = kubectl(
            "get endpoints kafka-controller -o jsonpath={.subsets[0].addresses[*].ip}",
            namespace=config.MESSAGING_NAMESPACE
        )
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for kafka-controller: {output}"

    @pytest.mark.connectivity
    def test_postgresql_endpoints(self, kubectl, config):
        """Verify postgresql service has endpoints"""
        success, output = kubectl(
            "get endpoints postgresql -o jsonpath={.subsets[0].addresses[*].ip}",
            namespace=config.DATA_NAMESPACE
        )
        assert success, f"Failed to get endpoints: {output}"
        assert output != "", f"No endpoints for postgresql: {output}"
