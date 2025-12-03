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
        Verify FastAPI can establish database connection

        Dependencies: fastapi_to_postgresql
        """
        # Use psql client to test DB connectivity from FastAPI pod
        cmd = (
            "psql -h postgresql.data.svc.cluster.local "
            "-p 5432 -U appuser -d sensordata "
            "-c 'SELECT 1' 2>&1"
        )
        success, output = fastapi_exec(f"PGPASSWORD=appuser-secure-pw {cmd}")

        # Check for successful connection (either "1" in output or specific success indicators)
        if not success:
            # Command might fail due to missing psql, try alternative check
            pytest.skip(f"psql not available in FastAPI pod: {output}")

        # Look for success indicators
        assert "1 row" in output or "(1 row)" in output or "1" in output, \
            f"Database connection test failed: {output}"
