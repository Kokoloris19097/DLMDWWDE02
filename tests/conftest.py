"""
Pytest Configuration and Fixtures
Centralized fixtures for Kubernetes-based integration tests
"""

import pytest
import subprocess
import json
import uuid
import shlex
from dataclasses import dataclass
from typing import Tuple, Callable


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass(frozen=True)
class TestConfig:
    """Immutable test configuration"""
    MESSAGING_NAMESPACE: str = "messaging"
    DATA_NAMESPACE: str = "data"
    KAFKA_TOPIC: str = "analytics-data"
    POSTGRESQL_DB: str = "sensordata"
    POSTGRESQL_TABLE: str = "sensor_readings"
    COMMAND_TIMEOUT: int = 30
    # Pod/Deployment names
    KAFKA_BROKER_POD: str = "kafka-broker-0"
    POSTGRESQL_POD: str = "postgresql-0"
    # Deployment label selectors (for pods without fixed names)
    KAFKA_CONNECT_SELECTOR: str = "app=kafka-connect"


@pytest.fixture(scope="session")
def config() -> TestConfig:
    """Provide test configuration"""
    return TestConfig()


# =============================================================================
# KUBECTL HELPERS
# =============================================================================

def run_kubectl(args: list[str], timeout: int = 30, input_data: str = None) -> Tuple[bool, str]:
    """Execute kubectl command and return (success, output)"""
    try:
        result = subprocess.run(
            ["kubectl"] + args,
            capture_output=True,
            text=True,
            timeout=timeout,
            input=input_data
        )
        output = result.stdout if result.returncode == 0 else result.stderr
        return result.returncode == 0, output.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def run_kubectl_exec(
    namespace: str,
    pod: str,
    command: str,
    timeout: int = 30,
    use_shell: bool = False
) -> Tuple[bool, str]:
    """
    Execute command in a pod.

    Args:
        namespace: Kubernetes namespace
        pod: Pod name or selector (if starts with 'selector=')
        command: Command to execute
        timeout: Command timeout
        use_shell: If True, wrap command in sh -c
    """
    # Build base kubectl command
    args = ["-n", namespace, "exec"]

    # Handle deployment selector vs direct pod name
    if pod.startswith("selector="):
        selector = pod.replace("selector=", "")
        # Get first pod matching selector
        get_pod_args = [
            "-n", namespace, "get", "pod",
            "-l", selector,
            "-o", "jsonpath={.items[0].metadata.name}"
        ]
        success, pod_name = run_kubectl(get_pod_args, timeout)
        if not success or not pod_name:
            return False, f"No pod found for selector {selector}: {pod_name}"
        args.append(pod_name)
    else:
        args.append(pod)

    args.append("--")

    # Handle command execution
    if use_shell:
        args.extend(["sh", "-c", command])
    else:
        # Use shlex to properly split command while preserving quotes
        try:
            args.extend(shlex.split(command))
        except ValueError:
            # Fallback to shell execution if shlex fails
            args.extend(["sh", "-c", command])

    return run_kubectl(args, timeout)


@pytest.fixture(scope="session")
def kubectl(config) -> Callable:
    """Fixture for running kubectl commands"""
    def _kubectl(command: str, namespace: str = None) -> Tuple[bool, str]:
        args = []
        if namespace:
            args.extend(["-n", namespace])
        args.extend(shlex.split(command))
        return run_kubectl(args, config.COMMAND_TIMEOUT)
    return _kubectl


# =============================================================================
# POD EXECUTION FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def kafka_exec(config) -> Callable[[str], Tuple[bool, str]]:
    """Execute command in kafka-broker-0"""
    def execute(command: str) -> Tuple[bool, str]:
        return run_kubectl_exec(
            config.MESSAGING_NAMESPACE,
            config.KAFKA_BROKER_POD,
            command,
            config.COMMAND_TIMEOUT,
            use_shell=True
        )
    return execute


@pytest.fixture(scope="session")
def connect_exec(config) -> Callable[[str], Tuple[bool, str]]:
    """Execute command in kafka-connect pod (found via selector)"""
    def execute(command: str) -> Tuple[bool, str]:
        return run_kubectl_exec(
            config.MESSAGING_NAMESPACE,
            f"selector={config.KAFKA_CONNECT_SELECTOR}",
            command,
            config.COMMAND_TIMEOUT,
            use_shell=True
        )
    return execute


@pytest.fixture(scope="session")
def postgres_exec(config) -> Callable[[str], Tuple[bool, str]]:
    """Execute command in postgresql-0"""
    def execute(command: str) -> Tuple[bool, str]:
        return run_kubectl_exec(
            config.DATA_NAMESPACE,
            config.POSTGRESQL_POD,
            command,
            config.COMMAND_TIMEOUT,
            use_shell=True
        )
    return execute


# =============================================================================
# POD STATUS HELPERS
# =============================================================================

@pytest.fixture(scope="session")
def get_pod_phase(config) -> Callable:
    """Get the phase of a pod"""
    def _get_phase(pod_name: str, namespace: str) -> Tuple[bool, str]:
        args = [
            "-n", namespace,
            "get", "pod", pod_name,
            "-o", "jsonpath={.status.phase}"
        ]
        return run_kubectl(args, config.COMMAND_TIMEOUT)
    return _get_phase


@pytest.fixture(scope="session")
def get_deployment_ready_replicas(config) -> Callable:
    """Get ready replicas count for a deployment"""
    def _get_replicas(deployment_name: str, namespace: str) -> Tuple[bool, str]:
        args = [
            "-n", namespace,
            "get", "deployment", deployment_name,
            "-o", "jsonpath={.status.readyReplicas}"
        ]
        return run_kubectl(args, config.COMMAND_TIMEOUT)
    return _get_replicas


@pytest.fixture(scope="session")
def get_endpoints(config) -> Callable:
    """Get endpoint IPs for a service"""
    def _get_endpoints(service_name: str, namespace: str) -> Tuple[bool, str]:
        args = [
            "-n", namespace,
            "get", "endpoints", service_name,
            "-o", "jsonpath={.subsets[0].addresses[*].ip}"
        ]
        return run_kubectl(args, config.COMMAND_TIMEOUT)
    return _get_endpoints


# =============================================================================
# TEST DATA FIXTURES
# =============================================================================

@pytest.fixture
def test_message():
    """Generate unique test message for pipeline tests"""
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    message = json.dumps({
        "schema": {
            "type": "struct",
            "fields": [
                {"field": "sensor_id", "type": "string"},
                {"field": "temperature", "type": "double"},
                {"field": "humidity", "type": "int32"},
                {"field": "timestamp", "type": "int64", "name": "org.apache.kafka.connect.data.Timestamp"}
            ]
        },
        "payload": {
            "sensor_id": test_id,
            "temperature": 22.5,
            "humidity": 55,
            "timestamp": 1699000000000
        }
    })
    return {"id": test_id, "message": message}


# =============================================================================
# PYTEST CONFIGURATION
# =============================================================================

def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line("markers", "health: Health check tests (Layer 1)")
    config.addinivalue_line("markers", "connectivity: Connectivity tests (Layer 2)")
    config.addinivalue_line("markers", "functional: Functional/pipeline tests (Layer 3)")
    config.addinivalue_line("markers", "slow: Tests that take longer to execute")
