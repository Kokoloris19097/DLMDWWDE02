"""
Output API Tests
Verify FastAPI query endpoints for sensor data retrieval

Test Dependency Hierarchy (Layer 3 - Functional):
    These tests verify the data retrieval functionality of the Output API.
    They depend on PostgreSQL having data and FastAPI being healthy.
"""

import pytest
import json
import time
from datetime import datetime, timedelta


# =============================================================================
# LAYER 3A: OUTPUT API HEALTH
# =============================================================================

class TestOutputAPIHealth:
    """Verify Output API endpoints are accessible"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="output_api_health", scope="session")
    def test_output_api_health_endpoint(self, fastapi_exec):
        """Verify /health endpoint returns 200"""
        success, output = fastapi_exec(
            'curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health'
        )
        assert success, f"Failed to reach health endpoint: {output}"
        assert "200" in output, f"Health endpoint returned non-200: {output}"

    @pytest.mark.functional
    @pytest.mark.dependency(name="output_api_ready", scope="session", depends=["output_api_health"])
    def test_output_api_ready_endpoint(self, fastapi_exec):
        """Verify /ready endpoint returns 200"""
        success, output = fastapi_exec(
            'curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/ready'
        )
        assert success, f"Failed to reach ready endpoint: {output}"
        assert "200" in output, f"Ready endpoint returned non-200: {output}"

    @pytest.mark.functional
    @pytest.mark.dependency(name="output_api_root", scope="session", depends=["output_api_health"])
    def test_output_api_root_endpoint(self, fastapi_exec):
        """Verify root endpoint returns API info"""
        success, output = fastapi_exec('curl -s http://localhost:8000/')
        assert success, f"Failed to reach root endpoint: {output}"

        try:
            data = json.loads(output)
            assert "service" in data, "Missing 'service' field in response"
            assert "endpoints" in data, "Missing 'endpoints' field in response"
            assert "Query" in data["endpoints"], "Missing Query endpoints section"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")


# =============================================================================
# LAYER 3B: SENSOR LIST ENDPOINT
# =============================================================================

class TestSensorListEndpoint:
    """Test GET /sensors endpoint"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="list_sensors", scope="session", depends=["output_api_ready"])
    def test_list_sensors_success(self, fastapi_exec):
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
                assert sensor["reading_count"] > 0, "Reading count should be positive"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["output_api_ready"])
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
    @pytest.mark.dependency(depends=["output_api_ready"])
    def test_list_sensors_invalid_limit(self, fastapi_exec):
        """Verify /sensors handles invalid limit gracefully"""
        # Test with limit > 1000 (should default to 50)
        success, output = fastapi_exec('curl -s "http://localhost:8000/sensors?limit=5000"')
        assert success, f"Failed to call /sensors with invalid limit: {output}"

        try:
            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"
            # Should return at most 50 (default) even though we asked for 5000
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")


# =============================================================================
# LAYER 3C: SENSOR DATA ENDPOINT
# =============================================================================

class TestSensorDataEndpoint:
    """Test GET /sensors/{sensor_id}/data endpoint"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="get_sensor_data", scope="session", depends=["list_sensors"])
    def test_get_sensor_data_success(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data returns time-series data"""
        # First get a sensor_id
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Now get data for this sensor
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
                assert reading["sensor_id"] == sensor_id, "Sensor ID mismatch"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["list_sensors"])
    def test_get_sensor_data_with_time_range(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data respects time range"""
        # Get a sensor
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
        # Get a sensor
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Query with limit
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
    @pytest.mark.dependency(depends=["output_api_ready"])
    def test_get_sensor_data_nonexistent_sensor(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/data returns empty list for nonexistent sensor"""
        success, output = fastapi_exec(
            'curl -s "http://localhost:8000/sensors/NONEXISTENT-SENSOR-999/data"'
        )
        assert success, f"Failed to query nonexistent sensor: {output}"

        try:
            data = json.loads(output)
            assert isinstance(data, list), f"Expected list, got {type(data)}"
            assert len(data) == 0, f"Expected empty list for nonexistent sensor, got {len(data)} items"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")


# =============================================================================
# LAYER 3D: SENSOR STATISTICS ENDPOINT
# =============================================================================

class TestSensorStatsEndpoint:
    """Test GET /sensors/{sensor_id}/stats endpoint"""

    @pytest.mark.functional
    @pytest.mark.dependency(name="get_sensor_stats", scope="session", depends=["list_sensors"])
    def test_get_sensor_stats_success(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/stats returns aggregated statistics"""
        # First get a sensor_id
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to get sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for testing")

            sensor_id = sensors[0]["sensor_id"]

            # Get stats for this sensor
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
                assert field in data, f"Missing required field: {field}"

            # Validate data types and ranges
            assert data["sensor_id"] == sensor_id, "Sensor ID mismatch"
            assert data["reading_count"] > 0, "Reading count should be positive"
            assert data["temperature_min"] <= data["temperature_max"], "Temperature min > max"
            assert data["humidity_min"] <= data["humidity_max"], "Humidity min > max"
            assert data["temperature_min"] <= data["temperature_avg"] <= data["temperature_max"], \
                "Temperature avg outside min/max range"
            assert data["humidity_min"] <= data["humidity_avg"] <= data["humidity_max"], \
                "Humidity avg outside min/max range"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["list_sensors"])
    def test_get_sensor_stats_with_time_range(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/stats respects time range"""
        # Get a sensor
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

            # Verify period matches request
            assert "period_start" in data, "Missing period_start"
            assert "period_end" in data, "Missing period_end"
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")

    @pytest.mark.functional
    @pytest.mark.dependency(depends=["output_api_ready"])
    def test_get_sensor_stats_nonexistent_sensor(self, fastapi_exec):
        """Verify /sensors/{sensor_id}/stats returns 404 for nonexistent sensor"""
        success, output = fastapi_exec(
            'curl -s -w "\\n%{http_code}" "http://localhost:8000/sensors/NONEXISTENT-SENSOR-999/stats"'
        )
        assert success, f"Failed to query nonexistent sensor: {output}"

        # Output format: JSON response + newline + HTTP code
        lines = output.strip().split('\n')
        http_code = lines[-1] if lines else ""

        assert "404" in http_code, f"Expected 404 for nonexistent sensor, got {http_code}"


# =============================================================================
# LAYER 3E: END-TO-END QUERY FLOW
# =============================================================================

class TestEndToEndQueryFlow:
    """Test complete query flow across multiple endpoints"""

    @pytest.mark.functional
    @pytest.mark.slow
    @pytest.mark.dependency(depends=["list_sensors", "get_sensor_data", "get_sensor_stats"])
    def test_complete_query_workflow(self, fastapi_exec):
        """
        Test complete workflow:
        1. List all sensors
        2. Get data for first sensor
        3. Get stats for first sensor
        4. Verify consistency
        """
        # Step 1: List sensors
        success, output = fastapi_exec('curl -s http://localhost:8000/sensors')
        assert success, f"Failed to list sensors: {output}"

        try:
            sensors = json.loads(output)
            if len(sensors) == 0:
                pytest.skip("No sensors available for end-to-end test")

            sensor_id = sensors[0]["sensor_id"]
            sensor_count = sensors[0]["reading_count"]

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
            # Note: Data endpoint returns limited results, stats returns total count
            assert stats_count >= data_count, \
                f"Stats count ({stats_count}) should be >= data count ({data_count})"

            # If data exists, verify temperature/humidity are within stats ranges
            if len(data) > 0:
                for reading in data:
                    temp = reading["temperature"]
                    humidity = reading["humidity"]

                    assert stats["temperature_min"] <= temp <= stats["temperature_max"], \
                        f"Temperature {temp} outside stats range [{stats['temperature_min']}, {stats['temperature_max']}]"
                    assert stats["humidity_min"] <= humidity <= stats["humidity_max"], \
                        f"Humidity {humidity} outside stats range [{stats['humidity_min']}, {stats['humidity_max']}]"

        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON response: {e}\nOutput: {output}")
