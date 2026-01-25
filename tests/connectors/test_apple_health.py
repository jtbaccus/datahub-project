"""Tests for Apple Health XML import connector."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile

from datahub.connectors.fitness.apple_health import (
    parse_apple_date,
    get_source_name,
    AppleHealthConnector,
    HEALTH_TYPE_MAP,
    SOURCE_MAP,
)
from datahub.db import DataPoint, DataType


class TestParseAppleDate:
    """Tests for parse_apple_date function."""

    def test_standard_format(self):
        """Should parse standard Apple Health date format."""
        result = parse_apple_date("2024-01-15 08:30:00 -0500")
        assert result == datetime(2024, 1, 15, 8, 30, 0)

    def test_different_timezone(self):
        """Should parse dates with different timezone offsets."""
        result = parse_apple_date("2024-01-15 14:45:30 +0000")
        assert result == datetime(2024, 1, 15, 14, 45, 30)

    def test_midnight(self):
        """Should parse midnight correctly."""
        result = parse_apple_date("2024-01-15 00:00:00 -0800")
        assert result == datetime(2024, 1, 15, 0, 0, 0)

    def test_end_of_day(self):
        """Should parse end of day correctly."""
        result = parse_apple_date("2024-01-15 23:59:59 -0500")
        assert result == datetime(2024, 1, 15, 23, 59, 59)


class TestGetSourceName:
    """Tests for get_source_name function."""

    def test_oura_bundle_id(self):
        """Should recognize Oura from bundle ID."""
        result = get_source_name("Oura", "com.ouraring.oura")
        assert result == "oura"

    def test_tonal_bundle_id(self):
        """Should recognize Tonal from bundle ID."""
        result = get_source_name("Tonal", "com.tonal.app")
        assert result == "tonal"

    def test_apple_health_bundle_id(self):
        """Should recognize Apple Health from bundle ID."""
        result = get_source_name("Apple Watch", "com.apple.health")
        assert result == "apple_watch"

    def test_apple_health_app_bundle_id(self):
        """Should recognize Apple Health app from bundle ID."""
        result = get_source_name("Health", "com.apple.Health")
        assert result == "apple_health_app"

    def test_oura_name_fallback(self):
        """Should recognize Oura from source name if no bundle match."""
        result = get_source_name("Oura Ring", "")
        assert result == "oura"

    def test_tonal_name_fallback(self):
        """Should recognize Tonal from source name if no bundle match."""
        result = get_source_name("My Tonal Workout", "")
        assert result == "tonal"

    def test_watch_name_fallback(self):
        """Should recognize Apple Watch from source name."""
        result = get_source_name("John's Apple Watch", "")
        assert result == "apple_watch"

    def test_peloton_name_fallback(self):
        """Should recognize Peloton from source name."""
        result = get_source_name("Peloton", "")
        assert result == "peloton"

    def test_unknown_source_defaults_to_apple_health(self):
        """Should default to apple_health for unknown sources."""
        result = get_source_name("Unknown Device", "com.unknown.app")
        assert result == "apple_health"


class TestHealthTypeMap:
    """Tests for HEALTH_TYPE_MAP configuration."""

    def test_step_count_mapped(self):
        """Step count should be mapped to STEPS."""
        assert "HKQuantityTypeIdentifierStepCount" in HEALTH_TYPE_MAP
        assert HEALTH_TYPE_MAP["HKQuantityTypeIdentifierStepCount"] == DataType.STEPS

    def test_heart_rate_mapped(self):
        """Heart rate should be mapped to HEART_RATE."""
        assert "HKQuantityTypeIdentifierHeartRate" in HEALTH_TYPE_MAP
        assert HEALTH_TYPE_MAP["HKQuantityTypeIdentifierHeartRate"] == DataType.HEART_RATE

    def test_hrv_mapped(self):
        """HRV should be mapped to HEART_RATE_VARIABILITY."""
        key = "HKQuantityTypeIdentifierHeartRateVariabilitySDNN"
        assert key in HEALTH_TYPE_MAP
        assert HEALTH_TYPE_MAP[key] == DataType.HEART_RATE_VARIABILITY

    def test_active_energy_mapped(self):
        """Active energy should be mapped to ACTIVE_CALORIES."""
        key = "HKQuantityTypeIdentifierActiveEnergyBurned"
        assert key in HEALTH_TYPE_MAP
        assert HEALTH_TYPE_MAP[key] == DataType.ACTIVE_CALORIES

    def test_sleep_mapped(self):
        """Sleep analysis should be mapped to SLEEP_STAGE."""
        key = "HKCategoryTypeIdentifierSleepAnalysis"
        assert key in HEALTH_TYPE_MAP
        assert HEALTH_TYPE_MAP[key] == DataType.SLEEP_STAGE

    def test_weight_mapped(self):
        """Body mass should be mapped to WEIGHT."""
        key = "HKQuantityTypeIdentifierBodyMass"
        assert key in HEALTH_TYPE_MAP
        assert HEALTH_TYPE_MAP[key] == DataType.WEIGHT


class TestSourceMap:
    """Tests for SOURCE_MAP configuration."""

    def test_oura_in_source_map(self):
        """Oura should be in SOURCE_MAP."""
        assert "com.ouraring.oura" in SOURCE_MAP
        assert SOURCE_MAP["com.ouraring.oura"] == "oura"

    def test_tonal_in_source_map(self):
        """Tonal should be in SOURCE_MAP."""
        assert "com.tonal.app" in SOURCE_MAP
        assert SOURCE_MAP["com.tonal.app"] == "tonal"

    def test_apple_health_in_source_map(self):
        """Apple Health should be in SOURCE_MAP."""
        assert "com.apple.health" in SOURCE_MAP
        assert SOURCE_MAP["com.apple.health"] == "apple_watch"


class TestAppleHealthConnector:
    """Tests for AppleHealthConnector class."""

    def test_connector_name(self, test_session):
        """Connector should have correct name."""
        connector = AppleHealthConnector(test_session)
        assert connector.name == "apple_health"

    def test_import_file_not_found(self, test_session):
        """Should raise FileNotFoundError for missing file."""
        connector = AppleHealthConnector(test_session)
        with pytest.raises(FileNotFoundError, match="File not found"):
            connector.import_file(Path("/nonexistent/export.xml"))

    def test_import_step_records(self, test_session, tmp_path):
        """Should import step count records."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE HealthData>
<HealthData>
  <Record type="HKQuantityTypeIdentifierStepCount"
          unit="count"
          value="1500"
          sourceName="Apple Watch"
          sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500"
          endDate="2024-01-15 10:15:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        assert added == 1
        assert skipped == 0

        # Verify data in database
        datapoint = test_session.query(DataPoint).first()
        assert datapoint is not None
        assert datapoint.data_type == "steps"
        assert datapoint.value == 1500.0
        assert datapoint.unit == "count"
        assert datapoint.source == "apple_watch"

    def test_import_heart_rate_records(self, test_session, tmp_path):
        """Should import heart rate records."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierHeartRate"
          unit="count/min"
          value="72"
          sourceName="Apple Watch"
          sourceVersion="com.apple.health"
          startDate="2024-01-15 14:30:00 -0500"
          endDate="2024-01-15 14:30:05 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        assert added == 1

        datapoint = test_session.query(DataPoint).first()
        assert datapoint.data_type == "heart_rate"
        assert datapoint.value == 72.0
        assert datapoint.unit == "count/min"

    def test_import_workout_records(self, test_session, tmp_path):
        """Should import workout records."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="30.5"
           durationUnit="min"
           totalEnergyBurned="350.0"
           totalDistance="5.2"
           sourceName="Apple Watch"
           sourceVersion="com.apple.health"
           startDate="2024-01-15 06:00:00 -0500"
           endDate="2024-01-15 06:30:30 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        assert added == 1

        datapoint = test_session.query(DataPoint).first()
        assert datapoint.data_type == "workout"
        assert datapoint.value == 30.5
        assert datapoint.unit == "min"
        # Check metadata stored correctly
        assert "Running" in datapoint.metadata_json

    def test_import_multiple_records(self, test_session, tmp_path):
        """Should import multiple records of different types."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierStepCount"
          unit="count" value="1000"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:15:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierHeartRate"
          unit="count/min" value="75"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:00:05 -0500"/>
  <Record type="HKQuantityTypeIdentifierActiveEnergyBurned"
          unit="Cal" value="25"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:15:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        assert added == 3
        assert skipped == 0

        # Verify all types imported
        datapoints = test_session.query(DataPoint).all()
        data_types = {dp.data_type for dp in datapoints}
        assert "steps" in data_types
        assert "heart_rate" in data_types
        assert "active_calories" in data_types

    def test_duplicate_prevention(self, test_session, tmp_path):
        """Should skip duplicate records on reimport."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierStepCount"
          unit="count" value="1500"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:15:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)

        # First import
        added1, skipped1 = connector.import_file(xml_file)
        assert added1 == 1
        assert skipped1 == 0

        # Second import (same data)
        added2, skipped2 = connector.import_file(xml_file)
        assert added2 == 0
        assert skipped2 == 1

        # Verify only one record in database
        count = test_session.query(DataPoint).count()
        assert count == 1

    def test_ignores_unsupported_record_types(self, test_session, tmp_path):
        """Should ignore record types not in HEALTH_TYPE_MAP."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierStepCount"
          unit="count" value="1000"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:15:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierUnsupportedType"
          unit="unknown" value="123"
          sourceName="Unknown" sourceVersion=""
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:15:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        # Only the supported step count should be imported
        assert added == 1

    def test_source_detection_oura(self, test_session, tmp_path):
        """Should correctly identify Oura as source."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN"
          unit="ms" value="45"
          sourceName="Oura" sourceVersion="com.ouraring.oura"
          startDate="2024-01-15 06:00:00 -0500" endDate="2024-01-15 06:00:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        connector.import_file(xml_file)

        datapoint = test_session.query(DataPoint).first()
        assert datapoint.source == "oura"

    def test_source_detection_peloton(self, test_session, tmp_path):
        """Should correctly identify Peloton as source."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierActiveEnergyBurned"
          unit="Cal" value="350"
          sourceName="Peloton" sourceVersion=""
          startDate="2024-01-15 07:00:00 -0500" endDate="2024-01-15 07:30:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        connector.import_file(xml_file)

        datapoint = test_session.query(DataPoint).first()
        assert datapoint.source == "peloton"

    def test_handles_malformed_value(self, test_session, tmp_path):
        """Should skip records with non-numeric values."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierStepCount"
          unit="count" value="not_a_number"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 10:00:00 -0500" endDate="2024-01-15 10:15:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierStepCount"
          unit="count" value="1000"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 11:00:00 -0500" endDate="2024-01-15 11:15:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        # Only the valid record should be imported
        assert added == 1

    def test_workout_metadata_stored(self, test_session, tmp_path):
        """Should store workout metadata correctly."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeCycling"
           duration="45.0"
           durationUnit="min"
           totalEnergyBurned="500.0"
           totalDistance="20.0"
           sourceName="Peloton"
           sourceVersion=""
           startDate="2024-01-15 08:00:00 -0500"
           endDate="2024-01-15 08:45:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        connector.import_file(xml_file)

        datapoint = test_session.query(DataPoint).first()
        assert datapoint is not None
        assert "Cycling" in datapoint.metadata_json
        assert "500.0" in datapoint.metadata_json or "500" in datapoint.metadata_json
        assert datapoint.source == "peloton"

    def test_empty_xml_file(self, test_session, tmp_path):
        """Should handle empty XML file."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
</HealthData>"""

        xml_file = tmp_path / "empty.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, skipped = connector.import_file(xml_file)

        assert added == 0
        assert skipped == 0

    def test_hrv_import(self, test_session, tmp_path):
        """Should import HRV data correctly."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN"
          unit="ms" value="55.5"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 23:00:00 -0500" endDate="2024-01-15 23:00:05 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, _ = connector.import_file(xml_file)

        assert added == 1
        datapoint = test_session.query(DataPoint).first()
        assert datapoint.data_type == "hrv"
        assert datapoint.value == 55.5
        assert datapoint.unit == "ms"

    def test_sleep_import(self, test_session, tmp_path):
        """Should import sleep data correctly."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData>
  <Record type="HKCategoryTypeIdentifierSleepAnalysis"
          value="1"
          sourceName="Apple Watch" sourceVersion="com.apple.health"
          startDate="2024-01-15 22:00:00 -0500" endDate="2024-01-16 06:00:00 -0500"/>
</HealthData>"""

        xml_file = tmp_path / "export.xml"
        xml_file.write_text(xml_content)

        connector = AppleHealthConnector(test_session)
        added, _ = connector.import_file(xml_file)

        assert added == 1
        datapoint = test_session.query(DataPoint).first()
        assert datapoint.data_type == "sleep_stage"
