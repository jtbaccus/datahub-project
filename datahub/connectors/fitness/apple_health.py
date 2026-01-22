"""Apple Health XML export connector.

To export your Apple Health data:
1. Open the Health app on your iPhone
2. Tap your profile picture in the top right
3. Scroll down and tap "Export All Health Data"
4. Save and transfer the export.zip to your computer
5. Extract the zip and use the export.xml file with this connector
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Iterator

from sqlalchemy.orm import Session
from sqlalchemy import select

from datahub.connectors.base import FileImportConnector
from datahub.db import DataPoint, DataType


# Map Apple Health type identifiers to our DataType enum
HEALTH_TYPE_MAP = {
    "HKQuantityTypeIdentifierStepCount": DataType.STEPS,
    "HKQuantityTypeIdentifierHeartRate": DataType.HEART_RATE,
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": DataType.HEART_RATE_VARIABILITY,
    "HKQuantityTypeIdentifierActiveEnergyBurned": DataType.ACTIVE_CALORIES,
    "HKQuantityTypeIdentifierBasalEnergyBurned": DataType.RESTING_CALORIES,
    "HKQuantityTypeIdentifierDistanceWalkingRunning": DataType.DISTANCE,
    "HKQuantityTypeIdentifierFlightsClimbed": DataType.FLOORS_CLIMBED,
    "HKQuantityTypeIdentifierBodyMass": DataType.WEIGHT,
    "HKQuantityTypeIdentifierBodyFatPercentage": DataType.BODY_FAT,
    "HKQuantityTypeIdentifierOxygenSaturation": DataType.OXYGEN_SATURATION,
    "HKQuantityTypeIdentifierRespiratoryRate": DataType.RESPIRATORY_RATE,
    "HKCategoryTypeIdentifierSleepAnalysis": DataType.SLEEP_STAGE,
}

# Sources we recognize for better attribution
SOURCE_MAP = {
    "com.ouraring.oura": "oura",
    "com.tonal.app": "tonal",
    "com.apple.health": "apple_watch",
    "com.apple.Health": "apple_health_app",
}


def parse_apple_date(date_str: str) -> datetime:
    """Parse Apple Health date format."""
    # Format: 2024-01-15 08:30:00 -0500
    return datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")


def get_source_name(source_name: str, source_bundle: str) -> str:
    """Determine the friendly source name."""
    # Check bundle ID first
    for bundle_prefix, name in SOURCE_MAP.items():
        if source_bundle and source_bundle.startswith(bundle_prefix):
            return name
    # Fall back to source name
    if "oura" in source_name.lower():
        return "oura"
    if "tonal" in source_name.lower():
        return "tonal"
    if "watch" in source_name.lower():
        return "apple_watch"
    if "peloton" in source_name.lower():
        return "peloton"
    return "apple_health"


class AppleHealthConnector(FileImportConnector):
    """Import data from Apple Health XML export."""

    name = "apple_health"

    def _iter_records(self, file_path: Path) -> Iterator[dict]:
        """Iterate over health records in the XML file."""
        # Use iterparse for memory efficiency with large files
        context = ET.iterparse(file_path, events=("end",))

        for event, elem in context:
            if elem.tag == "Record":
                record_type = elem.get("type", "")
                if record_type in HEALTH_TYPE_MAP:
                    yield {
                        "type": record_type,
                        "value": elem.get("value"),
                        "unit": elem.get("unit"),
                        "start_date": elem.get("startDate"),
                        "end_date": elem.get("endDate"),
                        "source_name": elem.get("sourceName", ""),
                        "source_bundle": elem.get("sourceVersion", ""),
                        "device": elem.get("device"),
                    }
                # Clear element to save memory
                elem.clear()

            elif elem.tag == "Workout":
                yield {
                    "type": "Workout",
                    "workout_type": elem.get("workoutActivityType", ""),
                    "duration": elem.get("duration"),
                    "duration_unit": elem.get("durationUnit"),
                    "calories": elem.get("totalEnergyBurned"),
                    "distance": elem.get("totalDistance"),
                    "start_date": elem.get("startDate"),
                    "end_date": elem.get("endDate"),
                    "source_name": elem.get("sourceName", ""),
                    "source_bundle": elem.get("sourceVersion", ""),
                }
                elem.clear()

    def _record_exists(self, timestamp: datetime, data_type: str, source: str, value: float) -> bool:
        """Check if a record already exists (to avoid duplicates)."""
        stmt = select(DataPoint).where(
            DataPoint.timestamp == timestamp,
            DataPoint.data_type == data_type,
            DataPoint.source == source,
            DataPoint.value == value,
        )
        return self.session.execute(stmt).first() is not None

    def import_file(self, file_path: Path) -> tuple[int, int]:
        """Import health data from Apple Health XML export."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        added = 0
        skipped = 0
        batch = []
        batch_size = 1000

        for record in self._iter_records(file_path):
            if record["type"] == "Workout":
                # Handle workout records
                try:
                    timestamp = parse_apple_date(record["start_date"])
                    duration_minutes = float(record.get("duration") or 0)
                    source = get_source_name(record["source_name"], record["source_bundle"])

                    # Store workout duration
                    if not self._record_exists(timestamp, DataType.WORKOUT.value, source, duration_minutes):
                        metadata = {
                            "workout_type": record["workout_type"],
                            "calories": record.get("calories"),
                            "distance": record.get("distance"),
                            "end_date": record.get("end_date"),
                        }
                        batch.append(DataPoint(
                            timestamp=timestamp,
                            data_type=DataType.WORKOUT.value,
                            value=duration_minutes,
                            unit="min",
                            source=source,
                            metadata_json=json.dumps(metadata),
                        ))
                        added += 1
                    else:
                        skipped += 1
                except (ValueError, TypeError):
                    continue

            else:
                # Handle regular records
                data_type = HEALTH_TYPE_MAP.get(record["type"])
                if not data_type:
                    continue

                try:
                    timestamp = parse_apple_date(record["start_date"])
                    value = float(record["value"])
                    source = get_source_name(record["source_name"], record["source_bundle"])

                    if not self._record_exists(timestamp, data_type.value, source, value):
                        batch.append(DataPoint(
                            timestamp=timestamp,
                            data_type=data_type.value,
                            value=value,
                            unit=record.get("unit"),
                            source=source,
                        ))
                        added += 1
                    else:
                        skipped += 1
                except (ValueError, TypeError):
                    continue

            # Commit in batches
            if len(batch) >= batch_size:
                self.session.add_all(batch)
                self.session.commit()
                batch = []

        # Commit remaining records
        if batch:
            self.session.add_all(batch)
            self.session.commit()

        return added, skipped
