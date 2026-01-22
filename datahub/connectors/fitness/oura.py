"""Oura Ring API connector.

Uses Oura's official API v2 to fetch sleep, readiness, and activity data.

Setup:
1. Go to https://cloud.ouraring.com/personal-access-tokens
2. Create a new Personal Access Token
3. Configure: datahub config oura.token YOUR_TOKEN

Usage:
    datahub sync oura
    datahub sync oura --days 30
"""

import json
from datetime import datetime, timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from datahub.connectors.base import BaseConnector
from datahub.db import DataPoint, DataType


OURA_API_BASE = "https://api.ouraring.com/v2/usercollection"


class OuraConnector(BaseConnector):
    """Sync data from Oura Ring API."""

    name = "oura"

    def __init__(self, session: Session, config: dict | None = None):
        super().__init__(session, config)
        self._http_client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        """Get or create authenticated HTTP client."""
        if self._http_client is None:
            token = self.config.get("token")
            if not token:
                raise ValueError(
                    "Oura token not configured. Run:\n"
                    "  datahub config oura.token YOUR_TOKEN\n\n"
                    "Get your token at: https://cloud.ouraring.com/personal-access-tokens"
                )

            self._http_client = httpx.Client(
                base_url=OURA_API_BASE,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30.0,
            )
        return self._http_client

    def _record_exists(self, source_id: str) -> bool:
        """Check if record already exists."""
        stmt = select(DataPoint).where(
            DataPoint.source == "oura",
            DataPoint.source_id == source_id,
        )
        return self.session.execute(stmt).first() is not None

    def _fetch_sleep(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch sleep data."""
        client = self._get_client()
        response = client.get(
            "/sleep",
            params={"start_date": start_date, "end_date": end_date},
        )
        if response.status_code != 200:
            return []
        return response.json().get("data", [])

    def _fetch_daily_sleep(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch daily sleep summaries."""
        client = self._get_client()
        response = client.get(
            "/daily_sleep",
            params={"start_date": start_date, "end_date": end_date},
        )
        if response.status_code != 200:
            return []
        return response.json().get("data", [])

    def _fetch_daily_readiness(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch daily readiness scores."""
        client = self._get_client()
        response = client.get(
            "/daily_readiness",
            params={"start_date": start_date, "end_date": end_date},
        )
        if response.status_code != 200:
            return []
        return response.json().get("data", [])

    def _fetch_daily_activity(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch daily activity data."""
        client = self._get_client()
        response = client.get(
            "/daily_activity",
            params={"start_date": start_date, "end_date": end_date},
        )
        if response.status_code != 200:
            return []
        return response.json().get("data", [])

    def _fetch_heart_rate(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch heart rate data."""
        client = self._get_client()
        response = client.get(
            "/heartrate",
            params={"start_datetime": f"{start_date}T00:00:00+00:00",
                    "end_datetime": f"{end_date}T23:59:59+00:00"},
        )
        if response.status_code != 200:
            return []
        return response.json().get("data", [])

    def _save_sleep_data(self, sleep_records: list[dict]) -> int:
        """Save sleep session data."""
        added = 0
        for record in sleep_records:
            record_id = record.get("id")
            if not record_id or self._record_exists(f"sleep_{record_id}"):
                continue

            # Parse bedtime
            bedtime_start = record.get("bedtime_start")
            if not bedtime_start:
                continue

            timestamp = datetime.fromisoformat(bedtime_start.replace("Z", "+00:00"))

            # Total sleep duration in minutes
            total_sleep = record.get("total_sleep_duration", 0) / 60

            metadata = {
                "id": record_id,
                "type": record.get("type"),  # "long_sleep", "rest", etc.
                "efficiency": record.get("efficiency"),
                "latency": record.get("latency"),
                "rem_sleep": record.get("rem_sleep_duration"),
                "deep_sleep": record.get("deep_sleep_duration"),
                "light_sleep": record.get("light_sleep_duration"),
                "awake_time": record.get("awake_time"),
                "average_hrv": record.get("average_hrv"),
                "lowest_hr": record.get("lowest_heart_rate"),
                "average_hr": record.get("average_heart_rate"),
            }

            self.session.add(DataPoint(
                timestamp=timestamp,
                data_type=DataType.SLEEP_MINUTES.value,
                value=total_sleep,
                unit="min",
                source="oura",
                source_id=f"sleep_{record_id}",
                metadata_json=json.dumps(metadata),
            ))
            added += 1

            # Also save HRV if available
            if record.get("average_hrv"):
                self.session.add(DataPoint(
                    timestamp=timestamp,
                    data_type=DataType.HEART_RATE_VARIABILITY.value,
                    value=float(record["average_hrv"]),
                    unit="ms",
                    source="oura",
                    source_id=f"sleep_hrv_{record_id}",
                ))
                added += 1

        return added

    def _save_readiness_data(self, readiness_records: list[dict]) -> int:
        """Save daily readiness scores."""
        added = 0
        for record in readiness_records:
            day = record.get("day")
            if not day:
                continue

            source_id = f"readiness_{day}"
            if self._record_exists(source_id):
                continue

            timestamp = datetime.strptime(day, "%Y-%m-%d")
            score = record.get("score")
            if score is None:
                continue

            metadata = {
                "temperature_deviation": record.get("temperature_deviation"),
                "temperature_trend_deviation": record.get("temperature_trend_deviation"),
                "contributors": record.get("contributors", {}),
            }

            self.session.add(DataPoint(
                timestamp=timestamp,
                data_type=DataType.READINESS_SCORE.value,
                value=float(score),
                unit="score",
                source="oura",
                source_id=source_id,
                metadata_json=json.dumps(metadata),
            ))
            added += 1

        return added

    def _save_activity_data(self, activity_records: list[dict]) -> int:
        """Save daily activity data."""
        added = 0
        for record in activity_records:
            day = record.get("day")
            if not day:
                continue

            timestamp = datetime.strptime(day, "%Y-%m-%d")

            # Steps
            steps = record.get("steps")
            if steps and not self._record_exists(f"activity_steps_{day}"):
                self.session.add(DataPoint(
                    timestamp=timestamp,
                    data_type=DataType.STEPS.value,
                    value=float(steps),
                    unit="steps",
                    source="oura",
                    source_id=f"activity_steps_{day}",
                ))
                added += 1

            # Active calories
            active_cal = record.get("active_calories")
            if active_cal and not self._record_exists(f"activity_cal_{day}"):
                self.session.add(DataPoint(
                    timestamp=timestamp,
                    data_type=DataType.ACTIVE_CALORIES.value,
                    value=float(active_cal),
                    unit="kcal",
                    source="oura",
                    source_id=f"activity_cal_{day}",
                ))
                added += 1

        return added

    def sync(self, since: datetime | None = None) -> tuple[int, int]:
        """Sync all Oura data."""
        if since is None:
            since = datetime.utcnow() - timedelta(days=30)

        start_date = since.strftime("%Y-%m-%d")
        end_date = datetime.utcnow().strftime("%Y-%m-%d")

        added = 0
        skipped = 0

        # Fetch and save sleep data
        sleep_data = self._fetch_sleep(start_date, end_date)
        added += self._save_sleep_data(sleep_data)

        # Fetch and save readiness data
        readiness_data = self._fetch_daily_readiness(start_date, end_date)
        added += self._save_readiness_data(readiness_data)

        # Fetch and save activity data
        activity_data = self._fetch_daily_activity(start_date, end_date)
        added += self._save_activity_data(activity_data)

        self.session.commit()

        return added, skipped

    def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None
