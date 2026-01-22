"""Peloton API connector.

Uses Peloton's unofficial API to fetch workout data.

Setup:
    datahub config peloton.username your_email
    datahub config peloton.password your_password

Usage:
    datahub sync peloton
"""

import json
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from datahub.connectors.base import BaseConnector
from datahub.db import DataPoint, DataType


PELOTON_API_BASE = "https://api.onepeloton.com"


class PelotonConnector(BaseConnector):
    """Sync workout data from Peloton."""

    name = "peloton"

    def __init__(self, session: Session, config: dict | None = None):
        super().__init__(session, config)
        self._http_client: httpx.Client | None = None
        self._user_id: str | None = None
        self._session_id: str | None = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.Client(
                base_url=PELOTON_API_BASE,
                timeout=30.0,
            )
        return self._http_client

    def _authenticate(self) -> None:
        """Authenticate with Peloton API."""
        username = self.config.get("username")
        password = self.config.get("password")

        if not username or not password:
            raise ValueError(
                "Peloton credentials not configured. Run:\n"
                "  datahub config peloton.username your_email\n"
                "  datahub config peloton.password your_password"
            )

        client = self._get_client()
        response = client.post(
            "/auth/login",
            json={"username_or_email": username, "password": password},
        )

        if response.status_code != 200:
            raise ValueError(f"Peloton authentication failed: {response.text}")

        data = response.json()
        self._user_id = data["user_id"]
        self._session_id = data["session_id"]

        # Set session cookie for subsequent requests
        client.cookies.set("peloton_session_id", self._session_id)

    def _fetch_workouts(self, limit: int = 100, page: int = 0) -> list[dict]:
        """Fetch workout list."""
        client = self._get_client()
        response = client.get(
            f"/api/user/{self._user_id}/workouts",
            params={"limit": limit, "page": page, "joins": "ride,ride.instructor"},
        )

        if response.status_code != 200:
            raise ValueError(f"Failed to fetch workouts: {response.text}")

        return response.json().get("data", [])

    def _fetch_workout_details(self, workout_id: str) -> dict:
        """Fetch detailed workout data including performance metrics."""
        client = self._get_client()

        # Get workout summary
        response = client.get(f"/api/workout/{workout_id}")
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch workout {workout_id}: {response.text}")

        workout = response.json()

        # Get performance graph data (heart rate, output, cadence, etc.)
        perf_response = client.get(
            f"/api/workout/{workout_id}/performance_graph",
            params={"every_n": 5},  # Sample every 5 seconds
        )

        if perf_response.status_code == 200:
            workout["performance"] = perf_response.json()

        return workout

    def _workout_exists(self, workout_id: str) -> bool:
        """Check if workout already imported."""
        stmt = select(DataPoint).where(
            DataPoint.source == "peloton",
            DataPoint.source_id == workout_id,
        )
        return self.session.execute(stmt).first() is not None

    def _save_workout(self, workout: dict) -> int:
        """Save workout data to database. Returns number of records added."""
        workout_id = workout.get("id")
        if not workout_id or self._workout_exists(workout_id):
            return 0

        # Parse workout data
        start_time = datetime.fromtimestamp(workout.get("start_time", 0))
        duration_seconds = workout.get("ride", {}).get("duration", 0)
        duration_minutes = duration_seconds / 60

        # Get ride info
        ride = workout.get("ride", {})
        instructor = ride.get("instructor", {})

        metadata = {
            "workout_id": workout_id,
            "title": ride.get("title"),
            "instructor": instructor.get("name"),
            "fitness_discipline": workout.get("fitness_discipline"),
            "total_output": workout.get("total_work"),
            "avg_output": workout.get("avg_watts"),
            "max_output": workout.get("max_watts"),
            "avg_cadence": workout.get("avg_cadence"),
            "max_cadence": workout.get("max_cadence"),
            "avg_resistance": workout.get("avg_resistance"),
            "max_resistance": workout.get("max_resistance"),
            "avg_speed": workout.get("avg_speed"),
            "max_speed": workout.get("max_speed"),
            "distance": workout.get("distance"),
            "calories": workout.get("calories"),
            "avg_heart_rate": workout.get("avg_heart_rate"),
            "max_heart_rate": workout.get("max_heart_rate"),
        }

        records = []

        # Main workout record
        records.append(DataPoint(
            timestamp=start_time,
            data_type=DataType.WORKOUT.value,
            value=duration_minutes,
            unit="min",
            source="peloton",
            source_id=workout_id,
            metadata_json=json.dumps(metadata),
        ))

        # Calories burned
        if workout.get("calories"):
            records.append(DataPoint(
                timestamp=start_time,
                data_type=DataType.ACTIVE_CALORIES.value,
                value=float(workout["calories"]),
                unit="kcal",
                source="peloton",
                source_id=f"{workout_id}_cal",
            ))

        # Distance
        if workout.get("distance"):
            records.append(DataPoint(
                timestamp=start_time,
                data_type=DataType.DISTANCE.value,
                value=float(workout["distance"]),
                unit="mi",
                source="peloton",
                source_id=f"{workout_id}_dist",
            ))

        # Average heart rate during workout
        if workout.get("avg_heart_rate"):
            records.append(DataPoint(
                timestamp=start_time,
                data_type=DataType.HEART_RATE.value,
                value=float(workout["avg_heart_rate"]),
                unit="bpm",
                source="peloton",
                source_id=f"{workout_id}_hr",
                metadata_json=json.dumps({"type": "workout_average"}),
            ))

        self.session.add_all(records)
        return len(records)

    def sync(self, since: datetime | None = None) -> tuple[int, int]:
        """Sync workouts from Peloton."""
        self._authenticate()

        added = 0
        skipped = 0
        page = 0

        while True:
            workouts = self._fetch_workouts(limit=50, page=page)

            if not workouts:
                break

            for workout_summary in workouts:
                workout_id = workout_summary.get("id")
                workout_time = datetime.fromtimestamp(workout_summary.get("start_time", 0))

                # Skip if before our cutoff
                if since and workout_time < since:
                    continue

                # Skip if already imported
                if self._workout_exists(workout_id):
                    skipped += 1
                    continue

                # Fetch full details and save
                try:
                    workout = self._fetch_workout_details(workout_id)
                    records = self._save_workout(workout)
                    added += records
                except Exception:
                    # Skip problematic workouts
                    continue

            self.session.commit()
            page += 1

            # Safety limit
            if page > 50:
                break

        return added, skipped

    def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None
