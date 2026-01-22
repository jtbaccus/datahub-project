"""Tonal API connector.

Uses Tonal's unofficial API to fetch strength workout data.

NOTE: Tonal does not have an official public API. This connector uses
reverse-engineered endpoints that may break if Tonal changes their API.

Setup:
    datahub config tonal.email your_email
    datahub config tonal.password your_password

Usage:
    datahub sync tonal
"""

import json
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from datahub.connectors.base import BaseConnector
from datahub.db import DataPoint, DataType


TONAL_API_BASE = "https://api.tonal.com"


class TonalConnector(BaseConnector):
    """Sync strength workout data from Tonal.

    NOTE: Tonal does not have an official public API. This connector attempts
    to use reverse-engineered endpoints. Authentication may fail if Tonal
    changes their API.
    """

    name = "tonal"

    def __init__(self, session: Session, config: dict | None = None):
        super().__init__(session, config)
        self._http_client: httpx.Client | None = None
        self._access_token: str | None = None
        self._user_id: str | None = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.Client(
                base_url=TONAL_API_BASE,
                timeout=30.0,
            )
        return self._http_client

    def _authenticate(self) -> None:
        """Authenticate with Tonal.

        Tries multiple authentication methods since Tonal's API is undocumented.
        """
        email = self.config.get("email")
        password = self.config.get("password")

        if not email or not password:
            raise ValueError(
                "Tonal credentials not configured. Run:\n"
                "  datahub config tonal.email your_email\n"
                "  datahub config tonal.password your_password"
            )

        # Try method 1: Direct API login
        auth_methods = [
            self._try_direct_login,
            self._try_auth0_login,
        ]

        last_error = None
        for method in auth_methods:
            try:
                method(email, password)
                if self._access_token:
                    return
            except Exception as e:
                last_error = e
                continue

        raise ValueError(
            f"Tonal authentication failed. Last error: {last_error}\n\n"
            "Tonal doesn't have a public API, so authentication may not work.\n"
            "You can still import Tonal data via Apple Health export:\n"
            "  datahub import apple-health export.xml"
        )

    def _try_direct_login(self, email: str, password: str) -> None:
        """Try direct login to Tonal API."""
        with httpx.Client(timeout=30.0) as client:
            # Try common login endpoints
            endpoints = [
                (f"{TONAL_API_BASE}/v1/auth/login", {"email": email, "password": password}),
                (f"{TONAL_API_BASE}/v1/login", {"email": email, "password": password}),
                (f"{TONAL_API_BASE}/auth/login", {"username": email, "password": password}),
            ]

            for url, payload in endpoints:
                try:
                    response = client.post(url, json=payload)
                    if response.status_code == 200:
                        data = response.json()
                        self._access_token = (
                            data.get("access_token") or
                            data.get("token") or
                            data.get("accessToken")
                        )
                        if self._access_token:
                            self._user_id = data.get("user_id") or data.get("userId") or data.get("id")
                            self._setup_client_auth()
                            return
                except Exception:
                    continue

    def _try_auth0_login(self, email: str, password: str) -> None:
        """Try Auth0-based login."""
        # Known Auth0 configurations to try
        auth0_configs = [
            {
                "url": "https://tonal.auth0.com/oauth/token",
                "client_id": "PRv2GEoXJqFbRLcZjCGXbUd9cvJR48yO",
                "audience": "https://api.tonal.com",
            },
            {
                "url": "https://auth.tonal.com/oauth/token",
                "client_id": "tonal-mobile-app",
                "audience": "https://api.tonal.com",
            },
        ]

        with httpx.Client(timeout=30.0) as client:
            for config in auth0_configs:
                try:
                    payload = {
                        "grant_type": "password",
                        "username": email,
                        "password": password,
                        "client_id": config["client_id"],
                        "scope": "openid profile email offline_access",
                        "audience": config["audience"],
                    }

                    response = client.post(config["url"], json=payload)
                    if response.status_code == 200:
                        data = response.json()
                        self._access_token = data.get("access_token")
                        if self._access_token:
                            self._setup_client_auth()
                            return
                except Exception:
                    continue

    def _setup_client_auth(self) -> None:
        """Set up authenticated client."""
        client = self._get_client()
        client.headers["Authorization"] = f"Bearer {self._access_token}"

        # Try to get user profile
        try:
            profile_response = client.get("/v1/users/me")
            if profile_response.status_code == 200:
                profile = profile_response.json()
                self._user_id = profile.get("id") or profile.get("userId")
        except Exception:
            pass

    def _fetch_workouts(self, limit: int = 50, offset: int = 0) -> list[dict]:
        """Fetch workout list from Tonal."""
        client = self._get_client()

        params = {"limit": limit, "offset": offset}
        if self._user_id:
            params["userId"] = self._user_id

        response = client.get("/v1/workouts", params=params)

        if response.status_code != 200:
            raise ValueError(f"Failed to fetch workouts: {response.text}")

        data = response.json()
        # Handle both direct list and wrapped response
        if isinstance(data, list):
            return data
        return data.get("workouts", data.get("data", []))

    def _fetch_workout_details(self, workout_id: str) -> dict:
        """Fetch detailed workout data including exercises and sets."""
        client = self._get_client()

        response = client.get(f"/v1/workouts/{workout_id}")
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch workout {workout_id}: {response.text}")

        return response.json()

    def _workout_exists(self, workout_id: str) -> bool:
        """Check if workout already imported."""
        stmt = select(DataPoint).where(
            DataPoint.source == "tonal",
            DataPoint.source_id == workout_id,
        )
        return self.session.execute(stmt).first() is not None

    def _save_workout(self, workout: dict) -> int:
        """Save workout data to database. Returns number of records added."""
        workout_id = str(workout.get("id") or workout.get("workoutId", ""))
        if not workout_id or self._workout_exists(workout_id):
            return 0

        # Parse workout timestamp
        timestamp_str = workout.get("startedAt") or workout.get("completedAt") or workout.get("createdAt")
        if timestamp_str:
            try:
                if "T" in timestamp_str:
                    start_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                else:
                    start_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                start_time = datetime.utcnow()
        else:
            start_time = datetime.utcnow()

        # Make timestamp naive for SQLite compatibility
        if start_time.tzinfo is not None:
            start_time = start_time.replace(tzinfo=None)

        # Extract workout info
        duration_seconds = workout.get("duration", 0) or workout.get("durationSeconds", 0)
        duration_minutes = duration_seconds / 60 if duration_seconds else 0

        workout_name = workout.get("name") or workout.get("title") or "Tonal Workout"
        workout_type = workout.get("type") or workout.get("workoutType") or "strength"
        instructor = workout.get("instructor", {})
        instructor_name = instructor.get("name") if isinstance(instructor, dict) else instructor

        # Calculate totals from exercises
        exercises = workout.get("exercises", workout.get("movements", []))
        total_volume = 0
        total_reps = 0
        total_sets = 0
        exercise_details = []

        for exercise in exercises:
            exercise_name = exercise.get("name") or exercise.get("movementName", "Unknown")
            sets = exercise.get("sets", exercise.get("setData", []))

            exercise_volume = 0
            exercise_reps = 0
            exercise_sets = len(sets) if isinstance(sets, list) else 0

            if isinstance(sets, list):
                for set_data in sets:
                    reps = set_data.get("reps", set_data.get("actualReps", 0)) or 0
                    weight = set_data.get("weight", set_data.get("actualWeight", 0)) or 0
                    exercise_reps += reps
                    exercise_volume += reps * weight

            total_volume += exercise_volume
            total_reps += exercise_reps
            total_sets += exercise_sets

            exercise_details.append({
                "name": exercise_name,
                "sets": exercise_sets,
                "reps": exercise_reps,
                "volume": exercise_volume,
            })

        # Build metadata
        metadata = {
            "workout_id": workout_id,
            "name": workout_name,
            "type": workout_type,
            "instructor": instructor_name,
            "total_sets": total_sets,
            "total_reps": total_reps,
            "total_volume_lbs": total_volume,
            "exercises": exercise_details,
            "calories": workout.get("caloriesBurned") or workout.get("calories"),
        }

        records = []

        # Main strength workout record
        records.append(DataPoint(
            timestamp=start_time,
            data_type=DataType.STRENGTH_WORKOUT.value,
            value=duration_minutes,
            unit="min",
            source="tonal",
            source_id=workout_id,
            metadata_json=json.dumps(metadata),
        ))

        # Volume record (total weight moved)
        if total_volume > 0:
            records.append(DataPoint(
                timestamp=start_time,
                data_type=DataType.VOLUME.value,
                value=float(total_volume),
                unit="lbs",
                source="tonal",
                source_id=f"{workout_id}_vol",
            ))

        # Also record as general workout for aggregate tracking
        records.append(DataPoint(
            timestamp=start_time,
            data_type=DataType.WORKOUT.value,
            value=duration_minutes,
            unit="min",
            source="tonal",
            source_id=f"{workout_id}_workout",
            metadata_json=json.dumps({"type": "strength", "name": workout_name}),
        ))

        # Calories if available
        calories = workout.get("caloriesBurned") or workout.get("calories")
        if calories:
            records.append(DataPoint(
                timestamp=start_time,
                data_type=DataType.ACTIVE_CALORIES.value,
                value=float(calories),
                unit="kcal",
                source="tonal",
                source_id=f"{workout_id}_cal",
            ))

        # Individual exercise records for detailed tracking
        for i, exercise in enumerate(exercise_details):
            if exercise["volume"] > 0:
                records.append(DataPoint(
                    timestamp=start_time,
                    data_type=DataType.STRENGTH_EXERCISE.value,
                    value=float(exercise["volume"]),
                    unit="lbs",
                    source="tonal",
                    source_id=f"{workout_id}_ex{i}",
                    metadata_json=json.dumps(exercise),
                ))

        self.session.add_all(records)
        return len(records)

    def sync(self, since: datetime | None = None) -> tuple[int, int]:
        """Sync workouts from Tonal."""
        self._authenticate()

        added = 0
        skipped = 0
        offset = 0

        while True:
            workouts = self._fetch_workouts(limit=50, offset=offset)

            if not workouts:
                break

            found_old = False
            for workout_summary in workouts:
                # Parse workout time
                timestamp_str = (
                    workout_summary.get("startedAt")
                    or workout_summary.get("completedAt")
                    or workout_summary.get("createdAt")
                )

                workout_time = None
                if timestamp_str:
                    try:
                        if "T" in timestamp_str:
                            workout_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        else:
                            workout_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                        # Make naive for comparison
                        if workout_time.tzinfo is not None:
                            workout_time = workout_time.replace(tzinfo=None)
                    except (ValueError, TypeError):
                        workout_time = None

                # Skip if before our cutoff
                if since and workout_time and workout_time < since:
                    found_old = True
                    continue

                workout_id = str(workout_summary.get("id") or workout_summary.get("workoutId", ""))

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

            # Stop if we've found old workouts (already synced)
            if found_old or len(workouts) < 50:
                break

            offset += 50

            # Safety limit
            if offset > 500:
                break

        return added, skipped

    def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None
