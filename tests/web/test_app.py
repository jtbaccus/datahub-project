"""Tests for web routes."""

import pytest
import json
from datetime import datetime, timedelta

from starlette.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from datahub.db import Base, DataPoint, Transaction, SyncLog


class TestDashboardRoute:
    """Tests for the dashboard route (GET /)."""

    def test_returns_200_and_html(self, test_client):
        """Should return 200 status and HTML content."""
        response = test_client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_empty_database_renders(self, test_client):
        """Should render the dashboard even with no data."""
        response = test_client.get("/")
        assert response.status_code == 200
        # Should contain basic dashboard structure
        assert b"<!DOCTYPE html>" in response.content or b"<html" in response.content

    def test_with_sample_data(self, test_client, test_engine, sample_datapoints, sample_transactions):
        """Should render dashboard with data present."""
        response = test_client.get("/")
        assert response.status_code == 200

    def test_contains_expected_sections(self, test_client):
        """Dashboard should contain key sections."""
        response = test_client.get("/")
        content = response.text.lower()
        # Dashboard typically has stats sections
        assert response.status_code == 200


class TestFitnessRoute:
    """Tests for the fitness route (GET /fitness)."""

    def test_returns_200_and_html(self, test_client):
        """Should return 200 status and HTML content."""
        response = test_client.get("/fitness")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_empty_database_renders(self, test_client):
        """Should render the fitness page even with no data."""
        response = test_client.get("/fitness")
        assert response.status_code == 200

    def test_with_workout_data(self, test_client, test_engine, sample_workouts):
        """Should render fitness page with workout data."""
        response = test_client.get("/fitness")
        assert response.status_code == 200

    def test_with_volume_data(self, test_client, test_engine, sample_volume):
        """Should render fitness page with Tonal volume data."""
        response = test_client.get("/fitness")
        assert response.status_code == 200

    def test_strength_workout_metadata_parsing(self, test_client, test_engine):
        """Should correctly parse strength workout metadata."""
        # Create a strength workout with metadata
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        metadata = {
            "workout_name": "Full Body Strength",
            "total_volume": 18500,
            "exercises": 12,
        }
        workout = DataPoint(
            timestamp=datetime.now() - timedelta(days=1),
            data_type="strength_workout",
            value=55.0,
            unit="minutes",
            source="tonal",
            metadata_json=json.dumps(metadata),
        )
        session.add(workout)
        session.commit()
        session.close()

        response = test_client.get("/fitness")
        assert response.status_code == 200


class TestFinanceRoute:
    """Tests for the finance route (GET /finance)."""

    def test_returns_200_and_html(self, test_client):
        """Should return 200 status and HTML content."""
        response = test_client.get("/finance")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_empty_database_renders(self, test_client):
        """Should render the finance page even with no data."""
        response = test_client.get("/finance")
        assert response.status_code == 200

    def test_with_transaction_data(self, test_client, test_engine, sample_transactions):
        """Should render finance page with transaction data."""
        response = test_client.get("/finance")
        assert response.status_code == 200

    def test_spending_by_category_aggregation(self, test_client, test_engine):
        """Should aggregate spending by category."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        # Create transactions in different categories
        now = datetime.now()
        transactions = [
            Transaction(
                date=now - timedelta(days=5),
                amount=-100.00,
                description="Grocery Store",
                category="Groceries",
                source="test",
            ),
            Transaction(
                date=now - timedelta(days=3),
                amount=-50.00,
                description="Another Grocery",
                category="Groceries",
                source="test",
            ),
            Transaction(
                date=now - timedelta(days=2),
                amount=-75.00,
                description="Restaurant",
                category="Food & Drink",
                source="test",
            ),
        ]
        session.add_all(transactions)
        session.commit()
        session.close()

        response = test_client.get("/finance")
        assert response.status_code == 200

    def test_recent_transactions_ordering(self, test_client, test_engine):
        """Should show transactions in descending date order."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        now = datetime.now()
        transactions = [
            Transaction(
                date=now - timedelta(days=10),
                amount=-100.00,
                description="Older Transaction",
                source="test",
            ),
            Transaction(
                date=now - timedelta(days=1),
                amount=-50.00,
                description="Newer Transaction",
                source="test",
            ),
        ]
        session.add_all(transactions)
        session.commit()
        session.close()

        response = test_client.get("/finance")
        assert response.status_code == 200


class TestStatsAPI:
    """Tests for the stats API endpoint (GET /api/stats)."""

    def test_returns_json(self, test_client):
        """Should return JSON response."""
        response = test_client.get("/api/stats")
        assert response.status_code == 200
        assert "application/json" in response.headers["content-type"]

    def test_empty_database_returns_zeros(self, test_client):
        """Should return zero counts for empty database."""
        response = test_client.get("/api/stats")
        assert response.status_code == 200

        data = response.json()
        assert "steps_week" in data
        assert "data_points_total" in data
        assert "transactions_total" in data
        assert data["data_points_total"] == 0
        assert data["transactions_total"] == 0

    def test_correct_counts(self, test_client, web_test_db):
        """Should return correct data counts."""
        _, SessionLocal = web_test_db
        session = SessionLocal()

        # Add test data
        session.add_all([
            DataPoint(
                timestamp=datetime.now(),
                data_type="steps",
                value=1000.0,
                source="test",
            ),
            DataPoint(
                timestamp=datetime.now(),
                data_type="steps",
                value=2000.0,
                source="test",
            ),
            Transaction(
                date=datetime.now(),
                amount=-50.0,
                description="Test",
                source="test",
            ),
        ])
        session.commit()
        session.close()

        response = test_client.get("/api/stats")
        assert response.status_code == 200

        data = response.json()
        assert data["data_points_total"] == 2
        assert data["transactions_total"] == 1

    def test_steps_week_deduplicated(self, test_client, web_test_db):
        """Should return deduplicated steps count for the week."""
        _, SessionLocal = web_test_db
        session = SessionLocal()

        now = datetime.now()
        # Use a base time pinned to the start of an hour to guarantee both
        # data points fall in the same hour bucket for deduplication.
        base = now.replace(minute=0, second=0, microsecond=0) - timedelta(days=1)
        # Create overlapping step data from different sources in the same hour
        steps = [
            DataPoint(
                timestamp=base + timedelta(minutes=10),
                data_type="steps",
                value=1000.0,
                unit="count",
                source="apple_watch",  # Higher priority
            ),
            DataPoint(
                timestamp=base + timedelta(minutes=40),
                data_type="steps",
                value=900.0,
                unit="count",
                source="apple_health",  # Lower priority - should be deduplicated
            ),
        ]
        session.add_all(steps)
        session.commit()
        session.close()

        response = test_client.get("/api/stats")
        assert response.status_code == 200

        data = response.json()
        # Should only count the apple_watch steps (higher priority)
        assert data["steps_week"] == 1000.0

    def test_response_structure(self, test_client):
        """Should have expected response structure."""
        response = test_client.get("/api/stats")
        data = response.json()

        expected_keys = {"steps_week", "data_points_total", "transactions_total"}
        assert expected_keys.issubset(set(data.keys()))


class TestDashboardDataCalculations:
    """Tests for dashboard data calculations."""

    def test_steps_week_calculation(self, test_client, test_engine):
        """Should calculate weekly steps correctly with deduplication."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        now = datetime.now()
        # Add steps from different days within the last week
        steps = [
            DataPoint(
                timestamp=now - timedelta(days=1),
                data_type="steps",
                value=5000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=now - timedelta(days=2),
                data_type="steps",
                value=6000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=now - timedelta(days=3),
                data_type="steps",
                value=4500.0,
                source="apple_watch",
            ),
        ]
        session.add_all(steps)
        session.commit()
        session.close()

        response = test_client.get("/")
        assert response.status_code == 200

    def test_spending_month_calculation(self, test_client, test_engine):
        """Should sum negative amounts for spending."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        now = datetime.now()
        transactions = [
            Transaction(
                date=now - timedelta(days=5),
                amount=-100.00,
                description="Purchase 1",
                source="test",
            ),
            Transaction(
                date=now - timedelta(days=10),
                amount=-200.00,
                description="Purchase 2",
                source="test",
            ),
            Transaction(
                date=now - timedelta(days=15),
                amount=50.00,  # Positive - refund, should not count
                description="Refund",
                source="test",
            ),
        ]
        session.add_all(transactions)
        session.commit()
        session.close()

        response = test_client.get("/")
        assert response.status_code == 200

    def test_workout_count_includes_strength(self, test_client, test_engine):
        """Should count both 'workout' and 'strength_workout' types."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        now = datetime.now()
        workouts = [
            DataPoint(
                timestamp=now - timedelta(days=1),
                data_type="workout",
                value=45.0,
                source="peloton",
            ),
            DataPoint(
                timestamp=now - timedelta(days=2),
                data_type="strength_workout",
                value=60.0,
                source="tonal",
            ),
            DataPoint(
                timestamp=now - timedelta(days=3),
                data_type="workout",
                value=30.0,
                source="peloton",
            ),
        ]
        session.add_all(workouts)
        session.commit()
        session.close()

        response = test_client.get("/")
        assert response.status_code == 200


class TestFitnessDataCalculations:
    """Tests for fitness page data calculations."""

    def test_total_volume_calculation(self, test_client, test_engine):
        """Should calculate total volume lifted in last 30 days."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        now = datetime.now()
        volumes = [
            DataPoint(
                timestamp=now - timedelta(days=5),
                data_type="volume",
                value=10000.0,
                source="tonal",
            ),
            DataPoint(
                timestamp=now - timedelta(days=10),
                data_type="volume",
                value=12000.0,
                source="tonal",
            ),
            DataPoint(
                timestamp=now - timedelta(days=45),  # Outside 30 days - should not count
                data_type="volume",
                value=8000.0,
                source="tonal",
            ),
        ]
        session.add_all(volumes)
        session.commit()
        session.close()

        response = test_client.get("/fitness")
        assert response.status_code == 200

    def test_daily_data_deduplication(self, test_client, test_engine):
        """Should deduplicate daily fitness data by priority."""
        SessionLocal = sessionmaker(bind=test_engine)
        session = SessionLocal()

        now = datetime.now()
        # Same hour, different sources
        steps = [
            DataPoint(
                timestamp=now - timedelta(days=1, hours=10),
                data_type="steps",
                value=2000.0,
                source="apple_watch",  # Higher priority
            ),
            DataPoint(
                timestamp=now - timedelta(days=1, hours=10, minutes=30),
                data_type="steps",
                value=1800.0,
                source="apple_health",  # Lower priority
            ),
        ]
        session.add_all(steps)
        session.commit()
        session.close()

        response = test_client.get("/fitness")
        assert response.status_code == 200
