"""Tests for database models and operations."""

import pytest
from datetime import datetime

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from datahub.db import (
    Base,
    DataPoint,
    Transaction,
    SyncLog,
    DataType,
    init_db,
    get_engine,
    get_session,
)


class TestDataPoint:
    """Tests for DataPoint model."""

    def test_creation(self, test_session):
        """Should create and persist a DataPoint."""
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="steps",
            value=5000.0,
            unit="count",
            source="apple_watch",
        )
        test_session.add(point)
        test_session.commit()

        # Query back
        result = test_session.query(DataPoint).first()

        assert result is not None
        assert result.timestamp == datetime(2024, 1, 15, 10, 0)
        assert result.data_type == "steps"
        assert result.value == 5000.0
        assert result.unit == "count"
        assert result.source == "apple_watch"

    def test_required_fields(self, test_session):
        """Should have required fields: timestamp, data_type, value, source."""
        # Missing timestamp
        point = DataPoint(
            data_type="steps",
            value=5000.0,
            source="apple_watch",
        )
        test_session.add(point)

        with pytest.raises(Exception):
            test_session.commit()

    def test_optional_fields_default_to_none(self, test_session):
        """Optional fields should default to None."""
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="steps",
            value=5000.0,
            source="apple_watch",
        )
        test_session.add(point)
        test_session.commit()

        result = test_session.query(DataPoint).first()

        assert result.unit is None
        assert result.source_id is None
        assert result.metadata_json is None

    def test_created_at_auto_populated(self, test_session):
        """created_at should be auto-populated."""
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="steps",
            value=5000.0,
            source="apple_watch",
        )
        test_session.add(point)
        test_session.commit()

        result = test_session.query(DataPoint).first()

        assert result.created_at is not None

    def test_float_value_precision(self, test_session):
        """Should handle float values with decimal precision."""
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="heart_rate",
            value=72.5,
            source="apple_watch",
        )
        test_session.add(point)
        test_session.commit()

        result = test_session.query(DataPoint).first()

        assert result.value == 72.5

    def test_metadata_json_storage(self, test_session):
        """Should store and retrieve JSON metadata."""
        import json
        metadata = {"workout_type": "running", "distance_km": 5.2}

        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="workout",
            value=1.0,
            source="apple_watch",
            metadata_json=json.dumps(metadata),
        )
        test_session.add(point)
        test_session.commit()

        result = test_session.query(DataPoint).first()
        retrieved_metadata = json.loads(result.metadata_json)

        assert retrieved_metadata["workout_type"] == "running"
        assert retrieved_metadata["distance_km"] == 5.2


class TestTransaction:
    """Tests for Transaction model."""

    def test_creation(self, test_session):
        """Should create and persist a Transaction."""
        txn = Transaction(
            date=datetime(2024, 1, 15),
            amount=-50.00,
            description="Coffee Shop",
            merchant="Starbucks",
            category="Food & Drink",
            source="chase_csv",
        )
        test_session.add(txn)
        test_session.commit()

        result = test_session.query(Transaction).first()

        assert result is not None
        assert result.date == datetime(2024, 1, 15)
        assert result.amount == -50.00
        assert result.description == "Coffee Shop"
        assert result.merchant == "Starbucks"
        assert result.category == "Food & Drink"
        assert result.source == "chase_csv"

    def test_negative_amount_for_purchases(self, test_session):
        """Purchases should have negative amounts."""
        txn = Transaction(
            date=datetime(2024, 1, 15),
            amount=-125.50,
            description="Grocery Store",
            source="chase_csv",
        )
        test_session.add(txn)
        test_session.commit()

        result = test_session.query(Transaction).first()

        assert result.amount < 0

    def test_positive_amount_for_refunds(self, test_session):
        """Refunds should have positive amounts."""
        txn = Transaction(
            date=datetime(2024, 1, 15),
            amount=25.00,
            description="Refund",
            source="chase_csv",
        )
        test_session.add(txn)
        test_session.commit()

        result = test_session.query(Transaction).first()

        assert result.amount > 0

    def test_optional_fields_default_to_none(self, test_session):
        """Optional fields should default to None."""
        txn = Transaction(
            date=datetime(2024, 1, 15),
            amount=-50.00,
            description="Purchase",
            source="chase_csv",
        )
        test_session.add(txn)
        test_session.commit()

        result = test_session.query(Transaction).first()

        assert result.merchant is None
        assert result.category is None
        assert result.account is None
        assert result.source_id is None
        assert result.metadata_json is None


class TestSyncLog:
    """Tests for SyncLog model."""

    def test_creation(self, test_session):
        """Should create and persist a SyncLog."""
        log = SyncLog(
            connector="peloton",
            started_at=datetime(2024, 1, 15, 10, 0),
            completed_at=datetime(2024, 1, 15, 10, 5),
            status="success",
            records_added=100,
            records_updated=5,
        )
        test_session.add(log)
        test_session.commit()

        result = test_session.query(SyncLog).first()

        assert result is not None
        assert result.connector == "peloton"
        assert result.status == "success"
        assert result.records_added == 100
        assert result.records_updated == 5

    def test_default_records_added_and_updated(self, test_session):
        """records_added and records_updated should default to 0."""
        log = SyncLog(
            connector="oura",
            started_at=datetime(2024, 1, 15, 10, 0),
            status="running",
        )
        test_session.add(log)
        test_session.commit()

        result = test_session.query(SyncLog).first()

        assert result.records_added == 0
        assert result.records_updated == 0

    def test_failed_sync_with_error_message(self, test_session):
        """Failed sync should store error message."""
        log = SyncLog(
            connector="peloton",
            started_at=datetime(2024, 1, 15, 10, 0),
            completed_at=datetime(2024, 1, 15, 10, 1),
            status="failed",
            error_message="Authentication failed: invalid credentials",
        )
        test_session.add(log)
        test_session.commit()

        result = test_session.query(SyncLog).first()

        assert result.status == "failed"
        assert "Authentication failed" in result.error_message


class TestDataType:
    """Tests for DataType enum."""

    def test_steps_value(self):
        """STEPS should have value 'steps'."""
        assert DataType.STEPS.value == "steps"

    def test_heart_rate_value(self):
        """HEART_RATE should have value 'heart_rate'."""
        assert DataType.HEART_RATE.value == "heart_rate"

    def test_hrv_value(self):
        """HRV should have value 'hrv'."""
        assert DataType.HEART_RATE_VARIABILITY.value == "hrv"

    def test_sleep_minutes_value(self):
        """SLEEP_MINUTES should have value 'sleep_minutes'."""
        assert DataType.SLEEP_MINUTES.value == "sleep_minutes"

    def test_strength_workout_value(self):
        """STRENGTH_WORKOUT should have value 'strength_workout'."""
        assert DataType.STRENGTH_WORKOUT.value == "strength_workout"


class TestInitDb:
    """Tests for init_db function."""

    def test_creates_tables(self, tmp_path):
        """Should create all tables in the database."""
        db_path = tmp_path / "test.db"

        init_db(db_path)

        engine = get_engine(db_path)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        assert "data_points" in tables
        assert "transactions" in tables
        assert "sync_logs" in tables

    def test_idempotent(self, tmp_path):
        """Should be safe to call multiple times."""
        db_path = tmp_path / "test.db"

        # Call twice - should not raise
        init_db(db_path)
        init_db(db_path)

        engine = get_engine(db_path)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        assert "data_points" in tables

    def test_creates_parent_directory(self, tmp_path):
        """Should create parent directory if it doesn't exist."""
        db_path = tmp_path / "subdir" / "nested" / "test.db"

        init_db(db_path)

        assert db_path.exists()


class TestGetSession:
    """Tests for get_session function."""

    def test_returns_session(self, tmp_path):
        """Should return a valid database session."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        session = get_session(db_path)

        # Should be able to query
        result = session.query(DataPoint).all()
        assert result == []

        session.close()

    def test_session_can_write(self, tmp_path):
        """Session should be able to write to database."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        session = get_session(db_path)
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="steps",
            value=5000.0,
            source="test",
        )
        session.add(point)
        session.commit()

        # Query back
        result = session.query(DataPoint).first()
        assert result.value == 5000.0

        session.close()


class TestGetEngine:
    """Tests for get_engine function."""

    def test_returns_engine(self, tmp_path):
        """Should return a SQLAlchemy engine."""
        db_path = tmp_path / "test.db"

        engine = get_engine(db_path)

        assert engine is not None
        assert str(engine.url).endswith("test.db")

        engine.dispose()
