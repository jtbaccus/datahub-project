"""Shared test fixtures for DataHub tests."""

import pytest
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from datahub.db import Base, DataPoint, Transaction, SyncLog
from datahub.config import Config


@pytest.fixture
def test_engine():
    """In-memory SQLite database engine for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def test_session(test_engine) -> Session:
    """Database session with automatic rollback."""
    SessionLocal = sessionmaker(bind=test_engine)
    session = SessionLocal()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def temp_config(tmp_path) -> Config:
    """Temporary config for testing."""
    config_path = tmp_path / "config.json"
    return Config(config_path=config_path)


@pytest.fixture
def sample_datapoints(test_session) -> list[DataPoint]:
    """Create sample DataPoints for testing."""
    points = [
        DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="steps",
            value=1000.0,
            unit="count",
            source="apple_watch",
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 30),
            data_type="steps",
            value=500.0,
            unit="count",
            source="apple_health",
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 15, 11, 0),
            data_type="steps",
            value=800.0,
            unit="count",
            source="apple_watch",
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 16, 9, 0),
            data_type="steps",
            value=1200.0,
            unit="count",
            source="apple_watch",
        ),
    ]
    test_session.add_all(points)
    test_session.commit()
    return points


@pytest.fixture
def sample_transactions(test_session) -> list[Transaction]:
    """Create sample Transactions for testing."""
    transactions = [
        Transaction(
            date=datetime(2024, 1, 15),
            amount=-50.00,
            description="Coffee Shop",
            merchant="Starbucks",
            category="Food & Drink",
            source="chase_csv",
        ),
        Transaction(
            date=datetime(2024, 1, 16),
            amount=-125.50,
            description="Grocery Store",
            merchant="Whole Foods",
            category="Groceries",
            source="chase_csv",
        ),
        Transaction(
            date=datetime(2024, 1, 17),
            amount=25.00,
            description="Refund",
            merchant="Amazon",
            category="Shopping",
            source="chase_csv",
        ),
    ]
    test_session.add_all(transactions)
    test_session.commit()
    return transactions
