"""Shared test fixtures for DataHub tests."""

import sys
from pathlib import Path

# Add the project root to sys.path so the web module can be imported
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from datetime import datetime

from click.testing import CliRunner
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from datahub.db import Base, DataPoint, Transaction, SyncLog, init_db
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


@pytest.fixture
def web_test_db(tmp_path):
    """Create a file-based test database for web tests."""
    # Create a file-based test database to avoid threading issues
    test_db_path = tmp_path / "web_test.db"
    init_db(test_db_path)

    # Create engine with check_same_thread=False for SQLite
    test_engine = create_engine(
        f"sqlite:///{test_db_path}",
        connect_args={"check_same_thread": False}
    )
    TestSessionLocal = sessionmaker(bind=test_engine)

    yield test_engine, TestSessionLocal

    test_engine.dispose()


@pytest.fixture
def test_client(web_test_db):
    """FastAPI TestClient with test database."""
    import importlib.util
    import sys

    from starlette.testclient import TestClient

    test_engine, TestSessionLocal = web_test_db

    # Use spec_from_file_location to load the web module directly
    web_init_path = PROJECT_ROOT / "web" / "__init__.py"
    web_app_path = PROJECT_ROOT / "web" / "app.py"

    # Load web package
    spec = importlib.util.spec_from_file_location("web", web_init_path)
    web_pkg = importlib.util.module_from_spec(spec)
    sys.modules["web"] = web_pkg
    spec.loader.exec_module(web_pkg)

    # Load web.app module
    spec = importlib.util.spec_from_file_location("web.app", web_app_path)
    web_app_module = importlib.util.module_from_spec(spec)
    sys.modules["web.app"] = web_app_module
    spec.loader.exec_module(web_app_module)

    app = web_app_module.app

    # Monkey-patch get_db to use the test database
    def test_get_db():
        return TestSessionLocal()

    original_get_db = web_app_module.get_db
    web_app_module.get_db = test_get_db

    client = TestClient(app)
    yield client

    # Restore the original get_db function
    web_app_module.get_db = original_get_db


@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_db(tmp_path, temp_config):
    """Database initialized and ready for CLI tests."""
    db_path = tmp_path / "test.db"
    init_db(db_path)
    temp_config.set("db_path", str(db_path))
    return db_path


@pytest.fixture
def sample_workouts(test_session) -> list[DataPoint]:
    """Create sample workout DataPoints for testing."""
    import json
    workouts = [
        DataPoint(
            timestamp=datetime(2024, 1, 15, 8, 0),
            data_type="workout",
            value=45.0,
            unit="minutes",
            source="peloton",
            metadata_json=json.dumps({"type": "cycling", "calories": 450}),
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 16, 7, 30),
            data_type="strength_workout",
            value=60.0,
            unit="minutes",
            source="tonal",
            metadata_json=json.dumps({
                "workout_name": "Upper Body",
                "total_volume": 12500,
                "exercises": 8,
            }),
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 17, 9, 0),
            data_type="workout",
            value=30.0,
            unit="minutes",
            source="peloton",
            metadata_json=json.dumps({"type": "running", "calories": 320}),
        ),
    ]
    test_session.add_all(workouts)
    test_session.commit()
    return workouts


@pytest.fixture
def sample_volume(test_session) -> list[DataPoint]:
    """Create sample volume DataPoints for testing (Tonal strength metrics)."""
    volumes = [
        DataPoint(
            timestamp=datetime(2024, 1, 16, 7, 30),
            data_type="volume",
            value=12500.0,
            unit="lbs",
            source="tonal",
        ),
        DataPoint(
            timestamp=datetime(2024, 1, 18, 8, 0),
            data_type="volume",
            value=15000.0,
            unit="lbs",
            source="tonal",
        ),
    ]
    test_session.add_all(volumes)
    test_session.commit()
    return volumes
