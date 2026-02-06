"""Database models for DataHub."""

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from sqlalchemy import create_engine, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class DataType(str, Enum):
    """Types of fitness/health data points."""
    STEPS = "steps"
    HEART_RATE = "heart_rate"
    HEART_RATE_VARIABILITY = "hrv"
    ACTIVE_CALORIES = "active_calories"
    RESTING_CALORIES = "resting_calories"
    DISTANCE = "distance"
    FLOORS_CLIMBED = "floors"
    SLEEP_MINUTES = "sleep_minutes"
    SLEEP_STAGE = "sleep_stage"
    WEIGHT = "weight"
    BODY_FAT = "body_fat"
    WORKOUT = "workout"
    OXYGEN_SATURATION = "spo2"
    RESPIRATORY_RATE = "respiratory_rate"
    READINESS_SCORE = "readiness"
    STRAIN_SCORE = "strain"
    # Strength training specific
    STRENGTH_WORKOUT = "strength_workout"
    STRENGTH_EXERCISE = "strength_exercise"
    VOLUME = "volume"  # Total weight lifted (lbs or kg)


class DataPoint(Base):
    """Generic timestamped health/fitness measurement."""

    __tablename__ = "data_points"

    id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(index=True)
    data_type: Mapped[str] = mapped_column(index=True)
    value: Mapped[float]
    unit: Mapped[str | None] = mapped_column(default=None)
    source: Mapped[str] = mapped_column(index=True)  # e.g., "apple_health", "oura", "peloton"
    source_id: Mapped[str | None] = mapped_column(default=None)  # Original ID from source
    metadata_json: Mapped[str | None] = mapped_column(default=None)  # Extra data as JSON
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_datapoint_type_time", "data_type", "timestamp"),
        Index("ix_datapoint_source_time", "source", "timestamp"),
    )


class Transaction(Base):
    """Financial transaction."""

    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[datetime] = mapped_column(index=True)
    amount: Mapped[float]
    description: Mapped[str]
    merchant: Mapped[str | None] = mapped_column(default=None, index=True)
    category: Mapped[str | None] = mapped_column(default=None, index=True)
    account: Mapped[str | None] = mapped_column(default=None)
    source: Mapped[str] = mapped_column(index=True)  # e.g., "chase_csv", "plaid"
    source_id: Mapped[str | None] = mapped_column(default=None)
    metadata_json: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_transaction_date_amount", "date", "amount"),
    )


class SyncLog(Base):
    """Track sync history for each connector."""

    __tablename__ = "sync_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    connector: Mapped[str] = mapped_column(index=True)
    started_at: Mapped[datetime]
    completed_at: Mapped[datetime | None] = mapped_column(default=None)
    status: Mapped[str]  # "running", "success", "failed"
    records_added: Mapped[int] = mapped_column(default=0)
    records_updated: Mapped[int] = mapped_column(default=0)
    error_message: Mapped[str | None] = mapped_column(default=None)


def get_engine(db_path: Path):
    """Create SQLAlchemy engine."""
    return create_engine(f"sqlite:///{db_path}", echo=False)


def init_db(db_path: Path) -> None:
    """Initialize the database schema."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)


def get_session(db_path: Path) -> Session:
    """Get a database session."""
    engine = get_engine(db_path)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()
