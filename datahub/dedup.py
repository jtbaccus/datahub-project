"""Deduplication utilities for health data.

Apple Health exports contain records from multiple sources (iPhone, Apple Watch,
Oura Ring, etc.) that often track the same activities. This module provides
deduplication logic to prevent double/triple counting.

Strategy:
1. Group records by time bucket (hourly for steps/activity, daily for sleep)
2. For each bucket, only use data from the highest-priority source
3. This prevents counting the same activity multiple times while preserving
   data from times when only one device was active
"""

from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import func, select, case, literal
from sqlalchemy.orm import Session

from datahub.db import DataPoint


# Source priority by data type (higher number = higher priority)
# Apple Watch is most accurate for activity, Oura for sleep/HRV
SOURCE_PRIORITY = {
    "steps": {
        "apple_watch": 100,
        "oura": 80,
        "apple_health": 50,  # iPhone - less accurate, often in pocket
        "peloton": 30,
    },
    "active_calories": {
        "apple_watch": 100,
        "oura": 80,
        "apple_health": 50,
        "peloton": 90,  # Peloton is accurate for workout calories
    },
    "heart_rate": {
        "apple_watch": 100,
        "oura": 90,
        "apple_health": 50,
        "peloton": 85,
    },
    "hrv": {
        "oura": 100,  # Oura is excellent for HRV
        "apple_watch": 90,
        "apple_health": 50,
    },
    "sleep_minutes": {
        "oura": 100,  # Oura is best for sleep tracking
        "apple_watch": 80,
        "apple_health": 50,
    },
    "distance": {
        "apple_watch": 100,
        "oura": 70,
        "apple_health": 50,
        "peloton": 95,
    },
}

# Default priority for unknown sources/types
DEFAULT_PRIORITY = 10


def get_source_priority(data_type: str, source: str) -> int:
    """Get the priority for a source for a given data type."""
    type_priorities = SOURCE_PRIORITY.get(data_type, {})
    return type_priorities.get(source, DEFAULT_PRIORITY)


def deduplicate_daily_totals(
    session: Session,
    data_type: str,
    start_date: datetime,
    end_date: datetime | None = None,
) -> list[dict]:
    """
    Get deduplicated daily totals for a data type.

    Groups records by hour, picks the highest priority source for each hour,
    then sums to get daily totals.

    Args:
        session: Database session
        data_type: The data type to query (e.g., "steps")
        start_date: Start of date range
        end_date: End of date range (defaults to now)

    Returns:
        List of dicts with 'date' and 'total' keys
    """
    if end_date is None:
        end_date = datetime.now()

    # Fetch all records in the date range
    stmt = (
        select(DataPoint)
        .where(DataPoint.data_type == data_type)
        .where(DataPoint.timestamp >= start_date)
        .where(DataPoint.timestamp <= end_date)
        .order_by(DataPoint.timestamp)
    )

    records = list(session.execute(stmt).scalars())

    if not records:
        return []

    # Group by hour bucket, keeping only highest priority source per bucket
    # bucket_key = (date, hour)
    hourly_buckets: dict[tuple, dict] = {}

    for record in records:
        date = record.timestamp.date()
        hour = record.timestamp.hour
        bucket_key = (date, hour)

        priority = get_source_priority(data_type, record.source)

        if bucket_key not in hourly_buckets:
            hourly_buckets[bucket_key] = {
                "source": record.source,
                "priority": priority,
                "value": record.value,
            }
        else:
            existing = hourly_buckets[bucket_key]
            if priority > existing["priority"]:
                # Higher priority source - replace
                hourly_buckets[bucket_key] = {
                    "source": record.source,
                    "priority": priority,
                    "value": record.value,
                }
            elif priority == existing["priority"] and record.source == existing["source"]:
                # Same source - accumulate (multiple records in same hour from same source)
                existing["value"] += record.value

    # Sum by day
    daily_totals: dict[str, float] = defaultdict(float)
    for (date, hour), data in hourly_buckets.items():
        daily_totals[str(date)] += data["value"]

    # Sort and return
    return [
        {"date": date, "total": total}
        for date, total in sorted(daily_totals.items())
    ]


def get_deduplicated_total(
    session: Session,
    data_type: str,
    start_date: datetime,
    end_date: datetime | None = None,
) -> float:
    """
    Get the deduplicated total for a data type over a date range.

    Args:
        session: Database session
        data_type: The data type to query
        start_date: Start of date range
        end_date: End of date range (defaults to now)

    Returns:
        Deduplicated total value
    """
    daily = deduplicate_daily_totals(session, data_type, start_date, end_date)
    return sum(d["total"] for d in daily)


def get_daily_average(
    session: Session,
    data_type: str,
    start_date: datetime,
    end_date: datetime | None = None,
) -> float:
    """
    Get the deduplicated daily average for a data type.

    Args:
        session: Database session
        data_type: The data type to query
        start_date: Start of date range
        end_date: End of date range (defaults to now)

    Returns:
        Average daily value
    """
    daily = deduplicate_daily_totals(session, data_type, start_date, end_date)
    if not daily:
        return 0.0
    return sum(d["total"] for d in daily) / len(daily)


def deduplicate_records_by_priority(
    session: Session,
    data_type: str,
    start_date: datetime,
    end_date: datetime | None = None,
    bucket_minutes: int = 60,
) -> list[dict]:
    """
    Get deduplicated records with more granular control over bucket size.

    Args:
        session: Database session
        data_type: The data type to query
        start_date: Start of date range
        end_date: End of date range
        bucket_minutes: Size of time buckets in minutes (default 60)

    Returns:
        List of deduplicated record dicts
    """
    if end_date is None:
        end_date = datetime.now()

    stmt = (
        select(DataPoint)
        .where(DataPoint.data_type == data_type)
        .where(DataPoint.timestamp >= start_date)
        .where(DataPoint.timestamp <= end_date)
        .order_by(DataPoint.timestamp)
    )

    records = list(session.execute(stmt).scalars())

    if not records:
        return []

    # Group by time bucket
    buckets: dict[int, dict] = {}

    for record in records:
        # Calculate bucket index (minutes since epoch / bucket_minutes)
        ts_minutes = int(record.timestamp.timestamp() / 60)
        bucket_idx = ts_minutes // bucket_minutes

        priority = get_source_priority(data_type, record.source)

        if bucket_idx not in buckets:
            buckets[bucket_idx] = {
                "timestamp": record.timestamp,
                "source": record.source,
                "priority": priority,
                "value": record.value,
                "unit": record.unit,
            }
        else:
            existing = buckets[bucket_idx]
            if priority > existing["priority"]:
                buckets[bucket_idx] = {
                    "timestamp": record.timestamp,
                    "source": record.source,
                    "priority": priority,
                    "value": record.value,
                    "unit": record.unit,
                }
            elif priority == existing["priority"] and record.source == existing["source"]:
                existing["value"] += record.value

    return list(buckets.values())
