"""Tests for deduplication logic."""

import pytest
from datetime import datetime

from datahub.db import DataPoint
from datahub.dedup import (
    get_source_priority,
    deduplicate_daily_totals,
    get_deduplicated_total,
    get_daily_average,
    deduplicate_records_by_priority,
    SOURCE_PRIORITY,
    DEFAULT_PRIORITY,
)


class TestGetSourcePriority:
    """Tests for get_source_priority function."""

    def test_known_source_steps_apple_watch(self):
        """Apple Watch should have priority 100 for steps."""
        priority = get_source_priority("steps", "apple_watch")
        assert priority == 100

    def test_known_source_steps_oura(self):
        """Oura should have priority 80 for steps."""
        priority = get_source_priority("steps", "oura")
        assert priority == 80

    def test_known_source_hrv_oura(self):
        """Oura should have priority 100 for HRV (best at HRV tracking)."""
        priority = get_source_priority("hrv", "oura")
        assert priority == 100

    def test_known_source_sleep_oura(self):
        """Oura should have priority 100 for sleep tracking."""
        priority = get_source_priority("sleep_minutes", "oura")
        assert priority == 100

    def test_unknown_source_returns_default(self):
        """Unknown source should return DEFAULT_PRIORITY."""
        priority = get_source_priority("steps", "unknown_device")
        assert priority == DEFAULT_PRIORITY

    def test_unknown_data_type_returns_default(self):
        """Unknown data type should return DEFAULT_PRIORITY."""
        priority = get_source_priority("unknown_type", "apple_watch")
        assert priority == DEFAULT_PRIORITY

    def test_all_configured_priorities_are_positive(self):
        """All configured priorities should be positive integers."""
        for data_type, sources in SOURCE_PRIORITY.items():
            for source, priority in sources.items():
                assert priority > 0, f"{data_type}/{source} has non-positive priority"


class TestDeduplicateDailyTotals:
    """Tests for deduplicate_daily_totals function."""

    def test_single_source_basic(self, test_session):
        """Single source should sum all values per day."""
        # Add records from same source on same day
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=1000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 11, 0),
                data_type="steps",
                value=2000.0,
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert len(result) == 1
        assert result[0]["date"] == "2024-01-15"
        assert result[0]["total"] == 3000.0

    def test_multiple_sources_picks_highest_priority(self, test_session):
        """When multiple sources exist for same hour, pick highest priority."""
        # Same hour, different sources
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=1000.0,
                source="apple_watch",  # priority 100
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 30),
                data_type="steps",
                value=800.0,
                source="apple_health",  # priority 50
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert len(result) == 1
        # Should only count apple_watch (higher priority)
        assert result[0]["total"] == 1000.0

    def test_same_priority_same_source_accumulates(self, test_session):
        """Multiple records from same source in same hour should accumulate."""
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=500.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 15),
                data_type="steps",
                value=300.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 45),
                data_type="steps",
                value=200.0,
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert len(result) == 1
        assert result[0]["total"] == 1000.0

    def test_empty_result_returns_empty_list(self, test_session):
        """No records in range should return empty list."""
        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert result == []

    def test_multiple_days(self, test_session):
        """Should group by day correctly."""
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=5000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 16, 10, 0),
                data_type="steps",
                value=6000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 17, 10, 0),
                data_type="steps",
                value=7000.0,
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 17, 23, 59),
        )

        assert len(result) == 3
        assert result[0]["date"] == "2024-01-15"
        assert result[0]["total"] == 5000.0
        assert result[1]["date"] == "2024-01-16"
        assert result[1]["total"] == 6000.0
        assert result[2]["date"] == "2024-01-17"
        assert result[2]["total"] == 7000.0

    def test_filters_by_data_type(self, test_session):
        """Should only return records matching requested data type."""
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=5000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="heart_rate",
                value=75.0,
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert len(result) == 1
        assert result[0]["total"] == 5000.0

    def test_default_end_date_uses_now(self, test_session):
        """When end_date is None, should use current time."""
        point = DataPoint(
            timestamp=datetime.now(),
            data_type="steps",
            value=100.0,
            source="apple_watch",
        )
        test_session.add(point)
        test_session.commit()

        result = deduplicate_daily_totals(
            test_session,
            "steps",
            datetime(2020, 1, 1),
            # end_date defaults to now
        )

        assert len(result) == 1


class TestGetDeduplicatedTotal:
    """Tests for get_deduplicated_total function."""

    def test_sums_deduplicated_values(self, test_session):
        """Should sum deduplicated daily totals."""
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=5000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 16, 10, 0),
                data_type="steps",
                value=6000.0,
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        total = get_deduplicated_total(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 16, 23, 59),
        )

        assert total == 11000.0

    def test_empty_range_returns_zero(self, test_session):
        """Empty date range should return 0."""
        total = get_deduplicated_total(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 16),
        )

        assert total == 0.0


class TestGetDailyAverage:
    """Tests for get_daily_average function."""

    def test_calculates_average(self, test_session):
        """Should calculate average of daily totals."""
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=5000.0,
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 16, 10, 0),
                data_type="steps",
                value=7000.0,
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        avg = get_daily_average(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 16, 23, 59),
        )

        assert avg == 6000.0  # (5000 + 7000) / 2

    def test_empty_returns_zero_not_divide_by_zero(self, test_session):
        """Empty result should return 0, not raise ZeroDivisionError."""
        avg = get_daily_average(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 16),
        )

        assert avg == 0.0


class TestDeduplicateRecordsByPriority:
    """Tests for deduplicate_records_by_priority function."""

    def test_custom_bucket_size(self, test_session):
        """Should use custom bucket size for grouping."""
        # Two records 20 minutes apart (within 30-minute bucket)
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=500.0,
                unit="count",
                source="apple_watch",
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 20),
                data_type="steps",
                value=300.0,
                unit="count",
                source="apple_watch",
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        # With 30-minute buckets, these should be in same bucket
        result = deduplicate_records_by_priority(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
            bucket_minutes=30,
        )

        assert len(result) == 1
        assert result[0]["value"] == 800.0  # accumulated

    def test_higher_priority_replaces_lower(self, test_session):
        """Higher priority source should replace lower priority in same bucket."""
        points = [
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 0),
                data_type="steps",
                value=500.0,
                unit="count",
                source="apple_health",  # lower priority
            ),
            DataPoint(
                timestamp=datetime(2024, 1, 15, 10, 5),
                data_type="steps",
                value=600.0,
                unit="count",
                source="apple_watch",  # higher priority
            ),
        ]
        test_session.add_all(points)
        test_session.commit()

        result = deduplicate_records_by_priority(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
            bucket_minutes=60,
        )

        assert len(result) == 1
        assert result[0]["value"] == 600.0
        assert result[0]["source"] == "apple_watch"

    def test_returns_record_details(self, test_session):
        """Should return full record details including unit."""
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 10, 0),
            data_type="steps",
            value=5000.0,
            unit="count",
            source="apple_watch",
        )
        test_session.add(point)
        test_session.commit()

        result = deduplicate_records_by_priority(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert len(result) == 1
        assert "timestamp" in result[0]
        assert result[0]["source"] == "apple_watch"
        assert result[0]["value"] == 5000.0
        assert result[0]["unit"] == "count"

    def test_empty_returns_empty_list(self, test_session):
        """No records should return empty list."""
        result = deduplicate_records_by_priority(
            test_session,
            "steps",
            datetime(2024, 1, 15),
            datetime(2024, 1, 15, 23, 59),
        )

        assert result == []
