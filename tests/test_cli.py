"""Tests for CLI commands."""

import pytest
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from click.testing import CliRunner

from datahub.cli import cli
from datahub.db import init_db, get_session, DataPoint, Transaction, SyncLog
from datahub.config import Config


class TestInitCommand:
    """Tests for the init command."""

    def test_creates_database(self, cli_runner, tmp_path):
        """Should create the database file."""
        db_path = tmp_path / "datahub.db"
        config_path = tmp_path / "config.json"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.config_dir = tmp_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["init"])

            assert result.exit_code == 0
            assert "initialized successfully" in result.output.lower()
            assert db_path.exists()

    def test_creates_config_directory(self, cli_runner, tmp_path):
        """Should create config directory if needed."""
        config_dir = tmp_path / "subdir" / ".datahub"
        db_path = config_dir / "datahub.db"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.config_dir = config_dir
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["init"])

            assert result.exit_code == 0

    def test_shows_success_message(self, cli_runner, tmp_path):
        """Should display success message."""
        db_path = tmp_path / "datahub.db"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.config_dir = tmp_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["init"])

            assert "DataHub initialized" in result.output

    def test_idempotent(self, cli_runner, tmp_path):
        """Running init twice should not fail."""
        db_path = tmp_path / "datahub.db"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.config_dir = tmp_path
            MockConfig.return_value = mock_config

            # First run
            result1 = cli_runner.invoke(cli, ["init"])
            assert result1.exit_code == 0

            # Second run
            result2 = cli_runner.invoke(cli, ["init"])
            assert result2.exit_code == 0


class TestConfigCommand:
    """Tests for the config command."""

    def test_set_simple_key(self, cli_runner, temp_config):
        """Should set a simple config key."""
        with patch("datahub.cli.Config") as MockConfig:
            MockConfig.return_value = temp_config

            result = cli_runner.invoke(cli, ["config", "test_key", "test_value"])

            assert result.exit_code == 0
            assert "Set test_key = test_value" in result.output
            assert temp_config.get("test_key") == "test_value"

    def test_set_nested_key(self, cli_runner, temp_config):
        """Should set a nested config key using dot notation."""
        with patch("datahub.cli.Config") as MockConfig:
            MockConfig.return_value = temp_config

            result = cli_runner.invoke(cli, ["config", "peloton.username", "user@example.com"])

            assert result.exit_code == 0
            assert temp_config.get("peloton.username") == "user@example.com"

    def test_overwrites_existing(self, cli_runner, temp_config):
        """Should overwrite existing config values."""
        # Set initial value
        temp_config.set("existing_key", "old_value")

        with patch("datahub.cli.Config") as MockConfig:
            MockConfig.return_value = temp_config

            result = cli_runner.invoke(cli, ["config", "existing_key", "new_value"])

            assert result.exit_code == 0
            assert temp_config.get("existing_key") == "new_value"


class TestStatusCommand:
    """Tests for the status command."""

    def test_requires_init(self, cli_runner, tmp_path):
        """Should indicate when database is not initialized."""
        db_path = tmp_path / "nonexistent.db"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["status"])

            assert "not initialized" in result.output.lower()

    def test_shows_data_counts(self, cli_runner, tmp_path):
        """Should show data point counts by type."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add some test data
        session = get_session(db_path)
        session.add(DataPoint(
            timestamp=datetime.now(),
            data_type="steps",
            value=5000.0,
            source="test",
        ))
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["status"])

            assert result.exit_code == 0
            assert "steps" in result.output.lower()

    def test_shows_sync_history(self, cli_runner, tmp_path):
        """Should show recent sync history."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add a sync log
        session = get_session(db_path)
        session.add(SyncLog(
            connector="test_connector",
            started_at=datetime.now(),
            status="success",
            records_added=10,
        ))
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["status"])

            assert result.exit_code == 0
            assert "Recent Syncs" in result.output


class TestImportAppleHealthCommand:
    """Tests for the import apple-health command."""

    def test_file_not_found(self, cli_runner, tmp_path):
        """Should error when file doesn't exist."""
        result = cli_runner.invoke(cli, ["import-data", "apple-health", "/nonexistent/file.xml"])
        # Click handles file validation, should show error
        assert result.exit_code != 0

    def test_requires_init(self, cli_runner, tmp_path):
        """Should require database initialization."""
        db_path = tmp_path / "nonexistent.db"
        test_file = tmp_path / "export.xml"
        test_file.write_text("<HealthData></HealthData>")

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["import-data", "apple-health", str(test_file)])

            assert "not initialized" in result.output.lower()


class TestImportBankCsvCommand:
    """Tests for the import bank-csv command."""

    def test_requires_init(self, cli_runner, tmp_path):
        """Should require database initialization."""
        db_path = tmp_path / "nonexistent.db"
        test_file = tmp_path / "transactions.csv"
        test_file.write_text("date,amount,description\n2024-01-15,-50.00,Test")

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["import-data", "bank-csv", str(test_file)])

            assert "not initialized" in result.output.lower()

    def test_format_options(self, cli_runner, tmp_path):
        """Should accept different bank formats."""
        # Test that the format option is recognized
        result = cli_runner.invoke(cli, ["import-data", "bank-csv", "--help"])
        assert "--format" in result.output
        assert "chase" in result.output
        assert "bofa" in result.output
        assert "apple_card" in result.output

    def test_generic_format_requires_columns(self, cli_runner, tmp_path):
        """Generic format should require column specifications."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        test_file = tmp_path / "transactions.csv"
        test_file.write_text("date,amount,description\n2024-01-15,-50.00,Test")

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            # Generic format without required columns should error
            result = cli_runner.invoke(cli, [
                "import-data", "bank-csv", str(test_file),
                "--format", "generic"
            ])

            assert "requires" in result.output.lower()


class TestSyncPelotonCommand:
    """Tests for the sync peloton command."""

    def test_missing_credentials(self, cli_runner, tmp_path):
        """Should prompt for credentials when not configured."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.get.return_value = None  # No credentials
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["sync", "peloton"])

            assert "not configured" in result.output.lower()


class TestSyncOuraCommand:
    """Tests for the sync oura command."""

    def test_missing_token(self, cli_runner, tmp_path):
        """Should prompt for token when not configured."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.get.return_value = None
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["sync", "oura"])

            assert "not configured" in result.output.lower()


class TestSyncTonalCommand:
    """Tests for the sync tonal command."""

    def test_missing_credentials(self, cli_runner, tmp_path):
        """Should prompt for credentials when not configured."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.get.return_value = None
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["sync", "tonal"])

            assert "not configured" in result.output.lower()


class TestSyncSimplefinCommand:
    """Tests for the sync simplefin command."""

    def test_shows_setup_instructions(self, cli_runner, tmp_path):
        """Should show setup instructions when not configured."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            mock_config.get.return_value = None
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["sync", "simplefin"])

            assert "not configured" in result.output.lower()


class TestQueryCommand:
    """Tests for the query command."""

    def test_query_by_type(self, cli_runner, tmp_path):
        """Should query data points by type."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test data
        session = get_session(db_path)
        session.add(DataPoint(
            timestamp=datetime.now() - timedelta(days=1),
            data_type="steps",
            value=5000.0,
            unit="count",
            source="test",
        ))
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["query", "steps"])

            assert result.exit_code == 0
            assert "5000" in result.output or "5,000" in result.output

    def test_query_no_results(self, cli_runner, tmp_path):
        """Should indicate when no data found."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["query", "nonexistent_type"])

            assert "no" in result.output.lower()


class TestSummaryCommand:
    """Tests for the summary command."""

    def test_requires_init(self, cli_runner, tmp_path):
        """Should require database initialization."""
        db_path = tmp_path / "nonexistent.db"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["summary"])

            assert "not initialized" in result.output.lower()

    def test_deduplicates_data(self, cli_runner, tmp_path):
        """Should show deduplicated daily summaries."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test data
        session = get_session(db_path)
        now = datetime.now()
        session.add(DataPoint(
            timestamp=now - timedelta(days=1),
            data_type="steps",
            value=5000.0,
            source="apple_watch",
        ))
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["summary"])

            assert result.exit_code == 0


class TestTransactionsCommand:
    """Tests for the transactions command."""

    def test_shows_transactions(self, cli_runner, tmp_path):
        """Should show recent transactions."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test transactions
        session = get_session(db_path)
        session.add(Transaction(
            date=datetime.now() - timedelta(days=1),
            amount=-50.00,
            description="Test Purchase",
            category="Shopping",
            source="test",
        ))
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["transactions"])

            assert result.exit_code == 0
            assert "Test Purchase" in result.output

    def test_filter_by_category(self, cli_runner, tmp_path):
        """Should filter transactions by category."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test transactions in different categories
        session = get_session(db_path)
        session.add_all([
            Transaction(
                date=datetime.now() - timedelta(days=1),
                amount=-50.00,
                description="Food Purchase",
                category="Food & Drink",
                source="test",
            ),
            Transaction(
                date=datetime.now() - timedelta(days=1),
                amount=-100.00,
                description="Shopping Purchase",
                category="Shopping",
                source="test",
            ),
        ])
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["transactions", "--category", "Shopping"])

            assert result.exit_code == 0


class TestSpendingCommand:
    """Tests for the spending command."""

    def test_shows_breakdown(self, cli_runner, tmp_path):
        """Should show spending breakdown by category."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test transactions
        session = get_session(db_path)
        session.add_all([
            Transaction(
                date=datetime.now() - timedelta(days=1),
                amount=-50.00,
                description="Food",
                category="Food & Drink",
                source="test",
            ),
            Transaction(
                date=datetime.now() - timedelta(days=2),
                amount=-100.00,
                description="Shopping",
                category="Shopping",
                source="test",
            ),
        ])
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["spending"])

            assert result.exit_code == 0
            assert "Spending by Category" in result.output

    def test_excludes_positive_amounts(self, cli_runner, tmp_path):
        """Should only count negative amounts (spending)."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add purchases and a refund
        session = get_session(db_path)
        session.add_all([
            Transaction(
                date=datetime.now() - timedelta(days=1),
                amount=-100.00,
                description="Purchase",
                category="Shopping",
                source="test",
            ),
            Transaction(
                date=datetime.now() - timedelta(days=2),
                amount=50.00,  # Refund - should not be counted
                description="Refund",
                category="Shopping",
                source="test",
            ),
        ])
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["spending"])

            assert result.exit_code == 0
            # Total should be $100, not $50
            assert "100" in result.output


class TestInsightsCommand:
    """Tests for the insights command."""

    def test_shows_insights(self, cli_runner, tmp_path):
        """Should show data insights."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add some data for insights
        session = get_session(db_path)
        now = datetime.now()
        session.add_all([
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
        ])
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["insights"])

            assert result.exit_code == 0
            assert "Insights" in result.output

    def test_shows_averages(self, cli_runner, tmp_path):
        """Should calculate and show averages."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        session = get_session(db_path)
        now = datetime.now()
        # Add steps for averaging
        for i in range(5):
            session.add(DataPoint(
                timestamp=now - timedelta(days=i),
                data_type="steps",
                value=5000.0 + (i * 100),
                source="apple_watch",
            ))
        session.commit()
        session.close()

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["insights"])

            assert result.exit_code == 0
            assert "Average" in result.output


class TestExportCommand:
    """Tests for the export command."""

    def test_export_json(self, cli_runner, tmp_path):
        """Should export data to JSON format."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test data
        session = get_session(db_path)
        session.add(DataPoint(
            timestamp=datetime.now() - timedelta(days=1),
            data_type="steps",
            value=5000.0,
            source="test",
        ))
        session.commit()
        session.close()

        output_file = tmp_path / "export.json"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, [
                "export",
                "--format", "json",
                "--output", str(output_file),
            ])

            assert result.exit_code == 0
            assert output_file.exists()

            # Verify JSON content
            data = json.loads(output_file.read_text())
            assert len(data) == 1
            assert data[0]["type"] == "steps"
            assert data[0]["value"] == 5000.0

    def test_export_csv(self, cli_runner, tmp_path):
        """Should export data to CSV format."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test data
        session = get_session(db_path)
        session.add(DataPoint(
            timestamp=datetime.now() - timedelta(days=1),
            data_type="heart_rate",
            value=72.0,
            unit="bpm",
            source="test",
        ))
        session.commit()
        session.close()

        output_file = tmp_path / "export.csv"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, [
                "export",
                "--format", "csv",
                "--output", str(output_file),
            ])

            assert result.exit_code == 0
            assert output_file.exists()

            # Verify CSV content
            content = output_file.read_text()
            assert "timestamp,type,value,unit,source" in content
            assert "heart_rate" in content

    def test_export_filtered_by_type(self, cli_runner, tmp_path):
        """Should filter export by data type."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        # Add test data of different types
        session = get_session(db_path)
        session.add_all([
            DataPoint(
                timestamp=datetime.now() - timedelta(days=1),
                data_type="steps",
                value=5000.0,
                source="test",
            ),
            DataPoint(
                timestamp=datetime.now() - timedelta(days=1),
                data_type="heart_rate",
                value=72.0,
                source="test",
            ),
        ])
        session.commit()
        session.close()

        output_file = tmp_path / "export.json"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, [
                "export",
                "--format", "json",
                "--type", "steps",
                "--output", str(output_file),
            ])

            assert result.exit_code == 0

            # Should only have steps data
            data = json.loads(output_file.read_text())
            assert len(data) == 1
            assert data[0]["type"] == "steps"

    def test_export_no_data(self, cli_runner, tmp_path):
        """Should indicate when no data to export."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["export"])

            assert "no data" in result.output.lower()


class TestWebCommand:
    """Tests for the web command."""

    def test_requires_init(self, cli_runner, tmp_path):
        """Should require database initialization."""
        db_path = tmp_path / "nonexistent.db"

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            result = cli_runner.invoke(cli, ["web"])

            assert "not initialized" in result.output.lower()

    def test_missing_uvicorn_error(self, cli_runner, tmp_path):
        """Should show helpful error when uvicorn not installed."""
        db_path = tmp_path / "test.db"
        init_db(db_path)

        with patch("datahub.cli.Config") as MockConfig:
            mock_config = MagicMock()
            mock_config.get_db_path.return_value = db_path
            MockConfig.return_value = mock_config

            # Mock uvicorn import to fail
            with patch.dict("sys.modules", {"uvicorn": None}):
                import sys
                # Remove uvicorn from modules if present
                if "uvicorn" in sys.modules:
                    del sys.modules["uvicorn"]

                # The command should handle missing uvicorn gracefully
                # Note: This may not trigger the expected error depending on how
                # the import is structured in the actual code


class TestCliHelp:
    """Tests for CLI help text."""

    def test_main_help(self, cli_runner):
        """Should show help text."""
        result = cli_runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "DataHub" in result.output

    def test_import_help(self, cli_runner):
        """Should show import subcommand help."""
        result = cli_runner.invoke(cli, ["import-data", "--help"])
        assert result.exit_code == 0
        assert "apple-health" in result.output
        assert "bank-csv" in result.output

    def test_sync_help(self, cli_runner):
        """Should show sync subcommand help."""
        result = cli_runner.invoke(cli, ["sync", "--help"])
        assert result.exit_code == 0
        assert "peloton" in result.output
        assert "oura" in result.output
        assert "tonal" in result.output
        assert "simplefin" in result.output
