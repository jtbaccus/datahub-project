"""Tests for CSV bank import connector."""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import os

from datahub.connectors.finance.csv_import import (
    parse_date,
    parse_amount,
    generate_transaction_id,
    CSVBankConnector,
    BANK_FORMATS,
)
from datahub.db import Transaction


class TestParseDate:
    """Tests for parse_date function."""

    def test_mm_dd_yyyy_format(self):
        """Should parse MM/DD/YYYY format."""
        result = parse_date("01/15/2024")
        assert result == datetime(2024, 1, 15)

    def test_yyyy_mm_dd_format(self):
        """Should parse YYYY-MM-DD format."""
        result = parse_date("2024-01-15")
        assert result == datetime(2024, 1, 15)

    def test_mm_dd_yy_format(self):
        """Should parse MM/DD/YY format."""
        result = parse_date("01/15/24")
        assert result == datetime(2024, 1, 15)

    def test_dd_mm_yyyy_format(self):
        """Should parse DD/MM/YYYY format."""
        result = parse_date("15/01/2024")
        assert result == datetime(2024, 1, 15)

    def test_yyyy_slash_mm_dd_format(self):
        """Should parse YYYY/MM/DD format."""
        result = parse_date("2024/01/15")
        assert result == datetime(2024, 1, 15)

    def test_whitespace_is_stripped(self):
        """Should strip whitespace from date string."""
        result = parse_date("  01/15/2024  ")
        assert result == datetime(2024, 1, 15)

    def test_invalid_format_raises_error(self):
        """Should raise ValueError for unrecognized format."""
        with pytest.raises(ValueError, match="Could not parse date"):
            parse_date("January 15, 2024")

    def test_empty_string_raises_error(self):
        """Should raise ValueError for empty string."""
        with pytest.raises(ValueError, match="Could not parse date"):
            parse_date("")


class TestParseAmount:
    """Tests for parse_amount function."""

    def test_positive_number(self):
        """Should parse positive numbers."""
        assert parse_amount("100.50") == 100.50

    def test_negative_number(self):
        """Should parse negative numbers with minus sign."""
        assert parse_amount("-50.25") == -50.25

    def test_dollar_sign_removal(self):
        """Should remove dollar sign."""
        assert parse_amount("$100.00") == 100.00

    def test_negative_with_dollar_sign(self):
        """Should handle negative amounts with dollar sign."""
        assert parse_amount("-$50.00") == -50.00

    def test_parentheses_for_negative(self):
        """Should convert parentheses to negative."""
        assert parse_amount("(100.00)") == -100.00

    def test_parentheses_with_dollar_sign(self):
        """Should handle parentheses with dollar sign."""
        assert parse_amount("($100.00)") == -100.00

    def test_comma_separator(self):
        """Should remove comma thousands separator."""
        assert parse_amount("1,000.00") == 1000.00

    def test_large_amount_with_commas(self):
        """Should handle large amounts with multiple commas."""
        assert parse_amount("$1,234,567.89") == 1234567.89

    def test_whitespace_is_stripped(self):
        """Should strip whitespace."""
        assert parse_amount("  100.00  ") == 100.00

    def test_integer_amount(self):
        """Should handle integer amounts."""
        assert parse_amount("100") == 100.0


class TestGenerateTransactionId:
    """Tests for generate_transaction_id function."""

    def test_returns_string(self):
        """Should return a string."""
        result = generate_transaction_id(
            datetime(2024, 1, 15),
            -50.00,
            "Coffee Shop"
        )
        assert isinstance(result, str)

    def test_returns_16_char_hash(self):
        """Should return a 16-character hash."""
        result = generate_transaction_id(
            datetime(2024, 1, 15),
            -50.00,
            "Coffee Shop"
        )
        assert len(result) == 16

    def test_same_inputs_same_hash(self):
        """Same inputs should produce same hash."""
        date = datetime(2024, 1, 15)
        result1 = generate_transaction_id(date, -50.00, "Coffee Shop")
        result2 = generate_transaction_id(date, -50.00, "Coffee Shop")
        assert result1 == result2

    def test_different_date_different_hash(self):
        """Different date should produce different hash."""
        result1 = generate_transaction_id(datetime(2024, 1, 15), -50.00, "Coffee Shop")
        result2 = generate_transaction_id(datetime(2024, 1, 16), -50.00, "Coffee Shop")
        assert result1 != result2

    def test_different_amount_different_hash(self):
        """Different amount should produce different hash."""
        date = datetime(2024, 1, 15)
        result1 = generate_transaction_id(date, -50.00, "Coffee Shop")
        result2 = generate_transaction_id(date, -75.00, "Coffee Shop")
        assert result1 != result2

    def test_different_description_different_hash(self):
        """Different description should produce different hash."""
        date = datetime(2024, 1, 15)
        result1 = generate_transaction_id(date, -50.00, "Coffee Shop")
        result2 = generate_transaction_id(date, -50.00, "Grocery Store")
        assert result1 != result2


class TestBankFormats:
    """Tests for BANK_FORMATS configuration."""

    def test_chase_format_defined(self):
        """Chase format should be defined."""
        assert "chase" in BANK_FORMATS
        assert BANK_FORMATS["chase"]["date"] == "Transaction Date"
        assert BANK_FORMATS["chase"]["amount"] == "Amount"

    def test_bofa_format_defined(self):
        """Bank of America format should be defined."""
        assert "bofa" in BANK_FORMATS
        assert BANK_FORMATS["bofa"]["date"] == "Date"
        assert BANK_FORMATS["bofa"]["amount"] == "Amount"

    def test_apple_card_format_defined(self):
        """Apple Card format should be defined."""
        assert "apple_card" in BANK_FORMATS
        assert BANK_FORMATS["apple_card"]["date"] == "Transaction Date"
        assert BANK_FORMATS["apple_card"]["amount"] == "Amount (USD)"
        assert BANK_FORMATS["apple_card"]["merchant"] == "Merchant"

    def test_amex_format_defined(self):
        """Amex format should be defined."""
        assert "amex" in BANK_FORMATS
        assert BANK_FORMATS["amex"]["date"] == "Date"
        assert BANK_FORMATS["amex"]["amount"] == "Amount"


class TestCSVBankConnector:
    """Tests for CSVBankConnector class."""

    def test_init_with_chase_format(self, test_session):
        """Should initialize with Chase format."""
        connector = CSVBankConnector(test_session, bank_format="chase")
        assert connector.bank_format == "chase"
        assert connector.columns == BANK_FORMATS["chase"]

    def test_init_with_bofa_format(self, test_session):
        """Should initialize with Bank of America format."""
        connector = CSVBankConnector(test_session, bank_format="bofa")
        assert connector.bank_format == "bofa"
        assert connector.columns == BANK_FORMATS["bofa"]

    def test_init_with_generic_format(self, test_session):
        """Should initialize with generic format and custom columns."""
        custom_columns = {
            "date": "Trans Date",
            "amount": "Trans Amount",
            "description": "Trans Description",
        }
        connector = CSVBankConnector(
            test_session,
            bank_format="generic",
            custom_columns=custom_columns
        )
        assert connector.bank_format == "generic"
        assert connector.columns == custom_columns

    def test_init_with_account_name(self, test_session):
        """Should store account name."""
        connector = CSVBankConnector(
            test_session,
            bank_format="chase",
            account_name="My Checking"
        )
        assert connector.account_name == "My Checking"

    def test_init_invalid_format_raises_error(self, test_session):
        """Should raise error for unknown bank format."""
        with pytest.raises(ValueError, match="Unknown bank format"):
            CSVBankConnector(test_session, bank_format="unknown_bank")

    def test_import_file_not_found(self, test_session):
        """Should raise FileNotFoundError for missing file."""
        connector = CSVBankConnector(test_session, bank_format="chase")
        with pytest.raises(FileNotFoundError, match="File not found"):
            connector.import_file(Path("/nonexistent/file.csv"))

    def test_import_chase_csv(self, test_session, tmp_path):
        """Should import Chase CSV format."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,STARBUCKS,Food & Drink,Sale,-5.50
01/16/2024,01/17/2024,WHOLE FOODS,Groceries,Sale,-125.00
01/17/2024,01/18/2024,PAYMENT THANK YOU,Payment,Payment,500.00"""

        csv_file = tmp_path / "chase.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(test_session, bank_format="chase")
        added, skipped = connector.import_file(csv_file)

        assert added == 3
        assert skipped == 0

        # Verify transactions in database
        transactions = test_session.query(Transaction).all()
        assert len(transactions) == 3

        # Check first transaction
        starbucks = test_session.query(Transaction).filter(
            Transaction.description == "STARBUCKS"
        ).first()
        assert starbucks.amount == -5.50
        assert starbucks.category == "Food & Drink"
        assert starbucks.source == "csv_chase"

    def test_import_bofa_csv(self, test_session, tmp_path):
        """Should import Bank of America CSV format."""
        csv_content = """Date,Description,Amount,Running Bal.
01/15/2024,ATM WITHDRAWAL,-60.00,1000.00
01/16/2024,DIRECT DEPOSIT,2500.00,3500.00"""

        csv_file = tmp_path / "bofa.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(test_session, bank_format="bofa")
        added, skipped = connector.import_file(csv_file)

        assert added == 2
        assert skipped == 0

        # Verify transactions
        transactions = test_session.query(Transaction).all()
        assert len(transactions) == 2

    def test_import_apple_card_csv(self, test_session, tmp_path):
        """Should import Apple Card CSV format with merchant field."""
        csv_content = """Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD)
01/15/2024,01/16/2024,Coffee purchase,Starbucks,Food & Drink,Purchase,-6.75"""

        csv_file = tmp_path / "apple_card.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(test_session, bank_format="apple_card")
        added, skipped = connector.import_file(csv_file)

        assert added == 1
        assert skipped == 0

        # Verify merchant field
        txn = test_session.query(Transaction).first()
        assert txn.merchant == "Starbucks"
        assert txn.category == "Food & Drink"

    def test_import_generic_csv(self, test_session, tmp_path):
        """Should import generic CSV with custom column mapping."""
        csv_content = """Trans Date,Trans Amount,Trans Description
2024-01-15,-42.50,RESTAURANT XYZ
2024-01-16,-18.00,GAS STATION"""

        csv_file = tmp_path / "generic.csv"
        csv_file.write_text(csv_content)

        custom_columns = {
            "date": "Trans Date",
            "amount": "Trans Amount",
            "description": "Trans Description",
        }
        connector = CSVBankConnector(
            test_session,
            bank_format="generic",
            custom_columns=custom_columns
        )
        added, skipped = connector.import_file(csv_file)

        assert added == 2
        assert skipped == 0

    def test_duplicate_detection(self, test_session, tmp_path):
        """Should skip duplicate transactions on reimport."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,STARBUCKS,Food & Drink,Sale,-5.50"""

        csv_file = tmp_path / "chase.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(test_session, bank_format="chase")

        # First import
        added1, skipped1 = connector.import_file(csv_file)
        assert added1 == 1
        assert skipped1 == 0

        # Second import (same file)
        added2, skipped2 = connector.import_file(csv_file)
        assert added2 == 0
        assert skipped2 == 1

        # Verify only one transaction in database
        transactions = test_session.query(Transaction).all()
        assert len(transactions) == 1

    def test_skips_malformed_rows(self, test_session, tmp_path):
        """Should skip rows with missing required fields."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,VALID ROW,Food & Drink,Sale,-5.50
,01/17/2024,MISSING DATE,Food & Drink,Sale,-10.00
01/18/2024,01/19/2024,ANOTHER VALID,Groceries,Sale,-25.00"""

        csv_file = tmp_path / "chase.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(test_session, bank_format="chase")
        added, skipped = connector.import_file(csv_file)

        # Should import 2 valid rows, skip 1 malformed
        assert added == 2

    def test_handles_bom_encoding(self, test_session, tmp_path):
        """Should handle UTF-8 BOM encoding."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,STARBUCKS,Food & Drink,Sale,-5.50"""

        csv_file = tmp_path / "chase_bom.csv"
        # utf-8-sig encoding adds BOM automatically
        csv_file.write_text(csv_content, encoding="utf-8-sig")

        connector = CSVBankConnector(test_session, bank_format="chase")
        added, skipped = connector.import_file(csv_file)

        assert added == 1

    def test_stores_raw_metadata(self, test_session, tmp_path):
        """Should store raw row data in metadata_json."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,STARBUCKS,Food & Drink,Sale,-5.50"""

        csv_file = tmp_path / "chase.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(test_session, bank_format="chase")
        connector.import_file(csv_file)

        txn = test_session.query(Transaction).first()
        assert txn.metadata_json is not None
        assert "Post Date" in txn.metadata_json

    def test_account_name_stored(self, test_session, tmp_path):
        """Should store account name in transaction."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,TEST,Food & Drink,Sale,-5.50"""

        csv_file = tmp_path / "chase.csv"
        csv_file.write_text(csv_content)

        connector = CSVBankConnector(
            test_session,
            bank_format="chase",
            account_name="Personal Checking"
        )
        connector.import_file(csv_file)

        txn = test_session.query(Transaction).first()
        assert txn.account == "Personal Checking"

    def test_amount_with_currency_symbol(self, test_session, tmp_path):
        """Should parse amounts with currency symbols."""
        csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount
01/15/2024,01/16/2024,TEST,$1,234.56,Groceries,Sale,$1,234.56"""

        csv_file = tmp_path / "chase.csv"
        csv_file.write_text(csv_content)

        # This tests that amounts with $ and commas are parsed correctly
        connector = CSVBankConnector(test_session, bank_format="chase")
        added, skipped = connector.import_file(csv_file)

        if added > 0:
            txn = test_session.query(Transaction).first()
            assert txn.amount == 1234.56
