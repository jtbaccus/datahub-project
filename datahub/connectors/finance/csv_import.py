"""CSV bank statement import connector.

Supports common bank export formats:
- Chase (default)
- Bank of America
- Generic (configurable columns)

Usage:
    datahub import bank-csv statement.csv --format chase
    datahub import bank-csv statement.csv --format generic --date-col "Date" --amount-col "Amount"
"""

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

from sqlalchemy import select
from sqlalchemy.orm import Session

from datahub.connectors.base import FileImportConnector
from datahub.db import Transaction


# Column mappings for known bank formats
BANK_FORMATS = {
    "chase": {
        "date": "Transaction Date",
        "description": "Description",
        "amount": "Amount",
        "category": "Category",
        "type": "Type",
    },
    "bofa": {
        "date": "Date",
        "description": "Description",
        "amount": "Amount",
        "category": None,
        "type": None,
    },
    "apple_card": {
        "date": "Transaction Date",
        "description": "Description",
        "amount": "Amount (USD)",
        "merchant": "Merchant",
        "category": "Category",
    },
    "amex": {
        "date": "Date",
        "description": "Description",
        "amount": "Amount",
        "category": "Category",
    },
}


def parse_date(date_str: str) -> datetime:
    """Parse common date formats from bank exports."""
    formats = [
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m/%d/%y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: {date_str}")


def parse_amount(amount_str: str) -> float:
    """Parse amount string, handling currency symbols and parentheses for negatives."""
    cleaned = amount_str.strip()
    # Remove currency symbols
    cleaned = cleaned.replace("$", "").replace(",", "")
    # Handle parentheses for negatives: (100.00) -> -100.00
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    return float(cleaned)


def generate_transaction_id(date: datetime, amount: float, description: str) -> str:
    """Generate a unique ID for deduplication."""
    content = f"{date.isoformat()}|{amount}|{description}"
    return hashlib.md5(content.encode()).hexdigest()[:16]


class CSVBankConnector(FileImportConnector):
    """Import transactions from bank CSV exports."""

    name = "csv_bank"

    def __init__(
        self,
        session: Session,
        config: dict | None = None,
        bank_format: str = "chase",
        custom_columns: dict | None = None,
        account_name: str | None = None,
    ):
        super().__init__(session, config)
        self.account_name = account_name

        if bank_format == "generic" and custom_columns:
            self.columns = custom_columns
        elif bank_format in BANK_FORMATS:
            self.columns = BANK_FORMATS[bank_format]
        else:
            raise ValueError(f"Unknown bank format: {bank_format}. Use one of: {list(BANK_FORMATS.keys())} or 'generic'")

        self.bank_format = bank_format

    def _iter_transactions(self, file_path: Path) -> Iterator[dict]:
        """Iterate over transactions in CSV file."""
        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    date_col = self.columns["date"]
                    amount_col = self.columns["amount"]
                    desc_col = self.columns["description"]

                    if date_col not in row or amount_col not in row:
                        continue

                    date = parse_date(row[date_col])
                    amount = parse_amount(row[amount_col])
                    description = row.get(desc_col, "").strip()

                    # Get optional fields
                    category = None
                    if self.columns.get("category") and self.columns["category"] in row:
                        category = row[self.columns["category"]].strip() or None

                    merchant = None
                    if self.columns.get("merchant") and self.columns["merchant"] in row:
                        merchant = row[self.columns["merchant"]].strip() or None

                    yield {
                        "date": date,
                        "amount": amount,
                        "description": description,
                        "category": category,
                        "merchant": merchant,
                        "raw": dict(row),
                    }
                except (ValueError, KeyError) as e:
                    # Skip malformed rows
                    continue

    def _transaction_exists(self, source_id: str) -> bool:
        """Check if transaction already exists."""
        stmt = select(Transaction).where(Transaction.source_id == source_id)
        return self.session.execute(stmt).first() is not None

    def import_file(self, file_path: Path) -> tuple[int, int]:
        """Import transactions from CSV file."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        added = 0
        skipped = 0
        batch = []
        batch_size = 500

        for txn in self._iter_transactions(file_path):
            source_id = generate_transaction_id(txn["date"], txn["amount"], txn["description"])

            if self._transaction_exists(source_id):
                skipped += 1
                continue

            batch.append(Transaction(
                date=txn["date"],
                amount=txn["amount"],
                description=txn["description"],
                merchant=txn.get("merchant"),
                category=txn.get("category"),
                account=self.account_name,
                source=f"csv_{self.bank_format}",
                source_id=source_id,
                metadata_json=json.dumps(txn["raw"]),
            ))
            added += 1

            if len(batch) >= batch_size:
                self.session.add_all(batch)
                self.session.commit()
                batch = []

        if batch:
            self.session.add_all(batch)
            self.session.commit()

        return added, skipped
