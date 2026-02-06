"""SimpleFIN Bridge connector for automated bank transaction syncing.

SimpleFIN Bridge is a $15/year service that connects to your banks and provides
a simple API for fetching transactions.

Setup:
1. Subscribe at https://simplefin.org
2. Create a connection to your bank(s)
3. Get a setup token from the SimpleFIN dashboard
4. Run: datahub sync simplefin --setup "YOUR_SETUP_TOKEN"

Usage:
    datahub sync simplefin               # Sync last 30 days
    datahub sync simplefin --days 60     # Sync last 60 days
"""

import base64
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from datahub.connectors.base import BaseConnector
from datahub.db import Transaction


class SimpleFINConnector(BaseConnector):
    """Sync transactions from SimpleFIN Bridge."""

    name = "simplefin"

    def __init__(self, session: Session, config: dict | None = None):
        super().__init__(session, config)
        self._http_client: httpx.Client | None = None

    def _parse_access_url(self, url: str) -> tuple[str, str, str]:
        """
        Parse SimpleFIN access URL into components.

        Access URL format: https://username:password@api.simplefin.org/simplefin

        Returns:
            Tuple of (base_url, username, password)
        """
        parsed = urlparse(url)
        username = parsed.username or ""
        password = parsed.password or ""

        # Reconstruct base URL without credentials
        base_url = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            base_url += f":{parsed.port}"
        base_url += parsed.path

        return base_url, username, password

    def _get_client(self) -> httpx.Client:
        """Get or create authenticated HTTP client."""
        if self._http_client is None:
            access_url = self.config.get("access_url")
            if not access_url:
                raise ValueError(
                    "SimpleFIN not configured. Run:\n"
                    "  datahub sync simplefin --setup YOUR_SETUP_TOKEN\n\n"
                    "Get your setup token from: https://simplefin.org"
                )

            base_url, username, password = self._parse_access_url(access_url)

            self._http_client = httpx.Client(
                base_url=base_url,
                auth=(username, password),
                timeout=60.0,
            )
        return self._http_client

    def claim_setup_token(self, token: str) -> str:
        """
        Claim a SimpleFIN setup token and return the access URL.

        Args:
            token: Base64-encoded setup token from SimpleFIN

        Returns:
            The access URL with embedded credentials
        """
        # Decode the base64 token to get the claim URL
        try:
            claim_url = base64.b64decode(token).decode("utf-8")
        except Exception as e:
            raise ValueError(f"Invalid setup token: could not decode. Error: {e}")

        # POST to the claim URL to get the access URL
        with httpx.Client(timeout=30.0) as client:
            response = client.post(claim_url)
            if response.status_code != 200:
                raise ValueError(
                    f"Failed to claim setup token: {response.status_code} - {response.text}"
                )
            access_url = response.text.strip()

        return access_url

    def _fetch_accounts(
        self, start_date: datetime, end_date: datetime
    ) -> dict:
        """
        Fetch accounts and transactions from SimpleFIN.

        Args:
            start_date: Start of date range
            end_date: End of date range

        Returns:
            API response with accounts and transactions
        """
        client = self._get_client()

        # SimpleFIN uses Unix timestamps
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())

        response = client.get(
            "/accounts",
            params={
                "start-date": start_ts,
                "end-date": end_ts,
            },
        )

        if response.status_code != 200:
            raise ValueError(
                f"Failed to fetch accounts: {response.status_code} - {response.text}"
            )

        return response.json()

    def _transaction_exists(self, source_id: str) -> bool:
        """Check if transaction already exists."""
        stmt = select(Transaction).where(
            Transaction.source == "simplefin",
            Transaction.source_id == source_id,
        )
        return self.session.execute(stmt).first() is not None

    def sync(self, since: datetime | None = None) -> tuple[int, int]:
        """
        Sync transactions from SimpleFIN.

        Args:
            since: Only sync transactions after this date

        Returns:
            Tuple of (records_added, records_skipped)
        """
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(days=30)

        end_date = datetime.now(timezone.utc)

        # Fetch accounts and transactions
        data = self._fetch_accounts(since, end_date)

        if data.get("errors"):
            error_msgs = [e.get("error", str(e)) for e in data["errors"]]
            raise ValueError(f"SimpleFIN API errors: {'; '.join(error_msgs)}")

        added = 0
        skipped = 0
        batch = []
        batch_size = 500

        for account in data.get("accounts", []):
            account_name = account.get("name", "Unknown Account")
            account_id = account.get("id", "")

            for txn in account.get("transactions", []):
                # Use SimpleFIN transaction ID as source_id
                txn_id = txn.get("id")
                if not txn_id:
                    continue

                source_id = f"simplefin_{txn_id}"

                if self._transaction_exists(source_id):
                    skipped += 1
                    continue

                # Parse posted timestamp (Unix timestamp)
                posted = txn.get("posted")
                if posted:
                    txn_date = datetime.fromtimestamp(posted)
                else:
                    # Fall back to transacted_at if posted not available
                    transacted = txn.get("transacted_at")
                    if transacted:
                        txn_date = datetime.fromtimestamp(transacted)
                    else:
                        continue

                # Get amount (SimpleFIN provides as string or number)
                amount_str = txn.get("amount", "0")
                try:
                    amount = float(amount_str)
                except (ValueError, TypeError):
                    continue

                description = txn.get("description", "") or txn.get("memo", "")
                payee = txn.get("payee")

                # Build metadata
                metadata = {
                    "id": txn_id,
                    "account_id": account_id,
                    "account_name": account_name,
                    "pending": txn.get("pending", False),
                    "payee": payee,
                    "memo": txn.get("memo"),
                    "transacted_at": txn.get("transacted_at"),
                    "posted": posted,
                }

                batch.append(Transaction(
                    date=txn_date,
                    amount=amount,
                    description=description,
                    merchant=payee,
                    category=None,  # SimpleFIN doesn't provide categories
                    account=account_name,
                    source="simplefin",
                    source_id=source_id,
                    metadata_json=json.dumps(metadata),
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

    def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None
