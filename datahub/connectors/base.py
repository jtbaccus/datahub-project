"""Base connector interface for all data sources."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from datahub.db import SyncLog


class BaseConnector(ABC):
    """Abstract base class for all data connectors."""

    name: str = "base"  # Override in subclasses

    def __init__(self, session: Session, config: dict | None = None):
        self.session = session
        self.config = config or {}

    @abstractmethod
    def sync(self, since: datetime | None = None) -> tuple[int, int]:
        """
        Sync data from the source.

        Args:
            since: Only sync data after this timestamp. If None, sync all available data.

        Returns:
            Tuple of (records_added, records_updated)
        """
        pass

    def _start_sync(self) -> SyncLog:
        """Record the start of a sync operation."""
        log = SyncLog(
            connector=self.name,
            started_at=datetime.now(timezone.utc),
            status="running",
        )
        self.session.add(log)
        self.session.commit()
        return log

    def _complete_sync(self, log: SyncLog, added: int, updated: int) -> None:
        """Record successful completion of a sync."""
        log.completed_at = datetime.now(timezone.utc)
        log.status = "success"
        log.records_added = added
        log.records_updated = updated
        self.session.commit()

    def _fail_sync(self, log: SyncLog, error: str) -> None:
        """Record failed sync."""
        log.completed_at = datetime.now(timezone.utc)
        log.status = "failed"
        log.error_message = error
        self.session.commit()

    def run_sync(self, since: datetime | None = None) -> SyncLog:
        """Run sync with logging."""
        log = self._start_sync()
        try:
            added, updated = self.sync(since)
            self._complete_sync(log, added, updated)
        except Exception as e:
            self._fail_sync(log, str(e))
            raise
        return log


class FileImportConnector(BaseConnector):
    """Base class for connectors that import from files."""

    @abstractmethod
    def import_file(self, file_path: Path) -> tuple[int, int]:
        """
        Import data from a file.

        Args:
            file_path: Path to the file to import.

        Returns:
            Tuple of (records_added, records_updated)
        """
        pass

    def sync(self, since: datetime | None = None) -> tuple[int, int]:
        """File connectors don't support incremental sync - use import_file instead."""
        raise NotImplementedError(
            f"{self.name} is a file-based connector. Use import_file() instead of sync()."
        )

    def run_import(self, file_path: Path) -> SyncLog:
        """Run file import with logging."""
        log = self._start_sync()
        try:
            added, updated = self.import_file(file_path)
            self._complete_sync(log, added, updated)
        except Exception as e:
            self._fail_sync(log, str(e))
            raise
        return log
