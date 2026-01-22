"""Configuration management for DataHub."""

import json
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_DIR = Path.home() / ".datahub"
DEFAULT_DB_PATH = DEFAULT_CONFIG_DIR / "datahub.db"
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.json"


class Config:
    """Manages DataHub configuration."""

    def __init__(self, config_path: Path | None = None):
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        self.config_dir = self.config_path.parent
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load config from disk."""
        if self.config_path.exists():
            self._data = json.loads(self.config_path.read_text())
        else:
            self._data = {}

    def _save(self) -> None:
        """Save config to disk."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(self._data, indent=2))

    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value using dot notation (e.g., 'peloton.username')."""
        keys = key.split(".")
        value = self._data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value

    def set(self, key: str, value: Any) -> None:
        """Set a config value using dot notation."""
        keys = key.split(".")
        data = self._data
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        data[keys[-1]] = value
        self._save()

    def get_db_path(self) -> Path:
        """Get the database path."""
        return Path(self.get("db_path", str(DEFAULT_DB_PATH)))

    @property
    def data_dir(self) -> Path:
        """Directory for storing imported data files."""
        path = self.config_dir / "data"
        path.mkdir(parents=True, exist_ok=True)
        return path
