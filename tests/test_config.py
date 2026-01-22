"""Tests for configuration management."""

import pytest
import json
from pathlib import Path

from datahub.config import Config, DEFAULT_DB_PATH


class TestConfigGet:
    """Tests for Config.get method."""

    def test_simple_key(self, temp_config):
        """Should retrieve simple top-level key."""
        temp_config._data = {"username": "test_user"}

        result = temp_config.get("username")

        assert result == "test_user"

    def test_nested_key_with_dot_notation(self, temp_config):
        """Should retrieve nested key using dot notation."""
        temp_config._data = {
            "peloton": {
                "username": "peloton_user",
                "password": "secret",
            }
        }

        result = temp_config.get("peloton.username")

        assert result == "peloton_user"

    def test_deeply_nested_key(self, temp_config):
        """Should retrieve deeply nested keys."""
        temp_config._data = {
            "services": {
                "api": {
                    "oauth": {
                        "token": "abc123"
                    }
                }
            }
        }

        result = temp_config.get("services.api.oauth.token")

        assert result == "abc123"

    def test_missing_key_returns_none(self, temp_config):
        """Missing key should return None by default."""
        temp_config._data = {}

        result = temp_config.get("nonexistent")

        assert result is None

    def test_missing_key_returns_custom_default(self, temp_config):
        """Missing key should return provided default value."""
        temp_config._data = {}

        result = temp_config.get("nonexistent", default="fallback")

        assert result == "fallback"

    def test_missing_nested_key_returns_default(self, temp_config):
        """Missing nested key should return default."""
        temp_config._data = {"peloton": {}}

        result = temp_config.get("peloton.missing_key", default="default_val")

        assert result == "default_val"

    def test_partial_nested_path_returns_default(self, temp_config):
        """Partial path (non-dict intermediate) should return default."""
        temp_config._data = {"peloton": "not_a_dict"}

        result = temp_config.get("peloton.username", default="fallback")

        assert result == "fallback"


class TestConfigSet:
    """Tests for Config.set method."""

    def test_simple_key(self, temp_config):
        """Should set simple top-level key."""
        temp_config.set("api_key", "my_key")

        assert temp_config._data["api_key"] == "my_key"

    def test_nested_key_creates_structure(self, temp_config):
        """Should create nested structure for dot notation keys."""
        temp_config.set("peloton.username", "test_user")

        assert temp_config._data["peloton"]["username"] == "test_user"

    def test_deeply_nested_key(self, temp_config):
        """Should create deeply nested structure."""
        temp_config.set("a.b.c.d", "deep_value")

        assert temp_config._data["a"]["b"]["c"]["d"] == "deep_value"

    def test_overwrites_existing(self, temp_config):
        """Should overwrite existing values."""
        temp_config._data = {"key": "old_value"}

        temp_config.set("key", "new_value")

        assert temp_config._data["key"] == "new_value"

    def test_overwrites_nested_existing(self, temp_config):
        """Should overwrite existing nested values."""
        temp_config._data = {"peloton": {"username": "old_user"}}

        temp_config.set("peloton.username", "new_user")

        assert temp_config._data["peloton"]["username"] == "new_user"

    def test_preserves_sibling_keys(self, temp_config):
        """Setting nested key should preserve sibling keys."""
        temp_config._data = {"peloton": {"username": "user", "password": "pass"}}

        temp_config.set("peloton.username", "new_user")

        assert temp_config._data["peloton"]["username"] == "new_user"
        assert temp_config._data["peloton"]["password"] == "pass"


class TestConfigPersistence:
    """Tests for Config persistence (save/load)."""

    def test_save_and_reload(self, tmp_path):
        """Config should persist across instances."""
        config_path = tmp_path / "config.json"

        # Create config and set value
        config1 = Config(config_path=config_path)
        config1.set("test_key", "test_value")

        # Create new instance from same file
        config2 = Config(config_path=config_path)

        assert config2.get("test_key") == "test_value"

    def test_saves_nested_structure(self, tmp_path):
        """Nested structure should persist correctly."""
        config_path = tmp_path / "config.json"

        config1 = Config(config_path=config_path)
        config1.set("peloton.username", "user123")
        config1.set("peloton.password", "secret")

        config2 = Config(config_path=config_path)

        assert config2.get("peloton.username") == "user123"
        assert config2.get("peloton.password") == "secret"

    def test_creates_config_directory(self, tmp_path):
        """Should create config directory if it doesn't exist."""
        config_path = tmp_path / "subdir" / "config.json"

        config = Config(config_path=config_path)
        config.set("key", "value")

        assert config_path.parent.exists()
        assert config_path.exists()

    def test_handles_missing_config_file(self, tmp_path):
        """Should handle missing config file gracefully."""
        config_path = tmp_path / "nonexistent.json"

        config = Config(config_path=config_path)

        assert config._data == {}

    def test_invalid_json_handling(self, tmp_path):
        """Should handle invalid JSON gracefully."""
        config_path = tmp_path / "config.json"
        config_path.write_text("{ invalid json }")

        # Should raise JSONDecodeError
        with pytest.raises(json.JSONDecodeError):
            Config(config_path=config_path)


class TestConfigGetDbPath:
    """Tests for Config.get_db_path method."""

    def test_default_path(self, temp_config):
        """Should return default path when not configured."""
        db_path = temp_config.get_db_path()

        assert db_path == DEFAULT_DB_PATH

    def test_custom_path(self, temp_config):
        """Should return custom path when configured."""
        temp_config._data = {"db_path": "/custom/path/data.db"}

        db_path = temp_config.get_db_path()

        assert db_path == Path("/custom/path/data.db")


class TestConfigDataDir:
    """Tests for Config.data_dir property."""

    def test_returns_data_subdirectory(self, temp_config):
        """Should return data subdirectory of config dir."""
        data_dir = temp_config.data_dir

        assert data_dir == temp_config.config_dir / "data"

    def test_creates_directory(self, temp_config):
        """Should create the data directory."""
        data_dir = temp_config.data_dir

        assert data_dir.exists()
        assert data_dir.is_dir()
