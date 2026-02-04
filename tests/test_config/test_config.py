"""Tests for config module."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from agrobr.config import (
    AgrobrConfig,
    configure,
    get_config,
    reset_config,
    set_mode,
)


class TestAgrobrConfig:
    def setup_method(self):
        reset_config()

    def teardown_method(self):
        reset_config()

    def test_default_config(self):
        config = get_config()
        assert config.mode == "normal"
        assert config.network_enabled is True
        assert config.cache_enabled is True
        assert config.snapshot_date is None

    def test_is_deterministic_false_by_default(self):
        config = get_config()
        assert config.is_deterministic() is False

    def test_is_deterministic_true_when_set(self):
        set_mode("deterministic", snapshot="2025-01-01")
        config = get_config()
        assert config.is_deterministic() is True

    def test_get_snapshot_dir_default(self):
        config = get_config()
        snapshot_dir = config.get_snapshot_dir()
        assert snapshot_dir == Path.home() / ".agrobr" / "snapshots"

    def test_get_snapshot_dir_custom(self):
        config = AgrobrConfig(snapshot_path=Path("/custom/path"))
        assert config.get_snapshot_dir() == Path("/custom/path")

    def test_get_current_snapshot_path_none(self):
        config = get_config()
        assert config.get_current_snapshot_path() is None

    def test_get_current_snapshot_path_with_date(self):
        set_mode("deterministic", snapshot="2025-06-15")
        config = get_config()
        expected = config.get_snapshot_dir() / "2025-06-15"
        assert config.get_current_snapshot_path() == expected


class TestSetMode:
    def setup_method(self):
        reset_config()

    def teardown_method(self):
        reset_config()

    def test_set_mode_normal(self):
        set_mode("normal")
        config = get_config()
        assert config.mode == "normal"
        assert config.network_enabled is True

    def test_set_mode_deterministic(self):
        set_mode("deterministic", snapshot="2025-01-01")
        config = get_config()
        assert config.mode == "deterministic"
        assert config.network_enabled is False

    def test_set_mode_with_string_date(self):
        set_mode("deterministic", snapshot="2025-12-31")
        config = get_config()
        assert config.snapshot_date == date(2025, 12, 31)

    def test_set_mode_with_date_object(self):
        set_mode("deterministic", snapshot=date(2025, 6, 15))
        config = get_config()
        assert config.snapshot_date == date(2025, 6, 15)

    def test_set_mode_with_custom_path(self):
        set_mode("deterministic", snapshot="2025-01-01", snapshot_path="/my/snapshots")
        config = get_config()
        assert config.snapshot_path == Path("/my/snapshots")


class TestConfigure:
    def setup_method(self):
        reset_config()

    def teardown_method(self):
        reset_config()

    def test_configure_cache_enabled(self):
        configure(cache_enabled=False)
        config = get_config()
        assert config.cache_enabled is False

    def test_configure_timeout(self):
        configure(timeout_seconds=60)
        config = get_config()
        assert config.timeout_seconds == 60

    def test_configure_browser_fallback(self):
        configure(browser_fallback=True)
        config = get_config()
        assert config.browser_fallback is True

    def test_configure_alternative_source(self):
        configure(alternative_source=False)
        config = get_config()
        assert config.alternative_source is False

    def test_configure_log_level(self):
        configure(log_level="DEBUG")
        config = get_config()
        assert config.log_level == "DEBUG"

    def test_configure_cache_path(self):
        configure(cache_path="/custom/cache")
        config = get_config()
        assert config.cache_path == Path("/custom/cache")

    def test_configure_multiple_options(self):
        configure(
            cache_enabled=False,
            timeout_seconds=45,
            log_level="WARNING",
        )
        config = get_config()
        assert config.cache_enabled is False
        assert config.timeout_seconds == 45
        assert config.log_level == "WARNING"


class TestResetConfig:
    def test_reset_config(self):
        set_mode("deterministic", snapshot="2025-01-01")
        config = get_config()
        assert config.mode == "deterministic"

        reset_config()
        config = get_config()
        assert config.mode == "normal"
