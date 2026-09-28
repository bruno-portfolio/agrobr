"""Tests for config module."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from agrobr.config import (
    AgrobrConfig,
    get_config,
    reset_config,
    set_mode,
)


class TestAgrobrConfig:
    def setup_method(self):
        reset_config()

    def teardown_method(self):
        reset_config()

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

    def test_set_mode_with_string_date(self):
        set_mode("deterministic", snapshot="2025-12-31")
        config = get_config()
        assert config.snapshot_date == date(2025, 12, 31)

    def test_set_mode_with_custom_path(self):
        set_mode("deterministic", snapshot="2025-01-01", snapshot_path="/my/snapshots")
        config = get_config()
        assert config.snapshot_path == Path("/my/snapshots")
