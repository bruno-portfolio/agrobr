"""Tests for agrobr.health.state module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from agrobr.alerts.notifier import AlertLevel
from agrobr.constants import AlertSettings, Fonte
from agrobr.health.state import (
    get_alertable_failures,
    get_consecutive_failures,
    get_last_success,
    record_check,
    should_send_alert,
)


@pytest.fixture()
def mock_conn():
    """Provide a mock DuckDB connection for health state tests."""
    import threading

    conn = MagicMock()
    store = MagicMock()
    store._lock = threading.Lock()
    store._get_conn.return_value = conn
    with patch("agrobr.health.state.get_store", return_value=store):
        yield conn


class TestRecordCheck:
    def test_record_check_persists(self, mock_conn):
        record_check(
            source=Fonte.CEPEA,
            status="ok",
            category=None,
            latency_ms=100.0,
            message="All OK",
        )
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO health_checks" in call_args[0][0]
        assert call_args[0][1] == ["cepea", "ok", None, 100.0, "All OK"]

    def test_record_check_with_category(self, mock_conn):
        record_check(
            source=Fonte.CONAB,
            status="failed",
            category="source_down",
            latency_ms=0.0,
            message="HTTP 503",
        )
        call_args = mock_conn.execute.call_args
        assert call_args[0][1][2] == "source_down"


class TestGetConsecutiveFailures:
    def test_returns_count(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (3,)
        result = get_consecutive_failures(Fonte.CEPEA)
        assert result == 3

    def test_returns_zero_when_no_rows(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (0,)
        result = get_consecutive_failures(Fonte.CEPEA)
        assert result == 0

    def test_returns_zero_on_none(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = None
        result = get_consecutive_failures(Fonte.CEPEA)
        assert result == 0


class TestGetLastSuccess:
    def test_returns_datetime(self, mock_conn):
        dt = datetime(2024, 6, 15, 10, 0, 0)
        mock_conn.execute.return_value.fetchone.return_value = (dt,)
        result = get_last_success(Fonte.CEPEA)
        assert result == dt

    def test_returns_none_when_no_success(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        result = get_last_success(Fonte.CEPEA)
        assert result is None


class TestShouldSendAlert:
    def _settings(self, **overrides):
        return AlertSettings(**overrides)

    def test_first_failure_no_alert(self, mock_conn):
        # 1 failure (current) < consecutive_failures_warning (2)
        mock_conn.execute.return_value.fetchall.return_value = [("source_down",)]
        alert, level = should_send_alert(Fonte.CEPEA, "failed", "source_down")
        assert alert is False
        assert level is None

    def test_second_failure_warning(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [("source_down",)] * 2
        alert, level = should_send_alert(Fonte.CEPEA, "failed", "source_down")
        assert alert is True
        assert level == AlertLevel.WARNING

    def test_third_failure_critical(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [("source_down",)] * 3
        alert, level = should_send_alert(Fonte.CEPEA, "failed", "source_down")
        assert alert is True
        assert level == AlertLevel.CRITICAL

    def test_recovery_sends_info(self, mock_conn):
        # current_status=ok but there were prior alertable failures
        mock_conn.execute.return_value.fetchall.return_value = [
            ("source_down",),
            ("source_down",),
        ]
        alert, level = should_send_alert(Fonte.CEPEA, "ok", None)
        assert alert is True
        assert level == AlertLevel.INFO

    def test_recovery_disabled(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("source_down",),
            ("source_down",),
        ]
        settings = self._settings(alert_on_recovery=False)
        alert, level = should_send_alert(Fonte.CEPEA, "ok", None, settings=settings)
        assert alert is False
        assert level is None

    def test_api_key_missing_never_alerts(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (5,)
        alert, level = should_send_alert(Fonte.USDA, "warning", "api_key_missing")
        assert alert is False
        assert level is None

    def test_source_down_flag_disabled(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (3,)
        settings = self._settings(alert_on_source_down=False)
        alert, level = should_send_alert(
            Fonte.CONAB,
            "failed",
            "source_down",
            settings=settings,
        )
        assert alert is False
        assert level is None

    def test_layout_change_flag_disabled(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (3,)
        settings = self._settings(alert_on_layout_change=False)
        alert, level = should_send_alert(
            Fonte.CEPEA,
            "failed",
            "layout_change",
            settings=settings,
        )
        assert alert is False
        assert level is None

    def test_parse_error_flag_enabled_critical(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [("parse_error",)] * 3
        settings = self._settings(alert_on_parse_error=True)
        alert, level = should_send_alert(
            Fonte.CEPEA,
            "failed",
            "parse_error",
            settings=settings,
        )
        assert alert is True
        assert level == AlertLevel.CRITICAL

    def test_anomaly_flag_disabled(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (3,)
        settings = self._settings(alert_on_anomaly=False)
        alert, level = should_send_alert(
            Fonte.CEPEA,
            "failed",
            "anomaly",
            settings=settings,
        )
        assert alert is False
        assert level is None

    def test_ok_no_prior_failures(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = []
        alert, level = should_send_alert(Fonte.CEPEA, "ok", None)
        assert alert is False
        assert level is None

    def test_soft_block_never_critical(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [("soft_block",)] * 3
        alert, level = should_send_alert(Fonte.CEPEA, "failed", "soft_block")
        assert alert is True
        assert level == AlertLevel.WARNING

    def test_no_repeat_above_threshold(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [("source_down",)] * 7
        alert, level = should_send_alert(Fonte.CEPEA, "failed", "source_down")
        assert alert is False
        assert level is None

    def test_recovery_uses_prior_failures(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (0,)
        alert, level = should_send_alert(Fonte.CEPEA, "ok", None, prior_failures=4)
        assert alert is True
        assert level == AlertLevel.INFO

    def test_recovery_below_threshold_is_silent(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (0,)
        alert, level = should_send_alert(Fonte.CEPEA, "ok", None, prior_failures=1)
        assert alert is False
        assert level is None

    def test_recovery_after_api_key_missing_is_silent(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("api_key_missing",),
            ("api_key_missing",),
        ]
        prior = get_alertable_failures(Fonte.USDA)
        assert prior == 0

        alert, level = should_send_alert(Fonte.USDA, "ok", None, prior_failures=prior)
        assert alert is False
        assert level is None

    def test_recovery_after_disabled_source_down_is_silent(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("source_down",),
            ("source_down",),
        ]
        settings = self._settings(alert_on_source_down=False)
        prior = get_alertable_failures(Fonte.CONAB, settings)
        assert prior == 0

        alert, level = should_send_alert(
            Fonte.CONAB, "ok", None, settings=settings, prior_failures=prior
        )
        assert alert is False
        assert level is None

    def test_recovery_after_alertable_outage_fires(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("source_down",),
            ("source_down",),
        ]
        prior = get_alertable_failures(Fonte.CONAB)
        assert prior == 2

        alert, level = should_send_alert(Fonte.CONAB, "ok", None, prior_failures=prior)
        assert alert is True
        assert level == AlertLevel.INFO

    def test_mixed_incident_does_not_reach_threshold(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("api_key_missing",),
            ("source_down",),
        ]
        alert, level = should_send_alert(Fonte.CONAB, "failed", "source_down")
        assert alert is False
        assert level is None

    def test_mixed_incident_escalates_on_alertable_count(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("api_key_missing",),
            ("source_down",),
            ("source_down",),
        ]
        alert, level = should_send_alert(Fonte.CONAB, "failed", "source_down")
        assert alert is True
        assert level == AlertLevel.WARNING

    def test_mixed_incident_recovery_fires(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("api_key_missing",),
            ("source_down",),
            ("source_down",),
        ]
        prior = get_alertable_failures(Fonte.CONAB)
        assert prior == 2

        alert, level = should_send_alert(Fonte.CONAB, "ok", None, prior_failures=prior)
        assert alert is True
        assert level == AlertLevel.INFO

    def test_alertable_failures_ignores_suppressed_categories(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("api_key_missing",),
            ("source_down",),
            (None,),
        ]
        assert get_alertable_failures(Fonte.CONAB) == 2

    def test_warning_status_never_critical(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [(None,)] * 3
        alert, level = should_send_alert(Fonte.ACERVO_FUNDIARIO, "warning", None)
        assert alert is True
        assert level == AlertLevel.WARNING

    def test_soft_block_flag_disabled_suppresses(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (3,)
        settings = self._settings(alert_on_soft_block=False)
        alert, level = should_send_alert(
            Fonte.CEPEA,
            "failed",
            "soft_block",
            settings=settings,
        )
        assert alert is False
        assert level is None
