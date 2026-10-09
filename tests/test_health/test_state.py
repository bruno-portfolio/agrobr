"""Tests for agrobr.health.state module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from agrobr.alerts.notifier import AlertLevel
from agrobr.constants import AlertSettings, Fonte
from agrobr.health.state import (
    close_store,
    get_alertable_failures,
    get_consecutive_failures,
    get_last_success,
    record_check,
    should_send_alert,
    store_degraded,
)


@pytest.fixture()
def mock_conn():
    conn = MagicMock()
    store = MagicMock()
    store._conexao.return_value.__enter__.return_value = conn
    with patch("agrobr.health.state.get_store", return_value=store):
        yield conn


class TestStoreLifecycle:
    def test_close_store_closes_shared_store(self):
        store = MagicMock()

        with patch("agrobr.health.state.get_store", return_value=store):
            close_store()

        store.close.assert_called_once_with()

    def test_store_degraded_reflects_shared_store(self):
        store = MagicMock()
        store._degraded = True

        with patch("agrobr.health.state.get_store", return_value=store):
            assert store_degraded() is True


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


class TestGetConsecutiveFailures:
    def test_returns_count(self, mock_conn):
        mock_conn.execute.return_value.fetchone.return_value = (3,)
        result = get_consecutive_failures(Fonte.CEPEA)
        assert result == 3


class TestGetLastSuccess:
    def test_returns_datetime(self, mock_conn):
        dt = datetime(2024, 6, 15, 10, 0, 0)
        mock_conn.execute.return_value.fetchone.return_value = (dt,)
        result = get_last_success(Fonte.CEPEA)
        assert result == dt


class TestShouldSendAlert:
    def _settings(self, **overrides):
        return AlertSettings(**overrides)

    def test_third_failure_critical(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [("source_down",)] * 3
        alert, level = should_send_alert(Fonte.CEPEA, "failed", "source_down")
        assert alert is True
        assert level == AlertLevel.CRITICAL

    def test_ok_no_prior_failures(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = []
        alert, level = should_send_alert(Fonte.CEPEA, "ok", None)
        assert alert is False
        assert level is None

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

    def test_mixed_incident_escalates_on_alertable_count(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [
            ("api_key_missing",),
            ("source_down",),
            ("source_down",),
        ]
        alert, level = should_send_alert(Fonte.CONAB, "failed", "source_down")
        assert alert is True
        assert level == AlertLevel.WARNING

    def test_warning_status_never_critical(self, mock_conn):
        mock_conn.execute.return_value.fetchall.return_value = [(None,)] * 3
        alert, level = should_send_alert(Fonte.ACERVO_FUNDIARIO, "warning", None)
        assert alert is True
        assert level == AlertLevel.WARNING


@pytest.mark.parametrize(
    ("category", "flag"),
    [
        ("parse_error", "alert_on_parse_error"),
        ("layout_change", "alert_on_layout_change"),
        ("source_down", "alert_on_source_down"),
        ("anomaly", "alert_on_anomaly"),
        ("soft_block", "alert_on_soft_block"),
    ],
)
@pytest.mark.parametrize("enabled", [True, False])
def test_category_flag_decides_alert(mock_conn, category, flag, enabled):
    mock_conn.execute.return_value.fetchall.return_value = [(category,)] * 2
    alert, level = should_send_alert(
        Fonte.CEPEA, "failed", category, settings=AlertSettings(**{flag: enabled})
    )
    assert (alert, level) == ((True, AlertLevel.WARNING) if enabled else (False, None))


def test_categoria_desligada_nao_alerta_mesmo_com_outras_falhas(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = [("source_down",)] * 3
    alert, level = should_send_alert(
        Fonte.CEPEA, "failed", "anomaly", settings=AlertSettings(alert_on_anomaly=False)
    )
    assert (alert, level) == (False, None)


def test_categoria_desconhecida_sempre_pode_alertar(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = [("outra",)] * 2
    settings = AlertSettings(alert_on_soft_block=False, alert_on_anomaly=False)
    alert, level = should_send_alert(Fonte.CEPEA, "failed", "outra", settings=settings)
    assert (alert, level) == (True, AlertLevel.WARNING)
