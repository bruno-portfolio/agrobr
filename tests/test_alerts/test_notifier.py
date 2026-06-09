"""Tests for agrobr.alerts.notifier module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.alerts.notifier import (
    AlertCategory,
    AlertLevel,
    _send_discord,
    _send_email,
    _send_slack,
    send_alert,
)
from tests.helpers import make_alert_settings, make_mock_async_client, make_mock_response


class TestAlertLevel:
    def test_alert_levels(self):
        assert AlertLevel.INFO == "info"
        assert AlertLevel.WARNING == "warning"
        assert AlertLevel.CRITICAL == "critical"

    def test_alert_level_from_string(self):
        assert AlertLevel("info") is AlertLevel.INFO
        assert AlertLevel("warning") is AlertLevel.WARNING
        assert AlertLevel("critical") is AlertLevel.CRITICAL


class TestAlertCategory:
    def test_alert_categories(self):
        assert AlertCategory.SOFT_BLOCK == "soft_block"
        assert AlertCategory.SOURCE_DOWN == "source_down"
        assert AlertCategory.PARSE_ERROR == "parse_error"
        assert AlertCategory.LAYOUT_CHANGE == "layout_change"
        assert AlertCategory.ANOMALY == "anomaly"
        assert AlertCategory.API_KEY_MISSING == "api_key_missing"
        assert AlertCategory.SLOW == "slow"

    def test_alert_category_from_string(self):
        assert AlertCategory("soft_block") is AlertCategory.SOFT_BLOCK
        assert AlertCategory("anomaly") is AlertCategory.ANOMALY


class TestSendAlert:
    @pytest.mark.asyncio
    async def test_disabled_alerts_do_nothing(self):
        mock_settings = make_alert_settings(enabled=False)

        with patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings):
            await send_alert(AlertLevel.CRITICAL, "test", {"key": "val"})

    @pytest.mark.asyncio
    async def test_no_channels_configured(self):
        mock_settings = make_alert_settings()

        with patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings):
            await send_alert(AlertLevel.INFO, "test", {})

    @pytest.mark.asyncio
    async def test_slack_channel_dispatched(self):
        mock_settings = make_alert_settings(slack_webhook="https://hooks.slack.com/test")

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_slack", new_callable=AsyncMock) as mock_slack,
        ):
            await send_alert(AlertLevel.WARNING, "Test Alert", {"error": "timeout"}, source="cepea")
            mock_slack.assert_awaited_once()
            args = mock_slack.call_args
            assert args[0][0] == "https://hooks.slack.com/test"
            assert args[0][1] == AlertLevel.WARNING

    @pytest.mark.asyncio
    async def test_discord_channel_dispatched(self):
        mock_settings = make_alert_settings(
            discord_webhook="https://discord.com/api/webhooks/test",
        )

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_discord", new_callable=AsyncMock) as mock_discord,
        ):
            await send_alert("critical", "Down", {"source": "conab"})
            mock_discord.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_email_channel_dispatched(self):
        mock_settings = make_alert_settings(
            sendgrid_api_key="SG.test_key",
            email_to=["admin@agrobr.dev"],
        )

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_email", new_callable=AsyncMock) as mock_email,
        ):
            await send_alert(AlertLevel.INFO, "Test", {"info": "ok"})
            mock_email.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_multiple_channels_dispatched(self):
        mock_settings = make_alert_settings(
            slack_webhook="https://hooks.slack.com/test",
            discord_webhook="https://discord.com/api/webhooks/test",
            sendgrid_api_key="SG.key",
            email_to=["admin@test.com"],
        )

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_slack", new_callable=AsyncMock) as ms,
            patch("agrobr.alerts.notifier._send_discord", new_callable=AsyncMock) as md,
            patch("agrobr.alerts.notifier._send_email", new_callable=AsyncMock) as me,
        ):
            await send_alert(AlertLevel.CRITICAL, "All channels", {"test": True})
            ms.assert_awaited_once()
            md.assert_awaited_once()
            me.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_category_passed_to_discord(self):
        mock_settings = make_alert_settings(
            discord_webhook="https://discord.com/api/webhooks/test",
        )

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_discord", new_callable=AsyncMock) as mock_discord,
        ):
            await send_alert(
                "warning",
                "Source blocked (Cloudflare): cepea",
                {"message": "Soft block"},
                source="cepea",
                category="soft_block",
            )
            mock_discord.assert_awaited_once()
            call_kwargs = mock_discord.call_args.kwargs
            assert call_kwargs.get("category") == "soft_block"

    @pytest.mark.asyncio
    async def test_channel_exception_logged(self):
        mock_settings = make_alert_settings(slack_webhook="https://hooks.slack.com/test")

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch(
                "agrobr.alerts.notifier._send_slack",
                new_callable=AsyncMock,
                side_effect=RuntimeError("network"),
            ),
        ):
            await send_alert(AlertLevel.WARNING, "Fail gracefully", {})

    @pytest.mark.asyncio
    async def test_level_as_string(self):
        mock_settings = make_alert_settings(slack_webhook="https://hooks.slack.com/test")

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_slack", new_callable=AsyncMock) as mock_slack,
        ):
            await send_alert("info", "String level", {})
            mock_slack.assert_awaited_once()
            assert mock_slack.call_args[0][1] == AlertLevel.INFO


class TestSendSlack:
    @pytest.mark.asyncio
    async def test_slack_payload_info(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_slack(
                "https://hooks.slack.com/test",
                AlertLevel.INFO,
                "Test Title",
                {"key": "value"},
                source="cepea",
            )

            mock_client.post.assert_awaited_once()
            call_args = mock_client.post.call_args
            payload = call_args.kwargs.get("json") or call_args[1].get("json")
            assert "attachments" in payload
            assert payload["attachments"][0]["color"] == "#36a64f"

    @pytest.mark.asyncio
    async def test_slack_payload_critical_no_source(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_slack(
                "https://hooks.slack.com/test",
                AlertLevel.CRITICAL,
                "Critical Alert",
                {},
                source=None,
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            assert payload["attachments"][0]["color"] == "#dc3545"

    @pytest.mark.asyncio
    async def test_slack_payload_warning(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_slack(
                "https://hooks.slack.com/test",
                AlertLevel.WARNING,
                "Warning",
                {"detail": "x"},
                source="conab",
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            assert payload["attachments"][0]["color"] == "#ff9800"


class TestSendDiscord:
    @pytest.mark.asyncio
    async def test_discord_payload_with_source(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.CRITICAL,
                "Test Alert",
                {"key": "value"},
                source="ibge",
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            assert "embeds" in payload
            assert payload["embeds"][0]["color"] == 0xDC3545
            # Source + Level fields
            source_fields = [
                f for f in payload["embeds"][0]["fields"] if f["name"] in ("Source", "Level")
            ]
            assert len(source_fields) == 2

    @pytest.mark.asyncio
    async def test_discord_payload_no_source_no_details(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.INFO,
                "No source",
                {},
                source=None,
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            assert payload["embeds"][0]["fields"] == []
            assert "description" not in payload["embeds"][0]

    @pytest.mark.asyncio
    async def test_discord_affected_datasets(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.WARNING,
                "Source degraded",
                {"msg": "slow"},
                source="conab",
                affected_datasets=["estimativa_safra", "producao_anual"],
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            fields = payload["embeds"][0]["fields"]
            dataset_field = next(f for f in fields if f["name"] == "Affected Datasets")
            assert "estimativa_safra" in dataset_field["value"]
            assert "producao_anual" in dataset_field["value"]

    @pytest.mark.asyncio
    async def test_discord_consecutive_failures(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.CRITICAL,
                "Source down",
                {},
                source="cepea",
                consecutive_failures=5,
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            fields = payload["embeds"][0]["fields"]
            fail_field = next(f for f in fields if f["name"] == "Consecutive Failures")
            assert fail_field["value"] == "5"

    @pytest.mark.asyncio
    async def test_discord_recovery_alert(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            last_ok = datetime(2024, 6, 15, 10, 0, 0)
            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.INFO,
                "Source recovered: cepea",
                {},
                source="cepea",
                is_recovery=True,
                last_success_at=last_ok,
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            embed = payload["embeds"][0]
            # Recovery uses green color
            assert embed["color"] == 0x36A64F
            # Has Last Success field
            last_field = next(f for f in embed["fields"] if f["name"] == "Last Success")
            assert "2024-06-15" in last_field["value"]

    @pytest.mark.asyncio
    async def test_discord_soft_block_formatting(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.WARNING,
                "Source blocked (Cloudflare): cepea",
                {"message": "Soft block detected"},
                source="cepea",
                category="soft_block",
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            embed = payload["embeds"][0]
            assert embed["color"] == 0x7289DA
            assert ":shield:" in embed["title"]
            cat_field = next(f for f in embed["fields"] if f["name"] == "Category")
            assert cat_field["value"] == "IP Blocked (Cloudflare)"

    @pytest.mark.asyncio
    async def test_discord_category_source_down(self):
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_discord(
                "https://discord.com/api/webhooks/test",
                AlertLevel.CRITICAL,
                "Source failed: conab",
                {},
                source="conab",
                category="source_down",
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            embed = payload["embeds"][0]
            assert embed["color"] == 0xDC3545
            cat_field = next(f for f in embed["fields"] if f["name"] == "Category")
            assert cat_field["value"] == "Source Down"


class TestSendEmail:
    @pytest.mark.asyncio
    async def test_email_payload(self):
        mock_settings = make_alert_settings(
            sendgrid_api_key="SG.test_key",
            email_to=["admin@test.com", "ops@test.com"],
        )
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_email(
                mock_settings,
                AlertLevel.CRITICAL,
                "System Down",
                {"error": "timeout"},
                source="cepea",
            )

            mock_client.post.assert_awaited_once()
            call_args = mock_client.post.call_args
            url = call_args[0][0] if call_args[0] else call_args.kwargs.get("url", "")
            assert "sendgrid" in url

            payload = call_args.kwargs.get("json") or call_args[1].get("json")
            assert payload["subject"] == "[agrobr CRITICAL] System Down"
            assert len(payload["personalizations"][0]["to"]) == 2

    @pytest.mark.asyncio
    async def test_email_without_source(self):
        mock_settings = make_alert_settings(
            sendgrid_api_key="SG.test_key",
            email_to=["admin@test.com"],
        )
        mock_response = make_mock_response()
        mock_client = make_mock_async_client()
        mock_client.post.return_value = mock_response

        with patch("agrobr.alerts.notifier.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = mock_client

            await _send_email(
                mock_settings,
                AlertLevel.INFO,
                "Info Alert",
                {},
                source=None,
            )

            payload = mock_client.post.call_args.kwargs.get("json") or mock_client.post.call_args[
                1
            ].get("json")
            html = payload["content"][0]["value"]
            assert "Source" not in html or "None" not in html
