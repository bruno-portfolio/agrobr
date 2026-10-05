"""Tests for agrobr.alerts.notifier module."""

from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.alerts.notifier import (
    AlertLevel,
    _send_discord,
    _send_email,
    _send_slack,
    send_alert,
)
from agrobr.exceptions import InvalidParameterError
from tests.helpers import (
    capturar_logs,
    levanta_exatamente,
    make_alert_settings,
    make_mock_async_client,
    make_mock_response,
)


class TestSendAlert:
    @pytest.mark.asyncio
    async def test_disabled_alerts_do_nothing(self):
        mock_settings = make_alert_settings(
            enabled=False, slack_webhook="https://hooks.slack.com/test"
        )

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_slack", new_callable=AsyncMock) as mock_slack,
        ):
            await send_alert(AlertLevel.CRITICAL, "test", {"key": "val"})

        mock_slack.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_channels_configured(self):
        mock_settings = make_alert_settings()

        with (
            patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=mock_settings),
            patch("agrobr.alerts.notifier._send_slack", new_callable=AsyncMock) as mock_slack,
            patch("agrobr.alerts.notifier._send_discord", new_callable=AsyncMock) as mock_discord,
        ):
            await send_alert(AlertLevel.INFO, "test", {})

        mock_slack.assert_not_called()
        mock_discord.assert_not_called()

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
async def test_nivel_em_texto_chega_ao_payload():
    settings = make_alert_settings(slack_webhook="https://hooks.slack.com/test")
    client = make_mock_async_client()
    client.post.return_value = make_mock_response()
    with (
        patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=settings),
        patch("agrobr.alerts.notifier.httpx.AsyncClient", return_value=client),
    ):
        await send_alert("critical", "Falhou", {})
    assert client.post.await_count == 1
    assert client.post.call_args.kwargs["json"]["attachments"][0]["color"] == "#dc3545"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configuracao", "efeito", "eventos"),
    [
        ({}, None, [("warning", "no_alert_channels_configured", None)]),
        ({"slack_webhook": "https://hooks.slack.com/test"}, None, []),
        (
            {"slack_webhook": "https://hooks.slack.com/test"},
            RuntimeError("rede"),
            [("error", "alert_send_failed", "RuntimeError")],
        ),
    ],
)
async def test_envio_registra_so_as_falhas_de_canal(configuracao, efeito, eventos):
    settings = make_alert_settings(**configuracao)
    with (
        patch("agrobr.alerts.notifier.constants.AlertSettings", return_value=settings),
        patch("agrobr.alerts.notifier._send_slack", new_callable=AsyncMock, side_effect=efeito),
        patch("agrobr.alerts.notifier._send_email", new_callable=AsyncMock) as email,
        capturar_logs() as registros,
    ):
        await send_alert(AlertLevel.WARNING, "Teste", {})
    obtidos = [
        (r["log_level"], r["event"], r.get("error"))
        for r in registros
        if r["log_level"] in ("warning", "error")
    ]
    assert obtidos == eventos
    email.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "details", "blocos"),
    [
        (
            None,
            {},
            [{"type": "header", "text": {"type": "plain_text", "text": ":warning: Titulo"}}],
        ),
        (
            "cepea",
            {"k": 1},
            [
                {"type": "header", "text": {"type": "plain_text", "text": ":warning: Titulo"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "*Source:* cepea"},
                        {"type": "mrkdwn", "text": "*Level:* WARNING"},
                    ],
                },
                {"type": "section", "text": {"type": "mrkdwn", "text": '```{\n  "k": 1\n}```'}},
            ],
        ),
    ],
)
async def test_slack_monta_blocos_de_fonte_e_detalhes(source, details, blocos):
    client = make_mock_async_client()
    client.post.return_value = make_mock_response()
    with patch("agrobr.alerts.notifier.httpx.AsyncClient", return_value=client):
        await _send_slack(
            "https://hooks.slack.com/test", AlertLevel.WARNING, "Titulo", details, source
        )
    assert client.post.call_args.kwargs["json"]["attachments"][0]["blocks"] == blocos


@pytest.mark.asyncio
async def test_slack_texto_da_fonte_nao_fecha_o_bloco_de_codigo():
    client = make_mock_async_client()
    client.post.return_value = make_mock_response()
    mensagem = "HTTP 503 ```\n<!channel> <https://exemplo.invalid|entre> & ```"
    with patch("agrobr.alerts.notifier.httpx.AsyncClient", return_value=client):
        await _send_slack(
            "https://hooks.slack.com/test",
            AlertLevel.CRITICAL,
            "Falhou",
            {"message": mensagem},
            "conab",
        )
    texto = client.post.call_args.kwargs["json"]["attachments"][0]["blocks"][-1]["text"]["text"]
    assert texto.startswith("```") and texto.endswith("```")
    assert "```" not in texto[3:-3]
    assert "<" not in texto and ">" not in texto
    assert "&lt;!channel&gt;" in texto and "&amp;" in texto


@pytest.mark.asyncio
async def test_discord_recuperacao_fica_verde_e_mostra_detalhes():
    client = make_mock_async_client()
    client.post.return_value = make_mock_response()
    with patch("agrobr.alerts.notifier.httpx.AsyncClient", return_value=client):
        await _send_discord(
            "https://discord.com/api/webhooks/test",
            AlertLevel.CRITICAL,
            "Voltou",
            {"k": 1},
            "cepea",
            is_recovery=True,
        )
    embed = client.post.call_args.kwargs["json"]["embeds"][0]
    assert embed["color"] == 0x36A64F
    assert embed.get("description") == '```json\n{\n  "k": 1\n}\n```'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enviar", "webhook"),
    [
        (_send_slack, "https://hooks.slack.com/services/T0/B0/credencial-slack"),
        (_send_discord, "https://discord.com/api/webhooks/1/credencial-discord"),
    ],
)
async def test_url_do_webhook_nao_aparece_no_log_do_httpx(caplog, enviar, webhook):
    real = httpx.AsyncClient
    transporte = httpx.MockTransport(lambda request: httpx.Response(500, request=request))
    caplog.set_level(logging.INFO, logger="httpx")
    with (
        patch("agrobr.alerts.notifier.httpx.AsyncClient", lambda: real(transport=transporte)),
        levanta_exatamente(httpx.HTTPStatusError),
    ):
        await enviar(webhook, AlertLevel.WARNING, "t", {}, None)
    mensagens = [registro.getMessage() for registro in caplog.records if registro.name == "httpx"]
    assert len(mensagens) == 1
    assert "credencial" not in mensagens[0]
    assert "HTTP Request: POST [REDACTED]" in mensagens[0]


@pytest.mark.asyncio
async def test_level_invalido_e_recusado_antes_de_ler_a_configuracao():
    with (
        patch("agrobr.alerts.notifier.constants.AlertSettings") as settings,
        levanta_exatamente(
            InvalidParameterError,
            r"level inválido: 'urgente'\. Valores válidos: info, warning, critical",
        ),
    ):
        await send_alert("urgente", "t", {})
    settings.assert_not_called()
