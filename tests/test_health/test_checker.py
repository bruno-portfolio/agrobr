"""Tests for agrobr.health.checker module."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from itertools import count
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agrobr import constants
from agrobr.constants import Fonte
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.health.checker import (
    CheckResult,
    CheckStatus,
    _check_http,
    check_cepea_deep,
    check_source,
    run_all_checks,
    run_checks_with_state,
)
from agrobr.health.registry import HEALTH_REGISTRY, SourceHealthConfig
from agrobr.utils import time as time_utils


class TestCheckHttp:
    @pytest.mark.asyncio
    async def test_http_ok(self):
        config = SourceHealthConfig(source=Fonte.CONAB, url="https://example.com")
        mock_response = MagicMock()
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.source == Fonte.CONAB
        assert result.status in (CheckStatus.OK, CheckStatus.WARNING)
        assert result.details["status_code"] == 200

    @pytest.mark.asyncio
    async def test_body_error_marker_absent_returns_ok(self):
        config = SourceHealthConfig(
            source=Fonte.SFB,
            url="https://example.com",
            body_error_markers=('"error"',),
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"count":42}'

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.OK
        assert result.category is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("corpo", "status", "categoria"),
        [
            ({"count": 1, "data": [{"cmdCode": "1201"}], "error": ""}, CheckStatus.OK, None),
            (
                {"count": 0, "data": [], "error": "Invalid reporterCode"},
                CheckStatus.FAILED,
                "source_down",
            ),
        ],
    )
    async def test_comtrade_so_falha_com_error_preenchido(self, corpo, status, categoria):
        config = HEALTH_REGISTRY[Fonte.COMTRADE]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(corpo)
        mock_response.json.return_value = corpo

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == status
        assert result.category == categoria
        if corpo["error"]:
            assert result.message == "Resposta com error: Invalid reporterCode"

    @pytest.mark.asyncio
    async def test_best_effort_http_error_returns_warning(self):
        config = SourceHealthConfig(
            source=Fonte.ANTAQ,
            url="https://example.com",
            tier="best_effort",
        )
        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.WARNING
        assert result.category == "source_down"

    @pytest.mark.asyncio
    async def test_soft_block_code_returns_warning(self):
        config = SourceHealthConfig(
            source=Fonte.CEPEA,
            url="https://example.com",
            soft_block_codes=(403,),
        )
        mock_response = MagicMock()
        mock_response.status_code = 403

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.WARNING
        assert result.category == "soft_block"
        assert result.message == "HTTP 403"

    @pytest.mark.asyncio
    async def test_best_effort_exception_returns_warning(self):
        import httpx as httpx_mod

        config = SourceHealthConfig(
            source=Fonte.ANTAQ,
            url="https://example.com",
            tier="best_effort",
        )
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx_mod.ConnectError("connection refused")

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.WARNING
        assert result.category == "source_down"

    @pytest.mark.asyncio
    async def test_slow_http_uses_warm_retry_latency(self):
        config = SourceHealthConfig(source=Fonte.ANA, url="https://example.com")
        first_response = MagicMock(status_code=200, text='{"count": 1}')
        second_response = MagicMock(status_code=200, text='{"count": 1}')
        mock_client = AsyncMock()
        mock_client.get.side_effect = [first_response, second_response]

        with (
            patch("httpx.AsyncClient") as mock_cls,
            patch(
                "agrobr.health.checker.time.monotonic",
                side_effect=[0.0, 12.0, 12.0, 12.3],
            ),
        ):
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
            result = await _check_http(config)

        assert mock_client.get.await_count == 2
        assert result.status == CheckStatus.OK
        assert result.latency_ms == pytest.approx(300.0)
        assert result.details["cold_start_ms"] == pytest.approx(12_000.0)


class TestCheckSource:
    @pytest.mark.asyncio
    async def test_known_source(self):
        with patch("agrobr.health.checker._check_http", new_callable=AsyncMock) as mock_http:
            mock_http.return_value = CheckResult(
                source=Fonte.CONAB,
                status=CheckStatus.OK,
                latency_ms=100,
                message="ok",
                details={},
                timestamp=time_utils.utcnow(),
            )
            result = await check_source(Fonte.CONAB)
        assert result.status == CheckStatus.OK

    @pytest.mark.asyncio
    async def test_deep_cepea(self):
        with patch(
            "agrobr.health.checker.check_cepea_deep",
            new_callable=AsyncMock,
        ) as mock_deep:
            mock_deep.return_value = CheckResult(
                source=Fonte.CEPEA,
                status=CheckStatus.OK,
                latency_ms=200,
                message="ok",
                details={},
                timestamp=time_utils.utcnow(),
            )
            result = await check_source(Fonte.CEPEA, deep=True)
        mock_deep.assert_called_once()
        assert result.status == CheckStatus.OK


class TestRunChecksWithState:
    @pytest.mark.asyncio
    async def test_returns_tuples(self):
        mock_result = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.OK,
            latency_ms=100,
            message="ok",
            details={},
            timestamp=time_utils.utcnow(),
        )

        with (
            patch(
                "agrobr.health.checker.run_all_checks",
                new_callable=AsyncMock,
                return_value=[mock_result],
            ),
            patch("agrobr.health.state.record_check") as mock_record,
            patch(
                "agrobr.health.state.should_send_alert",
                return_value=(False, None),
            ) as mock_alert,
            patch("agrobr.health.state.get_alertable_failures", return_value=2),
        ):
            results = await run_checks_with_state([Fonte.CEPEA])

        assert len(results) == 1
        result, should_alert, level, prior_failures = results[0]
        assert result.source == Fonte.CEPEA
        assert should_alert is False
        assert level is None
        assert prior_failures == 2
        mock_record.assert_called_once()
        mock_alert.assert_called_once()
        assert mock_alert.call_args.kwargs["prior_failures"] == 2


class TestCepeaDeepSoftBlock:
    @pytest.mark.asyncio
    async def test_soft_block_detected(self):
        with patch(
            "agrobr.cepea.client.fetch_indicador_page",
            new_callable=AsyncMock,
            side_effect=Exception("Soft block detected by Cloudflare"),
        ):
            result = await check_cepea_deep()

        assert result.status == CheckStatus.FAILED
        assert result.category == "soft_block"


async def test_run_all_checks_devolve_o_resultado_de_cada_fonte():
    async def por_fonte(source, deep=False):
        return CheckResult(
            source=source,
            status=CheckStatus.OK,
            latency_ms=1,
            message="ok",
            details={"deep": deep},
            timestamp=datetime(2026, 1, 1),
        )

    with patch("agrobr.health.checker.check_source", side_effect=por_fonte):
        resultados = await run_all_checks([Fonte.CEPEA, Fonte.CONAB], deep=True)
    assert [(getattr(r, "source", None), getattr(r, "details", None)) for r in resultados] == [
        (Fonte.CEPEA, {"deep": True}),
        (Fonte.CONAB, {"deep": True}),
    ]


@pytest.mark.parametrize("executar", [run_all_checks, run_checks_with_state])
@pytest.mark.parametrize("concurrency", [0, -1])
async def test_concurrency_menor_que_um_e_recusada_antes_dos_checks(executar, concurrency):
    with (
        patch("agrobr.health.checker.check_source", new_callable=AsyncMock) as check,
        pytest.raises(InvalidParameterError, match=f"concurrency deve ser .*{concurrency}"),
    ):
        await asyncio.wait_for(executar([Fonte.CEPEA], concurrency=concurrency), timeout=5)
    check.assert_not_awaited()


class TestSondaRnc:
    def test_sonda_usa_a_pagina_que_o_cliente_consulta_por_head(self):
        config = HEALTH_REGISTRY[Fonte.RNC]
        assert config.url == constants.RNC_PUBLIC_URLS["registradas"]
        assert config.method == "HEAD"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("resposta", "esperado"),
        [(200, CheckStatus.OK), (httpx.ReadError("conexão encerrada"), CheckStatus.FAILED)],
        ids=["pagina_no_ar", "read_error"],
    )
    async def test_read_error_continua_falha(self, resposta, esperado):
        cliente = AsyncMock()
        if isinstance(resposta, Exception):
            cliente.head.side_effect = resposta
        else:
            cliente.head.return_value = MagicMock(status_code=resposta, content=b"", headers={})
        with patch("httpx.AsyncClient") as fabrica:
            fabrica.return_value.__aenter__ = AsyncMock(return_value=cliente)
            fabrica.return_value.__aexit__ = AsyncMock(return_value=None)
            resultado = await _check_http(HEALTH_REGISTRY[Fonte.RNC])
        cliente.get.assert_not_awaited()
        assert resultado.status == esperado


@pytest.mark.parametrize(
    ("destino", "chave_no_destino"),
    [
        ("https://outro-host.example/coleta", None),
        ("http://api.fas.usda.gov/api/psd/commodities", None),
        ("https://api.fas.usda.gov/api/psd/commodities/", "secreta"),
    ],
    ids=["outro_host", "mesmo_host_sem_tls", "mesmo_host"],
)
async def test_sonda_com_chave_so_leva_a_chave_na_origem(monkeypatch, destino, chave_no_destino):
    config = HEALTH_REGISTRY[Fonte.USDA]
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "secreta")
    pedidos: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        if len(pedidos) == 1:
            return httpx.Response(302, headers={"Location": destino})
        return httpx.Response(200, json=[])

    cliente_real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: cliente_real(transport=httpx.MockTransport(responder), **kwargs),
    )
    resultado = await _check_http(config)

    assert [(str(p.url), p.headers.get("X-Api-Key")) for p in pedidos] == [
        (config.url, "secreta"),
        (destino, chave_no_destino),
    ]
    assert resultado.status == CheckStatus.OK


@pytest.mark.parametrize(
    "corpo,status",
    [
        ('{"count":68}', CheckStatus.OK),
        ('{"error":{"code":500,"message":"Service not started"}}', CheckStatus.FAILED),
    ],
)
async def test_sfb_health_distingue_contagem_de_erro_http_200(corpo, status):
    config = HEALTH_REGISTRY[Fonte.SFB]
    response = httpx.Response(200, text=corpo, request=httpx.Request("GET", config.url))
    mock_client = AsyncMock()
    mock_client.get.return_value = response
    with patch("httpx.AsyncClient") as mock_cls:
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)
        result = await _check_http(config)
    assert result.status == status


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (SourceUnavailableError("cepea", last_error="tentativas esgotadas"), "source_down"),
        (SourceUnavailableError("cepea", last_error="soft block persistente"), "source_down"),
        (ParseError("cepea", 1, "coluna ausente"), "parse_error"),
    ],
)
async def test_deep_classifica_indisponibilidade_e_layout(error, category):
    with patch(
        "agrobr.cepea.client.fetch_indicador_page", new_callable=AsyncMock, side_effect=error
    ):
        result = await check_cepea_deep()
    assert result.source == Fonte.CEPEA
    assert result.status == CheckStatus.FAILED
    assert result.category == category
    assert result.message == str(error)


def _cliente(resposta):
    cliente = AsyncMock()
    cliente.get.return_value = resposta
    fabrica = MagicMock()
    fabrica.return_value.__aenter__ = AsyncMock(return_value=cliente)
    fabrica.return_value.__aexit__ = AsyncMock(return_value=None)
    return fabrica


async def test_marcador_de_erro_no_corpo_sai_em_portugues():
    config = SourceHealthConfig(
        source=Fonte.SFB, url="https://example.com", body_error_markers=('"error"',)
    )
    resposta = MagicMock(status_code=200, text='{"error": "fora"}')

    with patch("httpx.AsyncClient", _cliente(resposta)):
        result = await _check_http(config)

    assert result.message == 'Resposta contém \'"error"\': {"error": "fora"}'


async def test_latencia_alta_sai_em_portugues(monkeypatch):
    relogio = count(0, 6)
    monkeypatch.setattr(
        "agrobr.health.checker.time", SimpleNamespace(monotonic=lambda: next(relogio))
    )
    config = SourceHealthConfig(source=Fonte.CONAB, url="https://example.com")

    with patch("httpx.AsyncClient", _cliente(MagicMock(status_code=200, text=""))):
        result = await _check_http(config)

    assert (result.status, result.message) == (CheckStatus.WARNING, "Latência alta: 6000ms")


async def test_fonte_sem_sonda_sai_em_portugues(monkeypatch):
    monkeypatch.delitem(HEALTH_REGISTRY, Fonte.CONAB)

    result = await check_source(Fonte.CONAB)

    assert (result.status, result.message) == (
        CheckStatus.FAILED,
        "Fonte sem sonda cadastrada: conab",
    )
