"""Tests for agrobr.health.checker module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.constants import Fonte
from agrobr.health.checker import (
    CheckResult,
    CheckStatus,
    _check_http,
    check_cepea_deep,
    check_source,
    format_results,
    run_all_checks,
    run_checks_with_state,
)
from agrobr.health.registry import HEALTH_REGISTRY, SourceHealthConfig


class TestCheckStatus:
    def test_status_values(self):
        assert CheckStatus.OK == "ok"
        assert CheckStatus.WARNING == "warning"
        assert CheckStatus.FAILED == "failed"


class TestCheckResult:
    def test_create_check_result(self):
        result = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.OK,
            latency_ms=150.0,
            message="All checks passed",
            details={"fetch_ok": True},
            timestamp=datetime(2024, 1, 1),
        )
        assert result.source == Fonte.CEPEA
        assert result.status == CheckStatus.OK
        assert result.latency_ms == 150.0
        assert result.category is None

    def test_create_check_result_with_category(self):
        result = CheckResult(
            source=Fonte.CONAB,
            status=CheckStatus.FAILED,
            latency_ms=0,
            message="HTTP 503",
            details={},
            timestamp=datetime(2024, 1, 1),
            category="source_down",
        )
        assert result.category == "source_down"


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
    async def test_http_error(self):
        config = SourceHealthConfig(source=Fonte.CONAB, url="https://example.com")
        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.FAILED
        assert result.category == "source_down"
        assert "503" in result.message

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
    async def test_http_exception(self):
        import httpx as httpx_mod

        config = SourceHealthConfig(source=Fonte.CONAB, url="https://example.com")
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx_mod.ConnectError("connection refused")

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.FAILED
        assert result.category == "source_down"

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
    async def test_http_exception_without_message_uses_type_name(self):
        import httpx as httpx_mod

        config = SourceHealthConfig(source=Fonte.CONAB, url="https://example.com")
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx_mod.ReadTimeout("")

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await _check_http(config)

        assert result.status == CheckStatus.FAILED
        assert result.category == "source_down"
        assert result.message == "ReadTimeout"

    @pytest.mark.asyncio
    async def test_api_key_missing(self):
        config = SourceHealthConfig(
            source=Fonte.USDA,
            url="https://example.com",
            requires_api_key=True,
            api_key_env_var="AGROBR_USDA_API_KEY",
        )
        with patch.dict("os.environ", {}, clear=True):
            result = await _check_http(config)

        assert result.status == CheckStatus.WARNING
        assert result.category == "api_key_missing"


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
                timestamp=datetime.utcnow(),
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
                timestamp=datetime.utcnow(),
            )
            result = await check_source(Fonte.CEPEA, deep=True)
        mock_deep.assert_called_once()
        assert result.status == CheckStatus.OK


class TestRunAllChecks:
    @pytest.mark.asyncio
    async def test_returns_all_sources(self):
        mock_result = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.OK,
            latency_ms=100,
            message="ok",
            details={},
            timestamp=datetime.utcnow(),
        )

        with patch(
            "agrobr.health.checker.check_source",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            results = await run_all_checks()

        assert isinstance(results, list)
        assert len(results) == len(HEALTH_REGISTRY)

    @pytest.mark.asyncio
    async def test_specific_sources(self):
        mock_result = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.OK,
            latency_ms=100,
            message="ok",
            details={},
            timestamp=datetime.utcnow(),
        )

        with patch(
            "agrobr.health.checker.check_source",
            new_callable=AsyncMock,
            return_value=mock_result,
        ):
            results = await run_all_checks([Fonte.CEPEA, Fonte.CONAB])

        assert len(results) == 2


class TestRunChecksWithState:
    @pytest.mark.asyncio
    async def test_returns_tuples(self):
        mock_result = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.OK,
            latency_ms=100,
            message="ok",
            details={},
            timestamp=datetime.utcnow(),
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
        ):
            results = await run_checks_with_state([Fonte.CEPEA])

        assert len(results) == 1
        result, should_alert, level = results[0]
        assert result.source == Fonte.CEPEA
        assert should_alert is False
        assert level is None
        mock_record.assert_called_once()
        mock_alert.assert_called_once()


class TestFormatResults:
    def test_format_ok(self):
        results = [
            CheckResult(Fonte.CEPEA, CheckStatus.OK, 100.0, "All OK", {}, datetime.utcnow()),
            CheckResult(
                Fonte.CONAB,
                CheckStatus.WARNING,
                5500.0,
                "High latency",
                {},
                datetime.utcnow(),
            ),
            CheckResult(
                Fonte.IBGE,
                CheckStatus.FAILED,
                0.0,
                "Connection refused",
                {},
                datetime.utcnow(),
            ),
        ]
        output = format_results(results)
        assert "Health Check Results" in output
        assert "CEPEA" in output.upper()
        assert "CONAB" in output.upper()
        assert "IBGE" in output.upper()

    def test_format_empty(self):
        output = format_results([])
        assert "Health Check Results" in output


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

    @pytest.mark.asyncio
    async def test_non_soft_block_error_remains_parse_error(self):
        with patch(
            "agrobr.cepea.client.fetch_indicador_page",
            new_callable=AsyncMock,
            side_effect=Exception("Connection timeout"),
        ):
            result = await check_cepea_deep()

        assert result.status == CheckStatus.FAILED
        assert result.category == "parse_error"
