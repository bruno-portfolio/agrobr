"""Tests for agrobr.health.checker module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.constants import Fonte
from agrobr.health.checker import (
    CheckResult,
    CheckStatus,
    check_conab,
    check_ibge,
    check_source,
    format_results,
    run_all_checks,
)


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


class TestCheckConab:
    @pytest.mark.asyncio
    async def test_conab_ok(self):
        mock_response = MagicMock()
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_conab()

        assert result.source == Fonte.CONAB
        assert result.status in (CheckStatus.OK, CheckStatus.WARNING)
        assert result.details["status_code"] == 200

    @pytest.mark.asyncio
    async def test_conab_http_error(self):
        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_client = AsyncMock()
        mock_client.head.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_conab()

        assert result.status == CheckStatus.FAILED
        assert "503" in result.message

    @pytest.mark.asyncio
    async def test_conab_exception(self):
        import httpx as httpx_mod

        mock_client = AsyncMock()
        mock_client.head.side_effect = httpx_mod.ConnectError("connection refused")

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_conab()

        assert result.status == CheckStatus.FAILED


class TestCheckIbge:
    @pytest.mark.asyncio
    async def test_ibge_ok(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"header": True}, {"D1N": "Brasil", "V": "1000"}]

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_ibge()

        assert result.source == Fonte.IBGE
        assert result.status in (CheckStatus.OK, CheckStatus.WARNING)
        assert result.details["records"] == 2

    @pytest.mark.asyncio
    async def test_ibge_http_error(self):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_ibge()

        assert result.status == CheckStatus.FAILED
        assert "500" in result.message

    @pytest.mark.asyncio
    async def test_ibge_empty_data(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"header": True}]

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_ibge()

        assert result.status == CheckStatus.WARNING
        assert "empty" in result.message.lower()

    @pytest.mark.asyncio
    async def test_ibge_exception(self):
        import httpx as httpx_mod

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx_mod.TimeoutException("read timeout")

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_ibge()

        assert result.status == CheckStatus.FAILED

    @pytest.mark.asyncio
    async def test_ibge_non_list_response(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "invalid"}

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with patch("httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await check_ibge()

        assert result.details["records"] == 0


class TestCheckSource:
    @pytest.mark.asyncio
    async def test_known_source(self):
        with patch("agrobr.health.checker.check_conab", new_callable=AsyncMock) as mock_conab:
            mock_conab.return_value = CheckResult(
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
    async def test_unknown_source(self):
        result = await check_source(Fonte.INMET)
        assert result.status == CheckStatus.FAILED
        assert "Unknown" in result.message


class TestRunAllChecks:
    @pytest.mark.asyncio
    async def test_returns_list(self):
        mock_result = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.OK,
            latency_ms=100,
            message="ok",
            details={},
            timestamp=datetime.utcnow(),
        )

        with patch(
            "agrobr.health.checker.check_source", new_callable=AsyncMock, return_value=mock_result
        ):
            results = await run_all_checks()

        assert isinstance(results, list)
        assert len(results) == 3


class TestFormatResults:
    def test_format_ok(self):
        results = [
            CheckResult(Fonte.CEPEA, CheckStatus.OK, 100.0, "All OK", {}, datetime.utcnow()),
            CheckResult(
                Fonte.CONAB, CheckStatus.WARNING, 5500.0, "High latency", {}, datetime.utcnow()
            ),
            CheckResult(
                Fonte.IBGE, CheckStatus.FAILED, 0.0, "Connection refused", {}, datetime.utcnow()
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
