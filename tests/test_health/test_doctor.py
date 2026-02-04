"""Tests for health.doctor module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.health.doctor import (
    CacheStats,
    DiagnosticsResult,
    SourceStatus,
    _check_source,
    run_diagnostics,
)


class TestSourceStatus:
    def test_source_status_ok(self):
        status = SourceStatus(
            name="CEPEA",
            url="https://example.com",
            status="ok",
            latency_ms=100,
        )
        assert status.name == "CEPEA"
        assert status.status == "ok"
        assert status.latency_ms == 100
        assert status.error is None

    def test_source_status_error(self):
        status = SourceStatus(
            name="CEPEA",
            url="https://example.com",
            status="error",
            latency_ms=5000,
            error="timeout",
        )
        assert status.status == "error"
        assert status.error == "timeout"


class TestCacheStats:
    def test_cache_stats_empty(self):
        stats = CacheStats(
            location="/tmp/cache.db",
            size_bytes=0,
            total_records=0,
            by_source={},
        )
        assert stats.total_records == 0
        assert stats.by_source == {}

    def test_cache_stats_with_data(self):
        stats = CacheStats(
            location="/tmp/cache.db",
            size_bytes=1024 * 1024,
            total_records=1000,
            by_source={
                "cepea": {"count": 500, "oldest": "2024-01-01", "newest": "2024-12-31"},
                "conab": {"count": 500, "oldest": "2024-01-01", "newest": "2024-12-31"},
            },
        )
        assert stats.total_records == 1000
        assert "cepea" in stats.by_source


class TestDiagnosticsResult:
    def test_to_dict(self):
        result = DiagnosticsResult(
            version="0.2.0",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            sources=[
                SourceStatus("CEPEA", "https://example.com", "ok", 100),
            ],
            cache=CacheStats("/tmp", 1024, 100, {}),
            last_collections={"cepea": datetime(2024, 1, 1, 10, 0, 0)},
            cache_expiry={"cepea": {"type": "smart", "expires_at": "18:00"}},
            config={"browser_fallback": False},
            overall_status="healthy",
        )

        d = result.to_dict()
        assert d["version"] == "0.2.0"
        assert d["overall_status"] == "healthy"
        assert len(d["sources"]) == 1
        assert d["sources"][0]["status"] == "ok"

    def test_to_rich_healthy(self):
        result = DiagnosticsResult(
            version="0.2.0",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            sources=[
                SourceStatus("CEPEA", "https://example.com", "ok", 100),
            ],
            cache=CacheStats("/tmp", 1024, 100, {}),
            last_collections={},
            cache_expiry={},
            config={},
            overall_status="healthy",
        )

        output = result.to_rich()
        assert "agrobr diagnostics" in output
        assert "[OK]" in output

    def test_to_rich_degraded(self):
        result = DiagnosticsResult(
            version="0.2.0",
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            sources=[
                SourceStatus("CEPEA", "https://example.com", "error", 5000, "timeout"),
            ],
            cache=CacheStats("/tmp", 1024, 100, {}),
            last_collections={},
            cache_expiry={},
            config={},
            overall_status="degraded",
        )

        output = result.to_rich()
        assert "[WARN]" in output or "[FAIL]" in output


class TestCheckSource:
    @pytest.mark.asyncio
    async def test_check_source_success(self):
        with patch("agrobr.health.doctor.httpx.AsyncClient") as mock_client:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock()
            mock_client.return_value.head = AsyncMock(return_value=mock_response)

            status = await _check_source("Test", "https://example.com")
            assert status.status in ("ok", "slow")

    @pytest.mark.asyncio
    async def test_check_source_timeout(self):
        import httpx

        with patch("agrobr.health.doctor.httpx.AsyncClient") as mock_client_class:
            mock_instance = MagicMock()
            mock_instance.head = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
            mock_client_class.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_client_class.return_value.__aexit__ = AsyncMock(return_value=None)

            status = await _check_source("Test", "https://example.com")
            assert status.status == "error"
            assert status.error == "timeout"


class TestRunDiagnostics:
    @pytest.mark.asyncio
    async def test_run_diagnostics_returns_result(self):
        with patch("agrobr.health.doctor._check_source") as mock_check:
            mock_check.return_value = SourceStatus("Test", "https://example.com", "ok", 100)

            with patch("agrobr.health.doctor._get_cache_stats") as mock_cache:
                mock_cache.return_value = CacheStats("/tmp", 0, 0, {})

                with patch("agrobr.health.doctor._get_last_collections") as mock_collections:
                    mock_collections.return_value = {}

                    with patch("agrobr.health.doctor.get_next_update_info") as mock_expiry:
                        mock_expiry.return_value = {"type": "ttl", "ttl": "24h"}

                        result = await run_diagnostics()

                        assert isinstance(result, DiagnosticsResult)
                        assert result.version is not None
