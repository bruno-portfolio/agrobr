"""Tests for health.doctor module."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.health.doctor import (
    CacheStats,
    DiagnosticsResult,
    SourceStatus,
    run_diagnostics,
)
from agrobr.health.registry import HEALTH_REGISTRY


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


@pytest.mark.parametrize(
    ("overall", "final"),
    [
        ("healthy", "[OK] All systems operational"),
        ("degraded", "[WARN] System degraded - check diagnostic warnings"),
        ("error", "[FAIL] System error - check cache and source diagnostics"),
    ],
)
def test_to_rich_status_lines(overall, final):
    result = DiagnosticsResult(
        version="2.0.0",
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        sources=[
            SourceStatus("A", "https://a", "ok", 10),
            SourceStatus("B", "https://b", "warning", 20),
            SourceStatus("C", "https://c", "slow", 30),
            SourceStatus("D", "https://d", "error", 40, "timeout"),
        ],
        cache=CacheStats("/tmp", 0, 0, {}),
        last_collections={},
        cache_expiry={},
        config={},
        overall_status=overall,
    )
    lines = result.to_rich().split("\n")
    assert f"  [OK] {'A':<35} {10:>5}ms" in lines
    assert f"  [WARN] {'B':<35} {20:>5}ms" in lines
    assert f"  [SLOW] {'C':<35} {30:>5}ms" in lines
    assert f"  [FAIL] {'D':<35} {40:>5}ms  (timeout)" in lines
    assert final in lines


@pytest.mark.parametrize(
    ("statuses", "cache_status", "overall"),
    [
        (("ok",), "ok", "healthy"),
        (("warning", "ok"), "ok", "degraded"),
        (("error",), "ok", "error"),
        (("ok",), "error", "error"),
    ],
)
async def test_run_diagnostics_overall_status(statuses, cache_status, overall):
    results = [
        SourceStatus(str(nome), "https://fonte", statuses[min(i, len(statuses) - 1)], 5)
        for i, nome in enumerate(HEALTH_REGISTRY)
    ]
    collected = {"cepea": datetime(2024, 1, 1)}
    with (
        patch("agrobr.health.doctor._check_source", AsyncMock(side_effect=results)),
        patch(
            "agrobr.health.doctor._get_cache_stats",
            return_value=CacheStats("/tmp", 0, 0, {}, status=cache_status),
        ),
        patch("agrobr.health.doctor._get_last_collections", return_value=collected) as last,
        patch("agrobr.health.doctor.get_next_update_info", return_value={}),
    ):
        result = await run_diagnostics()
    assert result.overall_status == overall
    assert result.last_collections == (collected if cache_status == "ok" else {})
    assert last.call_count == (1 if cache_status == "ok" else 0)


async def test_run_diagnostics_expiracao_so_de_fonte_com_cache():
    results = [SourceStatus(str(nome), "https://fonte", "ok", 5) for nome in HEALTH_REGISTRY]
    with (
        patch("agrobr.health.doctor._check_source", AsyncMock(side_effect=results)),
        patch("agrobr.health.doctor._get_cache_stats", return_value=CacheStats("/tmp", 0, 0, {})),
        patch("agrobr.health.doctor._get_last_collections", return_value={}),
        patch("agrobr.cache.policies.utcnow", return_value=datetime(2026, 9, 23, 12, 0)),
    ):
        result = await run_diagnostics()
    assert result.cache_expiry == {
        "cepea": {
            "type": "smart",
            "expires_at": "2026-09-23 21:00",
            "description": "Expira às 18h BRT (atualização CEPEA)",
        }
    }
    assert "TTL" not in result.to_rich()
