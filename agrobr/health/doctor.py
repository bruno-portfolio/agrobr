from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from agrobr import __version__
from agrobr.cache.duckdb_store import get_store
from agrobr.cache.policies import SOURCE_POLICY_MAP, get_next_update_info
from agrobr.health import checker
from agrobr.health.registry import HEALTH_REGISTRY, SourceHealthConfig
from agrobr.utils.time import utcnow

logger = structlog.get_logger()


@dataclass
class SourceStatus:
    name: str
    url: str
    status: str
    latency_ms: int
    error: str | None = None
    category: str | None = None


@dataclass
class CacheStats:
    location: str
    size_bytes: int
    total_records: int
    by_source: dict[str, dict[str, Any]] = field(default_factory=dict)
    status: str = "ok"
    error: str | None = None


@dataclass
class DiagnosticsResult:
    version: str
    timestamp: datetime
    sources: list[SourceStatus]
    cache: CacheStats
    last_collections: dict[str, datetime | None]
    cache_expiry: dict[str, dict[str, str]]
    config: dict[str, Any]
    overall_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "timestamp": self.timestamp.isoformat(),
            "sources": [
                {
                    "name": s.name,
                    "url": s.url,
                    "status": s.status,
                    "latency_ms": s.latency_ms,
                    "error": s.error,
                    "category": s.category,
                }
                for s in self.sources
            ],
            "cache": {
                "status": self.cache.status,
                "error": self.cache.error,
                "location": self.cache.location,
                "size_mb": round(self.cache.size_bytes / 1024 / 1024, 2),
                "total_records": self.cache.total_records,
                "by_source": self.cache.by_source,
            },
            "last_collections": {
                k: v.isoformat() if v else None for k, v in self.last_collections.items()
            },
            "cache_expiry": self.cache_expiry,
            "config": self.config,
            "overall_status": self.overall_status,
        }

    def to_rich(self) -> str:
        lines = [
            "",
            f"agrobr diagnostics v{self.version}",
            "=" * 50,
            "",
            "Sources Connectivity",
        ]

        for s in self.sources:
            if s.status == "ok":
                icon = "[OK]"
            elif s.status == "warning":
                icon = "[WARN]"
            elif s.status == "slow":
                icon = "[SLOW]"
            elif s.status == "not_verified":
                icon = "[NOT VERIFIED]"
            else:
                icon = "[FAIL]"

            line = f"  {icon} {s.name:<35} {s.latency_ms:>5}ms"
            if s.error:
                line += f"  ({s.error})"
            lines.append(line)

        lines.extend(
            [
                "",
                "Cache Status",
                f"  Status:        {self.cache.status}",
                f"  Error:         {self.cache.error or '-'}",
                f"  Location:      {self.cache.location}",
                f"  Size:          {self.cache.size_bytes / 1024 / 1024:.2f} MB",
                f"  Total records: {self.cache.total_records:,}",
                "",
                "  By source:",
            ]
        )

        for fonte, stats in self.cache.by_source.items():
            count = stats.get("count", 0)
            oldest = stats.get("oldest", "-")
            newest = stats.get("newest", "-")
            lines.append(f"    {fonte.upper()}: {count:,} records ({oldest} to {newest})")

        lines.extend(
            [
                "",
                "Cache Expiry",
            ]
        )

        for fonte, info in self.cache_expiry.items():
            exp_type = info.get("type", "unknown")
            if exp_type == "smart":
                lines.append(f"  {fonte.upper()}: {info.get('description', '')}")
            else:
                lines.append(f"  {fonte.upper()}: TTL {info.get('ttl', 'unknown')}")

        lines.extend(
            [
                "",
                "Configuration",
                f"  Browser fallback:   {'enabled' if self.config.get('browser_fallback') else 'disabled'}",
                f"  Alternative source: {'enabled' if self.config.get('alternative_source') else 'disabled'}",
                "",
            ]
        )

        if self.overall_status == "healthy":
            lines.append("[OK] All systems operational")
        elif self.overall_status == "degraded":
            lines.append("[WARN] System degraded - check diagnostic warnings")
        else:
            lines.append("[FAIL] System error - check cache and source diagnostics")

        lines.append("")
        return "\n".join(lines)


async def _check_source(config: SourceHealthConfig) -> SourceStatus:
    result = await checker._check_http(config)
    status = {
        checker.CheckStatus.OK: "ok",
        checker.CheckStatus.WARNING: "warning",
        checker.CheckStatus.FAILED: "error",
        checker.CheckStatus.NOT_VERIFIED: "not_verified",
    }[result.status]
    if result.category == "slow":
        status = "slow"
    return SourceStatus(
        config.source.value.upper(),
        config.url,
        status,
        int(result.latency_ms),
        error=result.message if result.status != checker.CheckStatus.OK else None,
        category=result.category,
    )


def _get_cache_stats() -> CacheStats:
    try:
        store = get_store()
        cache_path = Path(store.db_path)
        size_bytes = cache_path.stat().st_size if cache_path.exists() else 0

        by_source: dict[str, dict[str, Any]] = {}
        with store._conexao() as conn:
            if conn is None:
                raise RuntimeError("Conexão com o cache indisponível")
            rows = conn.execute(
                "SELECT LOWER(fonte), COUNT(*), MIN(data), MAX(data) FROM indicadores GROUP BY LOWER(fonte)"
            ).fetchall()
        for fonte, count, oldest, newest in rows:
            by_source[fonte] = {
                "count": count,
                "oldest": str(oldest) if oldest else None,
                "newest": str(newest) if newest else None,
            }

        total_records = sum(s.get("count", 0) for s in by_source.values())

        return CacheStats(
            location=str(cache_path),
            size_bytes=size_bytes,
            total_records=total_records,
            by_source=by_source,
        )

    except Exception as e:
        logger.warning("cache_stats_failed", error=str(e))
        return CacheStats(
            status="error",
            error=f"{type(e).__name__}: {e}",
            location="unknown",
            size_bytes=0,
            total_records=0,
            by_source={},
        )


def _get_last_collections() -> dict[str, datetime | None]:
    with get_store()._conexao() as conn:
        if conn is None:
            raise RuntimeError("Conexão com o cache indisponível")
        rows = conn.execute(
            "SELECT LOWER(fonte), MAX(collected_at) FROM indicadores GROUP BY LOWER(fonte)"
        ).fetchall()
    return dict(rows)


async def run_diagnostics(verbose: bool = False) -> DiagnosticsResult:  # noqa: ARG001
    semaphore = asyncio.Semaphore(8)

    async def probe(config: SourceHealthConfig) -> SourceStatus:
        async with semaphore:
            return await _check_source(config)

    sources = await asyncio.gather(*[probe(config) for config in HEALTH_REGISTRY.values()])

    cache = _get_cache_stats()

    cache_expiry: dict[str, dict[str, str]] = {}
    for fonte in SOURCE_POLICY_MAP:
        cache_expiry[fonte.value] = get_next_update_info(fonte.value)

    last_collections: dict[str, datetime | None] = {}
    if cache.status == "ok":
        try:
            last_collections = _get_last_collections()
        except Exception as exc:
            cache.status = "error"
            cache.error = f"{type(exc).__name__}: {exc}"
            logger.warning("last_collections_failed", error=cache.error)

    error_count = sum(1 for s in sources if s.status == "error")
    if cache.status == "error" or (sources and error_count == len(sources)):
        overall_status = "error"
    elif any(s.status != "ok" for s in sources):
        overall_status = "degraded"
    else:
        overall_status = "healthy"

    return DiagnosticsResult(
        version=__version__,
        timestamp=utcnow(),
        sources=list(sources),
        cache=cache,
        last_collections=last_collections,
        cache_expiry=cache_expiry,
        config={
            "browser_fallback": False,
            "alternative_source": True,
        },
        overall_status=overall_status,
    )
