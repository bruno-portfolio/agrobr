from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import structlog

from agrobr import __version__
from agrobr.cache.duckdb_store import get_store
from agrobr.cache.policies import get_next_update_info
from agrobr.http.user_agents import UserAgentRotator

logger = structlog.get_logger()


@dataclass
class SourceStatus:
    name: str
    url: str
    status: str
    latency_ms: int
    error: str | None = None


@dataclass
class CacheStats:
    location: str
    size_bytes: int
    total_records: int
    by_source: dict[str, dict[str, Any]] = field(default_factory=dict)


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
                }
                for s in self.sources
            ],
            "cache": {
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
            elif s.status == "slow":
                icon = "[SLOW]"
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
            lines.append("[WARN] System degraded - some sources unavailable")
        else:
            lines.append("[FAIL] System error - check source connectivity")

        lines.append("")
        return "\n".join(lines)


async def _check_source(name: str, url: str, timeout: float = 10.0) -> SourceStatus:
    start = time.perf_counter()
    headers = UserAgentRotator.get_headers(source="health_check")

    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as http_client:
            response = await http_client.get(url, follow_redirects=True)
            latency_ms = int((time.perf_counter() - start) * 1000)

            if response.status_code < 400:
                status = "ok" if latency_ms < 2000 else "slow"
                return SourceStatus(name, url, status, latency_ms)

            return SourceStatus(
                name,
                url,
                "error",
                latency_ms,
                error=f"HTTP {response.status_code}",
            )

    except httpx.TimeoutException:
        latency_ms = int((time.perf_counter() - start) * 1000)
        return SourceStatus(name, url, "error", latency_ms, error="timeout")

    except httpx.ConnectError as e:
        latency_ms = int((time.perf_counter() - start) * 1000)
        return SourceStatus(name, url, "error", latency_ms, error=f"connection error: {e}")

    except Exception as e:
        latency_ms = int((time.perf_counter() - start) * 1000)
        return SourceStatus(name, url, "error", latency_ms, error=str(e))


def _get_cache_stats() -> CacheStats:
    try:
        store = get_store()
        cache_path = Path(store.db_path)
        size_bytes = cache_path.stat().st_size if cache_path.exists() else 0

        by_source: dict[str, dict[str, Any]] = {}
        conn = store._get_conn()

        for fonte in ["cepea", "conab", "ibge"]:
            try:
                result = conn.execute(
                    """
                    SELECT COUNT(*), MIN(data), MAX(data)
                    FROM indicadores
                    WHERE LOWER(fonte) = ?
                    """,
                    [fonte],
                ).fetchone()

                if result and result[0] > 0:
                    by_source[fonte] = {
                        "count": result[0],
                        "oldest": str(result[1]) if result[1] else None,
                        "newest": str(result[2]) if result[2] else None,
                    }
            except Exception:
                logger.warning("cache_stats_source_query_failed", fonte=fonte, exc_info=True)

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
            location="unknown",
            size_bytes=0,
            total_records=0,
            by_source={},
        )


def _get_last_collections() -> dict[str, datetime | None]:
    collections: dict[str, datetime | None] = {}

    try:
        store = get_store()
        conn = store._get_conn()

        for fonte in ["cepea", "conab", "ibge"]:
            try:
                result = conn.execute(
                    """
                    SELECT MAX(collected_at)
                    FROM indicadores
                    WHERE LOWER(fonte) = ?
                    """,
                    [fonte],
                ).fetchone()

                collections[fonte] = result[0] if result and result[0] else None

            except Exception:
                logger.warning("last_collection_query_failed", fonte=fonte, exc_info=True)
                collections[fonte] = None

    except Exception:
        logger.warning("last_collections_failed", exc_info=True)

    return collections


async def run_diagnostics(verbose: bool = False) -> DiagnosticsResult:  # noqa: ARG001
    sources_to_check = [
        ("CEPEA (Noticias Agricolas)", "https://www.noticiasagricolas.com.br"),
        ("CONAB", "https://www.gov.br/conab/pt-br"),
        ("IBGE/SIDRA", "https://sidra.ibge.gov.br"),
    ]

    source_tasks = [_check_source(name, url) for name, url in sources_to_check]
    sources = await asyncio.gather(*source_tasks)

    cache = _get_cache_stats()

    cache_expiry = {}
    for fonte in ["cepea", "conab", "ibge"]:
        cache_expiry[fonte] = get_next_update_info(fonte)

    last_collections = _get_last_collections()

    error_count = sum(1 for s in sources if s.status == "error")
    if error_count == len(sources):
        overall_status = "error"
    elif error_count > 0:
        overall_status = "degraded"
    else:
        overall_status = "healthy"

    return DiagnosticsResult(
        version=__version__,
        timestamp=datetime.now(),
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
