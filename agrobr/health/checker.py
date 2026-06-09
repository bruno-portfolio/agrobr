"""Health checker — generic HTTP probes + optional deep checks."""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

import structlog

from agrobr.alerts.notifier import AlertLevel
from agrobr.constants import AlertSettings, Fonte
from agrobr.health.registry import HEALTH_REGISTRY, SourceHealthConfig
from agrobr.http.user_agents import UserAgentRotator
from agrobr.utils.time import utcnow

logger = structlog.get_logger()


class CheckStatus(StrEnum):
    OK = "ok"
    WARNING = "warning"
    FAILED = "failed"


@dataclass
class CheckResult:
    source: Fonte
    status: CheckStatus
    latency_ms: float
    message: str
    details: dict[str, Any]
    timestamp: datetime
    category: str | None = None


def _exception_message(error: Exception) -> str:
    return str(error) or type(error).__name__


def _source_down_status(config: SourceHealthConfig) -> CheckStatus:
    if config.tier == "best_effort":
        return CheckStatus.WARNING
    return CheckStatus.FAILED


# ---------------------------------------------------------------------------
# Generic HTTP probe
# ---------------------------------------------------------------------------


async def _check_http(config: SourceHealthConfig) -> CheckResult:
    """Probe a source with a simple HTTP request."""
    import httpx

    # API-key guard
    if (
        config.requires_api_key
        and config.api_key_env_var
        and not os.environ.get(config.api_key_env_var)
    ):
        return CheckResult(
            source=config.source,
            status=CheckStatus.WARNING,
            latency_ms=0,
            message=f"API key not set ({config.api_key_env_var})",
            details={},
            timestamp=utcnow(),
            category="api_key_missing",
        )

    start = time.monotonic()
    details: dict[str, Any] = {}
    headers = UserAgentRotator.get_headers(source="health_check")

    try:
        async with httpx.AsyncClient(
            timeout=config.timeout,
            headers=headers,
            verify=config.verify,
        ) as client:
            if config.method == "HEAD":
                response = await client.head(
                    config.url,
                    follow_redirects=config.follow_redirects,
                )
            else:
                response = await client.get(
                    config.url,
                    follow_redirects=config.follow_redirects,
                )
            latency = (time.monotonic() - start) * 1000

        details["status_code"] = response.status_code
        details["latency_ms"] = latency

        if response.status_code in config.soft_block_codes:
            return CheckResult(
                source=config.source,
                status=CheckStatus.WARNING,
                latency_ms=latency,
                message=f"HTTP {response.status_code}",
                details=details,
                timestamp=utcnow(),
                category="soft_block",
            )

        if response.status_code >= 400:
            return CheckResult(
                source=config.source,
                status=_source_down_status(config),
                latency_ms=latency,
                message=f"HTTP {response.status_code}",
                details=details,
                timestamp=utcnow(),
                category="source_down",
            )

        if latency > 5000:
            return CheckResult(
                source=config.source,
                status=CheckStatus.WARNING,
                latency_ms=latency,
                message=f"High latency: {latency:.0f}ms",
                details=details,
                timestamp=utcnow(),
                category="slow",
            )

        return CheckResult(
            source=config.source,
            status=CheckStatus.OK,
            latency_ms=latency,
            message=f"{config.source.value.upper()} reachable",
            details=details,
            timestamp=utcnow(),
        )

    except Exception as e:
        latency = (time.monotonic() - start) * 1000
        message = _exception_message(e)
        logger.error("health_check_failed", source=config.source.value, error=message)
        return CheckResult(
            source=config.source,
            status=_source_down_status(config),
            latency_ms=latency,
            message=message,
            details=details,
            timestamp=utcnow(),
            category="source_down",
        )


# ---------------------------------------------------------------------------
# Deep check — CEPEA fingerprint + parse
# ---------------------------------------------------------------------------


async def check_cepea_deep() -> CheckResult:
    """Deep check: fetch CEPEA page, compare fingerprint, parse data."""
    from agrobr.cepea import client as cepea_client
    from agrobr.cepea.parsers import fingerprint as fp
    from agrobr.cepea.parsers.detector import get_parser_with_fallback

    start = time.monotonic()
    details: dict[str, Any] = {}

    try:
        fetch_result = await cepea_client.fetch_indicador_page("soja")
        html = fetch_result.html
        latency = (time.monotonic() - start) * 1000

        details["fetch_ok"] = True
        details["latency_ms"] = latency

        if latency > 5000:
            return CheckResult(
                source=Fonte.CEPEA,
                status=CheckStatus.WARNING,
                latency_ms=latency,
                message=f"High latency: {latency:.0f}ms",
                details=details,
                timestamp=utcnow(),
                category="slow",
            )

        current_fp = fp.extract_fingerprint(html, Fonte.CEPEA, "health_check")
        baseline_fp = fp.load_baseline_fingerprint(".structures/baseline.json")

        if baseline_fp:
            similarity, diff = fp.compare_fingerprints(current_fp, baseline_fp)
            details["fingerprint_similarity"] = similarity
            details["fingerprint_diff"] = diff

            if similarity < 0.70:
                return CheckResult(
                    source=Fonte.CEPEA,
                    status=CheckStatus.FAILED,
                    latency_ms=latency,
                    message=f"Layout changed significantly: {similarity:.1%} similarity",
                    details=details,
                    timestamp=utcnow(),
                    category="layout_change",
                )
            elif similarity < 0.85:
                details["warning"] = "Fingerprint drift detected"

        parser, results = await get_parser_with_fallback(html, "soja")
        details["parser_version"] = parser.version
        details["records_parsed"] = len(results)

        if not results:
            return CheckResult(
                source=Fonte.CEPEA,
                status=CheckStatus.FAILED,
                latency_ms=latency,
                message="Parser returned no results",
                details=details,
                timestamp=utcnow(),
                category="parse_error",
            )

        status = CheckStatus.WARNING if details.get("warning") else CheckStatus.OK
        return CheckResult(
            source=Fonte.CEPEA,
            status=status,
            latency_ms=latency,
            message="All checks passed" if status == CheckStatus.OK else details["warning"],
            details=details,
            timestamp=utcnow(),
        )

    except Exception as e:
        latency = (time.monotonic() - start) * 1000
        message = _exception_message(e)
        is_soft_block = "soft block" in message.lower()
        category = "soft_block" if is_soft_block else "parse_error"
        logger.error("health_check_failed", source="cepea", error=message, category=category)
        return CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.FAILED,
            latency_ms=latency,
            message=message,
            details=details,
            timestamp=utcnow(),
            category=category,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def check_source(source: Fonte, *, deep: bool = False) -> CheckResult:
    """Check a single source via registry; use deep check when available."""
    config = HEALTH_REGISTRY.get(source)
    if not config:
        return CheckResult(
            source=source,
            status=CheckStatus.FAILED,
            latency_ms=0,
            message=f"Source not in registry: {source}",
            details={},
            timestamp=utcnow(),
        )

    if deep and config.has_deep_check and source == Fonte.CEPEA:
        return await check_cepea_deep()

    return await _check_http(config)


async def run_all_checks(
    sources: list[Fonte] | None = None,
    *,
    deep: bool = False,
    concurrency: int = 8,
) -> list[CheckResult]:
    """Run health checks for *sources* (default: all registered)."""
    targets = sources or list(HEALTH_REGISTRY.keys())
    semaphore = asyncio.Semaphore(concurrency)

    async def run_one(source: Fonte) -> CheckResult:
        async with semaphore:
            return await check_source(source, deep=deep)

    results = await asyncio.gather(*[run_one(s) for s in targets])
    return list(results)


async def run_checks_with_state(
    sources: list[Fonte] | None = None,
    *,
    deep: bool = False,
    concurrency: int = 8,
    settings: AlertSettings | None = None,
) -> list[tuple[CheckResult, bool, AlertLevel | None]]:
    """Run checks, persist to health_checks table, compute alert decisions.

    Returns list of (result, should_alert, alert_level).
    """
    from agrobr.health.state import record_check, should_send_alert

    settings = settings or AlertSettings()
    results = await run_all_checks(sources, deep=deep, concurrency=concurrency)
    out: list[tuple[CheckResult, bool, AlertLevel | None]] = []

    for result in results:
        record_check(
            source=result.source,
            status=result.status.value,
            category=result.category,
            latency_ms=result.latency_ms,
            message=result.message,
        )
        alert, level = should_send_alert(
            source=result.source,
            current_status=result.status.value,
            category=result.category,
            settings=settings,
        )
        out.append((result, alert, level))

    return out


def format_results(results: list[CheckResult]) -> str:
    lines = ["Health Check Results", "=" * 40]

    for result in results:
        status_emoji = {
            CheckStatus.OK: "\u2713",
            CheckStatus.WARNING: "\u26a0",
            CheckStatus.FAILED: "\u2717",
        }[result.status]

        lines.append(
            f"{status_emoji} {result.source.value.upper()}: "
            f"{result.status.value} ({result.latency_ms:.0f}ms)"
        )
        lines.append(f"  {result.message}")

    return "\n".join(lines)
