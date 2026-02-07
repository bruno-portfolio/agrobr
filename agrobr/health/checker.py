"""Health checks automatizados para fontes de dados."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

import structlog

from agrobr.constants import Fonte

logger = structlog.get_logger()


class CheckStatus(StrEnum):
    OK = "ok"
    WARNING = "warning"
    FAILED = "failed"


@dataclass
class CheckResult:
    """Resultado de um health check."""

    source: Fonte
    status: CheckStatus
    latency_ms: float
    message: str
    details: dict[str, Any]
    timestamp: datetime


async def check_cepea() -> CheckResult:
    """Executa health check para CEPEA."""
    from agrobr.cepea import client as cepea_client
    from agrobr.cepea.parsers import fingerprint as fp
    from agrobr.cepea.parsers.detector import get_parser_with_fallback

    start = time.monotonic()
    details: dict[str, Any] = {}

    try:
        html = await cepea_client.fetch_indicador_page("soja")
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
                timestamp=datetime.utcnow(),
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
                    timestamp=datetime.utcnow(),
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
                timestamp=datetime.utcnow(),
            )

        status = CheckStatus.WARNING if details.get("warning") else CheckStatus.OK
        return CheckResult(
            source=Fonte.CEPEA,
            status=status,
            latency_ms=latency,
            message="All checks passed" if status == CheckStatus.OK else details["warning"],
            details=details,
            timestamp=datetime.utcnow(),
        )

    except Exception as e:
        latency = (time.monotonic() - start) * 1000
        logger.error("health_check_failed", source="cepea", error=str(e))
        return CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.FAILED,
            latency_ms=latency,
            message=str(e),
            details=details,
            timestamp=datetime.utcnow(),
        )


async def check_conab() -> CheckResult:
    """Executa health check para CONAB."""
    import httpx

    start = time.monotonic()
    details: dict[str, Any] = {}
    url = "https://www.conab.gov.br"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.head(url, follow_redirects=True)
            latency = (time.monotonic() - start) * 1000

            details["status_code"] = response.status_code
            details["latency_ms"] = latency

            if response.status_code >= 400:
                return CheckResult(
                    source=Fonte.CONAB,
                    status=CheckStatus.FAILED,
                    latency_ms=latency,
                    message=f"HTTP {response.status_code}",
                    details=details,
                    timestamp=datetime.utcnow(),
                )

            if latency > 5000:
                return CheckResult(
                    source=Fonte.CONAB,
                    status=CheckStatus.WARNING,
                    latency_ms=latency,
                    message=f"High latency: {latency:.0f}ms",
                    details=details,
                    timestamp=datetime.utcnow(),
                )

            return CheckResult(
                source=Fonte.CONAB,
                status=CheckStatus.OK,
                latency_ms=latency,
                message="CONAB reachable",
                details=details,
                timestamp=datetime.utcnow(),
            )

    except Exception as e:
        latency = (time.monotonic() - start) * 1000
        logger.error("health_check_failed", source="conab", error=str(e))
        return CheckResult(
            source=Fonte.CONAB,
            status=CheckStatus.FAILED,
            latency_ms=latency,
            message=str(e),
            details=details,
            timestamp=datetime.utcnow(),
        )


async def check_ibge() -> CheckResult:
    """Executa health check para IBGE (API SIDRA)."""
    import httpx

    start = time.monotonic()
    details: dict[str, Any] = {}
    url = "https://apisidra.ibge.gov.br/values/t/5457/n1/all/v/allxp/p/last%201/c782/40124"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(url, follow_redirects=True)
            latency = (time.monotonic() - start) * 1000

            details["status_code"] = response.status_code
            details["latency_ms"] = latency

            if response.status_code >= 400:
                return CheckResult(
                    source=Fonte.IBGE,
                    status=CheckStatus.FAILED,
                    latency_ms=latency,
                    message=f"SIDRA API HTTP {response.status_code}",
                    details=details,
                    timestamp=datetime.utcnow(),
                )

            data = response.json()
            details["records"] = len(data) if isinstance(data, list) else 0

            if not data or (isinstance(data, list) and len(data) < 2):
                return CheckResult(
                    source=Fonte.IBGE,
                    status=CheckStatus.WARNING,
                    latency_ms=latency,
                    message="SIDRA API returned empty data",
                    details=details,
                    timestamp=datetime.utcnow(),
                )

            if latency > 5000:
                return CheckResult(
                    source=Fonte.IBGE,
                    status=CheckStatus.WARNING,
                    latency_ms=latency,
                    message=f"High latency: {latency:.0f}ms",
                    details=details,
                    timestamp=datetime.utcnow(),
                )

            return CheckResult(
                source=Fonte.IBGE,
                status=CheckStatus.OK,
                latency_ms=latency,
                message=f"SIDRA API OK ({details['records']} records)",
                details=details,
                timestamp=datetime.utcnow(),
            )

    except Exception as e:
        latency = (time.monotonic() - start) * 1000
        logger.error("health_check_failed", source="ibge", error=str(e))
        return CheckResult(
            source=Fonte.IBGE,
            status=CheckStatus.FAILED,
            latency_ms=latency,
            message=str(e),
            details=details,
            timestamp=datetime.utcnow(),
        )


async def check_source(source: Fonte) -> CheckResult:
    """
    Executa health check para uma fonte específica.

    Args:
        source: Fonte a verificar

    Returns:
        CheckResult com status do check
    """
    checkers = {
        Fonte.CEPEA: check_cepea,
        Fonte.CONAB: check_conab,
        Fonte.IBGE: check_ibge,
    }

    checker = checkers.get(source)
    if not checker:
        return CheckResult(
            source=source,
            status=CheckStatus.FAILED,
            latency_ms=0,
            message=f"Unknown source: {source}",
            details={},
            timestamp=datetime.utcnow(),
        )

    return await checker()


async def run_all_checks() -> list[CheckResult]:
    """Executa health checks para todas as fontes."""
    sources = [Fonte.CEPEA, Fonte.CONAB, Fonte.IBGE]
    results = await asyncio.gather(*[check_source(s) for s in sources])
    return list(results)


def format_results(results: list[CheckResult]) -> str:
    """Formata resultados para exibição."""
    lines = ["Health Check Results", "=" * 40]

    for result in results:
        status_emoji = {
            CheckStatus.OK: "✓",
            CheckStatus.WARNING: "⚠",
            CheckStatus.FAILED: "✗",
        }[result.status]

        lines.append(
            f"{status_emoji} {result.source.value.upper()}: "
            f"{result.status.value} ({result.latency_ms:.0f}ms)"
        )
        lines.append(f"  {result.message}")

    return "\n".join(lines)
