"""
Multi-parser consensus para validação cruzada.

Executa múltiplos parsers e compara resultados para detectar
problemas de parsing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from ...alerts.notifier import AlertLevel, send_alert
from ...exceptions import ParseError
from ...models import Indicador
from .base import BaseParser
from .v1 import CepeaParserV1

logger = structlog.get_logger()

CONSENSUS_PARSERS: list[type[BaseParser]] = [
    CepeaParserV1,
]

DIVERGENCE_THRESHOLD_COUNT = 0.1
DIVERGENCE_THRESHOLD_VALUE = 0.01


@dataclass
class ConsensusResult:
    """Resultado de parsing com consensus."""

    indicadores: list[Indicador]
    parser_used: BaseParser
    all_results: dict[int, list[Indicador]]
    has_consensus: bool
    divergences: list[dict[str, Any]]
    report: dict[str, Any]


@dataclass
class ParserDivergence:
    """Divergência entre parsers."""

    divergence_type: str
    versions: list[int]
    details: dict[str, Any]


async def parse_with_consensus(
    html: str,
    produto: str,
    require_consensus: bool = False,
) -> ConsensusResult:
    """
    Executa múltiplos parsers e compara resultados.

    Args:
        html: Conteúdo HTML
        produto: Produto a parsear
        require_consensus: Se True, levanta erro se parsers divergem

    Returns:
        ConsensusResult

    Raises:
        ParseError: Se require_consensus e parsers divergem
    """
    results: dict[int, list[Indicador]] = {}
    errors: dict[int, str] = {}

    for parser_cls in CONSENSUS_PARSERS:
        parser = parser_cls()
        try:
            can_parse, confidence = parser.can_parse(html)
            if can_parse and confidence > 0.5:
                parsed = parser.parse(html, produto)
                results[parser.version] = parsed
                logger.debug(
                    "consensus_parser_success",
                    version=parser.version,
                    count=len(parsed),
                )
        except Exception as e:
            errors[parser.version] = str(e)
            logger.warning(
                "consensus_parser_failed",
                version=parser.version,
                error=str(e),
            )

    divergences, report = analyze_consensus(results, errors)

    has_consensus = len(divergences) == 0

    if not has_consensus:
        logger.warning(
            "consensus_divergence_detected",
            divergence_count=len(divergences),
        )

        if require_consensus:
            await send_alert(
                level=AlertLevel.WARNING,
                title="Parser consensus failed",
                details=report,
            )
            raise ParseError(
                source="cepea",
                parser_version=0,
                reason=f"Parsers diverged: {len(divergences)} differences",
            )

    latest_version = max(results.keys()) if results else 0
    best_results = results.get(latest_version, [])

    parser_used: BaseParser = CepeaParserV1()
    for parser_cls in CONSENSUS_PARSERS:
        if parser_cls().version == latest_version:
            parser_used = parser_cls()
            break

    return ConsensusResult(
        indicadores=best_results,
        parser_used=parser_used,
        all_results=results,
        has_consensus=has_consensus,
        divergences=divergences,
        report=report,
    )


def analyze_consensus(
    results: dict[int, list[Indicador]],
    errors: dict[int, str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Analisa resultados de múltiplos parsers.

    Args:
        results: Resultados por versão de parser
        errors: Erros por versão de parser

    Returns:
        Tupla (lista de divergências, relatório completo)
    """
    report = {
        "parser_count": len(CONSENSUS_PARSERS),
        "successful": list(results.keys()),
        "failed": list(errors.keys()),
        "errors": errors,
    }

    divergences: list[dict[str, Any]] = []

    if len(results) < 2:
        return divergences, report

    counts = {v: len(r) for v, r in results.items()}
    unique_counts = set(counts.values())

    if len(unique_counts) > 1:
        divergences.append(
            {
                "type": "count_mismatch",
                "versions": list(counts.keys()),
                "counts": counts,
                "description": f"Different record counts: {counts}",
            }
        )

    versions = list(results.keys())
    base_version = versions[0]
    base_results = results[base_version]

    for other_version in versions[1:]:
        other_results = results[other_version]

        if base_results and other_results:
            if base_results[0].data != other_results[0].data:
                divergences.append(
                    {
                        "type": "first_date_mismatch",
                        "versions": [base_version, other_version],
                        "values": [str(base_results[0].data), str(other_results[0].data)],
                    }
                )

            first_diff = abs(float(base_results[0].valor) - float(other_results[0].valor))
            if first_diff > DIVERGENCE_THRESHOLD_VALUE:
                divergences.append(
                    {
                        "type": "first_value_mismatch",
                        "versions": [base_version, other_version],
                        "values": [str(base_results[0].valor), str(other_results[0].valor)],
                        "difference": first_diff,
                    }
                )

            if base_results[-1].data != other_results[-1].data:
                divergences.append(
                    {
                        "type": "last_date_mismatch",
                        "versions": [base_version, other_version],
                        "values": [str(base_results[-1].data), str(other_results[-1].data)],
                    }
                )

            last_diff = abs(float(base_results[-1].valor) - float(other_results[-1].valor))
            if last_diff > DIVERGENCE_THRESHOLD_VALUE:
                divergences.append(
                    {
                        "type": "last_value_mismatch",
                        "versions": [base_version, other_version],
                        "values": [str(base_results[-1].valor), str(other_results[-1].valor)],
                        "difference": last_diff,
                    }
                )

    report["divergences"] = divergences
    report["has_divergence"] = len(divergences) > 0

    return divergences, report


def select_best_result(
    results: dict[int, list[Indicador]],
    divergences: list[dict[str, Any]],
) -> tuple[int, list[Indicador]]:
    """
    Seleciona melhor resultado quando há divergência.

    Estratégia:
    1. Prefere parser mais recente
    2. Se contagens diferem, prefere quem tem mais dados
    3. Em último caso, usa mais recente

    Args:
        results: Resultados por versão
        divergences: Divergências detectadas

    Returns:
        Tupla (versão selecionada, indicadores)
    """
    if not results:
        return 0, []

    has_count_mismatch = any(d["type"] == "count_mismatch" for d in divergences)

    if has_count_mismatch:
        best_version = max(results.keys(), key=lambda v: len(results[v]))
    else:
        best_version = max(results.keys())

    return best_version, results[best_version]


class ConsensusValidator:
    """Validador de consensus para uso contínuo."""

    def __init__(self) -> None:
        self.history: list[ConsensusResult] = []
        self.divergence_count = 0

    async def validate(self, html: str, produto: str) -> ConsensusResult:
        """
        Executa validação com tracking de histórico.

        Args:
            html: Conteúdo HTML
            produto: Produto

        Returns:
            ConsensusResult
        """
        result = await parse_with_consensus(html, produto, require_consensus=False)

        self.history.append(result)
        if not result.has_consensus:
            self.divergence_count += 1

        return result

    @property
    def divergence_rate(self) -> float:
        """Taxa de divergência no histórico."""
        if not self.history:
            return 0.0
        return self.divergence_count / len(self.history)

    def get_statistics(self) -> dict[str, Any]:
        """Retorna estatísticas do validador."""
        return {
            "total_validations": len(self.history),
            "divergence_count": self.divergence_count,
            "divergence_rate": self.divergence_rate,
            "consensus_rate": 1 - self.divergence_rate,
        }
