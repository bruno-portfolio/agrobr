"""Detector e seletor de parser com fallback em cascata."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import structlog

from agrobr import constants, exceptions
from agrobr.cepea.parsers import base
from agrobr.cepea.parsers.v1 import CepeaParserV1

if TYPE_CHECKING:
    from agrobr import models

logger = structlog.get_logger()

PARSERS: list[type[base.BaseParser]] = [
    CepeaParserV1,
]


async def get_parser_with_fallback(
    html: str,
    produto: str,
    data_referencia: date | None = None,
    strict: bool = False,
) -> tuple[base.BaseParser, list[models.Indicador]]:
    """Seleciona parser e executa com fallback em cascata."""
    if not PARSERS:
        raise exceptions.ParseError(
            source="cepea",
            parser_version=0,
            reason="No parsers registered. CEPEA parser will be implemented in WEEK 3.",
            html_snippet=html[:200],
        )

    errors: list[tuple[str, str]] = []
    warnings: list[str] = []

    for parser_cls in reversed(PARSERS):
        parser = parser_cls()

        if data_referencia:
            if parser.valid_from > data_referencia:
                continue
            if parser.valid_until and data_referencia > parser.valid_until:
                continue

        can_parse, confidence = parser.can_parse(html)

        logger.debug(
            "parser_check",
            parser_version=parser.version,
            can_parse=can_parse,
            confidence=confidence,
        )

        if not can_parse:
            continue

        if confidence < constants.CONFIDENCE_LOW and strict:
            raise exceptions.FingerprintMismatchError(
                source=parser.source,
                similarity=confidence,
                threshold=constants.CONFIDENCE_LOW,
            )

        if confidence < constants.CONFIDENCE_HIGH:
            warnings.append(
                f"Parser v{parser.version} confidence {confidence:.1%} "
                f"(expected >= {constants.CONFIDENCE_HIGH:.1%})"
            )

        try:
            result = parser.parse(html, produto)

            if not result:
                errors.append((f"v{parser.version}", "No data extracted"))
                continue

            if warnings:
                logger.warning(
                    "parser_low_confidence",
                    parser_version=parser.version,
                    confidence=confidence,
                    warnings=warnings,
                )

            return parser, result

        except Exception as e:
            errors.append((f"v{parser.version}", str(e)))
            logger.warning(
                "parser_failed",
                parser_version=parser.version,
                error=str(e),
            )
            continue

    error_summary = "; ".join(f"{v}: {e}" for v, e in errors)
    raise exceptions.ParseError(
        source=PARSERS[0]().source if PARSERS else "cepea",
        parser_version=0,
        reason=f"All parsers failed: {error_summary}",
        html_snippet=html[:500],
    )
