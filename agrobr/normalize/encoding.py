"""Tratamento de encoding com fallback chain."""

from __future__ import annotations

from collections.abc import Sequence

import chardet
import structlog

logger = structlog.get_logger()

ENCODING_CHAIN: Sequence[str] = (
    "utf-8",
    "iso-8859-1",
    "windows-1252",
    "utf-16",
    "ascii",
)


def decode_content(
    content: bytes,
    declared_encoding: str | None = None,
    source: str | None = None,
) -> tuple[str, str]:
    """
    Decodifica bytes com fallback chain inteligente.

    Args:
        content: Bytes a decodificar
        declared_encoding: Encoding declarado pelo servidor (Content-Type)
        source: Nome da fonte para logging

    Returns:
        tuple[str, str]: (texto decodificado, encoding usado)
    """
    if declared_encoding:
        try:
            decoded = content.decode(declared_encoding)
            logger.debug(
                "encoding_success",
                source=source,
                encoding=declared_encoding,
                method="declared",
            )
            return decoded, declared_encoding
        except (UnicodeDecodeError, LookupError):
            logger.debug(
                "encoding_declared_failed",
                source=source,
                declared=declared_encoding,
            )

    for encoding in ENCODING_CHAIN:
        try:
            decoded = content.decode(encoding)
            if encoding != "utf-8":
                logger.info(
                    "encoding_fallback",
                    source=source,
                    declared=declared_encoding,
                    actual=encoding,
                    method="chain",
                )
            return decoded, encoding
        except UnicodeDecodeError:
            continue

    detected = chardet.detect(content)
    if detected["encoding"] and detected["confidence"] > 0.7:
        try:
            decoded = content.decode(detected["encoding"])
            logger.info(
                "encoding_fallback",
                source=source,
                declared=declared_encoding,
                actual=detected["encoding"],
                confidence=detected["confidence"],
                method="chardet",
            )
            return decoded, detected["encoding"]
        except (UnicodeDecodeError, LookupError):
            pass

    logger.warning(
        "encoding_forced",
        source=source,
        declared=declared_encoding,
        chardet_result=detected,
    )
    return content.decode("utf-8", errors="replace"), "utf-8-replaced"


def detect_encoding(content: bytes) -> tuple[str, float]:
    """
    Detecta encoding provável do conteúdo.

    Returns:
        tuple[str, float]: (encoding, confidence 0-1)
    """
    result = chardet.detect(content)
    return result["encoding"] or "utf-8", result["confidence"] or 0.0
