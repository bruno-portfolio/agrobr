from __future__ import annotations

import codecs
from collections.abc import Sequence

import chardet

from agrobr import _log

logger = _log.get_logger(__name__)

ENCODING_CHAIN: Sequence[str] = ("utf-8", "windows-1252", "iso-8859-1")


def decode_content(
    content: bytes,
    declared_encoding: str | None = None,
    source: str | None = None,
) -> tuple[str, str]:
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

    *tentativas, ultima = ENCODING_CHAIN
    for encoding in tentativas:
        try:
            decoded = content.decode(encoding)
        except UnicodeDecodeError:
            continue
        if encoding != "utf-8":
            logger.info(
                "encoding_fallback",
                source=source,
                declared=declared_encoding,
                actual=encoding,
                method="chain",
            )
        return decoded, encoding
    logger.info(
        "encoding_fallback",
        source=source,
        declared=declared_encoding,
        actual=ultima,
        method="chain",
    )
    return content.decode(ultima), ultima


def detect_encoding(content: bytes) -> tuple[str, float]:
    result = chardet.detect(content)
    return result["encoding"] or "utf-8", result["confidence"] or 0.0


def detect_encoding_chain(content: bytes) -> str:
    if content.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    for enc in ("utf-8", "windows-1252"):
        if _decodifica(content, enc):
            return enc
    return "iso-8859-1"


def _decodifica(content: bytes, encoding: str) -> bool:
    """Confere a codificação por blocos de 1 MiB, sem montar o texto do corpo inteiro."""
    decoder = codecs.getincrementaldecoder(encoding)()
    view = memoryview(content)
    try:
        for inicio in range(0, len(view), 1 << 20):
            decoder.decode(view[inicio : inicio + (1 << 20)])
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return False
    return True
