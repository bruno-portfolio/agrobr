from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

import httpx

from agrobr.exceptions import SourceUnavailableError

_JSON_ERROR_KEY = re.compile(rb'"(?:e|\\u0065)(?:r|\\u0072){2}(?:o|\\u006[fF])(?:r|\\u0072)"\s*:')


def arcgis_error_message(data: object) -> str | None:
    if not isinstance(data, dict) or "error" not in data:
        return None
    error = data["error"]
    if not isinstance(error, dict):
        return f"ArcGIS error unknown: {error}"
    return f"ArcGIS error {error.get('code', 'unknown')}: {error.get('message', 'unknown error')}"


def raise_for_service_error(response: httpx.Response, *, source: str, url: str) -> None:
    head = response.content[:1024].lstrip(b"\xef\xbb\xbf \t\r\n")
    if head.lower().startswith((b"<!doctype", b"<html")):
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error="WFS returned HTML instead of feature data (possible maintenance or URL redirect)",
        )
    if head.startswith(b"<") and re.search(
        rb"<(?:[\w.-]+:)?(?:ExceptionReport|ServiceExceptionReport|ServiceException|ExceptionText)(?:\s|>)",
        head,
    ):
        text = response.content[:4096].decode("utf-8", errors="replace")
        match = re.search(
            r"<(?:[\w.-]+:)?(?:ServiceException|ExceptionText)[^>]*>(.*?)</", text, re.DOTALL
        )
        message = match[1].strip() if match else text[:300].strip()
        raise SourceUnavailableError(
            source=source, url=url, last_error=f"WFS server exception: {message}"
        )
    if head.startswith(b"{") and _JSON_ERROR_KEY.search(response.content):
        try:
            data = json.loads(response.content.lstrip(b"\xef\xbb\xbf \t\r\n"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            message = "Resposta JSON inválida do serviço geográfico"
        else:
            message = arcgis_error_message(data)
        if message:
            raise SourceUnavailableError(source=source, url=url, last_error=message)


def parse_json_response(
    response: httpx.Response,
    *,
    source: str,
    url: str,
    object_pairs_hook: Callable[[list[tuple[str, Any]]], Any] | None = None,
) -> Any:
    try:
        return (
            response.json()
            if object_pairs_hook is None
            else response.json(object_pairs_hook=object_pairs_hook)
        )
    except ValueError as exc:
        content_type = response.headers.get("content-type", "desconhecido")
        preview = response.text[:200].replace("\r", " ").replace("\n", " ")
        raise SourceUnavailableError(
            source=source,
            url=url,
            last_error=(
                f"Resposta não é JSON (content-type {content_type!r}; "
                f"provável WAF ou manutenção): {preview!r}"
            ),
        ) from exc


_MOTIVOS_HTTP = {
    403: "a fonte recusou o pedido (bloqueio de WAF ou permissão)",
    404: "o recurso não existe na URL",
}


def raise_for_status(response: httpx.Response, *, source: str) -> None:
    """``response.raise_for_status()`` que sai como ``SourceUnavailableError``, com a fonte, a URL e o status.

    É o caminho único do agrobr para status HTTP de erro; o ``httpx.HTTPStatusError`` fica como causa.
    """
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = response.status_code
        motivo = _MOTIVOS_HTTP.get(status, response.reason_phrase)
        raise SourceUnavailableError(
            source=source, url=str(response.url), last_error=f"HTTP {status}: {motivo}"
        ) from exc
