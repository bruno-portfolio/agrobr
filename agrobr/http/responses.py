from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Sequence
from typing import Any
from urllib.parse import quote

import httpx

from agrobr.exceptions import SourceUnavailableError

_JSON_ERROR_KEY = re.compile(rb'"(?:e|\\u0065)(?:r|\\u0072){2}(?:o|\\u006[fF])(?:r|\\u0072)"\s*:')
_SEGREDOS_NO_AMBIENTE = (
    "AGROBR_USDA_API_KEY",
    "AGROBR_INMET_TOKEN",
    "AGROBR_COMTRADE_API_KEY",
    "AGROBR_MAPBIOMAS_ALERTA_TOKEN",
    "AGROBR_CONAB_CEASA_PASS",
    "AGROBR_ALERT_SLACK_WEBHOOK",
    "AGROBR_ALERT_DISCORD_WEBHOOK",
    "AGROBR_ALERT_SENDGRID_API_KEY",
)


def redact_secrets(text: str, *secrets: str | None) -> str:
    """Troca por ``[REDACTED]`` as credenciais do ambiente do agrobr e as de ``secrets``.

    Cobre o valor cru, a forma codificada em URL e a escapada em JSON, que é como um eco do servidor costuma voltar.
    """
    valores = [os.environ.get(nome) for nome in _SEGREDOS_NO_AMBIENTE]
    variantes = {
        forma
        for valor in (*valores, *secrets)
        if valor
        for forma in (valor, quote(valor, safe=""), json.dumps(valor)[1:-1])
    }
    for forma in sorted(variantes, key=len, reverse=True):
        text = text.replace(forma, "[REDACTED]")
    return text


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
    secrets: Sequence[str | None] = (),
) -> Any:
    """``secrets`` leva a credencial passada por argumento, que não está no ambiente, para a máscara da prévia."""
    try:
        return (
            response.json()
            if object_pairs_hook is None
            else response.json(object_pairs_hook=object_pairs_hook)
        )
    except ValueError as exc:
        content_type = response.headers.get("content-type", "desconhecido")
        preview = redact_secrets(response.text[:1024], *secrets)[:200]
        preview = preview.replace("\r", " ").replace("\n", " ")
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
