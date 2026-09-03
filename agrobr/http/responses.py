from __future__ import annotations

from typing import Any

import httpx

from agrobr.exceptions import SourceUnavailableError


def parse_json_response(
    response: httpx.Response,
    *,
    source: str,
    url: str,
) -> Any:
    try:
        return response.json()
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
