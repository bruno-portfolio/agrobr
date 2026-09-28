from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from urllib.parse import quote

import httpx

_TOKENS: ContextVar[tuple[str, ...]] = ContextVar("inmet_transport_tokens", default=())


def _token_variants(token: str) -> tuple[str, ...]:
    variants = {token, quote(token, safe=""), quote(token), json.dumps(token)[1:-1]}
    for encoding in ("utf-8", "latin-1"):
        try:
            variants.add(repr(token.encode(encoding))[2:-1])
        except UnicodeEncodeError:
            continue
    return tuple(sorted(variants, key=len, reverse=True))


def redact(value: str) -> str:
    for token in _TOKENS.get():
        value = value.replace(token, "[REDACTED]")
    return value


def redact_token(value: str, token: str) -> str:
    for secret in _token_variants(token):
        value = value.replace(secret, "[REDACTED]")
    return value


class _TransportTokenFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if _TOKENS.get():
            record.msg = redact(record.getMessage())
            record.args = ()
        return True


for logger_name in (
    "httpx",
    "httpcore.connection",
    "httpcore.proxy",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.socks",
):
    logging.getLogger(logger_name).addFilter(_TransportTokenFilter())


def _redact_bytes(value: bytes) -> bytes:
    for token in _TOKENS.get():
        for encoding in ("utf-8", "latin-1"):
            try:
                value = value.replace(token.encode(encoding), b"[REDACTED]")
            except UnicodeEncodeError:
                continue
    return value


def _sanitize_response(response: httpx.Response, public_url: str) -> httpx.Response:
    response.request = httpx.Request(response.request.method, public_url)
    reason = getattr(response, "extensions", {}).get("reason_phrase")
    if isinstance(reason, bytes):
        response.extensions["reason_phrase"] = _redact_bytes(reason)
    next_request = getattr(response, "next_request", None)
    if next_request is not None:
        response.next_request = httpx.Request(next_request.method, public_url)
    response.headers = httpx.Headers(
        [
            (_redact_bytes(key), _redact_bytes(value))
            for key, value in httpx.Headers(response.headers).raw
        ]
    )
    response.history = [_sanitize_response(item, public_url) for item in response.history]
    if response.status_code < 300 or not any(token in response.text for token in _TOKENS.get()):
        return response
    encoding = response.encoding or "utf-8"
    content = redact(response.text).encode(encoding)
    response.headers.pop("content-encoding", None)
    response.headers["content-length"] = str(len(content))
    sanitized = httpx.Response(
        response.status_code,
        content=content,
        headers=response.headers,
        request=response.request,
        history=response.history,
        extensions=response.extensions,
    )
    sanitized.encoding = encoding
    return sanitized


def sanitize_parse_error(error: ValueError, token: str) -> None:
    error.args = tuple(redact_token(str(arg), token) for arg in error.args)
    if isinstance(error, json.JSONDecodeError):
        error.doc = redact_token(error.doc, token)
    elif isinstance(error, UnicodeDecodeError):
        for secret in _token_variants(token):
            error.object = error.object.replace(secret.encode(), b"[REDACTED]")


def _sanitize_error(error: httpx.HTTPError, public_url: str) -> None:
    seen: set[int] = set()
    pending: list[BaseException] = [error]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        current.args = tuple(redact(str(arg)) for arg in current.args)
        if isinstance(current, httpx.RequestError):
            current.request = httpx.Request("GET", public_url)
        if isinstance(current, httpx.HTTPStatusError):
            current.request = httpx.Request("GET", public_url)
            current.response = _sanitize_response(current.response, public_url)
        for nested in (current.__cause__, current.__context__):
            if nested is not None:
                pending.append(nested)


async def get(
    client: httpx.AsyncClient, url: str, *, public_url: str, token: str | None
) -> httpx.Response:
    if not token:
        return await client.get(url)
    token_state = _TOKENS.set(_token_variants(token))
    try:
        try:
            response = await client.get(url)
        except httpx.HTTPError as exc:
            _sanitize_error(exc, public_url)
            raise
        return _sanitize_response(response, public_url)
    finally:
        _TOKENS.reset(token_state)
