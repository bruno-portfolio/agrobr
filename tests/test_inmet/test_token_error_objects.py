from __future__ import annotations

import json

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.inmet import client, transport


@pytest.mark.asyncio
async def test_http_error_response_body_and_headers_are_redacted(monkeypatch: pytest.MonkeyPatch):
    token = "synthetic-error-object-secret"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(403, text=f"Denied {token}", headers={"x-debug": token})
        )
    ) as http:
        with pytest.raises(SourceUnavailableError) as error:
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)

    cause = error.value.__cause__
    assert isinstance(cause, httpx.HTTPStatusError)
    assert token not in cause.response.text
    assert token not in str(cause.response.headers)
    assert "Denied [REDACTED]" in cause.response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", ["Denied ", "x" * 192])
async def test_non_json_error_document_and_partial_preview_are_redacted(
    monkeypatch: pytest.MonkeyPatch, prefix: str
):
    token = "synthetic-parse-object-secret"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, text=prefix + token))
    ) as http:
        with pytest.raises(SourceUnavailableError) as error:
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)

    assert isinstance(error.value.__cause__, json.JSONDecodeError)
    assert token not in error.value.__cause__.doc
    assert token[:8] not in str(error.value)


@pytest.mark.asyncio
async def test_success_payload_with_matching_string_remains_unchanged(
    monkeypatch: pytest.MonkeyPatch,
):
    token = "synthetic-success-preserved"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)
    data = [{"original": token}]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=data))
    ) as http:
        assert (
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)
            == data
        )


@pytest.mark.asyncio
async def test_non_json_decode_error_does_not_retain_token_bytes(monkeypatch: pytest.MonkeyPatch):
    token = "synthetic-unicode-decode-secret"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, content=b"\xff" + token.encode())
        )
    ) as http:
        with pytest.raises(SourceUnavailableError) as error:
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)

    cause = error.value.__cause__
    assert isinstance(cause, UnicodeDecodeError)
    assert token.encode() not in cause.object
    assert token not in repr(cause.args)
    assert token not in str(error.value)


@pytest.mark.asyncio
async def test_redirect_not_followed_has_no_token_in_next_request():
    token = "synthetic-next-request-secret"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(302, headers={"location": f"/final/{token}"})
        )
    ) as http:
        result = await transport.get(
            http,
            f"https://inmet.invalid/token/{token}",
            public_url="https://inmet.invalid/data",
            token=token,
        )

    assert result.next_request is not None
    assert token not in str(result.next_request.url)
    assert token not in str(result.next_request.headers)


@pytest.mark.asyncio
async def test_error_reason_phrase_does_not_reintroduce_token(monkeypatch: pytest.MonkeyPatch):
    token = "synthetic-reason-phrase-secret"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                400, extensions={"reason_phrase": f"Denied {token}".encode()}
            )
        )
    ) as http:
        with pytest.raises(SourceUnavailableError, match=r"HTTP 400: Denied \[REDACTED\]") as error:
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)

    cause = error.value.__cause__
    assert isinstance(cause, httpx.HTTPStatusError)
    assert token not in str(error.value) and token not in error.value.url
    assert token not in str(cause)
    assert token not in cause.response.reason_phrase
    assert b"[REDACTED]" in cause.response.extensions["reason_phrase"]
