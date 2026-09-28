from __future__ import annotations

import asyncio
import logging
import traceback
from urllib.parse import quote

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.inmet import client, transport


@pytest.mark.asyncio
async def test_retry_transport_error_and_nested_cause_are_redacted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    token = "synthetic-retry-secret"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)

    def handler(request: httpx.Request) -> httpx.Response:
        try:
            raise RuntimeError(f"nested {request.url}")
        except RuntimeError as exc:
            raise httpx.ReadTimeout(f"timeout {request.url}", request=request) from exc

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(SourceUnavailableError) as error:
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)

    assert token not in "".join(traceback.format_exception(error.value))
    assert token not in capsys.readouterr().out
    assert isinstance(error.value.__cause__, httpx.ReadTimeout)
    assert token not in str(error.value.__cause__.request.url)


@pytest.mark.asyncio
async def test_redirect_history_and_parallel_public_logs_remain_safe(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.INFO, logger="httpx")
    token = "synthetic-redirect-secret"
    public = "https://public.invalid/open"
    url = f"https://inmet.invalid/start/{token}"

    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0)
        if "/start/" in request.url.path:
            return httpx.Response(302, headers={"location": f"/final/{token}"})
        return httpx.Response(200, json=[])

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as http:
        protected, opened = await asyncio.gather(
            transport.get(http, url, public_url="https://inmet.invalid/start", token=token),
            http.get(public),
        )

    assert token not in caplog.text
    assert "[REDACTED]" in caplog.text
    assert public in caplog.text
    assert str(opened.url) == public
    assert protected.history
    assert all(token not in str(item.request.url) for item in [*protected.history, protected])
    assert all(token not in str(item.headers) for item in [*protected.history, protected])


@pytest.mark.asyncio
async def test_cancellation_restores_transport_context():
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        entered.set()
        await release.wait()
        return httpx.Response(200)

    async def request(http: httpx.AsyncClient) -> None:
        try:
            await transport.get(
                http,
                "https://inmet.invalid/secret",
                public_url="https://inmet.invalid",
                token="secret",
            )
        finally:
            assert transport._TOKENS.get() == ()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        task = asyncio.create_task(request(http))
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_non_json_preview_redacts_reflected_encoded_token(monkeypatch: pytest.MonkeyPatch):
    token = "synthetic+reflected-á"
    monkeypatch.setenv("AGROBR_INMET_TOKEN", token)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, text=f"rejected {quote(token, safe='')}")
        )
    ) as http:
        with pytest.raises(SourceUnavailableError) as error:
            await client._get_json("/estacao/date/date/A001", http=http, requires_token=True)

    assert quote(token, safe="") not in str(error.value)
    assert token not in str(error.value)
    assert "rejected [REDACTED]" in str(error.value)
