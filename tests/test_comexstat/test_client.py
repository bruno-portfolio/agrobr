from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import ValidationError

from agrobr import constants
from agrobr.comexstat import client
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from agrobr.http import rate_limiter


@pytest.fixture
def transport(monkeypatch):
    constructor = httpx.AsyncClient
    calls = []
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    monkeypatch.setattr(client.retry.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(rate_limiter, "_async_sleep", AsyncMock())

    def install(handler):
        def observe(request):
            calls.append(request)
            return handler(request)

        def factory(**kwargs):
            return constructor(transport=httpx.MockTransport(observe), **kwargs)

        monkeypatch.setattr(client.httpx, "AsyncClient", factory)
        return calls

    return install


def response(body=b"CODE;TEXT\n01;literal\n", status=200, headers=None):
    return httpx.Response(status, headers=headers, stream=httpx.ByteStream(body))


@pytest.mark.asyncio
async def test_seekable_complete_and_details_after_close(transport):
    body = b'CODE;TEXT\n01;"a\nb"\n'
    calls = transport(lambda _: response(body, headers={"content-length": str(len(body))}))
    async with client.open_csv(fluxo="exportacao", ano=2026) as resource:
        assert resource.file.tell() == 0
        assert resource.file.read() == body
        assert resource.sha256 == hashlib.sha256(body).hexdigest()
        assert resource.size_bytes == len(body)
        assert resource.complete and resource.receipts[0].closed
        assert resource.fetched_at.utcoffset().total_seconds() == 0
    assert resource.file.closed
    details = resource.details()
    assert details["spool_closed"] and details["client_closed"]
    assert details["receipts"][0]["saved_sha256"] == resource.sha256
    details["receipts"][0]["headers"].append(["fake", "value"])
    assert ["fake", "value"] not in resource.details()["receipts"][0]["headers"]
    assert len(calls) == 1 and calls[0].headers["accept-encoding"] == "identity"
    assert str(calls[0].url).endswith("/EXP_2026.csv")


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 404, 500])
async def test_http_failure_does_not_yield(transport, status):
    calls = transport(lambda _: response(b"error", status))
    with pytest.raises(SourceUnavailableError) as error:
        async with client.open_csv(fluxo="exportacao", ano=2026):
            pytest.fail("failure yielded a resource")
    detail = error.value.comexstat_acquisition
    assert len(calls) == (3 if status == 500 else 1)
    assert detail["receipts"][-1]["status"] == status
    assert detail["client_closed"] and detail["spool_closed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "location",
    [
        "https://evil.example/x.csv",
        "http://balanca.mdic.gov.br/x.csv",
        "https://user@balanca.mdic.gov.br/x.csv",
    ],
)
async def test_redirect_rejected_before_foreign_send(transport, location):
    calls = transport(lambda _: response(b"", 302, {"location": location}))
    with pytest.raises(ValueError) as error:
        async with client.open_dictionary("vias"):
            pytest.fail("unsafe redirect yielded")
    assert len(calls) == 1
    assert error.value.comexstat_acquisition["receipts"][0]["closed"]


@pytest.mark.asyncio
async def test_same_host_redirect_and_physical_limit(transport, monkeypatch):
    index = 0

    def handler(_):
        nonlocal index
        index += 1
        return response(b"", 302, {"location": f"/redirect_{index}.csv"})

    calls = transport(handler)
    monkeypatch.setattr(constants, "COMEXSTAT_MAX_PHYSICAL_REQUESTS", 3)
    with pytest.raises(ResourceLimitError, match="envios físicos") as error:
        async with client.open_dictionary("vias"):
            pytest.fail("unbounded redirect")
    assert len(calls) == 3 and len(error.value.comexstat_acquisition["receipts"]) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fluxo,ano", [("bad", 2026), ("exportacao", True), ("importacao", 1996), ("exportacao", "2026")]
)
async def test_guards_before_tls_tempfile_http(monkeypatch, fluxo, ano):
    def forbidden():
        pytest.fail("TLS created before guard")

    monkeypatch.setattr(client._tls, "build_context", forbidden)
    with pytest.raises(ValueError):
        async with client.open_csv(fluxo=fluxo, ano=ano):
            pytest.fail("invalid query yielded")


class FailingStream(httpx.AsyncByteStream):
    def __init__(self, *, close_failure=False, cancel=False):
        self.close_failure = close_failure
        self.cancel = cancel

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"x" * constants.COMEXSTAT_CHUNK_BYTES
        if self.cancel:
            raise asyncio.CancelledError("cancel-primary")
        raise httpx.ReadError("read-primary")

    async def aclose(self):
        if self.close_failure:
            raise httpx.CloseError("close-secondary")


@pytest.mark.asyncio
async def test_partial_read_retries_and_preserves_digests(transport):
    calls = transport(lambda _: httpx.Response(200, stream=FailingStream()))
    with pytest.raises(SourceUnavailableError) as error:
        async with client.open_dictionary("vias"):
            pytest.fail("partial yielded")
    detail = error.value.comexstat_acquisition
    assert len(calls) == 3
    assert detail["transfer_bytes"] == 3 * constants.COMEXSTAT_CHUNK_BYTES
    assert all(
        not item["complete_body"] and item["size_bytes"] == constants.COMEXSTAT_CHUNK_BYTES
        for item in detail["receipts"]
    )


@pytest.mark.asyncio
async def test_read_and_close_failure_preserve_primary_without_retry(transport):
    calls = transport(lambda _: httpx.Response(200, stream=FailingStream(close_failure=True)))
    with pytest.raises(SourceUnavailableError) as error:
        async with client.open_dictionary("vias"):
            pytest.fail("failure yielded")
    assert len(calls) == 1 and isinstance(error.value.__cause__, httpx.ReadError)
    receipt = error.value.comexstat_acquisition["receipts"][0]
    assert receipt["error_message"] == "read-primary"
    assert receipt["close_error_message"] == "close-secondary"
    assert not receipt["closed"] and receipt["size_bytes"] == constants.COMEXSTAT_CHUNK_BYTES


@pytest.mark.asyncio
async def test_parser_error_is_preserved_and_receipts_survive(transport):
    transport(lambda _: response())
    primary = ValueError("parser-sentinel")
    with pytest.raises(ValueError) as error:
        async with client.open_dictionary("vias"):
            raise primary
    assert error.value is primary
    assert primary.comexstat_acquisition["complete"]
    assert primary.comexstat_acquisition["spool_closed"]


@pytest.mark.asyncio
async def test_response_close_error_after_body_is_not_retried(transport):
    class CloseOnly(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"A;B\n1;2\n"

        async def aclose(self):
            raise httpx.CloseError("response-close")

    calls = transport(lambda _: httpx.Response(200, stream=CloseOnly()))
    with pytest.raises(SourceUnavailableError) as error:
        async with client.open_dictionary("vias"):
            pytest.fail("response close failure yielded")
    assert len(calls) == 1
    receipt = error.value.comexstat_acquisition["receipts"][0]
    assert receipt["received_bytes"] == receipt["size_bytes"] == 8
    assert receipt["close_error_message"] == "response-close" and not receipt["closed"]


@pytest.mark.asyncio
async def test_teto_do_download_vem_do_ambiente(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_DOWNLOAD_COMEXSTAT", "0.05")

    async def sem_fim(*_args):
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "_download", sem_fim)
    with pytest.raises(SourceUnavailableError, match="TimeoutError") as error:
        async with client.open_dictionary("vias"):
            pytest.fail("download passou do teto")
    assert error.value.comexstat_acquisition["budgets"]["download_timeout_seconds"] == 0.05


@pytest.mark.parametrize("valor", ["0", "-1", "inf"])
def test_teto_do_download_recusa_valor_nao_positivo(monkeypatch, valor):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_DOWNLOAD_COMEXSTAT", valor)
    with pytest.raises(ValidationError):
        constants.HTTPSettings()
