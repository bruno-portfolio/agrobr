from __future__ import annotations

import gzip
import hashlib
from collections.abc import AsyncIterator

import httpx
import pytest

from agrobr import constants
from agrobr.alt.antt_pedagio import _transport, acquisition
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError


class Stream(httpx.AsyncByteStream):
    def __init__(
        self, chunks: list[bytes], *, read_error: bool = False, close_error: bool = False
    ) -> None:
        self.chunks = chunks
        self.read_error = read_error
        self.close_error = close_error
        self.closed = False
        self.delivered_chunks = 0
        self.read_exception = httpx.ReadError("synthetic read error")
        self.close_exception = httpx.CloseError("synthetic close error")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.delivered_chunks += 1
            yield chunk
        if self.read_error:
            raise self.read_exception

    async def aclose(self) -> None:
        self.closed = True
        if self.close_error:
            raise self.close_exception


@pytest.mark.asyncio
async def test_partial_read_retry_preserves_small_unbuffered_prefix():
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    transport = _transport.Transport(bundle)
    streams = [Stream([b"partial"], read_error=True), Stream([b"whole"])]
    attempts = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        stream = streams[attempts]
        attempts += 1
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        file, index = await transport.fetch(
            http, "https://dados.antt.gov.br/data.csv", role="trafego", limit=100, expected=5
        )
        try:
            assert file.read() == b"whole" and index == 1
        finally:
            file.close()
    first, second = bundle.attempts
    assert first.size_bytes == 7 and first.sha256 == hashlib.sha256(b"partial").hexdigest()
    assert first.error_type == "ReadError" and not first.complete_body
    assert second.complete_body and bundle.transfer_bytes == 12
    assert all(stream.closed for stream in streams)


@pytest.mark.parametrize("read_error", [False, True])
@pytest.mark.asyncio
async def test_close_error_stops_retry_and_preserves_primary(read_error: bool):
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    stream = Stream([b"saved"], read_error=read_error, close_error=True)
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        try:
            await _transport.Transport(bundle).fetch(
                http, "https://dados.antt.gov.br/data.csv", role="trafego", limit=100
            )
        except Exception as exc:
            captured = exc
        else:
            captured = None
    receipt = bundle.attempts[0]
    assert captured is (stream.read_exception if read_error else stream.close_exception), captured
    assert (
        calls == 1
        and receipt.size_bytes == 5
        and receipt.sha256 == hashlib.sha256(b"saved").hexdigest()
    )
    assert receipt.close_error_type == "CloseError" and receipt.finished_at is not None
    assert receipt.error_type == ("ReadError" if read_error else "CloseError")


@pytest.mark.asyncio
async def test_unexpected_gzip_rejected_before_reading_or_decoding():
    content = b"x;y\n1;2\n" * 300
    wire = gzip.compress(content)
    stream = Stream([wire[:7], wire[7:]])
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            200,
            stream=stream,
            headers={"content-encoding": "gzip", "content-length": str(len(wire))},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(SourceUnavailableError, match="Content-Encoding"):
            await _transport.Transport(bundle).fetch(
                http,
                "https://dados.antt.gov.br/data.csv",
                role="trafego",
                limit=10000,
                expected=len(content),
            )
    assert stream.delivered_chunks == 0 and stream.closed
    assert len(calls) == 1 and calls[0].headers["accept-encoding"] == "identity"
    assert bundle.transfer_bytes == 0 and bundle.attempts[0].size_bytes == 0
    assert not bundle.attempts[0].complete_body
    assert bundle.attempts[0].headers["content-encoding"] == "gzip"


@pytest.mark.parametrize("budget", ["body", "transfer", "spool"])
@pytest.mark.asyncio
async def test_chunk_budget_accounts_received_bytes_and_closes(
    monkeypatch: pytest.MonkeyPatch, budget: str
):
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    limit = 6 if budget == "body" else 100
    if budget == "transfer":
        monkeypatch.setattr(constants, "ANTT_MAX_TRANSFER_BYTES", 6)
    if budget == "spool":
        monkeypatch.setattr(constants, "ANTT_MAX_SPOOL_BYTES", 6)
    stream = Stream([b"1234", b"5678"])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=stream))
    ) as http:
        with pytest.raises(ResourceLimitError, match="chunk recebido"):
            await _transport.Transport(bundle).fetch(
                http, "https://dados.antt.gov.br/data.csv", role="trafego", limit=limit
            )
    assert stream.closed and bundle.transfer_bytes == 8
    assert bundle.attempts[0].size_bytes == 8 and not bundle.attempts[0].complete_body
    assert bundle.attempts[0].sha256 == hashlib.sha256(b"12345678").hexdigest()


@pytest.mark.asyncio
async def test_full_content_range_accepted_and_private_headers_omitted():
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                content=b"a;b\n",
                headers={
                    "content-range": "bytes 0-3/4",
                    "authorization": "sensitive",
                    "set-cookie": "sensitive",
                    "etag": "public",
                },
            )
        )
    ) as http:
        file, _ = await _transport.Transport(bundle).fetch(
            http, "https://dados.antt.gov.br/data.csv", role="trafego", limit=100
        )
        file.close()
    assert bundle.attempts[0].headers["etag"] == "public"
    assert (
        "authorization" not in bundle.attempts[0].headers
        and "set-cookie" not in bundle.attempts[0].headers
    )


@pytest.mark.asyncio
async def test_announced_size_over_limit_refused_before_get():
    bundle = acquisition.TrafegoAcquisition([2026], "mensal")
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, content=b"x" * 150)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        try:
            await _transport.Transport(bundle).fetch(
                http, "https://dados.antt.gov.br/data.csv", role="trafego", limit=100, expected=150
            )
        except Exception as exc:
            caught = exc
        else:
            caught = None
    assert isinstance(caught, ResourceLimitError) and "antes do GET" in str(caught), caught
    assert calls == []
