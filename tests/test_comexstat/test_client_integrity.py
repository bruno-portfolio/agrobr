from __future__ import annotations

import hashlib
import io
from collections.abc import AsyncIterator

import httpx
import pytest

from agrobr import constants
from agrobr.comexstat import client, transport_models
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError


class Stream(httpx.AsyncByteStream):
    def __init__(self, body: bytes):
        self.body = body
        self.read = False
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.read = True
        yield self.body

    async def aclose(self):
        self.closed = True


async def receive(stream, *, headers=None, status=200, limit=100):
    url = constants.COMEXSTAT_DICTIONARY_URLS["vias"]
    bundle = transport_models.DownloadedResource(
        transport_models.ResourceSpec(kind="dictionary", url=url, tabela="vias"), _file=io.BytesIO()
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(status, headers=headers, stream=stream)
        )
    ) as http:
        try:
            await client._attempt(http, bundle, url, 1, limit)
        except (SourceUnavailableError, ResourceLimitError) as exc:
            exc.bundle = bundle
            raise
    return bundle


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers,status,error",
    [
        ({"content-encoding": "gzip"}, 200, SourceUnavailableError),
        ({"content-length": "101"}, 200, ResourceLimitError),
        ({"content-length": "1.0"}, 200, SourceUnavailableError),
        ({"content-length": "9" * 5000}, 200, ResourceLimitError),
        ({"content-range": "bytes 0-7/8"}, 200, SourceUnavailableError),
        ({}, 206, SourceUnavailableError),
    ],
)
async def test_invalid_headers_rejected_before_read(headers, status, error):
    stream = Stream(b"A;B\n1;2\n")
    with pytest.raises(error):
        await receive(stream, headers=headers, status=status)
    assert not stream.read and stream.closed


@pytest.mark.asyncio
async def test_content_length_mismatch_after_eof():
    stream = Stream(b"A;B\n1;2\n")
    with pytest.raises(SourceUnavailableError, match="diverge") as error:
        await receive(stream, headers={"content-length": "7"})
    receipt = error.value.bundle.receipts[0]
    assert receipt.complete_body and receipt.closed
    assert receipt.size_bytes == 8 and receipt.sha256 == hashlib.sha256(stream.body).hexdigest()


@pytest.mark.asyncio
async def test_transfer_budget_includes_prior_attempts(monkeypatch):
    monkeypatch.setattr(constants, "COMEXSTAT_MAX_TRANSFER_BYTES", 15)
    stream = Stream(b"A;B\n1;2\n")
    url = constants.COMEXSTAT_DICTIONARY_URLS["vias"]
    bundle = transport_models.DownloadedResource(
        transport_models.ResourceSpec(kind="dictionary", url=url),
        _file=io.BytesIO(),
        transfer_bytes=8,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream))
    ) as http:
        with pytest.raises(ResourceLimitError):
            await client._attempt(http, bundle, url, 2, 100)
    assert bundle.transfer_bytes == 16 and bundle.receipts[0].size_bytes == 0


@pytest.mark.asyncio
async def test_spool_short_write_preserves_received_and_saved_hashes():
    class ShortWriter(io.BytesIO):
        def write(self, body):
            return super().write(body[:3])

    url = constants.COMEXSTAT_DICTIONARY_URLS["vias"]
    bundle = transport_models.DownloadedResource(
        transport_models.ResourceSpec(kind="dictionary", url=url), _file=ShortWriter()
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream(b"A;B\n1;2\n")))
    ) as http:
        with pytest.raises(SourceUnavailableError, match="Gravação"):
            await client._attempt(http, bundle, url, 1, 100)
    assert bundle.receipts[0].received_bytes == 8
    assert bundle.receipts[0].size_bytes == 3
    assert bundle.receipts[0].saved_sha256 == hashlib.sha256(b"A;B").hexdigest()


@pytest.mark.asyncio
async def test_both_cleanup_errors_are_retained():
    class BadFile(io.BytesIO):
        def close(self):
            raise OSError("spool-close")

    class BadClient:
        async def aclose(self):
            raise httpx.CloseError("client-close")

    url = constants.COMEXSTAT_DICTIONARY_URLS["vias"]
    bundle = transport_models.DownloadedResource(
        transport_models.ResourceSpec(kind="dictionary", url=url), _file=BadFile()
    )
    failure = await client._close(bundle, BadClient())
    assert str(failure) == "client-close"
    assert [item.scope for item in bundle.close_errors] == ["client", "spool"]
    assert bundle.client_closed is False and bundle.spool_closed is False
    io.BytesIO.close(bundle.file)
