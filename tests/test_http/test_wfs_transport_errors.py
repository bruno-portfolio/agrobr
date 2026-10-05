from __future__ import annotations

import gzip
import hashlib
import importlib
from types import SimpleNamespace

import httpx
import pytest

from agrobr import constants
from agrobr.desmatamento import client as desmatamento_client
from agrobr.exceptions import ResourceLimitError, SourceUnavailableError
from agrobr.http import wfs_transport
from tests.helpers import TrackedAsyncStream


@pytest.mark.parametrize("source", ["funai", "incra", "embrapa_solos", "desmatamento"])
async def test_session_preserva_status_e_hash_do_corpo_descomprimido(monkeypatch, source):
    transport = (
        desmatamento_client._TransportContext()
        if source == "desmatamento"
        else importlib.import_module(f"agrobr.{source}._transport").Transport()
    )
    body = b'{"type":"FeatureCollection","features":[]}'
    requested = []

    def respond(request):
        requested.append(request)
        if request.url.path == "/missing":
            return httpx.Response(404, content=b"unavailable")
        return httpx.Response(
            200, content=gzip.compress(body), headers={"content-encoding": "gzip"}
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(respond), **kwargs
    )
    monkeypatch.setattr(wfs_transport, "httpx", namespace)
    async with transport.session() as http:
        assert not http.follow_redirects
        assert await transport.fetch(http, "https://example.test/wfs", "page") == body
        receipt = transport.resources[-1]
        assert receipt.sha256 == hashlib.sha256(body).hexdigest()
        assert receipt.size_bytes == len(body)
        assert "user-agent" in requested[0].headers
        with pytest.raises(SourceUnavailableError, match="HTTP 404"):
            await transport.fetch(http, "https://example.test/missing", "page")
        assert transport.resources[-1].error_type == "HTTPStatusError"
        for method, url in (
            ("GET", "https://example.test/unplanned"),
            ("POST", "https://example.test/missing"),
        ):
            with pytest.raises(SourceUnavailableError, match="fora da seleção"):
                await http.request(method, url)
    assert len(requested) == 2
    assert len(transport.resources) == 2


@pytest.mark.parametrize("source", ["funai", "incra", "embrapa_solos", "desmatamento"])
@pytest.mark.parametrize(
    "body,message",
    [
        (b"\xef\xbb\xbf <html><body>Maintenance</body></html>", "HTML"),
        (b'<!DOCTYPE svg><svg xmlns="http://www.w3.org/2000/svg"/>', "HTML"),
        (
            b"<ogc:ServiceExceptionReport><ogc:ServiceException>Offline</ogc:ServiceException></ogc:ServiceExceptionReport>",
            "Offline",
        ),
        (b'{"requestId":"abc","error":{"code":500,"message":"Offline"}}', "500"),
        (
            b'<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">'
            b'<ows:Exception exceptionCode="NoApplicableCode">'
            b"<ows:ExceptionText>Service offline</ows:ExceptionText>"
            b"</ows:Exception></ows:ExceptionReport>",
            "Service offline",
        ),
    ],
)
async def test_transport_classifica_erro_de_servico_http_200(source, body, message):
    transport = (
        desmatamento_client._TransportContext()
        if source == "desmatamento"
        else importlib.import_module(f"agrobr.{source}._transport").Transport()
    )
    url = "https://example.test/wfs"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=body)),
        event_hooks={"request": [transport.request]},
    ) as http:
        with pytest.raises(SourceUnavailableError, match=message):
            await transport.fetch(http, url, "page")
    receipt = transport.resources[-1]
    assert receipt.status == 200
    assert receipt.complete_body is True
    assert receipt.error_type == "SourceUnavailableError"


@pytest.mark.parametrize("limit", ["DESMATAMENTO_MAX_BODY_BYTES", "DESMATAMENTO_MAX_TOTAL_BYTES"])
async def test_desmatamento_aborta_stream_sem_consumir_corpo_completo(monkeypatch, limit):
    monkeypatch.setattr(constants, limit, 5)
    stream = TrackedAsyncStream([b"1234", b"5678", b"not downloaded"])
    transport = desmatamento_client._TransportContext()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream)),
        event_hooks={"request": [transport.request]},
    ) as http:
        with pytest.raises(ResourceLimitError, match="Limite operacional"):
            await transport.fetch(http, "https://example.test/wfs", "page")
    receipt = transport.resources[-1]
    assert stream.received == 2
    assert stream.closed
    assert receipt.size_bytes == 8
    assert receipt.sha256 == hashlib.sha256(b"12345678").hexdigest()
    assert not receipt.complete_body
    assert receipt.error_type == "ResourceLimitError"
