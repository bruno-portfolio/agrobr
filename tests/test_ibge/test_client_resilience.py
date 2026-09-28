from __future__ import annotations

import asyncio
import time
from functools import partial
from unittest.mock import patch

import httpx
import pytest

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import client
from agrobr.sync import run_sync


@pytest.fixture
def sidra_http():
    with patch.object(client.httpx, "AsyncClient") as factory:
        http = factory.return_value.__aenter__.return_value
        http.get.return_value = httpx.Response(
            200,
            json=[{"V": "100"}],
            request=httpx.Request("GET", "https://example.test"),
        )
        yield http


@pytest.mark.parametrize("status, attempts", [(500, 3), (429, 3), (403, 1), (404, 1)])
async def test_http_failure(sidra_http, status, attempts):
    sidra_http.get.return_value = httpx.Response(
        status,
        request=httpx.Request("GET", "https://example.test"),
    )
    with pytest.raises(SourceUnavailableError, match=str(status)):
        await client.fetch_sidra("5457")
    sidra_calls = [
        call for call in sidra_http.get.await_args_list if "apisidra" in str(call.args[0])
    ]
    assert len(sidra_calls) == attempts


@pytest.mark.parametrize("data", [{"error": "indisponível"}, ["invalid row"]])
async def test_invalid_json_shape_rejected(sidra_http, data):
    sidra_http.get.return_value = httpx.Response(
        200,
        json=data,
        request=httpx.Request("GET", "https://example.test"),
    )
    with pytest.raises(SourceUnavailableError, match="lista de registros"):
        await client.fetch_sidra("5457")


async def test_dimensions_and_list_parameters(sidra_http):
    await client.fetch_sidra(
        "5457",
        territorial_level="6",
        ibge_territorial_code="in n3 11",
        variable=["214", "215"],
        period=["2022", "2023"],
        classifications={"782": ["40122", "40124"]},
    )
    url = sidra_http.get.call_args.args[0]
    assert url.endswith("/t/5457/n6/in%20n3%2011/h/n/p/2022,2023/v/214,215/c782/40122,40124")


@pytest.mark.benchmark
def test_sync_timeout_does_not_wait_for_background_thread(sidra_http, monkeypatch):
    async def stalled_request(*_args, **_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "SIDRA_FETCH_TIMEOUT", 0.02)
    monkeypatch.setattr(client, "retry_async", partial(client.retry_async, max_attempts=1))
    client.RateLimiter.reset()
    sidra_http.get.side_effect = stalled_request
    started = time.monotonic()
    with pytest.raises(SourceUnavailableError, match="TimeoutError"):
        run_sync(client.fetch_sidra("5457"))
    assert time.monotonic() - started < 1


@pytest.mark.parametrize("error", [httpx.ReadTimeout("timeout"), httpx.ConnectError("connection")])
async def test_network_failure_retried(sidra_http, error):
    sidra_http.get.side_effect = [error, sidra_http.get.return_value]
    df = await client.fetch_sidra("5457")
    assert df["V"].tolist() == ["100"]
    assert sidra_http.get.await_count == 2


async def test_network_failure_exhausted_keeps_network_cause(sidra_http):
    sidra_http.get.side_effect = httpx.ConnectError("connection")
    with pytest.raises(SourceUnavailableError, match="ConnectError: connection") as error:
        await client.fetch_sidra("5457")
    assert isinstance(error.value.__cause__, httpx.ConnectError)
    assert all("apisidra" in str(call.args[0]) for call in sidra_http.get.await_args_list)


async def test_non_json_response_retried(sidra_http):
    sidra_http.get.return_value = httpx.Response(
        200,
        text="Service Unavailable",
        request=httpx.Request("GET", "https://example.test"),
    )
    with pytest.raises(SourceUnavailableError, match="Service Unavailable"):
        await client.fetch_sidra("5457")
    sidra_calls = [
        call for call in sidra_http.get.await_args_list if "apisidra" in str(call.args[0])
    ]
    assert len(sidra_calls) == constants.HTTPSettings().max_retries
