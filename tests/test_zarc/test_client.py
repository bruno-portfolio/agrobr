from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from agrobr import constants
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.zarc import api, client
from tests.helpers import TrackedAsyncStream, levanta_exatamente


@pytest.mark.parametrize(
    "csv,limit",
    [
        (True, "ZARC_MAX_DOWNLOAD_BYTES"),
        (False, "ZARC_MAX_CATALOG_BYTES"),
        (True, "ZARC_MAX_TRANSFER_BYTES"),
    ],
)
async def test_orcamento_aborta_antes_do_resto_do_stream(monkeypatch, csv, limit):
    monkeypatch.setattr(constants, limit, 5)
    stream = TrackedAsyncStream([b"1234", b"5678", b"not downloaded"])
    original = httpx.AsyncClient
    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: original(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream)), **kwargs
        ),
    )
    with levanta_exatamente(ResourceLimitError, match="orçamento"):
        await client._get("https://dados.agricultura.gov.br/resource", csv=csv)
    assert stream.received == 2
    assert stream.closed


@pytest.mark.asyncio
async def test_discover_legacy_resources_keeps_four_fields(zarc_replay):
    resources = await client.discover_resources()
    assert len(resources) == 3
    assert set(resources[0]) == {"id", "name", "url", "format"}
    assert len(zarc_replay["requests"]) == 1


@pytest.mark.asyncio
async def test_typed_catalog_retains_announced_revision(zarc_replay):
    listing = await client.discover_catalog()
    safra, resource = listing.select(None)
    assert safra == "2026/2027"
    assert resource.last_modified == "2026-09-07T00:00:00"
    assert listing.acquisition.sha256
    assert len(zarc_replay["requests"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"success": False, "result": {"resources": []}},
        {"success": "true", "result": {"resources": []}},
        {"success": True, "result": {"resources": {}}},
        {"success": True, "result": {"resources": [{"id": 1}]}},
    ],
)
async def test_invalid_catalog_is_parse_error(zarc_replay, payload):
    zarc_replay["catalog"] = payload
    with pytest.raises(ParseError):
        await client.discover_catalog()


@pytest.mark.asyncio
async def test_official_catalog_types_are_accepted(zarc_replay):
    path = Path(__file__).parents[1] / "golden_data/zarc/selecao_20260907/catalog.json"
    zarc_replay["catalog"] = json.loads(path.read_bytes())
    listing = await client.discover_catalog()
    assert listing.select("2016/2017")[0] == "2016/2017"
    assert listing.select("2026/2027")[0] == "2026/2027"
    assert listing.select("perene")[0] == "perene"


@pytest.mark.asyncio
async def test_duplicate_safra_is_not_first_match(zarc_replay):
    resources = zarc_replay["catalog"]["result"]["resources"]
    duplicate = copy.deepcopy(resources[1])
    duplicate["id"] = "other"
    resources.append(duplicate)
    listing = await client.discover_catalog()
    with pytest.raises(ParseError, match="ambígu"):
        listing.select("2026/2027")
    assert len(zarc_replay["requests"]) == 1


@pytest.mark.asyncio
async def test_unavailable_safra_never_downloads(zarc_replay):
    with pytest.raises(InvalidParameterError, match="não encontrada"):
        await api.zoneamento(safra="2000/2001", use_cache=False)
    assert len(zarc_replay["requests"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("safra", [True, "2026/2028"])
async def test_invalid_safra_precedes_http(zarc_replay, safra):
    with pytest.raises(InvalidParameterError):
        await api.zoneamento(safra=safra, use_cache=False)
    assert not zarc_replay["requests"]


@pytest.mark.asyncio
async def test_download_legacy_wrapper_preserves_bytes(zarc_replay):
    expected = zarc_replay["bodies"]["2016_2017"]
    assert await client.download_csv("https://dados.agricultura.gov.br/2016_2017.csv") == expected


@pytest.mark.asyncio
async def test_download_acquisition_uses_csv_hash(zarc_replay):
    captured = await client.download_acquisition("https://dados.agricultura.gov.br/perene.csv")
    assert captured.resource.sha256 == hashlib.sha256(captured.content).hexdigest()
    assert captured.resource.size_bytes == len(captured.content)
    assert captured.resource.headers["etag"] == "replayed"
    assert len(zarc_replay["requests"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"tiny", b"<!DOCTYPE html><html>" + b"wrong" * 100])
async def test_invalid_download_is_not_a_csv(zarc_replay, body):
    zarc_replay["bodies"]["perene"] = body
    with pytest.raises(SourceUnavailableError):
        await client.download_csv("https://dados.agricultura.gov.br/perene.csv")
