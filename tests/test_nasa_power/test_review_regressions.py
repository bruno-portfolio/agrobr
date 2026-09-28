from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.nasa_power import api, client, output, parser, provenance
from tests.helpers import RETRY_SLEEP


@pytest.mark.parametrize(
    "raw",
    [
        b'{"properties":{},"properties":{}}',
        b'{"properties":{"parameter":{"PS":{"20240101":97,"20240101":99}}}}',
        b'{"properties":{"parameter":{"PS":{},"PS":{}}}}',
        b'{"a":[{"b":1,"b":2}]}',
    ],
)
async def test_duplicate_json_keys_rejected(raw):
    response = httpx.Response(200, content=raw, request=httpx.Request("GET", client.BASE_URL))
    http = AsyncMock()
    http.get.return_value = response
    with pytest.raises(ParseError, match="duplicada"):
        await client._get_json({}, http=http)


async def test_optional_absence_before_acquisition():
    with (
        patch.object(output.importlib, "import_module", side_effect=ImportError),
        patch.object(client, "fetch_daily", new_callable=AsyncMock) as fetch,
        pytest.raises(ImportError, match="polars"),
    ):
        await api.clima_ponto(0, 0, "2024-01-01", "2024-01-02", as_polars=True)
    fetch.assert_not_awaited()


@pytest.mark.parametrize("kwargs", [{"return_meta": 1}, {"as_polars": "true"}, {"unknown": True}])
async def test_uf_invalid_options_before_acquisition(kwargs):
    with (
        patch.object(client, "fetch_daily", new_callable=AsyncMock) as fetch,
        pytest.raises(InvalidParameterError),
    ):
        await api.clima_uf("MT", 2024, **kwargs)
    fetch.assert_not_awaited()


async def test_api_rejects_dates_outside_request():
    data = {
        "properties": {"parameter": {"PS": {"20000101": 97}}},
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {"PS": {"units": "kPa"}},
    }
    with (
        patch.object(client, "fetch_daily", new_callable=AsyncMock, return_value=data),
        pytest.raises(ParseError, match="fora do intervalo"),
    ):
        await api.clima_ponto(0, 0, "2024-01-01", "2024-01-02", parameters=["PS"])


async def test_receipts_retries_hashes_and_acquisition_before_parse():
    raw = (
        b'{"properties":{"parameter":{"PS":{"20240101":97}}},'
        b'"header":{"fill_value":-999,"time_standard":"LST"},'
        b'"parameters":{"PS":{"units":"kPa"}}}'
    )
    request = httpx.Request("GET", client.BASE_URL)
    http = AsyncMock()
    http.get.side_effect = [
        httpx.Response(503, content=b"unavailable", request=request),
        httpx.Response(200, content=raw, request=request),
    ]
    with patch(RETRY_SLEEP, new_callable=AsyncMock):
        result = await client._get_json({"parameters": "PS"}, http=http)
    assert isinstance(result, provenance.FetchResult)
    assert len(result.receipts) == 2
    assert result.receipts[0].status == 503
    assert result.receipts[1].sha256 == hashlib.sha256(raw).hexdigest()
    assert "parameters=PS" in result.receipts[1].request_url
    assert result.receipts[1].size_bytes == len(raw)
    parse_times = []
    original = parser.parse_daily

    def parse(*args, **kwargs):
        parse_times.append(datetime.now(UTC))
        return original(*args, **kwargs)

    with (
        patch.object(client, "fetch_daily", new_callable=AsyncMock, return_value=result),
        patch.object(parser, "parse_daily", side_effect=parse),
    ):
        _, meta = await api.clima_ponto(
            0, 0, "2024-01-01", "2024-01-02", parameters=["PS"], return_meta=True
        )
    assert all(
        getattr(meta, key).tzinfo is not None
        for key in ("fetched_at", "fetch_timestamp", "timestamp")
    )
    assert meta.fetched_at <= parse_times[0] <= meta.timestamp
    manifest = json.dumps(
        meta.source_details["http_receipts"],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    assert meta.raw_content_hash == hashlib.sha256(manifest).hexdigest()
    assert meta.raw_content_size == len(raw) + len(b"unavailable")
    assert meta.source_url == str(request.url)
    assert meta.source_details["source_url_scope"] == "response"


def test_polars_bridge_explicit_types_without_arrow():
    polars = pytest.importorskip("polars")
    data = {
        "properties": {"parameter": {"PS": {"20240101": None}}},
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {"PS": {"units": "kPa"}},
    }
    frame = parser.parse_daily(data, 0, 0, parameters=["PS"])
    with patch.object(polars, "from_pandas", side_effect=AssertionError("Arrow bridge forbidden")):
        result = output.finalize(frame, None, polars, False)
    assert result.schema["uf"] == polars.Utf8
    assert result.schema["ps_kpa"] == polars.Float64
    assert result["ps_kpa"].to_list() == [None]


async def test_real_transport_receipts_preserved_across_blocks():
    http = AsyncMock()
    http.__aenter__.return_value = http

    async def response(url, params):
        request = httpx.Request("GET", url, params=params)
        return httpx.Response(
            200,
            request=request,
            json={
                "properties": {"parameter": {"PS": {params["start"]: 97}}},
                "header": {"fill_value": -999.0, "time_standard": "LST"},
                "parameters": {"PS": {"units": "kPa"}},
            },
        )

    http.get.side_effect = response
    with patch.object(client.httpx, "AsyncClient", return_value=http):
        result = await client.fetch_daily(
            0, 0, date(2023, 1, 1), date(2024, 1, 2), parameters=["PS"]
        )
    assert len(result.receipts) == 2
    assert "start=20230101&end=20231231" in result.receipts[0].request_url
    assert "start=20240101&end=20240102" in result.receipts[1].request_url
    assert result.receipts[0].acquired_at <= result.receipts[1].acquired_at
    assert set(result["properties"]["parameter"]["PS"]) == {"20230101", "20240101"}
    with patch.object(client, "fetch_daily", new_callable=AsyncMock, return_value=result):
        _, meta = await api.clima_ponto(
            0, 0, "2023-01-01", "2024-01-02", parameters=["PS"], return_meta=True
        )
    assert meta.source_url == result.receipts[0].effective_url
    assert meta.source_details["source_url_scope"] == "first_response_block"
    assert meta.source_details["response_blocks"] == 2
    assert "end=20240102" in meta.source_details["logical_source_url"]


@pytest.mark.parametrize(
    "missing", ["header", "fill_value", "time_standard", "parameters", "PS", "units"]
)
def test_required_remote_metadata_absence_rejected(missing):
    data = {
        "properties": {"parameter": {"PS": {"20240101": 97}}},
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {"PS": {"units": "kPa"}},
    }
    if missing in {"header", "parameters"}:
        del data[missing]
    elif missing in {"fill_value", "time_standard"}:
        del data["header"][missing]
    elif missing == "PS":
        del data["parameters"][missing]
    else:
        del data["parameters"]["PS"][missing]
    with pytest.raises(ParseError):
        parser.parse_daily(data, 0, 0, parameters=["PS"])


async def test_network_retry_receipt_does_not_invent_body():
    request = httpx.Request("GET", client.BASE_URL)
    http = AsyncMock()
    http.get.side_effect = [
        httpx.TimeoutException("timeout"),
        httpx.Response(
            200,
            request=request,
            json={
                "properties": {"parameter": {"PS": {"20240101": 97}}},
                "header": {"fill_value": -999.0, "time_standard": "LST"},
                "parameters": {"PS": {"units": "kPa"}},
            },
        ),
    ]
    with patch(RETRY_SLEEP, new_callable=AsyncMock):
        result = await client._get_json({"parameters": "PS"}, http=http)
    assert result.receipts[0].error == "TimeoutException"
    assert result.receipts[0].sha256 is None
    assert result.receipts[0].size_bytes == 0
    assert result.receipts[1].status == 200
