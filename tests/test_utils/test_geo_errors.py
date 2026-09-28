from __future__ import annotations

import json
from unittest.mock import Mock

import httpx
import pytest

from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.http import responses
from agrobr.utils import geo


@pytest.mark.parametrize("indent,prefix", [(None, b""), (2, b" \n"), (4, b"\xef\xbb\xbf")])
async def test_error_envelope_rejected_before_features(indent, prefix):
    raw = (
        prefix
        + json.dumps(
            {"requestId": "abc", "error": {"code": 500, "message": "Unable to perform query"}},
            indent=indent,
        ).encode()
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=raw, request=request)
    )
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(SourceUnavailableError, match="500"):
            await geo.fetch_wfs(
                "https://example.com/query", source="ana", timeout=httpx.Timeout(1), client=http
            )


async def test_feature_collection_nao_e_decodificada_no_preflight(monkeypatch):
    raw = json.dumps(
        {"type": "FeatureCollection", "features": [{"id": i} for i in range(10000)]}
    ).encode()
    loads = Mock(wraps=json.loads)
    monkeypatch.setattr(responses.json, "loads", loads)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=raw))
    ) as http:
        assert (
            await geo.fetch_wfs(
                "https://example.test/query", source="sicar", timeout=httpx.Timeout(1), client=http
            )
            == raw
        )
    loads.assert_not_called()


@pytest.mark.parametrize("key", [b'"error"', b'"\\u0065rror"', b'"e\\u0072r\\u006fr"'])
async def test_erro_arcgis_apos_prefixo_longo_ou_com_escape(key):
    raw = b'{"requestId":"' + b"x" * 2048 + b'",' + key + b':{"code":503,"message":"Offline"}}'
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=raw))
    ) as http:
        with pytest.raises(SourceUnavailableError, match="503"):
            await geo.fetch_wfs(
                "https://example.test/query", source="sicar", timeout=httpx.Timeout(1), client=http
            )


@pytest.mark.parametrize("payload", [{}, [], {"features": None}, {"features": "invalid"}])
def test_malformed_geojson_is_not_empty_success(payload):
    with pytest.raises(ParseError, match="GeoJSON inválido"):
        geo.parse_geojson_base(
            json.dumps(payload).encode(),
            None,
            source="ana",
            parser_version=1,
            required_cols=set(),
            max_features=None,
            output_cols_empty=["geometry"],
            truncation_event="unused",
        )
