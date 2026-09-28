from __future__ import annotations

import copy
import hashlib

import httpx
import pytest
from pydantic import ValidationError

from agrobr.exceptions import ParseError
from agrobr.incra import _transport, client, query
from tests.helpers import incra_features, install_incra_wfs


@pytest.mark.parametrize(
    "raw,expected", [("MT", 1), (" mt ", 1), ("AMT", 0), (None, 0), ("MT, MT", 0)]
)
async def test_uf_whole_scalar_local_preserves_original(raw, expected, monkeypatch):
    features = incra_features()[:1]
    features[0]["properties"]["sg_uf"] = raw
    calls = install_incra_wfs(monkeypatch, features)
    result = await client.fetch_acquisition(
        query.build_query(include_geometry=False, max_registros=None, uf="MT")
    )
    assert len(result.frame) == expected
    assert all("CQL_FILTER" not in request.url.params for request, _ in calls)
    if expected:
        assert result.frame.iloc[0]["uf"] == raw


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("TITULADO", 1),
        ("TITULADO ", 0),
        ("titulado", 0),
        ("Nova fase publicada", 0),
        (None, 0),
    ],
)
async def test_fase_literal_local(raw, expected, monkeypatch):
    features = incra_features()[:1]
    features[0]["properties"]["ds_fase"] = raw
    install_incra_wfs(monkeypatch, features)
    result = await client.fetch_acquisition(
        query.build_query(include_geometry=False, max_registros=None, fase="TITULADO")
    )
    assert len(result.frame) == expected


@pytest.mark.parametrize(
    "defect", ["property", "signed_zero", "descending", "empty", "only_overlap", "excess", "total"]
)
async def test_second_page_invalid_before_local_filter(defect, monkeypatch):
    features = incra_features()
    features[1]["properties"]["nu_area_ha"] = -0.0

    def transform(request, envelope):
        if request.url.params.get("startIndex") != "1":
            return
        if defect == "property":
            envelope["features"][0]["properties"]["no_comunidade"] = "changed"
        elif defect == "signed_zero":
            envelope["features"][0]["properties"]["nu_area_ha"] = 0.0
        elif defect == "descending":
            envelope["features"][1]["properties"]["cd_quilomb"] = -1
        elif defect == "total":
            envelope.update(numberMatched=999, totalFeatures=999)
        else:
            envelope["features"] = {
                "empty": [],
                "only_overlap": [features[1]],
                "excess": features[1:],
            }[defect]
            envelope["numberReturned"] = len(envelope["features"])

    install_incra_wfs(monkeypatch, features, transform)
    with pytest.raises(ParseError):
        await client.fetch_acquisition(
            query.build_query(include_geometry=False, max_registros=None, tamanho_pagina=2, uf="SP")
        )


async def test_count_after_changed_fails(monkeypatch):
    probes = 0

    def transform(request, envelope):
        nonlocal probes
        if request.url.params.get("count") == "1":
            probes += 1
            if probes == 2:
                envelope.update(numberMatched=7, totalFeatures=7)

    install_incra_wfs(monkeypatch, incra_features(), transform)
    with pytest.raises((ParseError, ValidationError)) as caught:
        await client.fetch_acquisition(
            query.build_query(include_geometry=False, max_registros=None)
        )
    assert caught.type is ParseError


async def test_overlapping_geometry_change_fails(monkeypatch):
    features = incra_features(include_geometry=True) * 2
    features[1] = copy.deepcopy(features[0])
    features[1]["properties"]["cd_quilomb"] += 1

    def transform(request, envelope):
        if request.url.params.get("startIndex") == "0" and request.url.params.get("count") == "2":
            envelope["features"][0]["geometry"]["coordinates"][0][0][1][0] += 0.01

    install_incra_wfs(monkeypatch, features, transform)
    with pytest.raises(ParseError):
        await client.fetch_acquisition(
            query.build_query(include_geometry=True, max_registros=None, tamanho_pagina=1)
        )


async def test_bbox_prefix_disjoint_remains_unknown(monkeypatch):
    features = incra_features()[:2]
    features[0]["geometry"] = {
        "type": "MultiPolygon",
        "coordinates": [[[[2, 2], [3, 2], [3, 3], [2, 2]]]],
    }
    features[1]["geometry"] = {
        "type": "MultiPolygon",
        "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]],
    }
    install_incra_wfs(monkeypatch, features)
    result = await client.fetch_acquisition(
        query.build_query(include_geometry=False, max_registros=1, bbox=(0, 0, 1, 1))
    )
    assert result.frame.empty and result.coverage.remote.accepted_rows == 1
    assert result.coverage.local.status == "unknown"
    assert result.details["accepted_diagnostics"]["bbox_disjoint"]["count"] == 1


async def test_text_order_inversion_is_diagnostic_without_assumed_collation(monkeypatch):
    features = incra_features()[:2]
    features[0]["properties"].update(cd_quilomb=0, nu_processo="z")
    features[1]["properties"].update(cd_quilomb=0, nu_processo="a")
    install_incra_wfs(monkeypatch, features)
    result = await client.fetch_acquisition(
        query.build_query(include_geometry=False, max_registros=None, tamanho_pagina=1)
    )
    assert len(result.frame) == 2
    assert result.diagnostics["text_sort_python_inversion"]["count"] == 1
    assert result.details["sort_text_collation_verified"] is False


class _ReadAndCloseError(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"preserved prefix"
        raise httpx.ReadError("primary read failure")

    async def aclose(self):
        raise httpx.WriteError("secondary close failure")


async def test_transport_read_and_close_errors_preserve_primary_and_receipt():
    async def handle(request):
        return httpx.Response(200, request=request, stream=_ReadAndCloseError())

    transport = _transport.Transport()
    transport.url = "https://example.invalid/incra"
    transport.logical_index = 0
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle),
        event_hooks={"request": [transport.request]},
    ) as http:
        with pytest.raises(httpx.ReadError, match="primary"):
            await transport._attempt(http)
    resource = transport.resources[0]
    assert resource.error_type == "ReadError" and resource.close_error_type == "WriteError"
    assert resource.sha256 == hashlib.sha256(b"preserved prefix").hexdigest()
    assert resource.size_bytes == len(b"preserved prefix") and not resource.complete_body


@pytest.mark.parametrize(
    "defect",
    [
        "missing_matched",
        "missing_both",
        "contradictory",
        "returned",
        "empty",
        "excess",
        "invalid_property",
    ],
)
async def test_count_probe_invalid_before_filter_fails(defect, monkeypatch):
    features = incra_features()

    def transform(_request, envelope):
        if defect == "missing_matched":
            del envelope["numberMatched"]
        elif defect == "missing_both":
            del envelope["numberMatched"]
            del envelope["totalFeatures"]
        elif defect == "contradictory":
            envelope["totalFeatures"] += 1
        elif defect == "returned":
            envelope["numberReturned"] = True
        elif defect == "invalid_property":
            envelope["features"][0]["properties"]["no_comunidade"] = 9
        else:
            envelope["features"] = [] if defect == "empty" else features[:2]
            envelope["numberReturned"] = len(envelope["features"])

    calls = install_incra_wfs(monkeypatch, features, transform)
    with pytest.raises((ParseError, ValidationError)) as caught:
        await client.fetch_acquisition(
            query.build_query(include_geometry=False, max_registros=None, uf="SP")
        )
    assert caught.type is ParseError
    assert len(calls) == 1
