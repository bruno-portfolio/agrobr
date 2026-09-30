from __future__ import annotations

import copy

import httpx
import pytest

from agrobr import constants
from agrobr.desmatamento import acquisition, client, query
from agrobr.exceptions import ParseError, SourceUnavailableError


@pytest.fixture
def records():
    return [
        {
            "type": "Feature",
            "id": f"synthetic.{index}",
            "geometry": None,
            "properties": {
                "uuid": f"synthetic-{index}",
                "fid": index,
                "state": "PA",
                "path_row": "023007",
                "main_class": "DESMATAMENTO",
                "class_name": "d2022",
                "def_cloud": None,
                "julian_day": 205,
                "image_date": "2022-07-24",
                "year": 2022,
                "area_km": 0.125,
                "scene_id": 1,
                "publish_year": "2022-01-01",
                "source": "Synthetic",
                "satellite": "Landsat",
                "sensor": "OLI",
            },
        }
        for index in range(1, 7)
    ]


@pytest.fixture
def selection():
    return query.build_query(
        product="PRODES",
        include_geometry=False,
        bioma="Amazônia",
        max_registros=None,
        tamanho_pagina=2,
    )


@pytest.fixture
def deter_records():
    return [
        {
            "type": "Feature",
            "id": "synthetic.repeated",
            "geometry": None,
            "properties": {
                "gid": "repeated_hist",
                "classname": " D'ÁGUA ",
                "quadrant": "",
                "path_row": "023007",
                "view_date": published_date,
                "sensor": None,
                "satellite": None,
                "areauckm": None,
                "uc": None,
                "areamunkm": 0.125,
                "municipality": "Synthetic",
                "uf": "PA",
                "publish_month": None,
                "mun_geocod": "0150000",
            },
        }
        for published_date in ["2018-01-01", "2023-01-01"]
    ]


@pytest.fixture
def serve(monkeypatch):
    original = httpx.AsyncClient

    def install(records, transform=None):
        calls = []

        def respond(request):
            params = request.url.params
            calls.append(request)
            selected = (
                []
                if params.get("resultType") == "hits"
                else records[
                    int(params["startIndex"]) : int(params["startIndex"]) + int(params["count"])
                ]
            )
            payload = {
                "type": "FeatureCollection",
                "features": copy.deepcopy(selected),
                "numberMatched": len(records),
                "numberReturned": len(selected),
                "totalFeatures": len(records),
                "crs": None,
            }
            if transform is not None:
                changed = transform(request, payload, len(calls))
                if isinstance(changed, httpx.Response):
                    return changed
            return httpx.Response(
                200, json=payload, headers={"etag": "synthetic-version"}, request=request
            )

        monkeypatch.setattr(
            client.httpx,
            "AsyncClient",
            lambda **kwargs: original(transport=httpx.MockTransport(respond), **kwargs),
        )
        return calls

    return install


@pytest.mark.asyncio
async def test_acquisition_preserves_indistinguishable_duplicates(records, selection, serve):
    serve([copy.deepcopy(records[0]) for _ in range(6)])
    result = await client.fetch_acquisition(selection)
    assert len(result.frame) == 6 and result.frame["feature_id"].nunique() == 1
    assert "repeated_indistinguishable_window" in {
        item["kind"] for item in result.coverage.ambiguities
    }
    assert not result.coverage.semantic_progress_proven
    assert any("indistinguíveis" in aviso for aviso in result.warnings)


@pytest.mark.asyncio
async def test_acquisition_indistinguishable_window_without_repeated_page(
    records, selection, serve
):
    serve([copy.deepcopy(records[0]) for _ in range(3)])
    result = await client.fetch_acquisition(selection.model_copy(update={"page_size": 1}))
    assert len(result.frame) == 3
    assert any(
        item["kind"] == "indistinguishable_occurrences_in_window"
        for item in result.coverage.ambiguities
    )


@pytest.mark.asyncio
async def test_acquisition_local_limit_partial(records, selection, serve):
    serve(records)
    result = await client.fetch_acquisition(selection.model_copy(update={"max_records": 3}))
    assert result.frame["fid"].tolist() == [1, 2, 3]
    assert result.coverage.truncated and result.coverage.status == "partial"
    assert result.coverage.expected_rows == 6 and result.coverage.returned_rows == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "boundary_changed",
        "empty_page",
        "wrong_total",
        "no_progress",
        "wrong_filter",
        "hits_changed",
    ],
)
async def test_acquisition_rejects_inconsistent_block(mode, records, selection, serve):
    def transform(_request, payload, number):
        if mode == "hits_changed" and number == 5:
            payload["numberMatched"] = payload["totalFeatures"] = 7
        if number == 3:
            if mode == "boundary_changed":
                payload["features"][0]["properties"]["area_km"] = 99
            elif mode in {"empty_page", "no_progress"}:
                payload["features"] = payload["features"][: 0 if mode == "empty_page" else 1]
                payload["numberReturned"] = len(payload["features"])
            elif mode == "wrong_total":
                payload["numberMatched"] = payload["totalFeatures"] = 7
            elif mode == "wrong_filter":
                payload["features"][1]["properties"]["state"] = "MT"

    serve(records, transform)
    with pytest.raises(ParseError):
        await client.fetch_acquisition(selection.model_copy(update={"uf": "PA"}))


@pytest.mark.asyncio
async def test_acquisition_deter_literal_filter_and_duplicate_id(deter_records, serve):
    calls = serve(deter_records)
    selected = query.build_query(
        product="DETER",
        include_geometry=False,
        bioma="Amazônia",
        max_registros=None,
        classe=" D'ÁGUA ",
        uf="PA",
        inicio="2018-01-01",
        fim="2023-01-01",
        tamanho_pagina=1,
    )
    result = await client.fetch_acquisition(selected)
    assert result.frame["feature_id"].tolist() == ["synthetic.repeated"] * 2
    assert len(result.frame.columns) == 19
    assert all("classname=' D''ÁGUA '" in call.url.params["CQL_FILTER"] for call in calls)
    assert result.query.class_name == " D'ÁGUA "


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [("classname", "D'ÁGUA"), ("view_date", "2017-12-31"), ("view_date", None), ("uf", "AP")],
)
async def test_acquisition_deter_filter_membership(field, value, deter_records, serve):
    deter_records[0]["properties"][field] = value
    serve(deter_records)
    selected = query.build_query(
        product="DETER",
        include_geometry=False,
        bioma="Amazônia",
        max_registros=None,
        classe=" D'ÁGUA ",
        uf="PA",
        inicio="2018-01-01",
    )
    with pytest.raises(ParseError, match="contradiz filtro"):
        await client.fetch_acquisition(selected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constant,limit",
    [
        ("DESMATAMENTO_MAX_BODY_BYTES", 50),
        ("DESMATAMENTO_MAX_TOTAL_BYTES", 200),
        ("DESMATAMENTO_MAX_RETAINED_BYTES", 1),
        ("DESMATAMENTO_MAX_PAGES", 1),
    ],
)
async def test_acquisition_operational_limit_aborts(
    constant, limit, records, selection, serve, monkeypatch
):
    serve(records)
    monkeypatch.setattr(constants, constant, limit)
    with pytest.raises(SourceUnavailableError, match="Limite operacional"):
        await client.fetch_acquisition(selection)


@pytest.mark.parametrize(
    "content",
    [
        b'{"type":"FeatureCollection","features":[],"numberMatched":true,"numberReturned":0}',
        b'{"type":"FeatureCollection","features":[],"numberMatched":1,"numberReturned":0,"totalFeatures":2}',
        b'<FeatureCollection xmlns="urn:wrong" numberMatched="1" numberReturned="0"/>',
        (
            '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE FeatureCollection [<!ENTITY n "1">]>'
            '<FeatureCollection xmlns="http://www.opengis.net/wfs/2.0" numberMatched="&n;" numberReturned="0"/>'
        ).encode("utf-16"),
    ],
)
def test_hits_rejects_invalid_envelope(content):
    with pytest.raises(ParseError):
        acquisition.parse_hits(content)
