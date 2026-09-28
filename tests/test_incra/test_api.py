from __future__ import annotations

import copy
import hashlib
import json
import warnings
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from agrobr import deterministic
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ParseError
from agrobr.incra import api, client
from tests.helpers import INCRA_EXPECTED_ALIASES, incra_features, install_incra_wfs
from tests.test_incra import replay

ALIASES = INCRA_EXPECTED_ALIASES


@pytest.mark.parametrize(
    "selection", [{}, {"uf": "ba"}, {"fase": "TITULADO"}, {"uf": "MA", "fase": "RTID"}]
)
async def test_incra_national_replay_all_cells_and_selection(monkeypatch, selection):
    calls = replay.install_national_wfs(monkeypatch)
    frame = await api.quilombolas(**selection)
    expected = [
        feature
        for feature in replay.national_features()
        if (
            "uf" not in selection
            or (feature["properties"]["sg_uf"] or "").strip().upper() == selection["uf"].upper()
        )
        and ("fase" not in selection or feature["properties"]["ds_fase"] == selection["fase"])
    ]
    assert [sent for sent, _ in calls] == [recorded for _, recorded in calls]
    assert len(calls) == 4 and len(replay.national_features()) == 444
    assert list(frame) == list(ALIASES) and len(frame) == len(expected) > 0
    for column, raw in ALIASES.items():
        published = [
            feature["id"] if raw is None else feature["properties"][raw] for feature in expected
        ]
        observed = [None if pd.isna(value) else value for value in frame[column]]
        assert observed == published, column


async def test_incra_manifest_origin_and_full_coverage(monkeypatch):
    calls = install_incra_wfs(monkeypatch, incra_features())
    frame, meta = await api.quilombolas(max_registros=None, tamanho_pagina=2, return_meta=True)
    assert len(frame) == 6 and len(calls) == 5
    assert meta.source == "incra" and meta.selected_source == "incra_geoserver"
    assert meta.attempted_sources == [meta.selected_source]
    assert meta.schema_version == meta.contract_version == "2.0" and meta.parser_version == 2
    assert meta.records_count == len(frame) and meta.columns == list(ALIASES)
    assert meta.snapshot is None and not meta.from_cache
    assert meta.fetched_at.utcoffset().total_seconds() == 0
    assert meta.fetch_timestamp == meta.fetched_at
    details = meta.source_details
    remote = details["coverage"]["remote"]
    assert remote["expected_before"] == remote["expected_after"] == remote["accepted_rows"] == 6
    assert remote["overlap_rows"] == 2 and remote["status"] == "reconciled"
    assert details["coverage"]["local"]["returned_rows"] == 6
    manifest = json.dumps(
        {key: details[key] for key in ("query", "resources", "pages", "count_checks")},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    assert details["manifest_fields"] == ["query", "resources", "pages", "count_checks"]
    assert meta.raw_content_hash == hashlib.sha256(manifest).hexdigest()
    assert meta.raw_content_size == len(manifest)
    assert details["resource_bytes"] == sum(len(body) for _, body in calls)
    assert [resource["role"] for resource in details["resources"]] == [
        "count_before",
        "page",
        "page",
        "page",
        "count_after",
    ]
    assert [check["resource_index"] for check in details["count_checks"]] == [0, 4]
    for check in details["count_checks"]:
        assert check["reported_total"] == 6
        assert check["received_rows"] == check["validated_rows"] == 1
        request, body = calls[check["resource_index"]]
        assert request.url.params["count"] == "1"
        assert request.url.params["startIndex"] == "0"
        assert "resultType" not in request.url.params
        assert check["source_timestamp"] == json.loads(body)["timeStamp"]
    for resource, (request, body) in zip(details["resources"], calls, strict=True):
        assert resource["url"] == str(request.url) and resource["status"] == 200
        assert resource["sha256"] == hashlib.sha256(body).hexdigest()
        assert resource["size_bytes"] == len(body) and resource["complete_body"]
    assert json.loads(meta.to_json())["source_details"] == details
    assert meta.fetched_at == max(
        datetime.fromisoformat(resource["fetched_at"]) for resource in details["resources"]
    )


async def test_incra_partial_prefix_empty_filter_unknown(monkeypatch):
    calls = install_incra_wfs(monkeypatch, incra_features())
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame, meta = await api.quilombolas(uf="MT", max_registros=1, return_meta=True)
    assert any("prefixo remoto de 1 de 6" in str(item.message) for item in caught)
    assert frame.empty and len(frame.columns) == 22 and len(calls) == 3
    assert meta.source_details["coverage"]["remote"]["truncated"]
    assert meta.source_details["coverage"]["local"]["status"] == "unknown"


@pytest.mark.parametrize("function", ["quilombolas", "quilombolas_geo"])
@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"unknown": 1}, TypeError),
        ({"return_meta": 1}, InvalidParameterError),
        ({"max_registros": True}, InvalidParameterError),
        ({"max_registros": 0}, InvalidParameterError),
        ({"max_registros": 1.5}, InvalidParameterError),
        ({"tamanho_pagina": False}, InvalidParameterError),
        ({"tamanho_pagina": 0}, InvalidParameterError),
        ({"uf": []}, InvalidParameterError),
        ({"uf": "XX"}, InvalidParameterError),
        ({"fase": "regularizada"}, InvalidParameterError),
        ({"fase": []}, InvalidParameterError),
        ({"bbox": (0, 0, 0, 1)}, InvalidParameterError),
        ({"bbox": (False, 0, 1, 1)}, InvalidParameterError),
        ({"bbox": (0, 0, float("nan"), 1)}, InvalidParameterError),
    ],
)
async def test_incra_invalid_query_precedes_optional_and_http(monkeypatch, function, kwargs, error):
    fetch = AsyncMock(side_effect=AssertionError("HTTP before guard"))
    optional = Mock(side_effect=AssertionError("optional before guard"))
    monkeypatch.setattr(client, "fetch_acquisition", fetch)
    monkeypatch.setattr(api.geo, "check_geopandas", optional)
    with pytest.raises(error):
        await getattr(api, function)(**kwargs)
    fetch.assert_not_called()
    optional.assert_not_called()


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("empty", [False, True])
async def test_incra_contract_mandatory_without_meta_and_when_empty(
    monkeypatch, return_meta, empty
):
    install_incra_wfs(monkeypatch, [] if empty else incra_features())
    fetch = client.fetch_acquisition

    async def invalid_frame(query):
        acquired = await fetch(query)
        acquired.frame["area_ha"] = acquired.frame["area_ha"].astype(object)
        return acquired

    monkeypatch.setattr(client, "fetch_acquisition", invalid_frame)
    with pytest.raises(ContractViolationError):
        await api.quilombolas(return_meta=return_meta)


async def test_incra_geo_attributes_geometry_crs_preserved(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    shapes = pytest.importorskip("shapely.geometry")
    features = incra_features(include_geometry=True)
    second = copy.deepcopy(features[0])
    second["id"] = "lim_quilombolas_a.998"
    second["properties"]["cd_quilomb"] += 1
    second["geometry"]["coordinates"] = [
        [[[x + 1, y - 2] for x, y in ring] for ring in polygon]
        for polygon in second["geometry"]["coordinates"]
    ]
    features.append(second)
    install_incra_wfs(monkeypatch, features)
    frame, meta = await api.quilombolas_geo(return_meta=True)
    assert isinstance(frame, gpd.GeoDataFrame) and frame.crs.to_epsg() == 4326
    assert list(frame) == [*ALIASES, "geometry"] and len(frame) == 2
    for feature in features:
        row = frame[frame["feature_id"] == feature["id"]]
        assert row.geometry.iloc[0].equals_exact(shapes.shape(feature["geometry"]), 0)
        assert row["data_cadastro"].iloc[0] == feature["properties"]["dt_cadastro"]
    assert meta.records_count == 2 and meta.selected_source == "incra_geoserver_geo"
    assert meta.source_details["geometry"]["declared_crs_verified"]


async def test_incra_geo_empty_schema_without_observed_crs(monkeypatch):
    pytest.importorskip("geopandas")
    install_incra_wfs(monkeypatch, [])
    frame, meta = await api.quilombolas_geo(return_meta=True)
    assert frame.empty and list(frame) == [*ALIASES, "geometry"]
    assert frame.crs.to_epsg() == 4326
    assert not meta.source_details["geometry"]["declared_crs_verified"]
    assert meta.source_details["geometry"]["crs_basis"] == "requested_crs_for_empty_frame"


@pytest.mark.parametrize(
    "bbox,expected_count",
    [
        ((-40.43524899581493, -17.1019747831612, -40.435048995814924, -17.1017747831612), 1),
        ((-40.40454173938971, -17.1019747831612, -40.4043417393897, -17.1017747831612), 0),
    ],
)
async def test_incra_same_bbox_tabular_geo_have_same_selection(monkeypatch, bbox, expected_count):
    pytest.importorskip("geopandas")
    calls = install_incra_wfs(
        monkeypatch, incra_features(include_geometry=True), volatile_ids=False
    )
    tabular, tab_meta = await api.quilombolas(bbox=bbox, max_registros=None, return_meta=True)
    geographic, geo_meta = await api.quilombolas_geo(
        bbox=bbox, max_registros=None, return_meta=True
    )
    pd.testing.assert_frame_equal(tabular, pd.DataFrame(geographic.drop(columns="geometry")))
    assert len(tabular) == expected_count
    assert tab_meta.source_details["coverage"] == geo_meta.source_details["coverage"]
    assert tab_meta.source_details["geometry"]["acquired_for_bbox_filter"]
    assert not tab_meta.source_details["geometry"]["included_in_output"]
    resources = [*tab_meta.source_details["resources"], *geo_meta.source_details["resources"]]
    for resource, (request, _) in zip(resources, calls, strict=True):
        if resource["role"] == "page":
            assert "geom" in request.url.params["propertyName"]
            assert request.url.params["srsName"] == "EPSG:4326"
        else:
            assert "geom" not in request.url.params["propertyName"]
            assert "srsName" not in request.url.params
            assert request.url.params["count"] == "1"


async def test_incra_geo_misaligned_geometries_rejected(monkeypatch):
    pytest.importorskip("geopandas")
    features = incra_features(include_geometry=True)
    features.append({**copy.deepcopy(features[0]), "id": "lim_quilombolas_a.999"})
    install_incra_wfs(monkeypatch, features)
    fetch = client.fetch_acquisition

    async def misaligned(query):
        acquired = await fetch(query)
        acquired.geometries = acquired.geometries[:-1]
        return acquired

    monkeypatch.setattr(client, "fetch_acquisition", misaligned)
    with pytest.raises(ParseError, match="Geometrias divergem"):
        await api.quilombolas_geo()


@pytest.mark.parametrize("function", ["quilombolas", "quilombolas_geo"])
async def test_incra_deterministic_precedes_optional_and_http(monkeypatch, function):
    fetch = AsyncMock(side_effect=AssertionError("HTTP before deterministic"))
    optional = Mock(side_effect=AssertionError("optional before deterministic"))
    monkeypatch.setattr(client, "fetch_acquisition", fetch)
    monkeypatch.setattr(api.geo, "check_geopandas", optional)
    async with deterministic("2026-09-07"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await getattr(api, function)()
    fetch.assert_not_called()
    optional.assert_not_called()
