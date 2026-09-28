from __future__ import annotations

import hashlib
import json
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from agrobr import constants, deterministic
from agrobr.desmatamento import api, client
from agrobr.exceptions import InvalidParameterError
from tests.helpers import desmatamento_features, install_desmatamento_wfs


async def test_api_local_limit_warns_and_records_partial_coverage(monkeypatch):
    calls = install_desmatamento_wfs(monkeypatch, desmatamento_features())
    with pytest.warns(UserWarning, match="2 de 6"):
        frame, meta = await api.prodes(
            bioma="Amazônia", max_registros=2, tamanho_pagina=1, return_meta=True
        )
    assert len(frame) == 2 and len(calls) == 4
    assert meta.source_details["coverage"]["truncated"]
    assert not meta.source_details["coverage"]["count_reconciled"]
    assert meta.source_details["coverage"]["status"] == "partial"
    assert any("Limite local: retornadas 2 de 6" in aviso for aviso in meta.validation_warnings)


@pytest.mark.parametrize("method", ["prodes", "deter", "prodes_geo", "deter_geo"])
@pytest.mark.parametrize(
    "kwargs",
    [
        {"bioma": "Atlantida"},
        {"bioma": None},
        {"uf": "ZZ"},
        {"uf": False},
        {"max_registros": 0},
        {"max_registros": True},
        {"max_registros": 1.0},
        {"tamanho_pagina": 0},
        {"tamanho_pagina": True},
        {"return_meta": 1},
    ],
)
async def test_api_invalid_query_before_optional_and_network(monkeypatch, method, kwargs):
    fetch = AsyncMock()
    optional = Mock(side_effect=AssertionError("optional should not be inspected"))
    monkeypatch.setattr(client, "fetch_acquisition", fetch)
    monkeypatch.setattr(api.geo, "check_geopandas", optional)
    with pytest.raises(InvalidParameterError):
        await getattr(api, method)(**kwargs)
    fetch.assert_not_awaited()
    optional.assert_not_called()


@pytest.mark.parametrize("method", ["prodes", "deter", "prodes_geo", "deter_geo"])
async def test_api_unknown_keyword_before_network(monkeypatch, method):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_acquisition", fetch)
    with pytest.raises(TypeError, match="desconhecidos"):
        await getattr(api, method)(snapshot="2020-01-01")
    fetch.assert_not_awaited()


@pytest.mark.parametrize("method", ["prodes", "deter", "prodes_geo", "deter_geo"])
async def test_api_deterministic_rejected_before_optional_and_network(monkeypatch, method):
    fetch = AsyncMock()
    optional = Mock(side_effect=AssertionError("optional should not be inspected"))
    monkeypatch.setattr(client, "fetch_acquisition", fetch)
    monkeypatch.setattr(api.geo, "check_geopandas", optional)
    async with deterministic("2020-01-01"):
        with pytest.raises(InvalidParameterError, match="deterministic"):
            await getattr(api, method)()
    fetch.assert_not_awaited()
    optional.assert_not_called()


async def test_api_geo_empty_has_geometry_and_crs(monkeypatch):
    gpd = pytest.importorskip("geopandas")
    install_desmatamento_wfs(monkeypatch, [])
    frame, meta = await api.prodes_geo(return_meta=True)
    assert isinstance(frame, gpd.GeoDataFrame)
    assert frame.empty and len(frame.columns) == 21
    assert frame.crs.to_epsg() == 4326
    assert not meta.source_details["geometry"]["declared_crs_verified"]
    assert meta.source_details["geometry"]["crs_basis"] == "requested_crs_for_empty_frame"


@pytest.mark.parametrize(
    "product,sample", [("prodes", "prodes_amazonia"), ("deter", "deter_cerrado")]
)
async def test_api_polars_preserves_nullable_text_and_float(monkeypatch, product, sample):
    pl = pytest.importorskip("polars")
    features = desmatamento_features(sample)
    install_desmatamento_wfs(monkeypatch, features)
    frame, meta = await getattr(api, product)(
        bioma="Cerrado" if sample.endswith("cerrado") else "Amazônia",
        as_polars=True,
        return_meta=True,
    )
    assert isinstance(frame, pl.DataFrame) and len(frame) == 6
    assert frame.schema["feature_id"] == pl.Utf8
    assert frame.schema["area_km2"] == pl.Float64
    if product == "prodes":
        assert frame.schema["ano"] == pl.Int64
    assert meta.records_count == len(frame)


@pytest.mark.parametrize(
    "product,biome,sample",
    [
        ("prodes", "Amazônia", "prodes_amazonia"),
        ("prodes", "Cerrado", "prodes_cerrado"),
        ("prodes", "Caatinga", "prodes_caatinga"),
        ("prodes", "Mata Atlântica", "prodes_mata_atlantica"),
        ("prodes", "Pampa", "prodes_pampa"),
        ("prodes", "Pantanal", "prodes_pantanal"),
        ("deter", "Amazônia", "deter_amazonia"),
        ("deter", "Cerrado", "deter_cerrado"),
    ],
)
async def test_api_all_layouts_json_and_complete_provenance(monkeypatch, product, biome, sample):
    features = desmatamento_features(sample)
    calls = install_desmatamento_wfs(monkeypatch, features)
    frame, meta = await getattr(api, product)(
        bioma=biome, tamanho_pagina=2, max_registros=None, return_meta=True
    )
    expected_columns = (
        constants.DESMATAMENTO_PRODES_COLUMNS
        if product == "prodes"
        else constants.DESMATAMENTO_DETER_COLUMNS
    )
    assert frame.columns.tolist() == list(expected_columns)
    assert frame["feature_id"].tolist() == [feature["id"] for feature in features]
    assert len(frame) == 6
    assert len(calls) == 5
    assert meta.schema_version == meta.contract_version == "2.0"
    assert meta.parser_version == 2 and not meta.from_cache and meta.snapshot is None
    if product == "prodes":
        assert str(frame["ano"].dtype) == "Int64"
    assert meta.selected_source == f"terrabrasilis_{product}"
    assert meta.attempted_sources == [meta.selected_source]
    assert meta.fetch_timestamp.utcoffset().total_seconds() == 0
    assert meta.fetch_timestamp == meta.fetched_at
    details = meta.source_details
    assert details["coverage"]["expected_rows"] == len(frame)
    assert details["coverage"]["overlap_rows"] == 2
    assert details["coverage"]["count_reconciled"]
    assert not details["coverage"]["transactional"]
    assert not details["coverage"]["semantic_progress_proven"]
    assert not details["revision_snapshot"]
    assert details["raw_content_hash_kind"] == "resource_manifest_sha256"
    manifest = json.dumps(
        {name: details[name] for name in ("query", "resources", "pages")},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    assert hashlib.sha256(manifest).hexdigest() == meta.raw_content_hash
    assert len(manifest) == meta.raw_content_size
    assert details["resource_bytes"] == sum(len(body) for _, body in calls)
    for resource, (request, body) in zip(details["resources"], calls, strict=True):
        assert resource["url"] == str(request.url)
        assert resource["size_bytes"] == len(body)
        assert resource["sha256"] == hashlib.sha256(body).hexdigest()
        assert resource["headers"]["etag"] == "synthetic"
        assert resource["status"] == 200 and resource["complete_body"]
    assert json.loads(meta.to_json())["source_details"] == details
    assert meta.fetched_at == max(
        datetime.fromisoformat(resource["fetched_at"]) for resource in details["resources"]
    )
