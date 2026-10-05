from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.icmbio import api
from tests import helpers

FIXTURE = Path(__file__).parents[1] / "golden_data/icmbio/official_20260905"


@pytest.mark.asyncio
async def test_official_cerrado_matches_tabular_and_compound_biomes():
    pytest.importorskip("geopandas")
    raw = (FIXTURE / "attributes.json").read_bytes()
    features = json.loads(raw)["features"]
    expected = {
        f["properties"]["cnuc"]
        for f in features
        if "cerrado" in str(f["properties"].get("biomas", "")).casefold()
    }
    assert len(expected) == 50
    with (
        patch.object(
            api.client,
            "fetch_ucs_count",
            AsyncMock(
                side_effect=[
                    (
                        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="347"/>',
                        "https://test/hits",
                    ),
                    (
                        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="50"/>',
                        "https://test/hits",
                    ),
                ]
            ),
        ),
        patch.object(
            api.client, "fetch_ucs_geo", AsyncMock(return_value=(raw, "https://inde.gov.br"))
        ),
        patch.object(
            api.client,
            "fetch_ucs",
            AsyncMock(
                return_value=(
                    (FIXTURE / "cerrado.csv").read_bytes(),
                    "https://inde.gov.br",
                )
            ),
        ),
    ):
        result, meta = await api.ucs_geo(bioma="Cerrado", return_meta=True)
        tabular = await api.ucs(bioma="Cerrado")
    assert set(result["codigo"]) == expected == set(tabular["codigo"])
    assert meta.records_count == 50
    helpers.conferir_corpo(meta, raw)
    assert (result["bioma"] != "CERRADO").any()


@pytest.mark.asyncio
async def test_geo_registra_a_selecao_e_a_hora_da_aquisicao(monkeypatch):
    pytest.importorskip("geopandas")
    raw = (FIXTURE / "attributes.json").read_bytes()
    aquisicao = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)

    class Relogio:
        @staticmethod
        def now(_tz):
            return aquisicao

    monkeypatch.setattr(api, "datetime", Relogio)
    hits = b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="347"/>'
    monkeypatch.setattr(
        api.client, "fetch_ucs_count", AsyncMock(return_value=(hits, "https://test/hits"))
    )
    monkeypatch.setattr(
        api.client, "fetch_ucs_geo", AsyncMock(return_value=(raw, "https://inde.gov.br"))
    )

    _, meta = await api.ucs_geo(bioma="Cerrado", grupo="PI", return_meta=True)

    assert meta.source_details["query"] == {
        "uf": None,
        "grupo": "PI",
        "bioma": "Cerrado",
        "bbox": None,
    }
    assert meta.source_details["filtros_locais"] == {"grupo": "PI", "bioma": "Cerrado"}
    assert meta.fetched_at == meta.fetch_timestamp == aquisicao
