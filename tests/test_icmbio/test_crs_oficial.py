from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from agrobr import icmbio
from agrobr.icmbio import api
from tests import helpers

GOLDEN = Path(__file__).parents[1] / "golden_data/icmbio/crs_20260923"
CONTAGEM = b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="1"/>'
BBOX = (-56.0, -16.0, -54.0, -14.0)


def servir_wfs(monkeypatch: pytest.MonkeyPatch, *, ignora_srs: bool) -> list[str]:
    corpos = {nome: (GOLDEN / f"{nome}.json").read_bytes() for nome in ("geo_4674", "geo_4326")}
    urls: list[str] = []
    original = httpx.AsyncClient

    def responder(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        pediu_4326 = "srsName=EPSG:4326" in str(request.url) and not ignora_srs
        corpo = corpos["geo_4326"] if pediu_4326 else corpos["geo_4674"]
        return httpx.Response(200, content=corpo, headers={"content-type": "application/json"})

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: original(
            *args, **{**kwargs, "transport": httpx.MockTransport(responder)}
        ),
    )
    monkeypatch.setattr(
        api.client, "fetch_ucs_count", AsyncMock(return_value=(CONTAGEM, "https://example.test"))
    )
    return urls


async def test_ucs_geo_pede_4326_e_publica_o_corpo_oficial(monkeypatch):
    pytest.importorskip("geopandas")
    urls = servir_wfs(monkeypatch, ignora_srs=False)
    with helpers.sem_excecao():
        gdf = await icmbio.ucs_geo(bbox=BBOX)
    assert "srsName=EPSG:4326" in urls[-1]
    assert gdf.crs.to_epsg() == 4326
    assert gdf["nome"].tolist() == ["PARQUE NACIONAL DA CHAPADA DOS GUIMARÃES"]
    assert gdf.geometry.iloc[0].geoms[0].exterior.coords[0] == (-55.92402649, -15.21559906)
