from __future__ import annotations

import warnings
from typing import Any

import pytest

from agrobr.ibge import malhas
from tests.helpers import sem_excecao
from tests.test_ibge.test_malhas import instalar


def _com_gravata(parse: Any, poligono: Any) -> Any:
    def com_gravata(*args: Any, **kwargs: Any) -> Any:
        gdf = parse(*args, **kwargs)
        if kwargs.get("geo"):
            x, y = gdf.geometry.iloc[0].representative_point().coords[0]
            anel = [(x, y), (x + 0.01, y + 0.01), (x + 0.01, y), (x, y + 0.01), (x, y)]
            gdf.loc[gdf.index[0], gdf.geometry.name] = poligono(anel)
        return gdf

    return com_gravata


async def _consultar(monkeypatch: pytest.MonkeyPatch) -> Any:
    instalar(monkeypatch)
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        gdf, meta = await malhas.malha_municipal_geo(uf="DF", return_meta=True)
    return gdf, meta, [str(a.message) for a in avisos if "geometrias inválidas" in str(a.message)]


async def test_geometria_invalida_publicada_vira_aviso_sem_reparo(monkeypatch):
    pytest.importorskip("geopandas")
    shapely_geometry = pytest.importorskip("shapely.geometry")
    _, meta, avisos = await _consultar(monkeypatch)
    assert avisos == [] and "geometrias_invalidas" not in meta.source_details

    monkeypatch.setattr(
        malhas, "parse_geojson", _com_gravata(malhas.parse_geojson, shapely_geometry.Polygon)
    )
    gdf, meta, avisos = await _consultar(monkeypatch)
    total = int(gdf.geometry.notna().sum())
    aviso = (
        f"IBGE malha_municipal: 1 de {total} geometrias inválidas como publicadas pela fonte; "
        "use make_valid antes de operações espaciais."
    )
    assert avisos == [aviso] and meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        1,
        total,
    )
    assert int((~gdf.geometry.is_valid).sum()) == 1
