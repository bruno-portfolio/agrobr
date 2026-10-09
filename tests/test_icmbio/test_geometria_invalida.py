from __future__ import annotations

import warnings
from typing import Any

import pytest

from agrobr.icmbio import api
from tests.helpers import sem_excecao
from tests.test_icmbio.test_crs_oficial import BBOX, servir_wfs


def _com_gravata(parse: Any, poligono: Any) -> Any:
    def com_gravata(*args: Any, **kwargs: Any) -> Any:
        gdf = parse(*args, **kwargs)
        if True:
            x, y = gdf.geometry.iloc[0].representative_point().coords[0]
            anel = [(x, y), (x + 0.01, y + 0.01), (x + 0.01, y), (x, y + 0.01), (x, y)]
            gdf.loc[gdf.index[0], gdf.geometry.name] = poligono(anel)
        return gdf

    return com_gravata


async def _consultar(monkeypatch: pytest.MonkeyPatch) -> Any:
    servir_wfs(monkeypatch, ignora_srs=False)
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        gdf, meta = await api.ucs_geo(bbox=BBOX, return_meta=True)
    return gdf, meta, [str(a.message) for a in avisos if "geometrias inválidas" in str(a.message)]


async def test_geometria_invalida_publicada_vira_aviso_sem_reparo(monkeypatch):
    pytest.importorskip("geopandas")
    shapely_geometry = pytest.importorskip("shapely.geometry")
    _, meta, avisos = await _consultar(monkeypatch)
    assert avisos == [] and "geometrias_invalidas" not in meta.source_details

    monkeypatch.setattr(
        api.parser,
        "parse_ucs_geojson",
        _com_gravata(api.parser.parse_ucs_geojson, shapely_geometry.Polygon),
    )
    gdf, meta, avisos = await _consultar(monkeypatch)
    total = int(gdf.geometry.notna().sum())
    aviso = (
        f"ICMBio: 1 de {total} geometrias inválidas como publicadas pela fonte; "
        "use make_valid antes de operações espaciais."
    )
    assert avisos == [aviso] and meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        1,
        total,
    )
    assert int((~gdf.geometry.is_valid).sum()) == 1
