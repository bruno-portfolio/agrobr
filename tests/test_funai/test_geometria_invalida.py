from __future__ import annotations

import copy
import warnings
from typing import Any

import pytest

from agrobr import funai
from tests.helpers import funai_features, install_funai_wfs, sem_excecao


def _primeiro_ponto(coordenadas: Any) -> tuple[float, float]:
    while not isinstance(coordenadas[0], int | float):
        coordenadas = coordenadas[0]
    return coordenadas[0], coordenadas[1]


def _gravata(geometria: dict[str, Any]) -> dict[str, Any]:
    x, y = _primeiro_ponto(geometria["coordinates"])
    anel = [[x, y], [x + 0.01, y + 0.01], [x + 0.01, y], [x, y + 0.01], [x, y]]
    if geometria["type"] == "MultiPolygon":
        return {"type": "MultiPolygon", "coordinates": [[anel]]}
    return {"type": "Polygon", "coordinates": [anel]}


async def _consultar(monkeypatch: pytest.MonkeyPatch, features: list[dict[str, Any]]) -> Any:
    install_funai_wfs(monkeypatch, features)
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        gdf, meta = await funai.terras_indigenas_geo(return_meta=True)
    return gdf, meta, [str(a.message) for a in avisos if "geometrias inválidas" in str(a.message)]


async def test_geometria_invalida_publicada_vira_aviso_sem_reparo(monkeypatch):
    pytest.importorskip("geopandas")
    features = funai_features(include_geometry=True)
    _, meta, avisos = await _consultar(monkeypatch, features)
    assert avisos == [] and "geometrias_invalidas" not in meta.source_details

    com_gravata = copy.deepcopy(features)
    com_gravata[0]["geometry"] = _gravata(com_gravata[0]["geometry"])
    gdf, meta, avisos = await _consultar(monkeypatch, com_gravata)
    total = int(gdf.geometry.notna().sum())
    aviso = (
        f"FUNAI: 1 de {total} geometrias inválidas como publicadas pela fonte; "
        "use make_valid antes de operações espaciais."
    )
    assert avisos == [aviso] and meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        1,
        total,
    )
    assert int((~gdf.geometry.is_valid).sum()) == 1
