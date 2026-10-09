from __future__ import annotations

import warnings

import pytest

from agrobr import ibama
from tests.helpers import sem_excecao
from tests.test_ibama import oficial
from tests.test_ibama.test_oficial import instalar


async def test_poligono_invalido_publicado_vira_aviso_sem_reparo(monkeypatch):
    pytest.importorskip("geopandas")
    shapely = pytest.importorskip("shapely")
    lidas = shapely.from_wkt(
        [linha["GEOM_AREA_EMBARGADA"] or None for linha in oficial.fonte()], on_invalid="ignore"
    )
    publicadas = [geometria for geometria in lidas if geometria is not None]
    invalidas = sum(not geometria.is_valid for geometria in publicadas)
    assert (invalidas, len(publicadas)) == (1, 23)
    instalar(monkeypatch)
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        gdf, meta = await ibama.embargos_geo(use_cache=False, return_meta=True)
        await ibama.embargos_geo(use_cache=False)
    total = int(gdf.geometry.notna().sum())
    aviso = (
        f"IBAMA embargos: 1 de {total} geometrias inválidas como publicadas pela fonte; "
        "use make_valid antes de operações espaciais."
    )
    assert [str(a.message) for a in avisos if "geometrias inválidas" in str(a.message)] == [
        aviso,
        aviso,
    ]
    assert meta.validation_warnings.count(aviso) == 1
    assert (meta.source_details["geometrias_invalidas"], meta.source_details["geometrias"]) == (
        1,
        total,
    )
    assert int((~gdf.geometry.is_valid).sum()) == 1
