from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from agrobr import ana
from agrobr.ana import client
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import geo

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ana"
BBOX = (-48.1, -16.1, -47.9, -15.9)


@pytest.mark.parametrize(
    "nome",
    [
        "hidrografia",
        "hidrografia_geo",
        "pivos_irrigacao",
        "pivos_irrigacao_geo",
        "demanda_irrigacao",
        "demanda_irrigacao_geo",
        "disponibilidade_hidrica",
        "disponibilidade_hidrica_geo",
        "massas_dagua",
        "massas_dagua_geo",
    ],
)
@pytest.mark.parametrize("limite", [-1, 0, True, 1.5, "10"])
async def test_limite_invalido_recusado_antes_da_rede(nome, limite, monkeypatch):
    http = Mock(side_effect=AssertionError("Cliente HTTP não deveria ser aberto"))
    monkeypatch.setattr(geo.httpx, "AsyncClient", http)
    with pytest.raises(InvalidParameterError, match="max_registros"):
        await getattr(ana, nome)(bbox=(-48.1, -16.1, -47.9, -15.9), max_registros=limite)
    http.assert_not_called()


async def test_corte_no_geo_avisa_com_e_sem_meta(monkeypatch):
    pytest.importorskip("geopandas")
    corpo = (GOLDEN / "oficial_20260923/hidrografia_df/chave_0.geojson").read_bytes()
    monkeypatch.setattr(
        client, "fetch_layer", AsyncMock(return_value=([corpo], "https://fonte.test/query", 47))
    )
    aviso = (
        "ANA hidrografia: retornadas 12 de 47 feições por limite local; "
        "restrinja filtros ou use max_registros=None."
    )
    with pytest.warns(UserWarning, match=re.escape(aviso)):
        gdf, meta = await ana.hidrografia_geo(bbox=BBOX, max_registros=12, return_meta=True)
    assert len(gdf) == 12
    assert meta.validation_warnings == [aviso]
    assert meta.source_details == {
        "coverage": {
            "expected_rows": 47,
            "returned_rows": 12,
            "local_limit": 12,
            "truncated": True,
        }
    }
    with pytest.warns(UserWarning, match=re.escape(aviso)):
        await ana.hidrografia_geo(bbox=BBOX, max_registros=12)


async def test_nome_antigo_do_limite_recusado_antes_da_rede(monkeypatch):
    http = Mock(side_effect=AssertionError("Cliente HTTP não deveria ser aberto"))
    monkeypatch.setattr(geo.httpx, "AsyncClient", http)
    with pytest.raises(TypeError, match="max_features"):
        await ana.hidrografia(bbox=(-48.1, -16.1, -47.9, -15.9), max_features=1)
    http.assert_not_called()
