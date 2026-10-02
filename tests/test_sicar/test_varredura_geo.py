from __future__ import annotations

import copy
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr.alt.sicar import api, client
from agrobr.exceptions import ParseError

GOLDEN = Path(__file__).parents[1] / "golden_data/sicar/geo_20260922/df_geo_srs4326_count3.json"


def publicar(monkeypatch: pytest.MonkeyPatch, paginas: list[tuple[list[int], int]]) -> None:
    corpo = json.loads(GOLDEN.read_bytes())
    extra = copy.deepcopy(corpo["features"][2])
    extra["id"] = "sicar_imoveis_df.9999999"
    extra["properties"]["cod_imovel"] = "DF-5300108-0009999999999999999999999999999"
    feicoes = [*corpo["features"], extra]
    base = {chave: valor for chave, valor in corpo.items() if chave not in ("features", "links")}
    respostas = [
        json.dumps(
            {
                **base,
                "features": [feicoes[indice] for indice in indices],
                "numberMatched": anunciadas,
                "totalFeatures": anunciadas,
                "numberReturned": len(indices),
            }
        ).encode()
        for indices, anunciadas in paginas
    ]
    monkeypatch.setattr(client, "PAGE_SIZE", 2)
    monkeypatch.setattr(client, "fetch_hits", AsyncMock(return_value=4))
    monkeypatch.setattr(client, "fetch_wfs", AsyncMock(side_effect=respostas))


@pytest.mark.parametrize(("max_registros", "segunda"), [(None, [2]), (3, [])])
async def test_geo_pagina_curta_vira_erro_de_varredura(
    monkeypatch: pytest.MonkeyPatch, max_registros: int | None, segunda: list[int]
):
    pytest.importorskip("geopandas")
    publicar(monkeypatch, [([0, 1], 4), (segunda, 4)])
    with pytest.raises(ParseError, match="Varredura inconsistente"):
        await api.imoveis_geo("DF", max_registros=max_registros)


async def test_geo_stream_pagina_curta_vira_erro_de_varredura(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("geopandas")
    publicar(monkeypatch, [([0, 1], 4), ([2], 4)])
    with pytest.raises(ParseError, match="Varredura inconsistente"):
        async for _ in api.imoveis_geo_stream("DF"):
            pass


async def test_geo_contagem_que_muda_vai_ao_meta(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("geopandas")
    publicar(monkeypatch, [([0, 1], 4), ([3], 3)])
    frame, meta = await api.imoveis_geo("DF", max_registros=None, return_meta=True)
    assert len(frame) == 3
    assert "Contagem mudou de 4 para 3" in " ".join(meta.validation_warnings)
