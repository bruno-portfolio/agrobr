from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from agrobr import ana
from agrobr.ana import api
from agrobr.exceptions import ParseError

BARRAGEM = Path(__file__).parents[1] / "golden_data/ana/massas_dagua_20261001/barragem_df"


def publicar(monkeypatch: pytest.MonkeyPatch, corpo: dict[str, Any]) -> None:
    pagina = json.dumps(corpo).encode()
    resposta = AsyncMock(return_value=([pagina], "https://fonte.invalid/query"))
    monkeypatch.setattr(api.client, "fetch_massas_dagua", resposta)


@pytest.mark.parametrize(("geo", "chave"), [(False, "attributes"), (True, "properties")])
async def test_campo_ausente_na_feicao_vira_erro_de_layout(
    monkeypatch: pytest.MonkeyPatch, geo: bool, chave: str
):
    if geo:
        pytest.importorskip("geopandas")
    corpo = json.loads((BARRAGEM / ("faixa_0.geojson" if geo else "faixa_0.json")).read_bytes())
    del corpo["features"][0][chave]["nuareaha"]
    publicar(monkeypatch, corpo)
    with pytest.raises(ParseError, match="nuareaha"):
        await (ana.massas_dagua_geo if geo else ana.massas_dagua)(uf="DF")


async def test_geometria_corrompida_vira_erro_de_layout(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("geopandas")
    corpo = json.loads((BARRAGEM / "faixa_0.geojson").read_bytes())
    corpo["features"][0]["geometry"] = {"type": "Polygon", "coordinates": [[[0, 0], [1, 1]]]}
    publicar(monkeypatch, corpo)
    with pytest.raises(ParseError, match="linearring requires at least 4 coordinates"):
        await ana.massas_dagua_geo(uf="DF")
