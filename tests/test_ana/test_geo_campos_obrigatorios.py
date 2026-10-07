from __future__ import annotations

import json

import pandas as pd
import pytest

from agrobr import exceptions
from agrobr.ana import parser
from tests.test_ana import oficial

pytest.importorskip("geopandas")


@pytest.fixture
def pagina():
    return json.loads((oficial.GOLDEN / "hidrografia_df/chave_0.geojson").read_bytes())


@pytest.mark.parametrize("campo", ["COCURSODAG", "NORIOCOMP"])
@pytest.mark.parametrize("indice", [0, 1])
def test_geo_recusa_campo_ausente_em_uma_feicao(pagina, campo, indice):
    assert len(pagina["features"]) > 1
    del pagina["features"][indice]["properties"][campo]
    dados = json.dumps(pagina).encode()
    with pytest.raises(exceptions.ParseError, match=campo):
        parser.parse_layer_geojson([dados], layer_key="hidrografia")


def test_geo_preserva_nulo_declarado_e_dados_publicados(pagina):
    pagina["features"][0]["properties"]["NORIOCOMP"] = None
    esperado = [oficial.row("hidrografia", feicao) for feicao in pagina["features"]]
    dados = json.dumps(pagina).encode()
    resultado = parser.parse_layer_geojson([dados], layer_key="hidrografia")
    assert pd.isna(resultado.loc[0, "nome_rio"])
    assert oficial.published(resultado) == esperado
    assert resultado.crs.to_epsg() == 4326
