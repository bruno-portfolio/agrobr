from __future__ import annotations

import json

import pytest

from agrobr.exceptions import ParseError
from agrobr.sfb import parser
from tests.test_sfb import oficial


@pytest.mark.parametrize(
    ("camada", "cenario", "geometria"),
    [
        ("cnfp", "cnfp_df", False),
        ("concessoes", "concessoes", False),
        ("cnfp", "cnfp_geo_df_arie", True),
        ("concessoes", "concessoes_geo_ro", True),
    ],
)
def test_cheio_oficial_e_vazio_preservam_tipos(camada, cenario, geometria):
    if geometria:
        pytest.importorskip("geopandas")
    registros = oficial.respostas(cenario)
    paginas = [(oficial.GOLDEN / registro["file"]).read_bytes() for registro in registros[1:]]
    parse = parser.parse_layer_geojson if geometria else parser.parse_layer_tabular
    cheio = parse(paginas, layer_key=camada)
    vazio = parse([], layer_key=camada)
    esperado = oficial.esperado_cnfp if camada == "cnfp" else oficial.esperado_concessoes
    assert oficial.publicado(cheio) == esperado(cenario)
    assert cheio.dtypes.equals(vazio.dtypes)
    assert str(cheio["fid"].dtype) == "Int64"


@pytest.mark.parametrize(("cenario", "geometria"), [("cnfp_df", False), ("cnfp_geo_df_arie", True)])
def test_campo_ausente_no_cnfp_vira_erro_de_layout(cenario, geometria):
    if geometria:
        pytest.importorskip("geopandas")
    paginas = []
    for registro in oficial.respostas(cenario)[1:]:
        corpo = json.loads((oficial.GOLDEN / registro["file"]).read_bytes())
        for feicao in corpo["features"]:
            del (feicao.get("attributes") or feicao["properties"])["anocriacao"]
        paginas.append(json.dumps(corpo).encode())
    parse = parser.parse_layer_geojson if geometria else parser.parse_layer_tabular
    with pytest.raises(ParseError, match="anocriacao"):
        parse(paginas, layer_key="cnfp")
