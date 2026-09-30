from __future__ import annotations

import pytest

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
