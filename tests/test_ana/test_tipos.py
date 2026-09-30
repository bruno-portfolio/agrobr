from __future__ import annotations

import pytest

from agrobr.ana import parser
from tests.test_ana import oficial


@pytest.mark.parametrize(
    ("camada", "caso"),
    [
        ("hidrografia", "hidrografia_df"),
        ("demanda_irrigacao", "demanda_df"),
        ("pivos_irrigacao", "pivos_piaui"),
        ("disponibilidade_hidrica", "disponibilidade_brasilia"),
    ],
)
@pytest.mark.parametrize("formato", ["json", "geojson"])
def test_cheio_oficial_e_vazio_preservam_tipos(camada, caso, formato):
    if formato == "geojson":
        pytest.importorskip("geopandas")
    dados = oficial.manifest()
    arquivos = sorted(
        {
            pedido["file"]
            for pedido in dados["requests"]
            if pedido["case"] == caso
            and pedido["params"]["f"] == formato
            and "orderByFields" in pedido["params"]
            and pedido["file"].split("/")[-1].startswith("chave_")
        }
    )
    assert arquivos
    parse = parser.parse_layer_geojson if formato == "geojson" else parser.parse_layer_tabular
    cheio = parse(
        [(oficial.GOLDEN / arquivo).read_bytes() for arquivo in arquivos], layer_key=camada
    )
    vazio = parse([], layer_key=camada)
    assert oficial.published(cheio) == [
        oficial.row(camada, feature) for feature in oficial.features(dados, caso, formato)
    ]
    assert cheio.dtypes.equals(vazio.dtypes)
    assert str(cheio["OBJECTID"].dtype) == "Int64"
