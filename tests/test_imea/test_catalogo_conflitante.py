from __future__ import annotations

import json

import pytest

from agrobr import exceptions
from agrobr.imea import parser
from tests.test_imea import oficial

INDICADOR = "708192508838936580"


@pytest.mark.parametrize("inverter", [False, True])
def test_id_com_nomes_diferentes_recusado_em_qualquer_ordem(inverter):
    catalogo = json.loads((oficial.GOLDEN / "indicadores_4.json").read_bytes())
    item = next(item for item in catalogo if item["Id"] == INDICADOR)
    catalogo.append({**item, "Nome": "Nome de outro indicador"})
    if inverter:
        catalogo.reverse()
    with pytest.raises(exceptions.ParseError, match=f"nomes conflitantes.*{INDICADOR}"):
        parser.parse_cotacoes(oficial.registros(4), catalogo, 4)


def test_id_repetido_com_mesmo_nome_preserva_preco_publicado():
    catalogo = json.loads((oficial.GOLDEN / "indicadores_4.json").read_bytes())
    item = next(item for item in catalogo if item["Id"] == INDICADOR)
    catalogo.append(dict(item))
    frame = parser.parse_cotacoes(oficial.registros(4), catalogo, 4)
    row = frame.loc[(frame["indicador_id"] == INDICADOR) & (frame["localidade"] == "Mato Grosso")]
    assert row[["indicador", "valor", "unidade"]].to_dict("records") == [
        {"indicador": "Preço soja disponível compra", "valor": 139.09, "unidade": "R$/sc"}
    ]
    assert row["data_publicacao"].dt.strftime("%Y-%m-%d").tolist() == ["2026-09-22"]
