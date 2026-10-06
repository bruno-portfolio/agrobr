from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr import exceptions
from agrobr.conab.ceasa import parser

GOLDEN = Path(__file__).parents[1] / "golden_data/conab_ceasa/precos_20260923/precos_response.json"


@pytest.mark.parametrize("indice", [0, 1, 2])
@pytest.mark.parametrize("valor", [None, -1, 999, "1", True, 1.0])
def test_colindex_invalido_nao_atribui_preco_a_outra_ceasa(indice, valor):
    payload = json.loads(GOLDEN.read_bytes())
    if valor is None:
        del payload["metadata"][indice]["colIndex"]
    else:
        payload["metadata"][indice]["colIndex"] = valor
    with pytest.raises(exceptions.ParseError, match="colIndex"):
        parser.parse_precos(payload)


def test_metadata_fora_de_ordem_recusa_troca_de_pracas():
    payload = json.loads(GOLDEN.read_bytes())
    payload["metadata"][1], payload["metadata"][2] = (
        payload["metadata"][2],
        payload["metadata"][1],
    )
    with pytest.raises(exceptions.ParseError, match="colIndex"):
        parser.parse_precos(payload)


def test_colindex_repetido_recusado():
    payload = json.loads(GOLDEN.read_bytes())
    payload["metadata"][2]["colIndex"] = 1
    with pytest.raises(exceptions.ParseError, match="colIndex"):
        parser.parse_precos(payload)


def test_indices_publicados_preservam_abacate_de_juazeiro():
    payload = json.loads(GOLDEN.read_bytes())
    frame = parser.parse_precos(payload)
    row = frame.loc[(frame["produto"] == "ABACATE") & (frame["ceasa"] == "AMA/BA - JUAZEIRO")]
    assert row[["unidade", "ceasa_uf", "preco"]].to_dict("records") == [
        {"unidade": "KG", "ceasa_uf": "BA", "preco": 3.65}
    ]
    assert row["data"].dt.strftime("%Y-%m-%d").tolist() == ["2026-09-21"]
