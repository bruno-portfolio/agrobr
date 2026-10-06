from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from agrobr import exceptions
from agrobr.antaq import parser

GOLDEN = Path(__file__).parents[1] / "golden_data/antaq/movimentacao_sample"


def _frames():
    return (
        parser.parse_atracacao((GOLDEN / "atracacao.txt").read_text(encoding="utf-8")),
        parser.parse_carga((GOLDEN / "carga.txt").read_text(encoding="utf-8")),
        parser.parse_mercadoria((GOLDEN / "mercadoria.txt").read_text(encoding="utf-8")),
    )


@pytest.mark.parametrize("inverter", [False, True])
@pytest.mark.parametrize(
    "coluna", ["Grupo de Mercadoria", "Mercadoria", "Nomenclatura Simplificada Mercadoria"]
)
@pytest.mark.parametrize("valor", ["Nome de outra mercadoria", None])
def test_codigo_com_nomes_diferentes_recusado(coluna, valor, inverter):
    atracacao, carga, mercadoria = _frames()
    duplicada = mercadoria.loc[mercadoria["CDMercadoria"] == "2201"].copy()
    assert len(duplicada) == 1
    duplicada[coluna] = valor
    catalogo = pd.concat([mercadoria, duplicada], ignore_index=True)
    if inverter:
        catalogo = catalogo.iloc[::-1]
    with pytest.raises(exceptions.ParseError, match="nomes conflitantes.*2201"):
        parser.join_movimentacao(atracacao, carga, catalogo)


def test_codigo_com_mesmos_nomes_nao_multiplica_carga():
    atracacao, carga, mercadoria = _frames()
    duplicada = mercadoria.loc[mercadoria["CDMercadoria"] == "2201"]
    catalogo = pd.concat([mercadoria, duplicada], ignore_index=True)
    frame = parser.join_movimentacao(atracacao, carga, catalogo)
    row = frame.loc[
        (frame["cd_mercadoria"] == "2201")
        & (frame["data_atracacao"] == pd.Timestamp("2024-01-02 07:28:00"))
    ]
    assert len(frame) == len(carga)
    assert row[["porto", "mercadoria", "peso_bruto_ton"]].to_dict("records") == [
        {
            "porto": "Terminal Navecunha",
            "mercadoria": "Bebidas, Líquidos Alcoólicos e Vinagres",
            "peso_bruto_ton": 2.0,
        }
    ]
