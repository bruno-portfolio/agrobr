from __future__ import annotations

import pytest

from agrobr.conab._custo_producao import _parse
from agrobr.exceptions import ParseError
from tests import helpers


def test_duas_medidas_na_mesma_faixa_mesclada_sao_recusadas():
    sheet, context = helpers.load_custo_sheet(
        "../conab/custos_20260908/soja.xls", "soja", "Barreiras-BA-2011"
    )
    sheet.linhas[18][5] = 12.0
    with pytest.raises(ParseError, match="Mais de um valor na coluna valor_ha"):
        _parse.parse_selected(sheet, context)


def test_cafe_referencia_textual_com_dia_e_regiao_sao_publicadas():
    _, context = helpers.load_custo_sheet(
        "feb4999ec69b7274.xls", "cafe_arabica", "Cristalina-GO-2013"
    )
    assert context.referencia == "13/12/2013"
    assert context.ano_referencia == 2013
    assert context.data_referencia is None
    _, region = helpers.load_custo_sheet("feb4999ec69b7274.xls", "cafe_arabica", "Manhuaçu-MG-2007")
    assert region.local == "MANHUAÇU"
    assert region.uf == "MG"


def test_nota_cambial_com_duas_medidas_e_recusada():
    sheet, context = helpers.load_custo_sheet(
        "122e85180138ccc3.xls", "milho", "P. do Leste-MT-2003"
    )
    sheet.linhas[48][1] = 2.0
    with pytest.raises(ParseError, match="Nota cambial ambígua"):
        _parse.parse_selected(sheet, context)
