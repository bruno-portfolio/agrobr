from datetime import datetime

import pytest

from agrobr.conab._custo_producao import _acquisition, _context, _parse, _workbook, models
from agrobr.exceptions import ParseError

PLANILHA = "milho_1a_safra_serie_historica_1997-2025.xls"


def _resource() -> models.RecursoCusto:
    return models.RecursoCusto(
        planilha=PLANILHA,
        cultura="milho",
        titulo=PLANILHA,
        pagina_url=f"{_acquisition.CATALOG_URL}/milho/{PLANILHA}/view",
    )


def _rio_verde() -> _workbook.Aba:
    return _workbook.Aba(
        "Rio Verde-GO-2009",
        [
            ["CUSTO DE PRODUÇÃO ESTIMADO", "", "", "", 60.0],
            ["MILHO  - PD ", "", "", "", ""],
            ["SAFRA DE VERÃO 2009/2010", "", "", "", ""],
            ["LOCAL: RIO VERDE", "", "", "", ""],
            ["Produtividade Média:", 6000.0, "kg/ha", "", ""],
            ["", "A PREÇOS DE:", datetime(2009, 5, 31), "PARTICI-", ""],
            ["DISCRIMINAÇÃO", "", "", "PAÇÃO", ""],
            ["", "(R$/ha)", "R$/60 kg", "(%)", ""],
        ],
        {},
    )


def test_rio_verde_identificado_recusa_zero_sem_cabecalho_no_parse():
    sheet = _rio_verde()
    sheet.linhas.extend([["", "", "", "", ""] for _ in range(20)])
    sheet.linhas.append(["CUSTO VARIÁVEL  (A+B+C = D)", 1.0, 1.0, 100.0, 0.0])
    ctx = _context.context(sheet, _resource(), 0)

    with pytest.raises(ParseError, match="Medida em coluna não reconhecida") as caught:
        _parse.parse_selected(sheet, ctx)

    assert "Rio Verde-GO-2009!R29C5=0.0" in str(caught.value)
    assert "sem cabeçalho reconhecido" in str(caught.value)


@pytest.mark.parametrize("name", ["Rio Verde", "Rio Verde-2009", "Rio Verde-GO-09"])
def test_local_sem_uf_em_qualquer_lugar_falha(name):
    sheet = _rio_verde()
    sheet.nome = name
    with pytest.raises(ParseError, match="Local não identificado"):
        _context.context(sheet, _resource(), 0)


def test_referencia_nao_e_inferida_do_nome_da_aba():
    sheet = _rio_verde()
    sheet.linhas[5][2] = ""
    with pytest.raises(ParseError, match="Contexto não unívoco ou incompleto"):
        _context.context(sheet, _resource(), 0)


@pytest.mark.parametrize(
    "local", ["LOCAL: RIO VERDE - GO", "LOCAL: RIO VERDE GO", "LOCAL: RIO VERDE (GO)"]
)
def test_uf_publicada_prevalece_sobre_nome_da_aba(local):
    sheet = _rio_verde()
    sheet.nome = "Rio Verde-PR-2009"
    sheet.linhas[3][0] = local
    result = _context.context(sheet, _resource(), 0)
    assert result.uf == "GO"
    assert "uf_origem" not in result.celulas_contexto
