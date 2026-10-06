from __future__ import annotations

import io
import zipfile

import openpyxl

from agrobr.exceptions import ParseError
from agrobr.mapbiomas import municipal_parser, parser
from tests import helpers
from tests.test_mapbiomas.test_municipal_parser import HEADERS, _row, _xlsx


def _planilha(aba, linhas):
    workbook = openpyxl.Workbook()
    workbook.active.title = aba
    for linha in linhas:
        workbook.active.append(linha)
    output = io.BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_municipal_cabecalho_booleano_recusado():
    headers = [*HEADERS]
    headers[0] = True
    with helpers.levanta_exatamente(ParseError, match="cabeçalho inválido na coluna 1"):
        municipal_parser.parse_cobertura_municipal(_xlsx([_row()], headers), colecao=11)


def test_municipal_area_inteira_excede_float_recusada():
    original = _xlsx([_row(y1985=123456789)])
    output = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(original)) as source, zipfile.ZipFile(output, "w") as target:
        for name in source.namelist():
            content = source.read(name)
            if name == "xl/worksheets/sheet1.xml":
                assert content.count(b"<v>123456789</v>") == 1
                content = content.replace(b"<v>123456789</v>", b"<v>" + b"9" * 400 + b"</v>")
            target.writestr(name, content)
    with helpers.levanta_exatamente(
        ParseError, match="área de 1985 excede a representação numérica"
    ):
        municipal_parser.parse_cobertura_municipal(output.getvalue(), colecao=11)


def test_municipal_aba_sem_cabecalho_recusada():
    with helpers.levanta_exatamente(ParseError, match="cabeçalho municipal ausente"):
        municipal_parser.parse_cobertura_municipal(_planilha("COVERAGE_11", []), colecao=11)


def test_municipal_cabecalho_sem_populacao_recusado():
    with helpers.levanta_exatamente(
        ParseError, match="população municipal sem linhas identificadas"
    ):
        municipal_parser.parse_cobertura_municipal(_xlsx([]), colecao=11)


def test_municipal_aba_incorreta_recusada():
    with helpers.levanta_exatamente(ParseError, match="aba municipal COVERAGE_11 ausente"):
        municipal_parser.parse_cobertura_municipal(_planilha("COVERAGE_10", []), colecao=11)


def test_cobertura_sem_coluna_anual_recusada():
    data = _planilha(
        "COVERAGE_11",
        [["biome", "state", "class", "class_level_0"], ["Cerrado", "Goiás", 3, "Natural"]],
    )
    with helpers.levanta_exatamente(ParseError, match="Nenhuma coluna de ano encontrada"):
        parser.parse_cobertura_xlsx(data, colecao=11)


def test_transicao_aba_vazia_recusada():
    data = _planilha("TRANSITION_11", [["biome", "state", "class_from", "class_to"]])
    with helpers.levanta_exatamente(ParseError, match="Sheet TRANSITION vazia"):
        parser.parse_transicao_xlsx(data, colecao=11)


def test_transicao_sem_periodo_recusada():
    data = _planilha(
        "TRANSITION_11",
        [["biome", "state", "class_from", "class_to"], ["Cerrado", "Goiás", 3, 15]],
    )
    with helpers.levanta_exatamente(ParseError, match="Nenhuma coluna de periodo encontrada"):
        parser.parse_transicao_xlsx(data, colecao=11)
