from __future__ import annotations

import csv

from agrobr.defensivos import parser
from agrobr.exceptions import ParseError
from tests import helpers


def test_formulados_campo_acima_do_limite_csv_recusado():
    data = (
        b"NR_REGISTRO;MARCA_COMERCIAL;INGREDIENTE_ATIVO;CULTURA\n"
        + b"1;"
        + b"a" * (csv.field_size_limit() + 1)
        + b";A;Soja\n"
    )
    with helpers.levanta_exatamente(ParseError, match="CSV inválido na linha 2"):
        parser.parse_formulados_bundle(data)


def test_formulados_cabecalho_acima_do_limite_csv_recusado():
    data = b"a" * (csv.field_size_limit() + 1) + b"\n"
    with helpers.levanta_exatamente(ParseError, match="Cabeçalho CSV inválido"):
        parser.parse_formulados_bundle(data)
