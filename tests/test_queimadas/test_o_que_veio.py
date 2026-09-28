from __future__ import annotations

import pytest

from agrobr.exceptions import ParseError
from agrobr.queimadas.parser import parse_focos_csv
from tests.helpers import levanta_exatamente

HTML = b"<!DOCTYPE html><html><head><title>Manutencao</title></head><body><h1>Sistema em manutencao</h1></body></html>"
ZIP_CORROMPIDO = b"PK\x03\x04" + b"\x00" * 40 + b"conteudo-corrompido" * 200


@pytest.mark.parametrize(
    ("corpo", "mensagem"),
    [
        (HTML, "a fonte devolveu uma página HTML, e não o CSV"),
        (ZIP_CORROMPIDO, "a fonte devolveu um ZIP que não se lê como o CSV"),
        (b"id,lat,lon,data_hora_gmt,satelite\n", "CSV vazio: o arquivo não tem linhas de dado"),
    ],
    ids=["html", "zip_corrompido", "so_cabecalho"],
)
def test_corpo_sem_linhas_diz_o_que_veio(corpo, mensagem):
    with levanta_exatamente(ParseError, mensagem):
        parse_focos_csv(corpo)
