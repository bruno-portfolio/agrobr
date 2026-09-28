from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.cepea.parsers.v1 import CepeaParserV1
from agrobr.exceptions import ParseError
from tests.helpers import levanta_exatamente, sem_excecao

CEPEA = Path(__file__).parents[1] / "golden_data" / "cepea"
SOJA = (CEPEA / "cache_ttl_20260923" / "soja_20260923.html").read_bytes().decode("utf-8")
CITROS = (CEPEA / "sanity_20260906" / "citros.html").read_bytes().decode("utf-8")
MENSAGEM = r"layout do CEPEA mudou: coluna de valor em R\$ não encontrada"


def _injetar(html: str, cabecalho: str | None) -> str:
    """A coluna "Valor R$*" das 2 tabelas do indicador sai (``None``) ou ganha outro cabeçalho."""
    soup = BeautifulSoup(html, "lxml")
    for tabela in soup.find_all("table", class_="imagenet-table"):
        linhas = tabela.find_all("tr")
        titulos = [celula.get_text(strip=True) for celula in linhas[0].find_all(["th", "td"])]
        indice = titulos.index("Valor R$*")
        for linha in linhas if cabecalho is None else linhas[:1]:
            celula = linha.find_all(["th", "td"])[indice]
            if cabecalho is None:
                celula.decompose()
            else:
                celula.string = cabecalho
    return str(soup)


def test_pagina_real_da_soja_traz_o_preco_em_reais():
    with sem_excecao():
        indicadores = CepeaParserV1().parse(SOJA, "soja")

    assert (indicadores[0].valor, indicadores[0].unidade) == (Decimal("161.65"), "BRL/sc60kg")
    assert indicadores[0].meta.get("valor_usd") == 31.28


@pytest.mark.parametrize(
    "cabecalho", [None, "US$", "Cotação"], ids=["sem_a_coluna", "usd", "cotacao"]
)
def test_sem_a_coluna_de_valor_em_reais_o_parser_recusa(cabecalho):
    with levanta_exatamente(ParseError, MENSAGEM):
        CepeaParserV1().parse(_injetar(SOJA, cabecalho), "soja")


async def test_recusa_chega_ao_detector_com_o_motivo():
    with levanta_exatamente(ParseError, MENSAGEM):
        await get_parser_with_fallback(_injetar(SOJA, None), "soja")


@pytest.mark.parametrize(
    ("produto", "primeiro"),
    [("laranja_industria", Decimal("30.73")), ("laranja_in_natura", Decimal("30.97"))],
)
def test_laranja_le_o_valor_da_coluna_a_prazo(produto, primeiro):
    with sem_excecao():
        indicadores = CepeaParserV1().parse(CITROS, produto)

    assert (indicadores[0].valor, indicadores[0].unidade) == (primeiro, "BRL/cx40.8kg")
