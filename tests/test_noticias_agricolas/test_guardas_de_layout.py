from __future__ import annotations

from decimal import Decimal
from typing import Any

from agrobr.exceptions import ParseError
from agrobr.noticias_agricolas import parser
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

LINHA = "<tr><td>25/09/2026</td><td>82,48</td><td>0,84%</td></tr>"
ESPERADA = ("2026-09-25", "Rio Grande do Sul", Decimal("82.48"), Decimal("0.84"))
POR_ESTADO = ["Estados", "Preço (R$/Litro)", "Variação (%)"]
ESTADO = "<tr><td>RS</td><td>2,6560</td><td>+4,71</td></tr>"


def tabela(cabecalho: list[str], linhas: str) -> str:
    colunas = "".join(f"<th>{texto}</th>" for texto in cabecalho)
    return (
        f'<table class="cot-fisicas"><thead class="head-box"><tr>{colunas}</tr></thead>'
        f'<tbody class="body-box">{linhas}</tbody></table>'
    )


def cotacao(conteudo: str, fechamento: str | None = "Fechamento: 25/09/2026") -> str:
    data = f'<div class="fechamento">{fechamento}</div>' if fechamento is not None else ""
    return (
        f'<div class="cotacao"><div class="info"><div class="title-descricao">{data}</div></div>'
        f'<div class="table-content">{conteudo}</div></div>'
    )


VALIDA = cotacao(tabela(["Data", "Valor R$/ Saca de 50 kg", "Variação (%)"], LINHA))


def saida(html: str) -> list[tuple[Any, ...]]:
    return [
        (
            ind.data.isoformat(),
            ind.praca,
            ind.valor,
            None
            if "variacao_percentual" not in ind.meta
            else Decimal(str(ind.meta["variacao_percentual"])),
        )
        for ind in parser.parse_indicador(html, "arroz")
    ]


def test_parser_pula_o_que_nao_e_linha_de_cotacao():
    invalidas = (
        '<tr><td colspan="3">Fonte: Cepea/Esalq</td></tr>'
        "<tr><td>24/09/2026</td><td>n/d</td><td>0,10%</td></tr>"
        "<tr><td>--/--/----</td><td>81,00</td><td>0,10%</td></tr>"
    )
    with collect_failures() as check:
        for caso, html, esperado in [
            (
                "tabela sem data nem estado",
                VALIDA
                + cotacao(tabela(["Produto", "Valor R$"], "<tr><td>Arroz</td><td>90,00</td></tr>")),
                [ESPERADA],
            ),
            (
                "tabela sem coluna de valor",
                VALIDA
                + cotacao(
                    tabela(["Data", "Quantidade"], "<tr><td>24/09/2026</td><td>10</td></tr>")
                ),
                [ESPERADA],
            ),
            (
                "tabela por estado sem fechamento",
                VALIDA + cotacao(tabela(POR_ESTADO, ESTADO), fechamento=None),
                [ESPERADA],
            ),
            (
                "fechamento sem data",
                VALIDA + cotacao(tabela(POR_ESTADO, ESTADO), fechamento="Fechamento: a confirmar"),
                [ESPERADA],
            ),
            ("tabela por estado fora do bloco", VALIDA + tabela(POR_ESTADO, ESTADO), [ESPERADA]),
            (
                "widget com data e valor ao lado da cotação",
                VALIDA
                + tabela(
                    ["Data", "Valor R$"], "<tr><td>24/09/2026</td><td>150,00</td></tr>"
                ).replace(' class="cot-fisicas"', ""),
                [ESPERADA],
            ),
            (
                "linha curta, valor e data inválidos",
                cotacao(tabela(["Data", "Valor R$", "Variação (%)"], LINHA + invalidas)),
                [ESPERADA],
            ),
            (
                "página sem a classe cot-fisicas",
                cotacao(
                    tabela(["Data", "Valor R$", "Variação (%)"], LINHA).replace(
                        ' class="cot-fisicas"', ""
                    )
                ),
                [ESPERADA],
            ),
            (
                "valor com R$",
                cotacao(
                    tabela(
                        ["Data", "Valor R$", "Variação (%)"],
                        "<tr><td>25/09/2026</td><td>R$ 82,48</td><td>0,84%</td></tr>",
                    )
                ),
                [ESPERADA],
            ),
            (
                "tabela sem coluna de variação",
                cotacao(tabela(["Data", "Valor R$"], "<tr><td>25/09/2026</td><td>82,48</td></tr>")),
                [(*ESPERADA[:3], None)],
            ),
        ]:
            with check(caso):
                with sem_excecao():
                    obtido = saida(html)
                assert obtido == esperado


def test_datas_invalidas_nao_viram_data():
    with collect_failures() as check:
        for texto in ("31/02/2026", "29 - 31/02/2026", "25/09", "semana 38"):
            with check(texto):
                with sem_excecao():
                    obtido = parser._parse_date(texto)
                assert obtido is None


def test_pagina_sem_linha_de_cotacao_levanta_parse_error():
    sem_linha = cotacao(tabela(["Produto", "Valor R$"], "<tr><td>Arroz</td><td>90,00</td></tr>"))
    with collect_failures() as check:
        for caso, html, motivo in [
            ("com tabela", sem_linha, "Tables found but no data rows matched expected format."),
            ("sem tabela", "<html><body><p>Cotações</p></body></html>", "No tables found in HTML."),
        ]:
            with (
                check(caso),
                levanta_exatamente(ParseError, match=f"No indicators found for 'arroz'. {motivo}"),
            ):
                parser.parse_indicador(html, "arroz")
