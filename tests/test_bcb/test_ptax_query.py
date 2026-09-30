from __future__ import annotations

import re
from datetime import date, datetime

import httpx
import pytest

from agrobr.bcb import ptax_query
from agrobr.exceptions import InvalidParameterError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

RECUSAS = [
    *(
        ({"data": valor}, "data deve ser date, datetime ou texto AAAA-MM-DD ou DD/MM/AAAA")
        for valor in ["", "1/09/2026", "０１/09/2026", 20260904]
    ),
    *(({"data": valor}, "data contém data inexistente") for valor in ["31/02/2026", "29/02/2025"]),
    *(
        (
            {"data": "04/09/2026", limite: valor},
            "data não pode ser combinada com limites de período",
        )
        for limite, valor in [("data_inicial", "01/09/2026"), ("data_final", "04/09/2026")]
    ),
    *(
        (argumentos, "Intervalo PTAX invertido")
        for argumentos in [
            {"data_inicial": "05/09/2026", "data_final": "04/09/2026"},
            {"data_inicial": "08/09/2026"},
        ]
    ),
    ({"data_final": "01/01/0001"}, "Período padrão PTAX excede o calendário representável"),
    *(
        ({"moeda": moeda}, "moeda deve conter três letras ASCII, sem espaços")
        for moeda in [" USD", "USD ", "US", "US1", "ÉUR", True]
    ),
    *(
        ({"boletim": boletim}, "boletim deve ser todos, fechamento, abertura ou intermediario")
        for boletim in ["semanal", "inexistente", None]
    ),
    *(({"top": top}, "top deve ser inteiro positivo") for top in [True, 1.0, 0, -1]),
    ({"reference_date": datetime(2026, 9, 7)}, "reference_date deve ser data civil"),
]


def selection(**kwargs):
    return ptax_query.build_query(reference_date=date(2026, 9, 7), **kwargs)


def test_selecao_invalida_e_recusada_com_o_motivo():
    with collect_failures() as check:
        for argumentos, motivo in RECUSAS:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                ptax_query.build_query(**{"reference_date": date(2026, 9, 7), **argumentos})
        for top in [True, 0, -1, 1.0, "3"]:
            with (
                check(("catalogo", top)),
                pytest.raises(InvalidParameterError, match="top deve ser inteiro positivo"),
            ):
                ptax_query.build_catalog_query(top=top)


def test_limites_padrao_e_explicitos_da_selecao():
    with sem_excecao():
        padrao = selection()
        inicio = selection(data_inicial="01/09/2026")
        fim = selection(data_final="01/03/2024")
        futuro = selection(data="04/09/2099", moeda="eur", boletim="todos")
    assert (padrao.inicio, padrao.fim) == (date(2026, 8, 8), date(2026, 9, 7))
    assert (padrao.fim - padrao.inicio).days == 30
    assert (padrao.mode, padrao.boletim, padrao.moeda) == ("periodo", "fechamento", "USD")
    assert set(padrao.defaulted_fields) == {"inicio", "fim"}
    assert (inicio.inicio, inicio.fim) == (date(2026, 9, 1), date(2026, 9, 7))
    assert (inicio.data_inicial, inicio.data_final) == (date(2026, 9, 1), None)
    assert inicio.defaulted_fields == ["fim"]
    assert (fim.inicio, fim.fim) == (date(2024, 1, 31), date(2024, 3, 1))
    assert fim.defaulted_fields == ["inicio"]
    assert futuro.inicio == futuro.fim == date(2099, 9, 4)
    assert (futuro.mode, futuro.defaulted_fields) == ("dia", [])
    assert (futuro.requested_moeda, futuro.moeda) == ("eur", "EUR")


def test_urls_das_cotacoes_e_do_catalogo():
    cotacoes = httpx.URL(ptax_query.build_page_url(selection(data="04/09/2026", top=3), skip=6))
    assert cotacoes.path.endswith("CotacaoMoedaDia(moeda=@m,dataCotacao=@d)")
    assert dict(cotacoes.params) == {
        "@m": "'USD'",
        "@d": "'09-04-2026'",
        "$format": "json",
        "$orderby": "dataHoraCotacao asc,tipoBoletim asc",
        "$top": "3",
        "$skip": "6",
    }
    catalogo = httpx.URL(ptax_query.build_page_url(ptax_query.build_catalog_query(top=3), skip=3))
    assert catalogo.path.endswith("/Moedas")
    assert dict(catalogo.params) == {
        "$format": "json",
        "$orderby": "simbolo asc",
        "$top": "3",
        "$skip": "3",
    }
