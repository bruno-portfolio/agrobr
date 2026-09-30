from __future__ import annotations

import re
from datetime import date

import httpx

from agrobr.bcb import focus_query
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

RECUSAS = [
    *(
        ({"indicador": valor}, "indicador deve ser texto não vazio")
        for valor in ["", " ", None, True]
    ),
    *(
        ({"periodicidade": valor}, "periodicidade deve ser anual ou mensal")
        for valor in ["semanal", "trimestral", None]
    ),
    *(({"top": valor}, "top deve ser inteiro positivo") for valor in [True, 1.0, 0, -1]),
    *(
        ({"max_registros": valor}, "max_registros deve ser inteiro positivo")
        for valor in [False, 0, 3.0]
    ),
    *(
        ({"data_inicial": valor}, "inicio contém data inexistente")
        for valor in ["2026-02-30", "2025-02-29"]
    ),
    *(
        (
            {"data_inicial": valor},
            "inicio deve ser date, datetime ou texto AAAA-MM-DD ou DD/MM/AAAA",
        )
        for valor in ["2026-1-01", "２０２６-01-01", 20260101]
    ),
]
CONTINUACOES_INVALIDAS = [
    "https://evil.example/next",
    "http://olinda.bcb.gov.br/next",
    "//evil.example/next",
    "?%24skip=3",
    "?%24skiptoken=opaque",
    "#fragment",
]


def test_consulta_invalida_e_recusada_com_o_motivo():
    with collect_failures() as check:
        for argumentos, motivo in RECUSAS:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                focus_query.build_query(**argumentos)
        query = focus_query.build_query("IPCA", periodicidade="mensal", top=3)
        current = focus_query.build_page_url(query, skip=0)
        for continuacao in CONTINUACOES_INVALIDAS:
            with (
                check(continuacao),
                levanta_exatamente(ParseError, match="altera origem, entidade ou seleção"),
            ):
                focus_query.validate_next_link(query, current, continuacao, skip=3)


def test_rotas_ordem_e_filtro_literal_da_consulta():
    with sem_excecao():
        anual = focus_query.build_query()
        mensal = focus_query.build_query("ipca", periodicidade="mensal", data_inicial="2099-01-01")
        literal = focus_query.build_query("D'Água + X&Y", top=7)
        raw = focus_query.build_page_url(literal, skip=14)
    assert (anual.indicador, anual.entity, anual.periodicidade) == (
        "PIB Agropecuária",
        "ExpectativasMercadoAnuais",
        "anual",
    )
    assert (anual.data_inicial, anual.top, anual.max_registros) == (None, 1000, None)
    assert (mensal.indicador, mensal.entity, mensal.data_inicial) == (
        "ipca",
        "ExpectativaMercadoMensais",
        date(2099, 1, 1),
    )
    assert mensal.order_by == "Data desc,DataReferencia asc,baseCalculo asc"
    url = httpx.URL(raw)
    assert url.params["$filter"] == "Indicador eq 'D''Água + X&Y'"
    assert (
        url.params["$orderby"]
        == "Data desc,DataReferencia asc,baseCalculo asc,IndicadorDetalhe asc"
    )
    assert url.params["$skip"] == "14" and url.params["$top"] == "7"
    assert len(url.params) == 5
    assert "+" not in raw and "%20" in raw and "%2B" in raw
