from __future__ import annotations

import json
import re
from datetime import date
from urllib.parse import quote

import httpx
import pytest

from agrobr.bcb import ptax_client, ptax_query
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao
from tests.test_bcb.ptax_replay import encode

CONTINUACAO = "Continuação ou redirect PTAX altera origem, rota ou seleção"


def selection(**kwargs):
    return ptax_query.build_query(data="04/09/2026", reference_date=date(2026, 9, 7), **kwargs)


def next_url(request, skip, **changes):
    params = dict(request.url.params)
    params["$skip"] = str(skip)
    params.update(changes)
    encoded = "&".join(f"{key}={quote(value, safe=chr(39))}" for key, value in params.items())
    return str(request.url).split("?", 1)[0] + "?" + encoded


@pytest.mark.parametrize(
    "boletim,count", [("todos", 5), ("fechamento", 1), ("abertura", 1), ("intermediario", 3)]
)
async def test_all_bulletins_acquired_before_local_selection(
    boletim, count, ptax_http, ptax_captures
):
    body = ptax_captures["bodies"]["usd_day"]
    trace = ptax_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    with sem_excecao():
        result = await ptax_client.fetch_ptax_acquisition(selection(boletim=boletim))
    assert len(result.records) == count and result.warnings == []
    assert result.coverage.received_count == 5
    assert result.coverage.returned_count == count
    assert result.coverage.filtered_by_boletim_count == 5 - count
    assert result.coverage.completeness == "unknown"
    assert result.coverage.terminal_empty_page
    assert len(trace["catalog"]) == 2 and len(trace["quotes"]) == 2
    assert all(row.role == "catalog" for row in result.catalog.resources)
    assert all(row.role == "quotes" for row in result.resources)


async def test_catalog_top_is_independent_and_short_quotes_advance_by_received_count(
    ptax_http, ptax_captures
):
    rows = json.loads(ptax_captures["bodies"]["usd_day"])["value"]

    def respond(request, _index):
        start = int(request.url.params["$skip"])
        return httpx.Response(200, content=encode(rows[start : start + 2]))

    trace = ptax_http(respond)
    with sem_excecao():
        result = await ptax_client.fetch_ptax_acquisition(selection(boletim="todos", top=3))
    assert len(result.records) == 5
    assert [request.url.params["$skip"] for request in trace["quotes"]] == ["0", "2", "4", "5"]
    assert {request.url.params["$top"] for request in trace["catalog"]} == {"1000"}
    assert {request.url.params["$top"] for request in trace["quotes"]} == {"3"}


async def test_catalogo_ausente_ou_vazio_recusa_a_moeda_antes_das_cotacoes(ptax_http):
    with collect_failures() as check:
        for simbolo in ["ZZZ", "ARS"]:
            trace = ptax_http()
            motivo = (
                f"Moeda {simbolo} ausente no catálogo PTAX atual; isso não determina sua "
                "validade histórica"
            )
            with check(simbolo), levanta_exatamente(InvalidParameterError, match=re.escape(motivo)):
                await ptax_client.fetch_ptax_acquisition(selection(moeda=simbolo))
            assert trace["catalog"] and trace["quotes"] == []
    trace = ptax_http(catalog=lambda _request, _index: httpx.Response(200, content=encode([])))
    with levanta_exatamente(
        SourceUnavailableError,
        match=re.escape("Catálogo PTAX vazio impede validar a moeda solicitada"),
    ):
        await ptax_client.fetch_ptax_acquisition(selection())
    assert len(trace["catalog"]) == 1 and trace["quotes"] == []
    query = selection()
    query.moeda = "EUR"
    trace = ptax_http()
    with levanta_exatamente(
        InvalidParameterError, match="Seleção PTAX incompatível com parâmetros validados"
    ):
        await ptax_client.fetch_ptax_acquisition(query)
    assert trace["all"] == []


async def test_falhas_de_rede_abortam_sem_devolver_pagina_parcial(ptax_http, quote_row):
    quote_row["tipoBoletim"] = "Fechamento PTAX"
    with collect_failures() as check:
        for status, motivo in [
            (400, "HTTP 400"),
            (401, "HTTP 401"),
            (403, "HTTP 403"),
            (404, "HTTP 404"),
            (429, "HTTP 429 after 4 retries"),
            (500, "HTTP 500 after 4 retries"),
            (599, "HTTP 599"),
        ]:
            ptax_http(
                lambda _request, index, status=status: (
                    httpx.Response(200, content=encode([quote_row]))
                    if index == 1
                    else httpx.Response(status, content=b"synthetic failure")
                )
            )
            with (
                check(status),
                levanta_exatamente(SourceUnavailableError, match=f"unavailable: {motivo}$"),
            ):
                await ptax_client.fetch_ptax_acquisition(selection(top=1))
    trace = ptax_http(
        catalog=lambda request, _index: httpx.ReadTimeout("synthetic timeout", request=request)
    )
    with levanta_exatamente(
        SourceUnavailableError, match=re.escape("ReadTimeout: synthetic timeout after 4 retries")
    ):
        await ptax_client.fetch_ptax_acquisition(selection())
    assert len(trace["catalog"]) == 4 and trace["quotes"] == []


@pytest.mark.parametrize("label", ["Novo boletim", None])
@pytest.mark.parametrize("boletim", ["todos", "fechamento", "abertura", "intermediario"])
async def test_unknown_bulletin_is_retained_only_without_specific_filter(
    label, boletim, ptax_http, quote_row
):
    quote_row["tipoBoletim"] = label
    ptax_http(
        lambda _request, index: httpx.Response(
            200, content=encode([quote_row] if index == 1 else [])
        )
    )
    if boletim == "todos":
        with sem_excecao():
            result = await ptax_client.fetch_ptax_acquisition(selection(boletim=boletim))
        assert len(result.records) == 1 and result.records[0].tipo_boletim == label
        assert len(result.warnings) == 1
        assert "tipo_boletim ausente ou não reconhecido" in result.warnings[0]
    else:
        with levanta_exatamente(
            ParseError,
            match=re.escape("Boletim PTAX desconhecido ou nulo impede aplicar filtro específico"),
        ):
            await ptax_client.fetch_ptax_acquisition(selection(boletim=boletim))


async def test_paginas_incoerentes_sao_recusadas_com_o_motivo(ptax_http, quote_row):
    segunda = dict(quote_row, dataHoraCotacao="2026-09-04 23:00:00")

    def contagem(pagina1, pagina2):
        def respond(_request, index):
            linhas, total = pagina1 if index == 1 else pagina2
            return httpx.Response(200, content=encode(linhas, **{"@odata.count": total}))

        return respond

    def continua(link, total=None, linhas=None):
        def respond(request, _index):
            anotacoes = {"@odata.nextLink": link(request)}
            if total is not None:
                anotacoes["@odata.count"] = total
            corpo = [quote_row] if linhas is None else linhas
            return httpx.Response(200, content=encode(corpo, **anotacoes))

        return respond

    cenarios = [
        (
            lambda _r, _i: httpx.Response(
                200, content=encode([dict(quote_row, cotacaoCompra="x")])
            ),
            {},
            "Cotação PTAX inválida na linha 1",
        ),
        *(
            (
                lambda _r, i, second=second: httpx.Response(
                    200, content=encode([quote_row if i == 1 else second])
                ),
                {"top": 1, "boletim": "todos"},
                "Chave PTAX repetida dentro ou entre páginas",
            )
            for second in [dict(quote_row), dict(quote_row, cotacaoCompra=999.0)]
        ),
        *(
            (
                lambda _r, _i, stamp=stamp: httpx.Response(
                    200, content=encode([dict(quote_row, dataHoraCotacao=stamp)])
                ),
                {"boletim": "todos"},
                "Cotação PTAX fora do intervalo solicitado",
            )
            for stamp in ["2026-09-03 13:00:00", "2026-09-05 13:00:00"]
        ),
        (
            lambda _r, i: httpx.Response(
                200,
                content=encode(
                    [
                        dict(
                            quote_row,
                            dataHoraCotacao="2026-09-04 13:00:00"
                            if i == 1
                            else "2026-09-04 12:00:00",
                        )
                    ]
                ),
            ),
            {"top": 1, "boletim": "todos"},
            "Ordem crescente dos horários PTAX foi violada",
        ),
        (
            contagem(([quote_row, segunda], 1), ([], 1)),
            {"top": 2, "boletim": "todos"},
            "Registros PTAX excedem a contagem declarada",
        ),
        (
            contagem(([quote_row], 2), ([segunda], 3)),
            {"top": 2, "boletim": "todos"},
            "Contagem PTAX mudou entre páginas",
        ),
        (
            contagem(([quote_row], 2), ([], 2)),
            {"top": 2, "boletim": "todos"},
            "Página PTAX vazia antes da contagem declarada",
        ),
        (
            continua(lambda request: next_url(request, 0), linhas=[]),
            {"top": 1, "boletim": "todos"},
            "Página PTAX vazia com continuação sem avanço",
        ),
        (
            continua(lambda request: next_url(request, 1), total=1),
            {"top": 2, "boletim": "todos"},
            "Contagem PTAX esgotada mas nextLink indica continuação",
        ),
        *(
            (continua(link), {"top": 1, "boletim": "todos"}, CONTINUACAO)
            for link in [
                lambda r: next_url(r, 1).replace("olinda.bcb.gov.br", "evil.example"),
                lambda r: next_url(r, 1, **{"@m": "'EUR'"}),
                lambda r: next_url(r, 1, **{"@d": "'09-05-2026'"}),
                lambda r: next_url(r, 1, **{"$top": "2"}),
                lambda r: next_url(r, 1) + "&$skip=1",
                lambda r: next_url(r, 0),
                lambda _r: "?$skip=1",
            ]
        ),
    ]
    with collect_failures() as check:
        for indice, (resposta, argumentos, motivo) in enumerate(cenarios):
            trace = ptax_http(resposta)
            with check(indice), levanta_exatamente(ParseError, match=re.escape(motivo)):
                await ptax_client.fetch_ptax_acquisition(selection(**argumentos))
            if motivo == CONTINUACAO:
                assert len(trace["quotes"]) == 1
    trace = ptax_http(
        catalog=lambda _request, _index: httpx.Response(
            302, headers={"Location": "https://evil.example/next"}
        )
    )
    with levanta_exatamente(ParseError, match=re.escape(CONTINUACAO)):
        await ptax_client.fetch_ptax_acquisition(selection())
    assert len(trace["all"]) == 1
    ptax_http(
        catalog=lambda _request, _index: httpx.Response(
            200, content=encode([{"simbolo": "USD", "nomeFormatado": "Dólar", "tipoMoeda": "A"}])
        )
    )
    with levanta_exatamente(
        ParseError, match=re.escape("Chave PTAX repetida dentro ou entre páginas")
    ):
        await ptax_client.fetch_currencies_acquisition(ptax_query.build_catalog_query(top=1))


async def test_contagem_e_continuacao_validas_fecham_a_selecao(ptax_http, ptax_captures, quote_row):
    rows = json.loads(ptax_captures["bodies"]["usd_day"])["value"]
    trace = ptax_http(
        lambda _request, _index: httpx.Response(200, content=encode(rows, **{"@odata.count": 5}))
    )
    with sem_excecao():
        result = await ptax_client.fetch_ptax_acquisition(selection())
    assert result.coverage.completeness == "complete"
    assert result.coverage.expected_count == result.coverage.received_count == 5
    assert result.coverage.returned_count == 1 and result.coverage.filtered_by_boletim_count == 4
    assert len(trace["quotes"]) == 1

    def respond(request, index):
        if index == 1:
            return httpx.Response(
                200, content=encode([quote_row], **{"@odata.nextLink": next_url(request, 1)})
            )
        return httpx.Response(200, content=encode([]))

    trace = ptax_http(respond)
    with sem_excecao():
        result = await ptax_client.fetch_ptax_acquisition(selection(top=1, boletim="todos"))
    assert len(result.records) == 1 and len(trace["quotes"]) == 2
    currencies = json.loads(ptax_captures["bodies"]["currencies"])["value"]
    trace = ptax_http(
        catalog=lambda request, _index: httpx.Response(
            200,
            content=encode(
                currencies[int(request.url.params["$skip"]) : int(request.url.params["$skip"]) + 3]
            ),
        )
    )
    with sem_excecao():
        catalogo = await ptax_client.fetch_currencies_acquisition(
            ptax_query.build_catalog_query(top=3)
        )
    assert len(catalogo.records) == 10
    assert [request.url.params["$skip"] for request in trace["catalog"]] == [
        "0",
        "3",
        "6",
        "9",
        "10",
    ]
    assert catalogo.coverage.completeness == "unknown" and catalogo.coverage.terminal_empty_page
