from __future__ import annotations

import json
import re
from urllib.parse import quote

import httpx
import pytest

from agrobr.bcb import focus_client, focus_query
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao
from tests.test_bcb.focus_replay import encode

CONTINUACAO = "Continuação ou redirect Focus altera origem, entidade ou seleção"


def selection(**kwargs):
    return focus_query.build_query("Balança comercial", data_inicial="2026-08-28", **kwargs)


def next_url(request, skip, **changes):
    params = dict(request.url.params)
    params["$skip"] = str(skip)
    params.update(changes)
    encoded = "&".join(f"{key}={quote(value, safe=chr(39))}" for key, value in params.items())
    return str(request.url).split("?", 1)[0] + "?" + encoded


@pytest.mark.parametrize(
    "periodicidade,indicador", [("anual", "Balança comercial"), ("mensal", "IPCA")]
)
async def test_exact_captured_api_selection_has_local_limit_without_fake_total(
    periodicidade, indicador, focus_http
):
    requests = focus_http()
    query = focus_query.build_query(
        indicador, periodicidade=periodicidade, data_inicial="2026-08-28", top=6, max_registros=6
    )
    with sem_excecao():
        result = await focus_client.fetch_focus_acquisition(query)
    assert len(requests) == 1 and len(result.records) == 6
    assert result.coverage.completeness == "unknown"
    assert result.coverage.expected_count is None
    assert result.coverage.local_limit_reached
    assert result.coverage.received_count == result.coverage.returned_count == 6
    assert result.coverage.discarded_by_local_limit == 0
    assert result.resources[0].received_count == result.resources[0].retained_count == 6
    assert result.resources[0].page_index == result.resources[0].index_base == 0
    assert len(result.warnings) == 1


async def test_short_page_advances_by_received_rows_until_explicit_empty(
    focus_http, focus_captures
):
    rows = json.loads(focus_captures["bodies"]["annual_api_ge"])["value"]

    def respond(request, _index):
        skip = int(request.url.params["$skip"])
        return httpx.Response(200, content=encode(rows[skip : skip + 2]))

    requests = focus_http(respond)
    with sem_excecao():
        result = await focus_client.fetch_focus_acquisition(selection(top=100))
    assert [request.url.params["$skip"] for request in requests] == ["0", "2", "4", "6"]
    assert len(result.records) == 6 and result.warnings == []
    assert result.coverage.completeness == "unknown"
    assert result.coverage.terminal_empty_page and result.coverage.pages_fetched == 4
    assert result.coverage.expected_count is None


@pytest.mark.parametrize(
    "maximum,received,state,discarded",
    [(2, 6, "partial", 4), (6, 6, "unknown", 0), (7, 6, "unknown", 0)],
)
async def test_local_limit_distinguishes_discarded_evidence_from_exact_unknown(
    maximum, received, state, discarded, focus_http, focus_captures
):
    body = focus_captures["bodies"]["annual_api_ge"]
    requests = focus_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    with sem_excecao():
        result = await focus_client.fetch_focus_acquisition(selection(top=6, max_registros=maximum))
    assert result.coverage.received_count == received
    assert result.coverage.returned_count == min(maximum, received)
    assert result.coverage.discarded_by_local_limit == discarded
    assert result.resources[0].retained_count == min(maximum, received)
    assert result.coverage.completeness == state
    assert len(requests) == (2 if maximum > received else 1)


async def test_falhas_de_rede_abortam_sem_devolver_pagina_parcial(focus_http, annual_row):
    with collect_failures() as check:
        for status, motivo in [
            (400, "HTTP 400"),
            (401, "HTTP 401"),
            (403, "HTTP 403"),
            (404, "HTTP 404"),
            (429, "HTTP 429 after 3 attempts"),
            (500, "HTTP 500 after 3 attempts"),
            (599, "HTTP 599"),
        ]:
            focus_http(
                lambda _request, index, status=status: (
                    httpx.Response(200, content=encode([annual_row]))
                    if index == 1
                    else httpx.Response(status, content=b"synthetic failure")
                )
            )
            with (
                check(status),
                levanta_exatamente(SourceUnavailableError, match=f"unavailable: {motivo}$"),
            ):
                await focus_client.fetch_focus_acquisition(selection(top=1))
    requests = focus_http(
        lambda request, _index: httpx.ReadTimeout("synthetic timeout", request=request)
    )
    with levanta_exatamente(
        SourceUnavailableError, match=re.escape("ReadTimeout: synthetic timeout after 3 attempts")
    ):
        await focus_client.fetch_focus_acquisition(selection())
    assert 1 < len(requests) <= 3


async def test_paginas_incoerentes_sao_recusadas_com_o_motivo(focus_http, annual_row):
    segunda = dict(annual_row, baseCalculo=1)

    def paginas(primeira, outra=None):
        return lambda _request, index: httpx.Response(
            200, content=primeira if index == 1 or outra is None else outra
        )

    def contagem(scenario):
        def respond(_request, index):
            if scenario == "union_exceeds_count":
                return httpx.Response(
                    200, content=encode([annual_row, segunda], **{"@odata.count": 1})
                )
            if index == 1:
                return httpx.Response(200, content=encode([annual_row], **{"@odata.count": 2}))
            rows = [] if scenario == "empty_below_count" else [segunda]
            total = 2 if scenario == "empty_below_count" else 3
            return httpx.Response(200, content=encode(rows, **{"@odata.count": total}))

        return respond

    def continua(link, linhas=None, **anotacoes):
        return lambda request, _index: httpx.Response(
            200,
            content=encode(
                [annual_row] if linhas is None else linhas,
                **{"@odata.nextLink": link(request), **anotacoes},
            ),
        )

    envelope = "Envelope Focus inválido: exige objeto com value lista e anotações tipadas"
    cenarios = [
        *(
            (paginas(encode([annual_row]), body), {"top": 1}, envelope)
            for body in [b"{}", b'{"value":null}', b"<html>bad</html>", b"[]"]
        ),
        *(
            (
                paginas(encode([annual_row]), encode([second])),
                {"top": 1, "max_registros": 2},
                "Chave de observação Focus repetida dentro ou entre páginas",
            )
            for second in [dict(annual_row), dict(annual_row, Media=999.0)]
        ),
        *(
            (
                paginas(encode([annual_row]), encode([dict(annual_row, baseCalculo=1, **mudanca)])),
                {"top": 1, "max_registros": 2},
                motivo,
            )
            for mudanca, motivo in [
                (
                    {"Indicador": "IPCA"},
                    "Registro Focus incompatível com indicador ou periodicidade solicitados",
                ),
                ({"Data": "2026-08-27"}, "Registro Focus anterior à data inicial solicitada"),
                ({"Data": "2026-08-29"}, "Ordem decrescente das datas Focus foi violada"),
            ]
        ),
        (
            paginas(encode([annual_row, dict(annual_row, baseCalculo=1, Media="invalid")])),
            {"top": 2, "max_registros": 1},
            "Observação Focus inválida na linha 2",
        ),
        (
            paginas(encode([annual_row, segunda])),
            {"top": 1},
            "Página Focus excede top solicitado",
        ),
        (contagem("count_changes"), {"top": 2}, "Contagem Focus mudou entre páginas"),
        (
            contagem("empty_below_count"),
            {"top": 2},
            "Página Focus vazia antes da contagem declarada",
        ),
        (
            contagem("union_exceeds_count"),
            {"top": 2},
            "Registros Focus excedem a contagem declarada",
        ),
        (
            continua(lambda request: next_url(request, 0), []),
            {"top": 1},
            "Página Focus vazia com continuação sem avanço",
        ),
        (
            continua(lambda request: next_url(request, 1), **{"@odata.count": 1}),
            {"top": 1},
            "Contagem Focus esgotada mas nextLink indica continuação",
        ),
        *(
            (continua(link), {"top": 1}, CONTINUACAO)
            for link in [
                lambda r: next_url(r, 1).replace("olinda.bcb.gov.br", "evil.example"),
                lambda r: next_url(r, 1, **{"$filter": "Indicador eq 'IPCA'"}),
                lambda r: next_url(r, 1).replace(
                    "ExpectativasMercadoAnuais", "ExpectativaMercadoMensais"
                ),
                lambda r: next_url(r, 1) + "&$skip=1",
                lambda r: next_url(r, 0),
                lambda _r: "?$skip=1",
            ]
        ),
        (
            lambda _request, _index: httpx.Response(
                302, headers={"Location": "https://evil.example/next"}
            ),
            {},
            CONTINUACAO,
        ),
    ]
    with collect_failures() as check:
        for indice, (resposta, argumentos, motivo) in enumerate(cenarios):
            requests = focus_http(resposta)
            with check(indice), levanta_exatamente(ParseError, match=re.escape(motivo)):
                await focus_client.fetch_focus_acquisition(selection(**argumentos))
            if motivo in {CONTINUACAO, "Contagem Focus esgotada mas nextLink indica continuação"}:
                assert len(requests) == 1
    query = selection()
    query.filter = "Indicador eq 'IPCA'"
    requests = focus_http()
    with levanta_exatamente(
        InvalidParameterError, match="Seleção Focus incompatível com parâmetros validados"
    ):
        await focus_client.fetch_focus_acquisition(query)
    assert requests == []


async def test_contagem_e_continuacao_validas_fecham_a_selecao(focus_http, annual_row):
    rows = [annual_row, dict(annual_row, baseCalculo=1)]
    for total, maximum, state in [(2, None, "complete"), (2, 2, "complete"), (2, 1, "partial")]:
        focus_http(
            lambda _request, _index, total=total: httpx.Response(
                200, content=encode(rows, **{"@odata.count": total})
            )
        )
        with sem_excecao():
            result = await focus_client.fetch_focus_acquisition(
                selection(top=2, max_registros=maximum)
            )
        assert (result.coverage.expected_count, result.coverage.completeness) == (total, state)
        assert result.coverage.returned_count == (maximum or total)

    def respond(request, index):
        if index == 1:
            return httpx.Response(
                200, content=encode([annual_row], **{"@odata.nextLink": next_url(request, 1)})
            )
        return httpx.Response(200, content=encode([]))

    requests = focus_http(respond)
    with sem_excecao():
        result = await focus_client.fetch_focus_acquisition(selection(top=1))
    assert len(requests) == 2 and requests[1].url.params["$skip"] == "1"
    assert result.coverage.terminal_empty_page
    focus_http(
        lambda request, _index: httpx.Response(
            200, content=encode([annual_row], **{"@odata.nextLink": next_url(request, 1)})
        )
    )
    with sem_excecao():
        parcial = await focus_client.fetch_focus_acquisition(selection(top=1, max_registros=1))
    assert parcial.coverage.completeness == "partial"
    assert parcial.coverage.next_link_remaining
    assert parcial.coverage.discarded_by_local_limit == 0


async def test_equal_statistical_warnings_keep_distinct_page_origins(focus_http, annual_row):
    annual_row["DesvioPadrao"] = -1.0

    def respond(_request, index):
        rows = [dict(annual_row, baseCalculo=index - 1)] if index <= 2 else []
        return httpx.Response(200, content=encode(rows))

    focus_http(respond)
    with sem_excecao():
        result = await focus_client.fetch_focus_acquisition(selection(top=1))
    assert len(result.warnings) == 2
    assert "Página Focus 0 (offset 0)" in result.warnings[0]
    assert "Página Focus 1 (offset 1)" in result.warnings[1]
    assert [resource.offset for resource in result.resources] == [0, 1, 2]
