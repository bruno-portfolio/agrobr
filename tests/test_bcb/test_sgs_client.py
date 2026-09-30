from __future__ import annotations

import hashlib
import json
import re
from datetime import date

import httpx
import pytest

from agrobr.bcb import sgs_client, sgs_query
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

AUSENCIA_404 = (
    "SGS declarou ausência de valores (HTTP 404); esse envelope não comprova existência ou "
    "validade do código da série."
)


def selection(codigo=1, **kwargs):
    return sgs_query.build_query(codigo, reference_date=date(2026, 9, 7), **kwargs)


def referencias(result, antes: int, depois: int) -> str:
    return (
        f"Bloco SGS {result.resources[0].block.id}: {antes} referências anteriores e {depois} "
        "posteriores aos limites diários foram preservadas; a resposta não informa frequência "
        "ou regras de seleção da série."
    )


def segundo_bloco(resposta):
    def override(request, _index):
        if request.url.params["dataInicial"] == "01/01/2020":
            return resposta(request)
        return None

    return override


async def test_replay_long_history_uses_two_exact_official_blocks(sgs_http, sgs_captures):
    requests = sgs_http()
    with sem_excecao():
        result = await sgs_client.fetch_sgs_acquisition(
            selection(data_inicial="01/01/2010", data_final="31/12/2024")
        )
    assert len(requests) == 2
    assert len(result.records) == 3767
    actual = {row.data.isoformat(): row.valor for row in result.records}
    expected = {key: float(value) for key, value in sgs_captures["oracles"]["union"].items()}
    assert actual == expected
    assert (result.warnings, result.reconciliation) == ([], [])
    assert result.coverage.completeness == "unknown"
    assert result.coverage.request_status == "all_blocks_succeeded"
    assert result.coverage.expected_count is None
    assert (
        result.coverage.received_count
        == result.coverage.unique_count
        == result.coverage.returned_count
        == 3767
    )
    assert result.coverage.reconciled_count == 0
    assert {resource.sha256 for resource in result.resources} == {
        hashlib.sha256(sgs_captures["bodies"][name]).hexdigest()
        for name in ["part2010_2019", "part2020_2024"]
    }
    assert all(
        resource.fetched_at.utcoffset().total_seconds() == 0 for resource in result.resources
    )


async def test_falha_em_qualquer_bloco_aborta_a_serie_com_o_motivo(sgs_http):
    historico = {"data_inicial": "01/01/2010", "data_final": "31/12/2024"}
    cenarios = [
        *(
            (
                segundo_bloco(
                    lambda _request, status=status: httpx.Response(status, json={"error": "x"})
                ),
                historico,
                SourceUnavailableError,
                motivo,
            )
            for status, motivo in [
                (401, "HTTP 401"),
                (403, "HTTP 403"),
                (404, "HTTP 404"),
                (429, "HTTP 429 after 2 attempts"),
                (500, "HTTP 500 after 2 attempts"),
                (599, "HTTP 599"),
            ]
        ),
        *(
            (
                segundo_bloco(lambda _request, body=body: httpx.Response(200, content=body)),
                historico,
                ParseError,
                motivo,
            )
            for body, motivo in [
                (b"{}", "Resposta SGS deve ser uma lista de observações"),
                (b"null", "Resposta SGS deve ser uma lista de observações"),
                (b"<html>bad</html>", "Resposta SGS não é JSON válido"),
                (b"bad JSON", "Resposta SGS não é JSON válido"),
                (
                    b'[{"data":"01/01/2024","valor":"bad"}]',
                    "Observação SGS inválida na linha 1; campos: ['valor']",
                ),
            ]
        ),
        (
            lambda request, _index: httpx.ReadTimeout("synthetic timeout", request=request),
            {"codigo": 999999999, "data_inicial": "01/01/2024", "data_final": "02/01/2024"},
            SourceUnavailableError,
            "ReadTimeout: synthetic timeout after 2 attempts",
        ),
        (
            lambda _request, _index: httpx.Response(
                200,
                json=[
                    {"data": "01/01/2024", "valor": "invalid"},
                    {"data": "02/01/2024", "valor": "1.2"},
                ],
            ),
            {"data_inicial": "01/01/2024", "data_final": "02/01/2024", "ultimos": 1},
            ParseError,
            "Observação SGS inválida na linha 1; campos: ['valor']",
        ),
        *(
            (
                lambda _request, _index, payload=payload: httpx.Response(404, json=payload),
                {"ultimos": 3},
                SourceUnavailableError,
                "HTTP 404",
            )
            for payload in [
                {"erro": {"statusCode": 404, "detail": "generic missing resource"}},
                {"erro": {"statusCode": "404", "detail": "Value(s) not found"}},
                {"error": "Value(s) not found"},
            ]
        ),
    ]
    with collect_failures() as check:
        for indice, (resposta, argumentos, erro, motivo) in enumerate(cenarios):
            requests = sgs_http(resposta)
            with check(indice), levanta_exatamente(erro, match=re.escape(motivo)):
                await sgs_client.fetch_sgs_acquisition(selection(**argumentos))
            if argumentos is historico:
                assert [r.url.params["dataInicial"] for r in requests][:1] == ["01/01/2010"]
                assert any(r.url.params["dataInicial"] == "01/01/2020" for r in requests)
            if erro is SourceUnavailableError and "timeout" in motivo:
                assert len(requests) == 2


async def test_selecao_recusada_pela_fonte_e_consulta_adulterada(sgs_http):
    with collect_failures() as check:
        for argumentos, motivo in [
            (
                {"data_final": "31/12/2024"},
                "O sistema aceita uma janela de consulta de, no máximo, 10 anos em séries de "
                "periodicidade diária",
            ),
            ({"ultimos": 21}, "A quantidade máxima de valores deve ser 20"),
        ]:
            requests = sgs_http()
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)) as capturada,
            ):
                await sgs_client.fetch_sgs_acquisition(selection(**argumentos))
            assert "br.gov.bcb" not in str(capturada.value)
            assert len(requests) == 1
    query = selection()
    query.fim = date(1999, 1, 1)
    requests = sgs_http()
    with levanta_exatamente(InvalidParameterError, match="Seleção SGS inválida"):
        await sgs_client.fetch_sgs_acquisition(query)
    assert requests == []


async def test_404_conhecido_e_vazio_valido_nao_inventam_observacoes(sgs_http, sgs_captures):
    sgs_http()
    with sem_excecao():
        ausente = await sgs_client.fetch_sgs_acquisition(
            selection(data_inicial="06/01/2024", data_final="07/01/2024")
        )
    assert ausente.records == [] and ausente.warnings == [AUSENCIA_404]
    assert ausente.resources[0].status_code == 404
    assert (
        ausente.resources[0].sha256
        == hashlib.sha256(sgs_captures["bodies"]["weekend_sdk"]).hexdigest()
    )
    assert (ausente.coverage.completeness, ausente.coverage.returned_count) == ("unknown", 0)
    sgs_http(lambda _request, _index: httpx.Response(200, json=[]))
    with sem_excecao():
        vazio = await sgs_client.fetch_sgs_acquisition(
            selection(data_inicial="01/01/2024", data_final="02/01/2024")
        )
    assert (vazio.records, vazio.warnings) == ([], [])
    assert (vazio.coverage.completeness, vazio.coverage.returned_count) == ("unknown", 0)


async def test_replay_latest_route_keeps_dates_omitted(sgs_http, sgs_captures):
    requests = sgs_http()
    with sem_excecao():
        result = await sgs_client.fetch_sgs_acquisition(selection(ultimos=3))
    expected = json.loads(sgs_captures["bodies"]["last3_sdk"])
    assert [row.valor for row in result.records] == [float(row["valor"]) for row in expected]
    assert "/dados/ultimos/3" in requests[0].url.path
    assert set(requests[0].url.params) == {"formato"}
    assert len(requests) == 1 and result.warnings == []


async def test_range_with_ultimos_selects_global_tail_after_two_blocks(sgs_http, sgs_captures):
    requests = sgs_http()
    with sem_excecao():
        result = await sgs_client.fetch_sgs_acquisition(
            selection(data_inicial="01/01/2010", data_final="31/12/2024", ultimos=1300)
        )
    expected = sorted(sgs_captures["oracles"]["union"])[-1300:]
    assert [row.data.isoformat() for row in result.records] == expected
    assert result.coverage.unique_count == 3767
    assert result.coverage.returned_count == 1300
    assert result.coverage.tail_applied
    assert len(requests) == 2
    assert all("/ultimos/" not in request.url.path for request in requests)


async def test_referencias_fora_da_janela_sao_preservadas_com_diagnostico(sgs_http):
    sgs_http()
    with sem_excecao():
        mensal = await sgs_client.fetch_sgs_acquisition(
            selection(433, data_inicial="02/01/2024", data_final="31/01/2024")
        )
    assert (mensal.records[0].data, mensal.records[0].valor) == (date(2024, 1, 1), 0.42)
    assert mensal.resources[0].reference_diagnostics.before_count == 1
    assert mensal.warnings == [referencias(mensal, 1, 0)]
    assert mensal.coverage.completeness == "unknown"
    sgs_http(
        lambda _request, _index: httpx.Response(200, json=[{"data": "02/01/2024", "valor": "1"}])
    )
    with sem_excecao():
        posterior = await sgs_client.fetch_sgs_acquisition(
            selection(999999999, data_inicial="01/01/2024", data_final="01/01/2024")
        )
    assert posterior.records[0].data == date(2024, 1, 2)
    assert posterior.resources[0].reference_diagnostics.after_count == 1
    assert posterior.warnings == [referencias(posterior, 0, 1)]


@pytest.mark.parametrize(
    "primeiro,segundo",
    [("0.21", "0.21"), (None, None), ("0.21", "0.22"), ("0.21", None), (None, "0")],
)
async def test_referencia_repetida_entre_blocos_so_reconcilia_valor_igual(
    primeiro, segundo, sgs_http
):
    sgs_http(
        lambda _request, index: httpx.Response(
            200, json=[{"data": "01/01/2020", "valor": primeiro if index == 1 else segundo}]
        )
    )
    query = selection(999999999, data_inicial="01/01/2010", data_final="31/12/2024")
    if primeiro != segundo:
        with levanta_exatamente(
            ParseError, match=re.escape("Valores conflitantes entre blocos SGS: 2020-01-01")
        ):
            await sgs_client.fetch_sgs_acquisition(query)
        return
    with sem_excecao():
        result = await sgs_client.fetch_sgs_acquisition(query)
    assert [(row.data, row.valor) for row in result.records] == [
        (date(2020, 1, 1), None if primeiro is None else float(primeiro))
    ]
    assert (
        result.coverage.received_count,
        result.coverage.unique_count,
        result.coverage.reconciled_count,
    ) == (2, 1, 1)
    assert result.warnings == [
        referencias(result, 0, 1),
        "1 repetições idênticas entre blocos SGS foram reconciliadas; todas as origens estão "
        "registradas em reconciliation.",
    ]
    origins = result.reconciliation[0].origins
    assert [(origin.resource_index, origin.row_index, origin.index_base) for origin in origins] == [
        (0, 0, 0),
        (1, 0, 0),
    ]
    assert {origin.block_id for origin in origins} == {
        resource.block.id for resource in result.resources
    }
