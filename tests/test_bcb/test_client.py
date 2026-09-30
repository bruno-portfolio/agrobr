"""Testes de resiliência HTTP para agrobr.bcb.client."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from agrobr.bcb import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    collect_failures,
    levanta_exatamente,
    make_mock_async_client,
    make_mock_response,
    make_sleep_tracker,
    sem_excecao,
)

SAFRA_2023 = (
    "((AnoEmissao eq '2023' and MesEmissao ge '07') or "
    "(AnoEmissao eq '2024' and MesEmissao lt '07'))"
)


def test_safra_vai_de_julho_a_junho():
    dentro = [("2023", "07"), ("2023", "12"), ("2024", "01"), ("2024", "06")]
    fora = [("2023", "06"), ("2024", "07")]
    assert [
        client._pertence_a_safra({"AnoEmissao": a, "MesEmissao": m}, 2023) for a, m in dentro
    ] == [True] * 4
    assert [
        client._pertence_a_safra({"AnoEmissao": a, "MesEmissao": m}, 2023) for a, m in fora
    ] == [False] * 2
    assert not client._pertence_a_safra({"MesEmissao": "07"}, 2023)
    assert not client._pertence_a_safra({"AnoEmissao": "x", "MesEmissao": "07"}, 2023)


async def test_olinda_repete_falhas_transitorias_e_recusa_as_definitivas():
    for falha in [httpx.TimeoutException("read timeout"), make_mock_response(500, json_data={})]:
        mock_client = make_mock_async_client()
        if isinstance(falha, Exception):
            mock_client.get.side_effect = falha
        else:
            mock_client.get = AsyncMock(return_value=falha)
        sleep_calls, track_sleep = make_sleep_tracker()
        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            levanta_exatamente(SourceUnavailableError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")
        assert mock_client.get.call_count == client.BCB_MAX_RETRIES
        assert len(sleep_calls) == client.BCB_MAX_RETRIES - 1
        assert all(depois > antes for antes, depois in zip(sleep_calls, sleep_calls[1:]))
    mock_client = make_mock_async_client()
    mock_client.get = AsyncMock(
        side_effect=[
            make_mock_response(429, json_data={"value": []}),
            make_mock_response(200, json_data={"value": [{"id": 1}]}),
        ]
    )
    with (
        patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
        patch(RETRY_SLEEP, new_callable=AsyncMock),
    ):
        assert (await client._fetch_odata("CusteioRegiaoUFProduto", top=1))["value"] == [{"id": 1}]
    url = mock_client.get.await_args.args[0]
    assert "$top=1" in url and "$skip" not in url
    mock_client = make_mock_async_client()
    mock_client.get = AsyncMock(return_value=make_mock_response(403, json_data={"value": []}))
    with (
        patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
        levanta_exatamente(SourceUnavailableError, "HTTP 403"),
    ):
        await client._fetch_odata("CusteioRegiaoUFProduto")


async def test_resposta_sem_registros_devolve_lista_vazia():
    mock_client = make_mock_async_client()
    mock_client.get = AsyncMock(return_value=make_mock_response(200, json_data={"value": []}))
    with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client), sem_excecao():
        assert await client.fetch_credito_rural(finalidade="custeio") == []


async def test_recusas_antes_da_rede():
    with (
        patch.object(client, "_fetch_odata", new_callable=AsyncMock) as mock_fetch,
        collect_failures() as check,
    ):
        for argumentos, motivo in [
            ({"finalidade": "invalida"}, "Finalidade inválida: 'invalida'"),
            ({"cd_uf": "99"}, "Codigo de UF invalido: '99'"),
            ({"safra_sicor": "abc"}, "safra inválida: 'abc'"),
        ]:
            with check(argumentos), levanta_exatamente(ValueError, match=re.escape(motivo)):
                await client.fetch_credito_rural(**argumentos)
    mock_fetch.assert_not_awaited()


async def test_filtros_do_servidor_e_do_cliente():
    golden = Path(__file__).parents[1] / "golden_data" / "bcb" / "custeio_sample"
    records = json.loads((golden / "response.json").read_text(encoding="utf-8"))
    with (
        patch.object(
            client, "_fetch_odata", new_callable=AsyncMock, return_value={"value": records}
        ) as mock_fetch,
        sem_excecao(),
    ):
        result = await client.fetch_credito_rural(
            finalidade="custeio", produto_sicor='"SOJA"', cd_uf="51"
        )
    assert len(result) == 3 and {record["nomeUF"] for record in result} == {"MT"}
    assert mock_fetch.call_args.kwargs["filters"] == [
        "nomeProduto eq '\"SOJA\"'",
        "nomeUF eq 'MT'",
    ]
    fora_da_safra = [
        {"AnoEmissao": "2023", "MesEmissao": "09"},
        {"AnoEmissao": "2024", "MesEmissao": "02"},
        {"AnoEmissao": "2023", "MesEmissao": "03"},
        {"AnoEmissao": "2024", "MesEmissao": "08"},
    ]
    for safra in ["2023/2024", "2023/24", "2024"]:
        with (
            patch.object(
                client,
                "_fetch_odata",
                new_callable=AsyncMock,
                return_value={"value": fora_da_safra},
            ) as mock_fetch,
            sem_excecao(),
        ):
            result = await client.fetch_credito_rural(safra_sicor=safra)
        assert (safra, result) == (safra, fora_da_safra[:2])
        assert mock_fetch.call_args.kwargs["filters"] == [SAFRA_2023]


async def test_limite_da_olinda_fatia_por_mes_e_recusa_mes_cheio():
    mensais = [
        {"AnoEmissao": "2024" if mes < 7 else "2023", "MesEmissao": f"{mes:02d}"}
        for mes in range(1, 13)
    ]
    respostas = [
        {"value": [{"truncated": 1}, {"truncated": 2}]},
        *({"value": [registro]} for registro in mensais),
    ]
    with (
        patch.object(client, "SICOR_RECORD_LIMIT", 2),
        patch.object(
            client, "_fetch_odata", new_callable=AsyncMock, side_effect=respostas
        ) as fetch,
        sem_excecao(),
    ):
        assert await client.fetch_credito_rural(safra_sicor="2023/2024") == mensais
    assert fetch.await_count == 13
    assert fetch.await_args_list[0].kwargs["filters"] == [SAFRA_2023]
    for mes, chamada in enumerate(fetch.await_args_list[1:], start=1):
        assert chamada.kwargs["filters"] == [SAFRA_2023, f"MesEmissao eq '{mes:02d}'"]
        assert chamada.kwargs["top"] == 2
    cheio = {"value": [{"id": 1}, {"id": 2}]}
    with (
        patch.object(client, "SICOR_RECORD_LIMIT", 2),
        patch.object(client, "_fetch_odata", new_callable=AsyncMock, side_effect=[cheio, cheio]),
        levanta_exatamente(SourceUnavailableError, match="volume acima do limite da Olinda"),
    ):
        await client.fetch_credito_rural()


async def test_falha_do_odata_cai_no_bigquery_e_a_dupla_falha_explica_as_duas():
    falhas = [
        SourceUnavailableError(source="bcb", url="test", last_error="timeout"),
        httpx.HTTPStatusError(
            "403", request=httpx.Request("GET", "https://x"), response=httpx.Response(403)
        ),
    ]
    with collect_failures() as check:
        for indice, falha in enumerate(falhas):
            with (
                check(type(falha).__name__),
                patch("agrobr.bcb.client.fetch_credito_rural", AsyncMock(side_effect=falha)),
                patch(
                    "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                    AsyncMock(return_value=[{"id": indice}]),
                ) as bigquery,
                sem_excecao(),
            ):
                assert await client.fetch_credito_rural_with_fallback(
                    finalidade="investimento", produto_sicor='"SOJA"'
                ) == ([{"id": indice}], "bigquery")
                assert bigquery.await_args.kwargs == {
                    "finalidade": "investimento",
                    "produto_sicor": '"SOJA"',
                    "safra_sicor": None,
                    "cd_uf": None,
                }
    with (
        patch(
            "agrobr.bcb.client.fetch_credito_rural",
            AsyncMock(side_effect=SourceUnavailableError(source="bcb", last_error="HTTP 500")),
        ),
        patch(
            "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
            AsyncMock(
                side_effect=SourceUnavailableError(source="bcb_bigquery", last_error="Auth failed")
            ),
        ),
        levanta_exatamente(
            SourceUnavailableError,
            match=re.escape("Ambas as fontes falharam. OData: HTTP 500; BigQuery: Auth failed"),
        ),
    ):
        await client.fetch_credito_rural_with_fallback(finalidade="custeio")
    with (
        patch(
            "agrobr.bcb.client.fetch_credito_rural", AsyncMock(return_value=[{"id": 9}])
        ) as odata,
        sem_excecao(),
    ):
        assert await client.fetch_credito_rural_with_fallback(safra_sicor="2023/2024") == (
            [{"id": 9}],
            "odata",
        )
    assert odata.await_args.kwargs["safra_sicor"] == "2023/2024"
