"""Testes de resiliência HTTP para agrobr.inmet.client."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.inmet import client
from tests.helpers import (
    levanta_exatamente,
    make_mock_async_client,
    make_mock_response,
    sem_excecao,
)


class TestInmetHTTP204:
    @pytest.mark.asyncio
    async def test_204_returns_empty_list(self):
        resp_204 = make_mock_response(204)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_204)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json("/estacao/A001/2024-01-01/2024-01-10")

        assert result == []

    @pytest.mark.asyncio
    async def test_204_sem_token_em_fetch_dados_raises_com_hint(self):
        resp_204 = make_mock_response(204)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_204)

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="AGROBR_INMET_TOKEN"),
        ):
            await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2024, 1, 10))


class TestInmetToken:
    @pytest.mark.asyncio
    async def test_token_nao_vaza_em_erro(self):
        resp = make_mock_response(200, text="CHAVE INVÁLIDA!")
        resp.json.side_effect = ValueError("not json")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "secret-tok"}),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError) as exc_info,
        ):
            await client._get_json("/estacao/2024-01-01/2024-01-10/A001", requires_token=True)

        assert "secret-tok" not in str(exc_info.value)
        assert "secret-tok" not in (exc_info.value.url or "")


class TestInmetEndpointPath:
    @pytest.mark.asyncio
    async def test_fetch_dados_usa_ordem_datas_primeiro(self):
        resp = make_mock_response(
            200, json_data=[{"CD_ESTACAO": "A001", "DT_MEDICAO": "2024-01-01"}]
        )
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
        ):
            await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2024, 1, 10))

        call_url = mock_client.get.call_args[0][0]
        assert "/estacao/2024-01-01/2024-01-10/A001" in call_url


class TestInmetEmptyResponse:
    @pytest.mark.asyncio
    async def test_resposta_fora_de_lista_levanta_parse_error(self):
        resp = make_mock_response(200, json_data={"error": "unexpected"})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ParseError, match="esperada lista JSON, veio dict"),
        ):
            await client._get_json("/test")


class TestInmetValidation:
    @pytest.mark.asyncio
    async def test_invalid_tipo_raises(self):
        with pytest.raises(InvalidParameterError, match="tipo inválido: 'X'. Valores válidos"):
            await client.fetch_estacoes("X")

    @pytest.mark.asyncio
    async def test_inicio_after_fim_raises(self):
        with pytest.raises(ValueError, match="inicio.*deve ser"):
            await client.fetch_dados_estacao("A001", date(2024, 12, 31), date(2024, 1, 1))


class TestInmet403InEstacoeUf:
    @pytest.mark.asyncio
    async def test_sem_token_falha_antes_de_listar_ou_buscar_estacoes(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch.object(client, "fetch_estacoes", new_callable=AsyncMock) as fetch_estacoes,
            patch.object(client, "fetch_dados_estacao", new_callable=AsyncMock) as fetch_dados,
            pytest.raises(SourceUnavailableError, match="AGROBR_INMET_TOKEN"),
        ):
            await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))

        fetch_estacoes.assert_not_awaited()
        fetch_dados.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sem_estacao_operante_raises_source_unavailable(self):
        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
            patch.object(client, "fetch_estacoes", new_callable=AsyncMock, return_value=[]),
            pytest.raises(SourceUnavailableError, match="Nenhuma estação operante"),
        ):
            await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))

    @pytest.mark.asyncio
    async def test_timeout_aborta_coleta_estacoes_uf(self):
        estacoes = [{"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A001"}]

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
            patch.object(client, "fetch_estacoes", new_callable=AsyncMock, return_value=estacoes),
            patch.object(
                client,
                "fetch_dados_estacao",
                new_callable=AsyncMock,
                side_effect=httpx.ReadTimeout("timeout"),
            ),
            pytest.raises(SourceUnavailableError, match="Falha HTTP na estação A001"),
        ):
            await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))

    @pytest.mark.asyncio
    async def test_source_unavailable_cancela_estacoes_irmas(self):
        estacoes = [
            {"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A001"},
            {"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A002"},
        ]
        irma_iniciou = asyncio.Event()
        canceladas: list[str] = []

        async def fetch_dados(codigo, *_args, **_kwargs):
            if codigo == "A001":
                await irma_iniciou.wait()
                raise SourceUnavailableError(source="inmet", last_error="falha da fonte")
            irma_iniciou.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                canceladas.append(codigo)
                raise

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
            patch.object(client, "fetch_estacoes", new_callable=AsyncMock, return_value=estacoes),
            patch.object(client, "fetch_dados_estacao", side_effect=fetch_dados),
            pytest.raises(SourceUnavailableError, match="falha da fonte"),
        ):
            await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))

        assert canceladas == ["A002"]


@pytest.mark.asyncio
async def test_resposta_nao_json_publica_so_o_inicio_do_corpo():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, text="x" * 500))
    ) as http:
        with levanta_exatamente(SourceUnavailableError, "Resposta não-JSON") as erro:
            await client._get_json("/estacoes/T", http=http)
    assert "x" * 200 in str(erro.value)
    assert "x" * 201 not in str(erro.value)


@pytest.mark.asyncio
async def test_estacoes_uf_reune_as_observacoes_de_cada_estacao():
    estacoes = [
        {"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A001"},
        {"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A002"},
    ]

    async def fetch_dados(codigo, *_args, **_kwargs):
        return [{"CD_ESTACAO": codigo}]

    with (
        patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
        patch.object(client, "fetch_estacoes", new_callable=AsyncMock, return_value=estacoes),
        patch.object(client, "fetch_dados_estacao", side_effect=fetch_dados),
        sem_excecao(),
    ):
        dados = await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))
    assert sorted(item["CD_ESTACAO"] for item in dados) == ["A001", "A002"]


@pytest.mark.asyncio
async def test_falha_de_estacao_posterior_sobe_como_erro_da_fonte():
    estacoes = [
        {"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A001"},
        {"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A002"},
    ]
    primeira_iniciou = asyncio.Event()

    async def fetch_dados(codigo, *_args, **_kwargs):
        if codigo == "A002":
            await primeira_iniciou.wait()
            raise SourceUnavailableError(source="inmet", last_error="falha da A002")
        primeira_iniciou.set()
        await asyncio.Event().wait()

    with (
        patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
        patch.object(client, "fetch_estacoes", new_callable=AsyncMock, return_value=estacoes),
        patch.object(client, "fetch_dados_estacao", side_effect=fetch_dados),
        pytest.raises(BaseException) as erro,
    ):
        await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))
    assert isinstance(erro.value, SourceUnavailableError)
    assert "falha da A002" in str(erro.value)
