"""Testes de resiliência HTTP para agrobr.inmet.client."""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.inmet import client
from tests.helpers import RETRY_SLEEP, make_mock_async_client, make_mock_response


class TestInmetTimeout:
    @pytest.mark.asyncio
    async def test_timeout_on_get_json(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_timeout_on_fetch_estacoes(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("timeout")

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_estacoes("T")


class TestInmetHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_raises(self):
        resp_500 = make_mock_response(500, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="inmet"),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_http_403_raises_source_unavailable(self):
        resp_403 = make_mock_response(403, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="AGROBR_INMET_TOKEN"),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_http_429_raises_after_retries(self):
        resp_429 = make_mock_response(429, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_429)

        with (
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="inmet"),
        ):
            await client._get_json("/estacoes/T")

    @pytest.mark.asyncio
    async def test_retriable_status_in_fetch_dados_logged_and_skipped(self):
        resp_ok = make_mock_response(200, json_data=[{"data": "d1"}])
        resp_502 = make_mock_response(502, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_502, resp_ok])

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2024, 1, 2))

        assert isinstance(result, list)


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

    @pytest.mark.asyncio
    async def test_204_com_token_em_fetch_dados_retorna_vazio(self):
        resp_204 = make_mock_response(204)
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_204)

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok123"}),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
        ):
            result = await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2024, 1, 10))

        assert result == []


class TestInmetToken:
    def test_get_token_returns_env_var(self):
        with patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "my-secret-token"}):
            assert client._get_token() == "my-secret-token"

    def test_get_token_returns_none_when_absent(self):
        with patch.dict("os.environ", {}, clear=True):
            result = client._get_token()
            assert result is None

    @pytest.mark.asyncio
    async def test_token_vai_no_path_nao_no_header(self):
        resp = make_mock_response(200, json_data=[{"data": "d1"}])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "test-token"}),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client) as mock_cls,
        ):
            await client._get_json("/estacao/2024-01-01/2024-01-10/A001", requires_token=True)

        call_url = mock_client.get.call_args[0][0]
        assert call_url.endswith("/token/estacao/2024-01-01/2024-01-10/A001/test-token")
        headers_used = mock_cls.call_args.kwargs.get("headers", {})
        assert "Authorization" not in headers_used

    @pytest.mark.asyncio
    async def test_sem_token_usa_path_publico(self):
        resp = make_mock_response(200, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
        ):
            await client._get_json("/estacao/2024-01-01/2024-01-10/A001", requires_token=True)

        call_url = mock_client.get.call_args[0][0]
        assert "/token/" not in call_url

    @pytest.mark.asyncio
    async def test_token_invalido_chave_invalida_raises(self):
        resp = make_mock_response(200, text="CHAVE INVÁLIDA!")
        resp.json.side_effect = ValueError("not json")
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "fake"}),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(SourceUnavailableError, match="Token INMET inválido"),
        ):
            await client._get_json("/estacao/2024-01-01/2024-01-10/A001", requires_token=True)

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
        resp = make_mock_response(200, json_data=[{"data": "d1"}])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
        ):
            await client.fetch_dados_estacao("A001", date(2024, 1, 1), date(2024, 1, 10))

        call_url = mock_client.get.call_args[0][0]
        assert "/estacao/2024-01-01/2024-01-10/A001" in call_url


class TestInmetChunksFalham:
    @pytest.mark.asyncio
    async def test_todos_chunks_falham_re_raise(self):
        resp_500 = make_mock_response(500, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client.fetch_dados_estacao("A001", date(2022, 1, 1), date(2024, 1, 1))

    @pytest.mark.asyncio
    async def test_falha_parcial_retorna_dados_dos_chunks_ok(self):
        resp_500 = make_mock_response(500, json_data=[])
        resp_ok = make_mock_response(200, json_data=[{"d": "1"}])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_500, resp_500, resp_500, resp_ok])

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
            patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client.fetch_dados_estacao("A001", date(2022, 1, 1), date(2023, 6, 1))

        assert result == [{"d": "1"}]


class TestInmetEmptyResponse:
    @pytest.mark.asyncio
    async def test_non_list_response_returns_empty(self):
        resp = make_mock_response(200, json_data={"error": "unexpected"})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json("/test")

        assert result == []

    @pytest.mark.asyncio
    async def test_empty_list_response(self):
        resp = make_mock_response(200, json_data=[])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client._get_json("/test")

        assert result == []


class TestInmetValidation:
    @pytest.mark.asyncio
    async def test_invalid_tipo_raises(self):
        with pytest.raises(ValueError, match="Tipo deve ser"):
            await client.fetch_estacoes("X")

    @pytest.mark.asyncio
    async def test_inicio_after_fim_raises(self):
        with pytest.raises(ValueError, match="inicio.*deve ser"):
            await client.fetch_dados_estacao("A001", date(2024, 12, 31), date(2024, 1, 1))


class TestInmetFetchDadosEstacaoChunking:
    @pytest.mark.asyncio
    async def test_chunking_respects_max_days(self):
        resp = make_mock_response(200, json_data=[{"d": "1"}])
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.inmet.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_dados_estacao("A001", date(2022, 1, 1), date(2024, 1, 1))

        assert mock_client.get.call_count >= 2
        assert isinstance(result, list)


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
    async def test_403_surfaces_through_estacoes_uf(self):
        estacoes = [{"SG_ESTADO": "SP", "CD_SITUACAO": "Operante", "CD_ESTACAO": "A001"}]

        with (
            patch.dict("os.environ", {"AGROBR_INMET_TOKEN": "tok"}),
            patch.object(client, "fetch_estacoes", new_callable=AsyncMock, return_value=estacoes),
            patch.object(
                client,
                "fetch_dados_estacao",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(
                    source="inmet", last_error="HTTP 403 Forbidden — defina AGROBR_INMET_TOKEN"
                ),
            ),
            pytest.raises(SourceUnavailableError, match="AGROBR_INMET_TOKEN"),
        ):
            await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))

    @pytest.mark.asyncio
    async def test_non_403_errors_still_swallowed_in_estacoes_uf(self):
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
        ):
            result = await client.fetch_dados_estacoes_uf("SP", date(2024, 1, 1), date(2024, 1, 10))

        assert result == []

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
