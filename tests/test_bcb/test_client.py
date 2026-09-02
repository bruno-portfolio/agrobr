"""Testes de resiliência HTTP para agrobr.bcb.client."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.bcb import client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import (
    RETRY_SLEEP,
    make_mock_async_client,
    make_mock_response,
    make_sleep_tracker,
)


class TestPertenceASafra:
    def test_jul_a_dez_do_ano_inicio(self):
        assert client._pertence_a_safra({"AnoEmissao": "2023", "MesEmissao": "07"}, 2023)
        assert client._pertence_a_safra({"AnoEmissao": "2023", "MesEmissao": "12"}, 2023)

    def test_jan_a_jun_do_ano_fim(self):
        assert client._pertence_a_safra({"AnoEmissao": "2024", "MesEmissao": "01"}, 2023)
        assert client._pertence_a_safra({"AnoEmissao": "2024", "MesEmissao": "06"}, 2023)

    def test_fora_da_safra(self):
        assert not client._pertence_a_safra({"AnoEmissao": "2023", "MesEmissao": "06"}, 2023)
        assert not client._pertence_a_safra({"AnoEmissao": "2024", "MesEmissao": "07"}, 2023)

    def test_record_sem_ano_descartado(self):
        assert not client._pertence_a_safra({"MesEmissao": "07"}, 2023)
        assert not client._pertence_a_safra({"AnoEmissao": "x", "MesEmissao": "07"}, 2023)


class TestBcbTimeout:
    @pytest.mark.asyncio
    async def test_timeout_retried_raises_source_unavailable(self):
        mock_client = make_mock_async_client()
        mock_client.get.side_effect = httpx.TimeoutException("read timeout")

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")

        assert mock_client.get.call_count == client.BCB_MAX_RETRIES


class TestBcbHTTPErrors:
    @pytest.mark.asyncio
    async def test_http_500_retries(self):
        resp_500 = make_mock_response(500, json_data={"value": []})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")

        assert mock_client.get.call_count == client.BCB_MAX_RETRIES

    @pytest.mark.asyncio
    async def test_http_429_retries(self):
        resp_429 = make_mock_response(429, json_data={"value": []})
        resp_ok = make_mock_response(200, json_data={"value": [{"id": 1}]})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(side_effect=[resp_429, resp_ok])

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, new_callable=AsyncMock),
        ):
            result = await client._fetch_odata("CusteioRegiaoUFProduto")

        assert result["value"] == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_http_403_raises_via_raise_for_status(self):
        resp_403 = make_mock_response(403, json_data={"value": []})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")


class TestBcbEmptyResponse:
    @pytest.mark.asyncio
    async def test_empty_value_list(self):
        resp = make_mock_response(200, json_data={"value": []})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_credito_rural(finalidade="custeio")

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_value_key(self):
        resp = make_mock_response(200, json_data={"odata.metadata": "..."})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_credito_rural(finalidade="custeio")

        assert result == []


class TestBcbRetry:
    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        resp_500 = make_mock_response(500, json_data={"value": []})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_500)

        sleep_calls, track_sleep = make_sleep_tracker()

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(RETRY_SLEEP, side_effect=track_sleep),
            pytest.raises(SourceUnavailableError),
        ):
            await client._fetch_odata("CusteioRegiaoUFProduto")

        assert len(sleep_calls) == client.BCB_MAX_RETRIES - 1
        for i in range(1, len(sleep_calls)):
            assert sleep_calls[i] > sleep_calls[i - 1]


class TestBcbFetchCreditoRural:
    @pytest.mark.asyncio
    async def test_invalid_finalidade_raises(self):
        with pytest.raises(ValueError, match="Finalidade inválida"):
            await client.fetch_credito_rural(finalidade="invalida")

    @pytest.mark.asyncio
    async def test_client_side_filtering_safra(self):
        records = [
            {"AnoEmissao": "2023", "MesEmissao": "09", "nomeProduto": "SOJA", "cdEstado": "51"},
            {"AnoEmissao": "2024", "MesEmissao": "02", "nomeProduto": "SOJA", "cdEstado": "51"},
            {"AnoEmissao": "2023", "MesEmissao": "03", "nomeProduto": "SOJA", "cdEstado": "51"},
            {"AnoEmissao": "2024", "MesEmissao": "08", "nomeProduto": "SOJA", "cdEstado": "51"},
        ]
        resp = make_mock_response(200, json_data={"value": records})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp)

        with patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client):
            result = await client.fetch_credito_rural(finalidade="custeio", safra_sicor="2023/2024")

        assert len(result) == 2
        assert {(r["AnoEmissao"], r["MesEmissao"]) for r in result} == {
            ("2023", "09"),
            ("2024", "02"),
        }

    @pytest.mark.asyncio
    async def test_filter_uf_uses_nome_uf_with_real_golden(self):
        path = Path(__file__).parents[1] / "golden_data" / "bcb" / "custeio_sample"
        records = json.loads((path / "response.json").read_text(encoding="utf-8"))

        with patch.object(
            client,
            "_fetch_odata",
            new_callable=AsyncMock,
            return_value={"value": records},
        ) as mock_fetch:
            result = await client.fetch_credito_rural(
                finalidade="custeio",
                produto_sicor="SOJA",
                cd_uf="51",
            )

        assert len(result) == 3
        assert {record["nomeUF"] for record in result} == {"MT"}
        filters = mock_fetch.call_args.kwargs["filters"]
        assert "contains(nomeProduto,'SOJA')" in filters
        assert "nomeUF eq 'MT'" in filters
        assert all("cdEstado" not in item for item in filters)

    @pytest.mark.asyncio
    async def test_server_filter_includes_safra(self):
        with patch.object(
            client,
            "_fetch_odata",
            new_callable=AsyncMock,
            return_value={"value": []},
        ) as mock_fetch:
            await client.fetch_credito_rural(safra_sicor="2023/2024")

        filters = mock_fetch.call_args.kwargs["filters"]
        assert filters == [
            "((AnoEmissao eq '2023' and MesEmissao ge '07') or "
            "(AnoEmissao eq '2024' and MesEmissao lt '07'))"
        ]

    @pytest.mark.asyncio
    async def test_invalid_uf_code_raises_before_request(self):
        with (
            patch.object(client, "_fetch_odata", new_callable=AsyncMock) as mock_fetch,
            pytest.raises(ValueError, match="Codigo de UF invalido"),
        ):
            await client.fetch_credito_rural(cd_uf="99")

        mock_fetch.assert_not_awaited()


class TestBcbFallback:
    @pytest.mark.asyncio
    async def test_odata_fails_tries_bigquery(self):
        with (
            patch("agrobr.bcb.client.fetch_credito_rural", new_callable=AsyncMock) as mock_odata,
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                new_callable=AsyncMock,
            ) as mock_bq,
        ):
            mock_odata.side_effect = SourceUnavailableError(
                source="bcb", url="test", last_error="timeout"
            )
            mock_bq.return_value = [{"id": 1}]
            records, source = await client.fetch_credito_rural_with_fallback()

            assert source == "bigquery"
            assert records == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_http_403_triggers_bigquery_fallback(self):
        resp_403 = make_mock_response(403, json_data={"value": []})
        mock_client = make_mock_async_client()
        mock_client.get = AsyncMock(return_value=resp_403)

        with (
            patch("agrobr.bcb.client.httpx.AsyncClient", return_value=mock_client),
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                new_callable=AsyncMock,
            ) as mock_bq,
        ):
            mock_bq.return_value = [{"id": 2}]
            records, source = await client.fetch_credito_rural_with_fallback()

        assert source == "bigquery"
        assert records == [{"id": 2}]

    @pytest.mark.asyncio
    async def test_network_error_triggers_bigquery_fallback(self):
        with (
            patch("agrobr.bcb.client.fetch_credito_rural", new_callable=AsyncMock) as mock_odata,
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                new_callable=AsyncMock,
            ) as mock_bq,
        ):
            mock_odata.side_effect = SourceUnavailableError(
                source="bcb", last_error="ConnectError: connection refused after 6 retries"
            )
            mock_bq.return_value = [{"id": 3}]
            records, source = await client.fetch_credito_rural_with_fallback()

            assert source == "bigquery"
            assert records == [{"id": 3}]

    @pytest.mark.asyncio
    async def test_timeout_triggers_bigquery_fallback(self):
        with (
            patch("agrobr.bcb.client.fetch_credito_rural", new_callable=AsyncMock) as mock_odata,
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                new_callable=AsyncMock,
            ) as mock_bq,
        ):
            mock_odata.side_effect = SourceUnavailableError(
                source="bcb", last_error="TimeoutException: read timeout after 6 retries"
            )
            mock_bq.return_value = [{"id": 4}]
            records, source = await client.fetch_credito_rural_with_fallback()

            assert source == "bigquery"
            assert records == [{"id": 4}]
