"""Testes para a API pública BCB/SICOR."""

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.bcb import api
from agrobr.exceptions import SourceUnavailableError


def _mock_sicor_data():
    return [
        {
            "Safra": "2023/2024",
            "AnoEmissao": 2023,
            "MesEmissao": 9,
            "cdUF": "51",
            "UF": "MT",
            "cdMunicipio": "5107248",
            "Municipio": "SORRISO",
            "Produto": "SOJA",
            "Valor": 285431200.0,
            "AreaFinanciada": 98500.0,
            "QtdContratos": 1240,
        },
        {
            "Safra": "2023/2024",
            "AnoEmissao": 2023,
            "MesEmissao": 10,
            "cdUF": "51",
            "UF": "MT",
            "cdMunicipio": "5106752",
            "Municipio": "SINOP",
            "Produto": "SOJA",
            "Valor": 142715600.0,
            "AreaFinanciada": 49250.0,
            "QtdContratos": 620,
        },
    ]


class TestCreditoRural:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24")

        assert len(df) == 2
        assert "valor" in df.columns
        assert "area_financiada" in df.columns
        assert all(df["produto"] == "soja")

    @pytest.mark.asyncio
    async def test_return_meta_odata(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df, meta = await api.credito_rural("soja", safra="2023/24", return_meta=True)

        assert meta.source == "bcb"
        assert meta.attempted_sources == ["bcb_odata"]
        assert meta.selected_source == "bcb_odata"
        assert meta.source_method == "httpx"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_return_meta_bigquery_fallback(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "bigquery"),
        ):
            df, meta = await api.credito_rural("soja", safra="2023/24", return_meta=True)

        assert meta.source == "bcb"
        assert meta.attempted_sources == ["bcb_odata", "bcb_bigquery"]
        assert meta.selected_source == "bcb_bigquery"
        assert meta.source_method == "bigquery"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_agregacao_uf(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", agregacao="uf")

        assert len(df) == 1
        assert df.iloc[0]["valor"] == pytest.approx(285431200.0 + 142715600.0)

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", uf="MT")

        assert len(df) == 2
        assert all(df["uf"] == "MT")


class TestCreditoRuralFallback:
    @pytest.mark.asyncio
    async def test_odata_success_no_fallback(self):
        mock_odata = AsyncMock(return_value=_mock_sicor_data())
        with patch.object(api.client, "fetch_credito_rural", mock_odata):
            records, source = await api.client.fetch_credito_rural_with_fallback(
                finalidade="custeio",
                produto_sicor="SOJA",
                safra_sicor="2023/2024",
            )

        assert source == "odata"
        assert len(records) == 2
        mock_odata.assert_called_once()

    @pytest.mark.asyncio
    async def test_odata_fails_bigquery_succeeds(self):
        mock_odata = AsyncMock(
            side_effect=SourceUnavailableError(source="bcb", last_error="HTTP 500")
        )
        mock_bq = AsyncMock(return_value=_mock_sicor_data())

        with (
            patch.object(api.client, "fetch_credito_rural", mock_odata),
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                mock_bq,
            ),
        ):
            records, source = await api.client.fetch_credito_rural_with_fallback(
                finalidade="custeio",
                produto_sicor="SOJA",
            )

        assert source == "bigquery"
        assert len(records) == 2

    @pytest.mark.asyncio
    async def test_both_fail_raises(self):
        mock_odata = AsyncMock(
            side_effect=SourceUnavailableError(source="bcb", last_error="HTTP 500")
        )
        mock_bq = AsyncMock(
            side_effect=SourceUnavailableError(source="bcb_bigquery", last_error="Auth failed")
        )

        with (
            patch.object(api.client, "fetch_credito_rural", mock_odata),
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                mock_bq,
            ),
            pytest.raises(SourceUnavailableError, match="Ambas as fontes"),
        ):
            await api.client.fetch_credito_rural_with_fallback(
                finalidade="custeio",
            )
