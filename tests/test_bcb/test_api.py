"""Testes para a API pública BCB/SICOR."""

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.bcb import api


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
        with patch.object(api.client, "fetch_credito_rural", new_callable=AsyncMock, return_value=_mock_sicor_data()):
            df = await api.credito_rural("soja", safra="2023/24")

        assert len(df) == 2
        assert "valor" in df.columns
        assert "area_financiada" in df.columns
        assert all(df["produto"] == "soja")

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch.object(api.client, "fetch_credito_rural", new_callable=AsyncMock, return_value=_mock_sicor_data()):
            df, meta = await api.credito_rural("soja", safra="2023/24", return_meta=True)

        assert meta.source == "bcb"
        assert meta.attempted_sources == ["bcb"]
        assert meta.selected_source == "bcb"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_agregacao_uf(self):
        with patch.object(api.client, "fetch_credito_rural", new_callable=AsyncMock, return_value=_mock_sicor_data()):
            df = await api.credito_rural("soja", safra="2023/24", agregacao="uf")

        assert len(df) == 1
        assert df.iloc[0]["valor"] == pytest.approx(285431200.0 + 142715600.0)

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        with patch.object(api.client, "fetch_credito_rural", new_callable=AsyncMock, return_value=_mock_sicor_data()):
            df = await api.credito_rural("soja", safra="2023/24", uf="MT")

        assert len(df) == 2
        assert all(df["uf"] == "MT")
