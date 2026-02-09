"""Testes para a API pública IMEA."""

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.imea import api


def _mock_api_records():
    """Simula resposta JSON da API IMEA /cotacoes."""
    return [
        {
            "Localidade": "Médio-Norte",
            "Valor": 125.50,
            "Variacao": -1.2,
            "Safra": "24/25",
            "CadeiaId": 4,
            "IndicadorFinalId": 10,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-15",
            "UnidadeSigla": "R$/sc",
            "UnidadeDescricao": "Reais por saca de 60 kg",
        },
        {
            "Localidade": "Norte",
            "Valor": 120.00,
            "Variacao": 0.5,
            "Safra": "24/25",
            "CadeiaId": 4,
            "IndicadorFinalId": 10,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-15",
            "UnidadeSigla": "R$/sc",
            "UnidadeDescricao": "Reais por saca de 60 kg",
        },
        {
            "Localidade": "Médio-Norte",
            "Valor": 85.5,
            "Variacao": 2.1,
            "Safra": "23/24",
            "CadeiaId": 4,
            "IndicadorFinalId": 20,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-15",
            "UnidadeSigla": "%",
            "UnidadeDescricao": "Percentual",
        },
        {
            "Localidade": "Sudeste",
            "Valor": 130.00,
            "Variacao": -0.8,
            "Safra": "24/25",
            "CadeiaId": 3,
            "IndicadorFinalId": 10,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-14",
            "UnidadeSigla": "R$/sc",
            "UnidadeDescricao": "Reais por saca de 60 kg",
        },
    ]


class TestCotacoes:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        mock_fn = AsyncMock(return_value=_mock_api_records())
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            df = await api.cotacoes("soja")

        assert len(df) == 4
        assert "cadeia" in df.columns
        assert "localidade" in df.columns
        assert "valor" in df.columns
        mock_fn.assert_called_once_with(4)

    @pytest.mark.asyncio
    async def test_filter_safra(self):
        mock_fn = AsyncMock(return_value=_mock_api_records())
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            df = await api.cotacoes("soja", safra="24/25")

        assert len(df) == 3
        assert all(df["safra"] == "24/25")

    @pytest.mark.asyncio
    async def test_filter_unidade(self):
        mock_fn = AsyncMock(return_value=_mock_api_records())
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            df = await api.cotacoes("soja", unidade="%")

        assert len(df) == 1
        assert df["unidade"].iloc[0] == "%"

    @pytest.mark.asyncio
    async def test_return_meta(self):
        mock_fn = AsyncMock(return_value=_mock_api_records())
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            df, meta = await api.cotacoes("soja", return_meta=True)

        assert meta.source == "imea"
        assert meta.attempted_sources == ["imea"]
        assert meta.selected_source == "imea"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)
        assert "cotacoes" in meta.source_url

    @pytest.mark.asyncio
    async def test_cadeia_resolution(self):
        mock_fn = AsyncMock(return_value=[])
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            await api.cotacoes("milho")

        mock_fn.assert_called_once_with(3)

    @pytest.mark.asyncio
    async def test_cadeia_english(self):
        mock_fn = AsyncMock(return_value=[])
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            await api.cotacoes("soybeans")

        mock_fn.assert_called_once_with(4)

    @pytest.mark.asyncio
    async def test_invalid_cadeia(self):
        with pytest.raises(ValueError, match="Cadeia desconhecida"):
            await api.cotacoes("cafe")

    @pytest.mark.asyncio
    async def test_combined_filters(self):
        mock_fn = AsyncMock(return_value=_mock_api_records())
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            df = await api.cotacoes("soja", safra="24/25", unidade="R$/sc")

        # 24/25 com R$/sc: Médio-Norte, Norte, Sudeste (mas Sudeste é milho cadeia 3)
        # Após parse, soja 24/25 R$/sc = Médio-Norte + Norte
        soja_rows = df[df["cadeia"] == "soja"]
        assert len(soja_rows) == 2

    @pytest.mark.asyncio
    async def test_empty_response(self):
        mock_fn = AsyncMock(return_value=[])
        with patch.object(api.client, "fetch_cotacoes", mock_fn):
            df = await api.cotacoes("soja")

        assert df.empty
        assert "cadeia" in df.columns
