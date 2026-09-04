"""Testes para a API pública INMET."""

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.inmet import api


def _mock_obs(
    estacao="A001",
    data="2024-01-15",
    hora="1200 UTC",
    uf="DF",
    chuva="5.0",
):
    return {
        "CD_ESTACAO": estacao,
        "DT_MEDICAO": data,
        "HR_MEDICAO": hora,
        "UF": uf,
        "TEM_INS": "25.3",
        "TEM_MAX": "28.0",
        "TEM_MIN": "22.1",
        "UMD_INS": "65.0",
        "UMD_MAX": "70.0",
        "UMD_MIN": "60.0",
        "CHUVA": chuva,
        "RAD_GLO": "500.0",
        "PRE_INS": "886.2",
        "PRE_MAX": "887.0",
        "PRE_MIN": "885.0",
        "VEN_VEL": "2.5",
        "VEN_DIR": "180",
        "VEN_RAJ": "5.0",
        "PTO_INS": "15.0",
        "PTO_MAX": "16.0",
        "PTO_MIN": "14.0",
        "VL_LATITUDE": "-15.789",
        "VL_LONGITUDE": "-47.925",
        "VL_ALTITUDE": "1160.96",
    }


def _mock_estacao(
    codigo="A001",
    nome="BRASILIA",
    uf="DF",
    situacao="Operante",
    tipo="Automatica",
    lat="-15.789",
    lon="-47.925",
    alt="1160.96",
    inicio="2000-05-07",
):
    return {
        "CD_ESTACAO": codigo,
        "DC_NOME": nome,
        "SG_ESTADO": uf,
        "CD_SITUACAO": situacao,
        "TP_ESTACAO": tipo,
        "VL_LATITUDE": lat,
        "VL_LONGITUDE": lon,
        "VL_ALTITUDE": alt,
        "DT_INICIO_OPERACAO": inicio,
    }


class TestEstacoes:
    @pytest.mark.asyncio
    async def test_estacoes_returns_dataframe(self):
        mock_data = [_mock_estacao()]

        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacoes()

        assert len(df) == 1
        assert "codigo" in df.columns
        assert df.iloc[0]["codigo"] == "A001"

    @pytest.mark.asyncio
    async def test_estacoes_filter_uf(self):
        mock_data = [
            _mock_estacao(),
            _mock_estacao(
                codigo="A002",
                nome="SAO PAULO",
                uf="SP",
                lat="-23.5",
                lon="-46.6",
                alt="800.0",
                inicio="2001-01-01",
            ),
        ]

        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacoes(uf="SP")

        assert len(df) == 1
        assert df.iloc[0]["uf"] == "SP"


class TestEstacao:
    @pytest.mark.asyncio
    async def test_reverse_range_raises_before_request(self):
        with (
            patch.object(api.client, "fetch_dados_estacao", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="inicio"),
        ):
            await api.estacao("A001", "2025-02-01", "2025-01-01")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_estacao_horario(self):
        mock_data = [_mock_obs(hora="1200 UTC"), _mock_obs(hora="1300 UTC")]

        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacao("A001", "2024-01-15", "2024-01-15")

        assert len(df) == 2
        assert "temperatura" in df.columns

    @pytest.mark.asyncio
    async def test_estacao_diario(self):
        mock_data = [
            _mock_obs(hora="0000 UTC", chuva="5.0"),
            _mock_obs(hora="0600 UTC", chuva="3.0"),
            _mock_obs(hora="1200 UTC", chuva="0.0"),
            _mock_obs(hora="1800 UTC", chuva="2.0"),
        ]

        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.estacao("A001", "2024-01-15", "2024-01-15", agregacao="diario")

        assert len(df) == 1
        assert "precipitacao_mm" in df.columns
        assert df.iloc[0]["precipitacao_mm"] == pytest.approx(10.0)

    @pytest.mark.asyncio
    async def test_estacao_return_meta(self):
        mock_data = [_mock_obs()]

        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            df, meta = await api.estacao("A001", "2024-01-15", "2024-01-15", return_meta=True)

        assert meta.source == "inmet"
        assert meta.attempted_sources == ["inmet"]
        assert meta.selected_source == "inmet"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)


class TestClimaUf:
    @pytest.mark.asyncio
    async def test_clima_uf_rejects_invalid_uf_before_request(self):
        with (
            patch.object(api.client, "fetch_dados_estacoes_uf", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="UF invalida"),
        ):
            await api.clima_uf("XX", 2024)

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_clima_uf_return_meta(self):
        mock_data = [
            _mock_obs(data="2024-01-15", chuva="10.0"),
            _mock_obs(data="2024-02-15", chuva="20.0"),
        ]

        with patch.object(
            api.client, "fetch_dados_estacoes_uf", new_callable=AsyncMock, return_value=mock_data
        ):
            df, meta = await api.clima_uf("DF", 2024, return_meta=True)

        assert meta.source == "inmet"
        assert meta.attempted_sources == ["inmet"]
        assert meta.selected_source == "inmet"
        assert len(df) > 0


class TestEstacoesReturnMeta:
    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=[_mock_estacao()]
        ):
            df, meta = await api.estacoes(return_meta=True)

        assert meta.source == "inmet"
        assert meta.records_count == len(df)
        assert meta.attempted_sources == ["inmet"]
        assert meta.selected_source == "inmet"
        assert meta.fetch_timestamp is not None

    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        with patch.object(
            api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=[_mock_estacao()]
        ):
            result = await api.estacoes(as_polars=True)

        assert isinstance(result, pl.DataFrame)

    @pytest.mark.asyncio
    async def test_empty_return_meta(self):
        with patch.object(api.client, "fetch_estacoes", new_callable=AsyncMock, return_value=[]):
            df, meta = await api.estacoes(return_meta=True)

        assert df.empty
        assert meta.records_count == 0


class TestEstacaoAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        mock_data = [_mock_obs(hora="1200 UTC"), _mock_obs(hora="1300 UTC")]
        with patch.object(
            api.client, "fetch_dados_estacao", new_callable=AsyncMock, return_value=mock_data
        ):
            result = await api.estacao("A001", "2024-01-15", "2024-01-15", as_polars=True)
        assert isinstance(result, pl.DataFrame)
