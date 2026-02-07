"""Testes para a API publica NASA POWER."""

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.nasa_power import api


def _mock_nasa_response(dates=None):
    """Gera resposta mock completa da API NASA POWER."""
    if dates is None:
        dates = ["20240115"]

    params = {}
    values = {
        "T2M": 25.0,
        "T2M_MAX": 30.0,
        "T2M_MIN": 20.0,
        "PRECTOTCORR": 5.0,
        "RH2M": 65.0,
        "ALLSKY_SFC_SW_DWN": 18.0,
        "WS2M": 2.5,
    }
    for param, val in values.items():
        params[param] = dict.fromkeys(dates, val)

    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-56.1, -12.6, 399.24]},
        "properties": {"parameter": params},
    }


class TestClimaPonto:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        mock_data = _mock_nasa_response()

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.clima_ponto(-12.6, -56.1, "2024-01-15", "2024-01-15")

        assert len(df) == 1
        assert "temp_media" in df.columns
        assert "precip_mm" in df.columns

    @pytest.mark.asyncio
    async def test_mensal_agregacao(self):
        dates_jan = [f"202401{d:02d}" for d in range(1, 4)]
        dates_feb = [f"202402{d:02d}" for d in range(1, 3)]
        mock_data = _mock_nasa_response(dates=dates_jan + dates_feb)

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.clima_ponto(-12.6, -56.1, "2024-01-01", "2024-02-28", agregacao="mensal")

        assert len(df) == 2
        assert "precip_acum_mm" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        mock_data = _mock_nasa_response()

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ):
            df, meta = await api.clima_ponto(
                -12.6, -56.1, "2024-01-15", "2024-01-15", return_meta=True
            )

        assert meta.source == "nasa_power"
        assert meta.attempted_sources == ["nasa_power"]
        assert meta.selected_source == "nasa_power"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)
        assert "latitude=-12.6" in meta.source_url


class TestClimaUf:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        dates = [f"202401{d:02d}" for d in range(1, 4)]
        mock_data = _mock_nasa_response(dates=dates)

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.clima_uf("MT", 2024)

        assert len(df) > 0
        assert "uf" in df.columns

    @pytest.mark.asyncio
    async def test_diario_agregacao(self):
        dates = [f"202401{d:02d}" for d in range(1, 4)]
        mock_data = _mock_nasa_response(dates=dates)

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ):
            df = await api.clima_uf("MT", 2024, agregacao="diario")

        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="nao reconhecida"):
            await api.clima_uf("XX", 2024)

    @pytest.mark.asyncio
    async def test_return_meta(self):
        dates = [f"202401{d:02d}" for d in range(1, 4)]
        mock_data = _mock_nasa_response(dates=dates)

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ):
            df, meta = await api.clima_uf("MT", 2024, return_meta=True)

        assert meta.source == "nasa_power"
        assert meta.attempted_sources == ["nasa_power"]
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_passes_correct_coords(self):
        dates = ["20240115"]
        mock_data = _mock_nasa_response(dates=dates)

        with patch.object(
            api.client, "fetch_daily", new_callable=AsyncMock, return_value=mock_data
        ) as mock_fetch:
            await api.clima_uf("SP", 2024, agregacao="diario")

        # SP coords: (-22.3, -49.1)
        call_args = mock_fetch.call_args
        assert call_args[0][0] == pytest.approx(-22.3)  # lat
        assert call_args[0][1] == pytest.approx(-49.1)  # lon
