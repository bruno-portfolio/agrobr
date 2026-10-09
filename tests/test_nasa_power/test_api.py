"""Testes para a API publica NASA POWER."""

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.nasa_power import api
from tests.helpers import levanta_exatamente


def _mock_nasa_response(dates=None):
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
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {
            "T2M": {"units": "C"},
            "T2M_MAX": {"units": "C"},
            "T2M_MIN": {"units": "C"},
            "PRECTOTCORR": {"units": "mm/day"},
            "RH2M": {"units": "%"},
            "ALLSKY_SFC_SW_DWN": {"units": "MJ/m^2/day"},
            "WS2M": {"units": "m/s"},
        },
    }


class TestClimaPonto:
    @pytest.mark.parametrize(
        "args",
        [
            (91, 0, "2024-01-01", "2024-01-02"),
            (0, 181, "2024-01-01", "2024-01-02"),
            (0, 0, "2024-02-01", "2024-01-01"),
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_parameters_raise_before_request(self, args):
        with (
            patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError),
        ):
            await api.clima_ponto(*args)

        fetch.assert_not_awaited()

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

    @pytest.mark.parametrize(
        ("inicio", "fim", "motivo"),
        [
            ("2024/01/01", "2024-01-02", "formato"),
            (datetime(2024, 1, 1), date(2024, 1, 2), "datas ou strings"),
            ("1980-12-31", "1981-01-02", "1981"),
        ],
    )
    async def test_periodo_invalido_recusado_antes_da_rede(self, inicio, fim, motivo):
        with (
            patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
            levanta_exatamente(InvalidParameterError, motivo),
        ):
            await api.clima_ponto(0, 0, inicio, fim)
        fetch.assert_not_awaited()


class TestClimaUf:
    @pytest.mark.parametrize(("uf", "ano", "motivo"), [(35, 2024, "sigla"), ("MT", 1980, "1981")])
    async def test_uf_ou_ano_invalido_recusado_antes_da_rede(self, uf, ano, motivo):
        with (
            patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
            levanta_exatamente(InvalidParameterError, motivo),
        ):
            await api.clima_uf(uf, ano)
        fetch.assert_not_awaited()

    async def test_invalid_aggregation_before_request(self):
        with (
            patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="agregacao"),
        ):
            await api.clima_uf("MT", 2024, agregacao="mensla")
        fetch.assert_not_awaited()

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
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="não reconhecida"):
            await api.clima_uf("XX", 2024)


@pytest.fixture
def hoje_em_brasilia_7_out(monkeypatch):
    from agrobr.utils import time as time_utils

    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2026, 10, 8, 1, 0, tzinfo=UTC))


@pytest.mark.usefixtures("hoje_em_brasilia_7_out")
class TestInicioNoFuturo:
    async def test_inicio_amanha_recusado_antes_da_rede(self):
        with (
            patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
            levanta_exatamente(InvalidParameterError, "2026-10-08 no futuro.*até 2026-10-07"),
        ):
            await api.clima_ponto(-15.8, -47.9, "2026-10-08", "2026-10-31")
        fetch.assert_not_awaited()

    async def test_inicio_hoje_vai_a_rede(self):
        with patch.object(
            api.client,
            "fetch_daily",
            new_callable=AsyncMock,
            return_value=_mock_nasa_response(dates=["20261007"]),
        ) as fetch:
            df = await api.clima_ponto(-15.8, -47.9, "2026-10-07", "2026-10-31")
        fetch.assert_awaited_once()
        assert len(df) == 1

    async def test_clima_uf_ano_seguinte_recusado_antes_da_rede(self):
        with (
            patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
            levanta_exatamente(InvalidParameterError, "entre 1981 e 2026"),
        ):
            await api.clima_uf("MT", 2027)
        fetch.assert_not_awaited()
