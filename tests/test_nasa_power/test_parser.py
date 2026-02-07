"""Testes para o parser NASA POWER."""

import pytest

from agrobr.exceptions import ParseError
from agrobr.nasa_power.parser import PARSER_VERSION, agregar_mensal, parse_daily


def _nasa_response(
    dates=None,
    t2m=25.0,
    t2m_max=30.0,
    t2m_min=20.0,
    precip=5.0,
    rh2m=65.0,
    rad=18.0,
    ws2m=2.5,
):
    """Gera resposta mock da API NASA POWER."""
    if dates is None:
        dates = ["20240115"]

    parameters = {}
    for param, value in [
        ("T2M", t2m),
        ("T2M_MAX", t2m_max),
        ("T2M_MIN", t2m_min),
        ("PRECTOTCORR", precip),
        ("RH2M", rh2m),
        ("ALLSKY_SFC_SW_DWN", rad),
        ("WS2M", ws2m),
    ]:
        parameters[param] = dict.fromkeys(dates, value)

    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-56.1, -12.6, 399.24]},
        "properties": {"parameter": parameters},
    }


class TestParseDaily:
    def test_basic(self):
        data = _nasa_response()
        df = parse_daily(data, lat=-12.6, lon=-56.1, uf="MT")

        assert len(df) == 1
        assert "temp_media" in df.columns
        assert "precip_mm" in df.columns
        assert "uf" in df.columns
        assert df.iloc[0]["uf"] == "MT"

    def test_numeric_values(self):
        data = _nasa_response(t2m=25.3, precip=12.5)
        df = parse_daily(data, lat=-12.6, lon=-56.1)

        assert df.iloc[0]["temp_media"] == pytest.approx(25.3)
        assert df.iloc[0]["precip_mm"] == pytest.approx(12.5)

    def test_sentinel_becomes_nan(self):
        data = _nasa_response(t2m=-999.0, precip=-999.0)
        df = parse_daily(data, lat=-12.6, lon=-56.1)

        assert df.iloc[0]["temp_media"] != df.iloc[0]["temp_media"]  # NaN
        assert df.iloc[0]["precip_mm"] != df.iloc[0]["precip_mm"]  # NaN

    def test_multiple_dates(self):
        dates = ["20240115", "20240116", "20240117"]
        data = _nasa_response(dates=dates)
        df = parse_daily(data, lat=-12.6, lon=-56.1)

        assert len(df) == 3

    def test_sorted_by_date(self):
        dates = ["20240117", "20240115", "20240116"]
        data = _nasa_response(dates=dates)
        df = parse_daily(data, lat=-12.6, lon=-56.1)

        assert df.iloc[0]["data"].day == 15
        assert df.iloc[1]["data"].day == 16
        assert df.iloc[2]["data"].day == 17

    def test_empty_raises(self):
        with pytest.raises(ParseError) as exc_info:
            parse_daily({}, lat=-12.6, lon=-56.1)
        assert "vazia" in str(exc_info.value)
        assert exc_info.value.parser_version == PARSER_VERSION

    def test_no_parameters_raises(self):
        data = {"type": "Feature", "properties": {"parameter": {}}}
        with pytest.raises(ParseError):
            parse_daily(data, lat=-12.6, lon=-56.1)

    def test_lat_lon_preserved(self):
        data = _nasa_response()
        df = parse_daily(data, lat=-22.3, lon=-49.1, uf="SP")

        assert df.iloc[0]["lat"] == pytest.approx(-22.3)
        assert df.iloc[0]["lon"] == pytest.approx(-49.1)

    def test_all_7_columns(self):
        data = _nasa_response()
        df = parse_daily(data, lat=-12.6, lon=-56.1)

        expected_cols = {
            "temp_media",
            "temp_max",
            "temp_min",
            "precip_mm",
            "umidade_rel",
            "radiacao_mj",
            "vento_ms",
        }
        assert expected_cols.issubset(set(df.columns))


class TestAgregarMensal:
    def test_basic(self):
        dates_jan = [f"202401{d:02d}" for d in range(1, 4)]
        dates_feb = [f"202402{d:02d}" for d in range(1, 3)]
        data = _nasa_response(dates=dates_jan + dates_feb, precip=10.0)
        df = parse_daily(data, lat=-12.6, lon=-56.1, uf="MT")
        df_mensal = agregar_mensal(df)

        assert len(df_mensal) == 2
        assert "precip_acum_mm" in df_mensal.columns
        assert "temp_media" in df_mensal.columns

    def test_precip_is_sum(self):
        dates = [f"202401{d:02d}" for d in range(1, 4)]
        data = _nasa_response(dates=dates, precip=10.0)
        df = parse_daily(data, lat=-12.6, lon=-56.1, uf="MT")
        df_mensal = agregar_mensal(df)

        assert df_mensal.iloc[0]["precip_acum_mm"] == pytest.approx(30.0)

    def test_temp_is_mean(self):
        dates = [f"202401{d:02d}" for d in range(1, 4)]
        data = _nasa_response(dates=dates, t2m=25.0)
        df = parse_daily(data, lat=-12.6, lon=-56.1, uf="MT")
        df_mensal = agregar_mensal(df)

        assert df_mensal.iloc[0]["temp_media"] == pytest.approx(25.0)

    def test_empty(self):
        import pandas as pd

        df = pd.DataFrame()
        result = agregar_mensal(df)
        assert result.empty

    def test_preserves_lat_lon(self):
        dates = [f"202401{d:02d}" for d in range(1, 4)]
        data = _nasa_response(dates=dates)
        df = parse_daily(data, lat=-12.6, lon=-56.1, uf="MT")
        df_mensal = agregar_mensal(df)

        assert "lat" in df_mensal.columns
        assert "lon" in df_mensal.columns


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
