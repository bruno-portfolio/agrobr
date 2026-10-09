"""Testes para o parser NASA POWER."""

from datetime import date

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.nasa_power.parser import (
    PARSER_VERSION,
    agregar_mensal,
    parse_daily,
    validate_period,
)
from tests.helpers import levanta_exatamente


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


class TestParseDaily:
    def test_sentinel_becomes_nan(self):
        data = _nasa_response(t2m=-999.0, precip=-999.0)
        df = parse_daily(data, lat=-12.6, lon=-56.1)

        assert df["temp_media"].dtype == "float64" and pd.isna(df.iloc[0]["temp_media"])
        assert df["precip_mm"].dtype == "float64" and pd.isna(df.iloc[0]["precip_mm"])

    def test_empty_raises(self):
        with pytest.raises(ParseError) as exc_info:
            parse_daily({}, lat=-12.6, lon=-56.1)
        assert "vazia" in str(exc_info.value)
        assert exc_info.value.parser_version == PARSER_VERSION


class TestAgregarMensal:
    def test_empty(self):
        import pandas as pd

        df = pd.DataFrame()
        result = agregar_mensal(df)
        assert result.empty


def test_validate_period_recusa_chave_fora_do_formato():
    with levanta_exatamente(ParseError, "Data inválida"):
        validate_period(_nasa_response(dates=["2024011"]), date(2024, 1, 1), date(2024, 1, 1))


def test_parse_daily_sem_nenhum_dia_recusa():
    with levanta_exatamente(ParseError, "Nenhuma data"):
        parse_daily(_nasa_response(dates=[]), -12.6, -56.1)
