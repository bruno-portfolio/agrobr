from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.nasa_power import api, client, parser, provenance
from tests.helpers import sem_excecao


@pytest.mark.parametrize("parameters", [[], "PS", ["UNKNOWN"], ["PS", "PS"], [True], ["PS"] * 21])
async def test_invalid_selection_before_http(parameters):
    with (
        patch.object(api.client, "fetch_daily", new_callable=AsyncMock) as fetch,
        pytest.raises(InvalidParameterError),
    ):
        await api.clima_ponto(0, 0, "2024-01-01", "2024-01-02", parameters=parameters)
    fetch.assert_not_awaited()


@pytest.mark.parametrize("value", [True, "oops", float("inf"), [], {}])
def test_invalid_external_value_raises_parse_error(value):
    data = {
        "properties": {"parameter": {"PS": {"20240101": value}}},
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {"PS": {"units": "kPa"}},
    }
    with pytest.raises(ParseError):
        parser.parse_daily(data, 0, 0, parameters=["PS"])


@pytest.mark.parametrize(
    "data",
    [
        {
            "properties": [],
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
        },
        {
            "properties": {"parameter": {"PS": []}},
            "header": {"fill_value": -999.0, "time_standard": "LST"},
            "parameters": {"PS": {"units": "kPa"}},
        },
        {
            "properties": {"parameter": {"PS": {"20240230": 1}}},
            "header": {"fill_value": -999.0, "time_standard": "LST"},
            "parameters": {"PS": {"units": "kPa"}},
        },
        {
            "properties": {"parameter": {"PS": {"202401011": 1}}},
            "header": {"fill_value": -999.0, "time_standard": "LST"},
            "parameters": {"PS": {"units": "kPa"}},
        },
        {
            "properties": {"parameter": {"T2M": {"20240101": 1}}},
            "header": {"fill_value": -999.0, "time_standard": "LST"},
            "parameters": {"T2M": {"units": "C"}},
        },
        {
            "properties": {"parameter": {"PS": {"20240101": 1}}},
            "parameters": {"PS": {"units": "Pa"}},
            "header": {"fill_value": -999.0, "time_standard": "LST"},
        },
    ],
)
def test_invalid_selected_response_raises_parse_error(data):
    with pytest.raises(ParseError):
        parser.parse_daily(data, 0, 0, parameters=["PS"])


def test_unexpected_time_basis_rejected():
    data = {
        "properties": {"parameter": {"PS": {"20240101": 97}}},
        "header": {"time_standard": "UTC", "fill_value": -999.0},
        "parameters": {"PS": {"units": "kPa"}},
    }
    with pytest.raises(ParseError, match="Base de tempo"):
        parser.parse_daily(data, 0, 0, parameters=["PS"])


async def test_inconsistent_chunk_rejects_partial_result():
    first = {
        "properties": {"parameter": {"PS": {"20230101": 97}}},
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {"PS": {"units": "kPa"}},
    }
    second = {
        "properties": {"parameter": {"T2M": {"20240101": 26}}},
        "header": {"fill_value": -999.0, "time_standard": "LST"},
        "parameters": {"T2M": {"units": "C"}},
    }
    with (
        patch.object(client, "_get_json", new_callable=AsyncMock, side_effect=[first, second]),
        pytest.raises(ParseError, match="divergentes entre blocos"),
    ):
        await client.fetch_daily(0, 0, date(2023, 1, 1), date(2024, 1, 2), parameters=["PS"])


async def test_blocos_com_fontes_diferentes_juntam_e_guardam_a_fonte_de_cada_um():
    primeiro = {
        "properties": {"parameter": {"PS": {"20230101": 97}}},
        "header": {"fill_value": -999.0, "time_standard": "LST", "sources": ["MERRA2"]},
        "parameters": {"PS": {"units": "kPa"}},
    }
    segundo = {
        "properties": {"parameter": {"PS": {"20240101": 98}}},
        "header": {"fill_value": -999.0, "time_standard": "LST", "sources": ["GEOSIT", "MERRA2"]},
        "parameters": {"PS": {"units": "kPa"}},
    }
    with (
        patch.object(client, "_get_json", new_callable=AsyncMock, side_effect=[primeiro, segundo]),
        sem_excecao(),
    ):
        resultado = await client.fetch_daily(
            0, 0, date(2023, 1, 1), date(2024, 1, 2), parameters=["PS"]
        )
    assert resultado["properties"]["parameter"]["PS"] == {"20230101": 97, "20240101": 98}
    assert resultado.blocks == (
        provenance.Bloco(date(2023, 1, 1), date(2023, 12, 31), ("MERRA2",)),
        provenance.Bloco(date(2024, 1, 1), date(2024, 1, 2), ("GEOSIT", "MERRA2")),
    )
