"""Testes para os modelos NASA POWER."""

from datetime import date

import pytest

from agrobr.nasa_power.models import (
    COLUNAS_MAP,
    PARAMS_AG,
    SENTINEL,
    UF_COORDS,
    ClimaObservacao,
)


class TestUFCoords:
    def test_all_27_ufs(self):
        assert len(UF_COORDS) == 27

    def test_main_agricultural_ufs_present(self):
        for uf in ["MT", "SP", "PR", "GO", "MS", "BA", "MG", "RS"]:
            assert uf in UF_COORDS, f"UF {uf} ausente"

    def test_coords_valid_ranges(self):
        for uf, (lat, lon) in UF_COORDS.items():
            assert -34.0 <= lat <= 6.0, f"{uf}: latitude {lat} fora do Brasil"
            assert -74.0 <= lon <= -35.0, f"{uf}: longitude {lon} fora do Brasil"


class TestParamsAG:
    def test_has_7_params(self):
        assert len(PARAMS_AG) == 7

    def test_expected_params(self):
        expected = {"T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR", "RH2M", "ALLSKY_SFC_SW_DWN", "WS2M"}
        assert set(PARAMS_AG) == expected


class TestColunasMap:
    def test_maps_all_params(self):
        for param in PARAMS_AG:
            assert param in COLUNAS_MAP, f"Parametro {param} sem mapeamento"

    def test_output_names(self):
        expected = {
            "temp_media",
            "temp_max",
            "temp_min",
            "precip_mm",
            "umidade_rel",
            "radiacao_mj",
            "vento_ms",
        }
        assert set(COLUNAS_MAP.values()) == expected


class TestClimaObservacao:
    def test_basic(self):
        obs = ClimaObservacao(
            data=date(2024, 1, 15),
            lat=-12.6,
            lon=-56.1,
            uf="MT",
            temp_media=25.3,
            precip_mm=5.0,
        )
        assert obs.data == date(2024, 1, 15)
        assert obs.temp_media == pytest.approx(25.3)
        assert obs.uf == "MT"

    def test_sentinel_becomes_none(self):
        obs = ClimaObservacao(
            data=date(2024, 1, 15),
            lat=-12.6,
            lon=-56.1,
            temp_media=SENTINEL,
            precip_mm=SENTINEL,
        )
        assert obs.temp_media is None
        assert obs.precip_mm is None

    def test_none_stays_none(self):
        obs = ClimaObservacao(
            data=date(2024, 1, 15),
            lat=-12.6,
            lon=-56.1,
            temp_media=None,
        )
        assert obs.temp_media is None

    def test_invalid_string_becomes_none(self):
        obs = ClimaObservacao(
            data=date(2024, 1, 15),
            lat=-12.6,
            lon=-56.1,
            temp_media="abc",
        )
        assert obs.temp_media is None
