from __future__ import annotations

import pytest

from agrobr.utils.validation import validate_bioma, validate_uf, validate_year_uf


class TestValidateBioma:
    @pytest.mark.parametrize(
        ("entrada", "esperado"),
        [
            ("Amazonia", "Amazônia"),
            ("AMAZÔNIA", "Amazônia"),
            (" mata atlantica ", "Mata Atlântica"),
            ("Cerrado", "Cerrado"),
            ("Caatinga", "Caatinga"),
            ("Pampa", "Pampa"),
            ("Pantanal", "Pantanal"),
        ],
    )
    def test_valid_bioma(self, entrada, esperado):
        assert validate_bioma(entrada) == esperado

    def test_none_returns_none(self):
        assert validate_bioma(None) is None

    def test_invalid_bioma(self):
        with pytest.raises(ValueError, match="Bioma inválido.*Atlantida"):
            validate_bioma("Atlantida")


class TestValidateUf:
    def test_valid_uf(self):
        assert validate_uf("SP") == "SP"

    def test_valid_uf_lowercase(self):
        assert validate_uf("sp") == "SP"

    def test_valid_uf_with_whitespace(self):
        assert validate_uf("  SP  ") == "SP"

    def test_invalid_uf(self):
        with pytest.raises(ValueError, match="UF invalida"):
            validate_uf("XX")

    def test_uf_none_returns_none(self):
        assert validate_uf(None) is None

    def test_invalid_long_string(self):
        with pytest.raises(ValueError, match="UF invalida"):
            validate_uf("INVALID")


class TestValidateYearUf:
    def test_valid_uf(self):
        validate_year_uf(uf="SP")

    def test_valid_uf_lowercase(self):
        validate_year_uf(uf="sp")

    def test_valid_uf_with_whitespace(self):
        validate_year_uf(uf="  SP  ")

    def test_invalid_uf(self):
        with pytest.raises(ValueError, match="UF"):
            validate_year_uf(uf="XX")

    def test_uf_none_skips(self):
        validate_year_uf(uf=None)

    def test_ano_valid(self):
        validate_year_uf(ano=2023, ano_min=2010)

    def test_ano_below_min(self):
        with pytest.raises(ValueError, match="fora do range"):
            validate_year_uf(ano=2005, ano_min=2010)

    def test_ano_above_current(self):
        with pytest.raises(ValueError, match="fora do range"):
            validate_year_uf(ano=2099)

    def test_ano_inicio_below_min(self):
        with pytest.raises(ValueError, match="anterior"):
            validate_year_uf(ano_inicio=2005, ano_min=2010)

    def test_ano_fim_future(self):
        with pytest.raises(ValueError, match="posterior"):
            validate_year_uf(ano_fim=2099)

    def test_inicio_after_fim(self):
        with pytest.raises(ValueError, match="ano_inicio"):
            validate_year_uf(ano_inicio=2024, ano_fim=2020)

    def test_custom_ufs_validas(self):
        custom = frozenset({"SP", "RJ"})
        validate_year_uf(uf="SP", ufs_validas=custom)
        with pytest.raises(ValueError, match="UF"):
            validate_year_uf(uf="MG", ufs_validas=custom)

    def test_all_params_valid(self):
        validate_year_uf(uf="SP", ano_inicio=2020, ano_fim=2023, ano_min=2010)
