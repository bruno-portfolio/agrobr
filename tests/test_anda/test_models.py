"""Testes para os modelos ANDA."""

import pytest

from agrobr.anda.models import (
    ANDA_UFS,
    FERTILIZANTES_MAP,
    normalize_fertilizante,
    resolve_produto,
)
from agrobr.exceptions import InvalidParameterError


class TestNormalizeFertilizante:
    def test_known_aliases(self):
        assert normalize_fertilizante("uréia") == "ureia"
        assert normalize_fertilizante("Uréia") == "ureia"
        assert normalize_fertilizante("cloreto de potássio") == "kcl"
        assert normalize_fertilizante("Cloreto de Potássio") == "kcl"
        assert normalize_fertilizante("KCL") == "kcl"
        assert normalize_fertilizante("superfosfato simples") == "ssp"
        assert normalize_fertilizante("SSP") == "ssp"

    def test_unknown_passthrough(self):
        assert normalize_fertilizante("fosfato natural") == "fosfato natural"

    def test_total(self):
        assert normalize_fertilizante("Total") == "total"
        assert normalize_fertilizante("TOTAL") == "total"


class TestAndaUfs:
    def test_count(self):
        assert len(ANDA_UFS) == 27

    def test_major_states(self):
        for uf in ["MT", "SP", "PR", "GO", "MG", "RS", "BA"]:
            assert uf in ANDA_UFS


class TestFertilizantesMap:
    def test_has_main_products(self):
        assert "npk" in FERTILIZANTES_MAP
        assert "ureia" in FERTILIZANTES_MAP
        assert "map" in FERTILIZANTES_MAP
        assert "kcl" in FERTILIZANTES_MAP
        assert "total" in FERTILIZANTES_MAP


class TestResolveProduto:
    def test_total(self):
        assert resolve_produto(" TOTAL ") == "total"

    def test_produto_especifico(self):
        with pytest.raises(InvalidParameterError, match="apenas entregas totais"):
            resolve_produto("ureia")

    def test_produto_desconhecido(self):
        with pytest.raises(InvalidParameterError, match="fosfato natural"):
            resolve_produto("fosfato natural")
