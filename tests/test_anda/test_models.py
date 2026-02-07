"""Testes para os modelos ANDA."""

import pytest

from agrobr.anda.models import (
    ANDA_UFS,
    FERTILIZANTES_MAP,
    EntregaFertilizante,
    normalize_fertilizante,
)


class TestEntregaFertilizante:
    def test_basic_creation(self):
        rec = EntregaFertilizante(
            ano=2024,
            mes=6,
            uf="MT",
            produto_fertilizante="npk",
            volume_ton=150000.0,
        )

        assert rec.ano == 2024
        assert rec.mes == 6
        assert rec.uf == "MT"
        assert rec.produto_fertilizante == "npk"
        assert rec.volume_ton == 150000.0

    def test_uf_normalization(self):
        rec = EntregaFertilizante(
            ano=2024,
            mes=1,
            uf="mt",
            produto_fertilizante="total",
            volume_ton=100.0,
        )

        assert rec.uf == "MT"

    def test_produto_normalization(self):
        rec = EntregaFertilizante(
            ano=2024,
            mes=1,
            uf="SP",
            produto_fertilizante="  NPK  ",
            volume_ton=100.0,
        )

        assert rec.produto_fertilizante == "npk"

    def test_validation_mes_invalid(self):
        with pytest.raises(ValueError):
            EntregaFertilizante(
                ano=2024,
                mes=13,
                uf="MT",
                produto_fertilizante="total",
                volume_ton=100.0,
            )

    def test_validation_volume_negative(self):
        with pytest.raises(ValueError):
            EntregaFertilizante(
                ano=2024,
                mes=1,
                uf="MT",
                produto_fertilizante="total",
                volume_ton=-100.0,
            )


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
