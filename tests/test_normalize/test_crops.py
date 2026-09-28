from __future__ import annotations

from agrobr.normalize.crops import (
    CANONICAL_CROPS,
    is_cultura_valida,
    listar_culturas,
    normalizar_cultura,
)
from tests.helpers import collect_failures


class TestNormalizarCultura:
    def test_unknown_returns_lowered(self):
        with collect_failures() as check:
            with check("test_unknown_returns_lowered"):
                assert normalizar_cultura("Quinoa Orgânica") == "quinoa_orgânica"
            with check("test_whitespace_handling"):
                assert normalizar_cultura("  soja  ") == "soja"
                assert normalizar_cultura("  café arábica  ") == "cafe_arabica"


class TestListarCulturas:
    def test_contains_main_crops(self):
        with collect_failures() as check:
            with check("test_returns_sorted_list"):
                culturas = listar_culturas()
                assert culturas == sorted(culturas)
            with check("test_contains_main_crops"):
                culturas = listar_culturas()
                for c in ["soja", "milho", "cafe", "algodao", "trigo", "arroz", "feijao", "boi"]:
                    assert c in culturas, f"{c} not in listar_culturas()"
            with check("test_count_above_20"):
                assert len(listar_culturas()) >= 20


class TestIsCulturaValida:
    def test_accent_variant(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_canonical", "soja", True),
                ("test_alias", "soybean", True),
                ("test_accent_variant", "café", True),
                ("test_invalid", "batata_doce_roxa", False),
            ]:
                with check(case):
                    assert is_cultura_valida(value) is expected


class TestCanonicalCrops:
    def test_all_lowercase(self):
        for crop in CANONICAL_CROPS:
            assert crop == crop.lower(), f"Canonical crop '{crop}' is not lowercase"

    def test_no_spaces(self):
        for crop in CANONICAL_CROPS:
            assert " " not in crop, f"Canonical crop '{crop}' contains spaces"
