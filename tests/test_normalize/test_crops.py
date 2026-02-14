from __future__ import annotations

import pytest

from agrobr.normalize.crops import (
    CANONICAL_CROPS,
    is_cultura_valida,
    listar_culturas,
    normalizar_cultura,
)


class TestNormalizarCultura:
    @pytest.mark.parametrize(
        "entrada,expected",
        [
            ("soja", "soja"),
            ("SOJA", "soja"),
            ("Soja", "soja"),
            ("soja em grão", "soja"),
            ("Soja em Grão", "soja"),
            ("SOJA EM GRAO", "soja"),
            ("soybean", "soja"),
            ("soybeans", "soja"),
            ("soy", "soja"),
        ],
        ids=[
            "lower",
            "upper",
            "title",
            "em_grao",
            "em_grao_title",
            "em_grao_upper",
            "soybean",
            "soybeans",
            "soy",
        ],
    )
    def test_soja_variants(self, entrada, expected):
        assert normalizar_cultura(entrada) == expected

    @pytest.mark.parametrize(
        "entrada,expected",
        [
            ("milho", "milho"),
            ("MILHO", "milho"),
            ("milho total", "milho"),
            ("corn", "milho"),
            ("maize", "milho"),
            ("milho 1a safra", "milho_1"),
            ("milho 1ª safra", "milho_1"),
            ("milho_1", "milho_1"),
            ("milho 2a safra", "milho_2"),
            ("milho 2ª safra", "milho_2"),
            ("milho_2", "milho_2"),
        ],
    )
    def test_milho_variants(self, entrada, expected):
        assert normalizar_cultura(entrada) == expected

    @pytest.mark.parametrize(
        "entrada,expected",
        [
            ("cafe", "cafe"),
            ("café", "cafe"),
            ("coffee", "cafe"),
            ("cafe arabica", "cafe_arabica"),
            ("café arábica", "cafe_arabica"),
            ("arabica", "cafe_arabica"),
            ("arábica", "cafe_arabica"),
            ("cafe robusta", "cafe_robusta"),
            ("conilon", "cafe_robusta"),
        ],
    )
    def test_cafe_variants(self, entrada, expected):
        assert normalizar_cultura(entrada) == expected

    @pytest.mark.parametrize(
        "entrada,expected",
        [
            ("algodao", "algodao"),
            ("algodão", "algodao"),
            ("cotton", "algodao"),
            ("algodão herbáceo", "algodao"),
            ("algodao em pluma", "algodao_pluma"),
        ],
    )
    def test_algodao_variants(self, entrada, expected):
        assert normalizar_cultura(entrada) == expected

    @pytest.mark.parametrize(
        "entrada,expected",
        [
            ("boi", "boi"),
            ("boi gordo", "boi"),
            ("boi_gordo", "boi"),
            ("cattle", "boi"),
            ("beef", "boi"),
        ],
    )
    def test_boi_variants(self, entrada, expected):
        assert normalizar_cultura(entrada) == expected

    @pytest.mark.parametrize(
        "entrada,expected",
        [
            ("trigo", "trigo"),
            ("wheat", "trigo"),
            ("arroz", "arroz"),
            ("rice", "arroz"),
            ("feijao", "feijao"),
            ("feijão", "feijao"),
            ("bean", "feijao"),
            ("acucar", "acucar"),
            ("açúcar", "acucar"),
            ("sugar", "acucar"),
            ("cana", "cana"),
            ("cana de açúcar", "cana"),
            ("sugarcane", "cana"),
            ("etanol", "etanol_hidratado"),
            ("ethanol", "etanol_hidratado"),
            ("frango", "frango_congelado"),
            ("chicken", "frango_congelado"),
            ("suino", "suino"),
            ("suíno", "suino"),
            ("pork", "suino"),
            ("leite", "leite"),
            ("milk", "leite"),
            ("laranja", "laranja"),
            ("orange", "laranja"),
            ("mandioca", "mandioca"),
            ("cassava", "mandioca"),
            ("farelo_soja", "farelo_soja"),
            ("soybean meal", "farelo_soja"),
            ("oleo_soja", "oleo_soja"),
            ("óleo de soja", "oleo_soja"),
            ("sorgo", "sorgo"),
            ("amendoim", "amendoim"),
        ],
    )
    def test_other_cultures(self, entrada, expected):
        assert normalizar_cultura(entrada) == expected

    def test_unknown_returns_lowered(self):
        assert normalizar_cultura("Quinoa Orgânica") == "quinoa_orgânica"

    def test_whitespace_handling(self):
        assert normalizar_cultura("  soja  ") == "soja"
        assert normalizar_cultura("  café arábica  ") == "cafe_arabica"


class TestListarCulturas:
    def test_returns_sorted_list(self):
        culturas = listar_culturas()
        assert culturas == sorted(culturas)

    def test_contains_main_crops(self):
        culturas = listar_culturas()
        for c in ["soja", "milho", "cafe", "algodao", "trigo", "arroz", "feijao", "boi"]:
            assert c in culturas, f"{c} not in listar_culturas()"

    def test_count_above_20(self):
        assert len(listar_culturas()) >= 20


class TestIsCulturaValida:
    def test_canonical(self):
        assert is_cultura_valida("soja") is True

    def test_alias(self):
        assert is_cultura_valida("soybean") is True

    def test_accent_variant(self):
        assert is_cultura_valida("café") is True

    def test_invalid(self):
        assert is_cultura_valida("batata_doce_roxa") is False


class TestCanonicalCrops:
    def test_is_set(self):
        assert isinstance(CANONICAL_CROPS, set)

    def test_no_duplicates_by_design(self):
        assert len(CANONICAL_CROPS) >= 20

    def test_all_lowercase(self):
        for crop in CANONICAL_CROPS:
            assert crop == crop.lower(), f"Canonical crop '{crop}' is not lowercase"

    def test_no_spaces(self):
        for crop in CANONICAL_CROPS:
            assert " " not in crop, f"Canonical crop '{crop}' contains spaces"
