"""Testes para os modelos ABIOVE."""

import pytest

from agrobr.abiove.models import ABIOVE_PRODUTOS, MESES_PT, normalize_produto, resolve_produto


class TestNormalizeProduto:
    def test_grao_variants(self):
        assert normalize_produto("grao") == "grao"
        assert normalize_produto("grão") == "grao"
        assert normalize_produto("soja em grão") == "grao"
        assert normalize_produto("Soja em Grao") == "grao"

    def test_farelo(self):
        assert normalize_produto("farelo") == "farelo"
        assert normalize_produto("Farelo de Soja") == "farelo"

    def test_oleo(self):
        assert normalize_produto("oleo") == "oleo"
        assert normalize_produto("óleo") == "oleo"
        assert normalize_produto("Óleo de Soja") == "oleo"

    def test_milho(self):
        assert normalize_produto("milho") == "milho"
        assert normalize_produto("Milho") == "milho"

    def test_unknown_passthrough(self):
        assert normalize_produto("algodão") == "algodão"

    def test_total(self):
        assert normalize_produto("Total") == "total"
        assert normalize_produto("TOTAL") == "total"

    def test_english_names(self):
        assert normalize_produto("soybeans") == "grao"
        assert normalize_produto("soybean meal") == "farelo"
        assert normalize_produto("soybean oil") == "oleo"
        assert normalize_produto("corn") == "milho"


class TestMesesPt:
    def test_all_12_months_covered(self):
        values = set(MESES_PT.values())
        assert values == set(range(1, 13))

    def test_january(self):
        assert MESES_PT["janeiro"] == 1
        assert MESES_PT["jan"] == 1

    def test_december(self):
        assert MESES_PT["dezembro"] == 12
        assert MESES_PT["dez"] == 12

    def test_marco_sem_acento(self):
        assert MESES_PT["marco"] == 3
        assert MESES_PT["março"] == 3


class TestAbioveeProdutos:
    def test_has_main_products(self):
        assert "grao" in ABIOVE_PRODUTOS
        assert "farelo" in ABIOVE_PRODUTOS
        assert "oleo" in ABIOVE_PRODUTOS
        assert "milho" in ABIOVE_PRODUTOS
        assert "total" in ABIOVE_PRODUTOS

    def test_canonical_values(self):
        assert ABIOVE_PRODUTOS["grao"] == "grao"
        assert ABIOVE_PRODUTOS["farelo"] == "farelo"
        assert ABIOVE_PRODUTOS["oleo"] == "oleo"
        assert ABIOVE_PRODUTOS["milho"] == "milho"

    def test_accent_variants(self):
        assert ABIOVE_PRODUTOS["grão"] == "grao"
        assert ABIOVE_PRODUTOS["óleo"] == "oleo"


class TestResolveProduto:
    def test_valid_returns_canonical(self):
        assert resolve_produto("soja em grão") == "grao"
        assert resolve_produto("Farelo") == "farelo"
        assert resolve_produto("corn") == "milho"

    def test_invalid_raises_valueerror(self):
        with pytest.raises(ValueError, match="produto desconhecido"):
            resolve_produto("banana")
