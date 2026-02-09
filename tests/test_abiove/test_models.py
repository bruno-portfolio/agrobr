"""Testes para os modelos ABIOVE."""

import pytest

from agrobr.abiove.models import (
    ABIOVE_PRODUTOS,
    MESES_PT,
    ExportacaoSoja,
    normalize_produto,
)


class TestExportacaoSoja:
    def test_basic_creation(self):
        rec = ExportacaoSoja(
            ano=2024,
            mes=6,
            produto="grao",
            volume_ton=5000000.0,
            receita_usd_mil=2500000.0,
        )

        assert rec.ano == 2024
        assert rec.mes == 6
        assert rec.produto == "grao"
        assert rec.volume_ton == 5000000.0
        assert rec.receita_usd_mil == 2500000.0

    def test_produto_normalization_grao(self):
        rec = ExportacaoSoja(ano=2024, mes=1, produto="Soja em Grão", volume_ton=100.0)
        assert rec.produto == "grao"

    def test_produto_normalization_farelo(self):
        rec = ExportacaoSoja(ano=2024, mes=1, produto="Farelo de Soja", volume_ton=100.0)
        assert rec.produto == "farelo"

    def test_produto_normalization_oleo(self):
        rec = ExportacaoSoja(ano=2024, mes=1, produto="Óleo de Soja", volume_ton=100.0)
        assert rec.produto == "oleo"

    def test_validation_mes_invalid_13(self):
        with pytest.raises(ValueError):
            ExportacaoSoja(ano=2024, mes=13, produto="grao", volume_ton=100.0)

    def test_validation_mes_zero(self):
        with pytest.raises(ValueError):
            ExportacaoSoja(ano=2024, mes=0, produto="grao", volume_ton=100.0)

    def test_validation_volume_negative(self):
        with pytest.raises(ValueError):
            ExportacaoSoja(ano=2024, mes=1, produto="grao", volume_ton=-100.0)

    def test_receita_optional(self):
        rec = ExportacaoSoja(ano=2024, mes=1, produto="grao", volume_ton=100.0)
        assert rec.receita_usd_mil is None


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
