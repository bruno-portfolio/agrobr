from __future__ import annotations

from decimal import Decimal

import pytest

from agrobr.normalize.units import (
    converter,
    preco_saca_para_tonelada,
    preco_tonelada_para_saca,
    sacas_para_toneladas,
    toneladas_para_sacas,
)


class TestConverterIdentidade:
    def test_mesma_unidade(self):
        assert converter(100, "kg", "kg") == Decimal("100")

    def test_mesma_unidade_ton(self):
        assert converter(5, "ton", "ton") == Decimal("5")


class TestConverterTonKg:
    def test_ton_para_kg(self):
        assert converter(1, "ton", "kg") == Decimal("1000")

    def test_kg_para_ton(self):
        assert converter(1000, "kg", "ton") == Decimal("1")

    def test_ida_e_volta(self):
        original = Decimal("42.5")
        kg = converter(original, "ton", "kg")
        volta = converter(kg, "kg", "ton")
        assert volta == original


class TestConverterSacas:
    def test_sc60_para_ton(self):
        result = converter(1, "sc60kg", "ton")
        assert result == Decimal("60") / Decimal("1000")

    def test_ton_para_sc60(self):
        result = converter(1, "ton", "sc60kg")
        expected = Decimal("1000") / Decimal("60")
        assert result == expected

    def test_kg_para_sc60(self):
        result = converter(60, "kg", "sc60kg")
        assert result == Decimal("1")

    def test_ida_e_volta_sacas(self):
        original = Decimal("100")
        ton = converter(original, "sc60kg", "ton")
        volta = converter(ton, "ton", "sc60kg")
        assert abs(volta - original) < Decimal("0.0001")


class TestConverterArrobas:
    def test_kg_para_arroba(self):
        result = converter(15, "kg", "arroba")
        assert result == Decimal("1")

    def test_arroba_para_kg(self):
        assert converter(1, "arroba", "kg") == Decimal("15")

    def test_alias_at(self):
        assert converter(1, "@", "kg") == Decimal("15")

    def test_at_arroba_equivalentes(self):
        assert converter(1, "@", "arroba") == Decimal("1")


class TestConverterBushel:
    def test_bu_para_kg_soja(self):
        result = converter(1, "bu", "kg", produto="soja")
        assert result == Decimal("27.2155")

    def test_kg_para_bu_milho(self):
        result = converter(Decimal("25.4012"), "kg", "bu", produto="milho")
        assert result == Decimal("1")

    def test_sem_produto_raises(self):
        with pytest.raises(ValueError, match="Produto"):
            converter(1, "bu", "kg")

    def test_produto_invalido_raises(self):
        with pytest.raises(ValueError, match="não definido"):
            converter(1, "bu", "kg", produto="arroz")


class TestConverterMilTon:
    def test_mil_ton_para_ton(self):
        assert converter(1, "mil_ton", "ton") == Decimal("1000")

    def test_ton_para_mil_ton(self):
        assert converter(1000, "ton", "mil_ton") == Decimal("1")


class TestConverterHa:
    def test_mil_ha_para_ha(self):
        assert converter(1, "mil_ha", "ha") == Decimal("1000")

    def test_ha_para_mil_ha(self):
        assert converter(1000, "ha", "mil_ha") == Decimal("1")


class TestConverterAliases:
    def test_tonelada_alias(self):
        assert converter(1, "tonelada", "kg") == Decimal("1000")

    def test_saca_alias(self):
        result = converter(1, "saca", "kg")
        assert result == Decimal("60")

    def test_quilograma_alias(self):
        assert converter(1000, "quilograma", "ton") == Decimal("1")


class TestConverterValorZero:
    def test_zero_kg_para_ton(self):
        assert converter(0, "kg", "ton") == Decimal("0")

    def test_zero_sacas_para_ton(self):
        result = converter(0, "sc60kg", "ton")
        assert result == Decimal("0")


class TestConverterInputTypes:
    def test_aceita_float(self):
        result = converter(1.5, "ton", "kg")
        assert result == Decimal("1500")

    def test_aceita_int(self):
        result = converter(2, "ton", "kg")
        assert result == Decimal("2000")

    def test_aceita_decimal(self):
        result = converter(Decimal("3.5"), "ton", "kg")
        assert result == Decimal("3500")


class TestConverterUnidadeDesconhecida:
    def test_origem_desconhecida_raises(self):
        with pytest.raises(ValueError):
            converter(1, "galao", "kg")

    def test_destino_desconhecido_raises(self):
        with pytest.raises(ValueError):
            converter(1, "kg", "galao")


class TestSacasParaToneladas:
    def test_padrao_60kg(self):
        result = sacas_para_toneladas(100)
        assert result == Decimal("6.0")
        assert result == Decimal("6000") / Decimal("1000")

    def test_saca_50kg(self):
        result = sacas_para_toneladas(100, peso_saca_kg=50)
        assert result == Decimal("5.0")

    def test_aceita_float(self):
        result = sacas_para_toneladas(10.5)
        assert isinstance(result, Decimal)


class TestToneladasParaSacas:
    def test_padrao_60kg(self):
        result = toneladas_para_sacas(1)
        expected = Decimal("1000") / Decimal("60")
        assert result == expected

    def test_ida_e_volta(self):
        original = Decimal("100")
        ton = sacas_para_toneladas(original)
        volta = toneladas_para_sacas(ton)
        assert abs(volta - original) < Decimal("0.001")


class TestPrecoSacaTonelada:
    def test_saca_para_tonelada(self):
        result = preco_saca_para_tonelada(135)
        sacas_por_ton = Decimal("1000") / Decimal("60")
        expected = Decimal("135") * sacas_por_ton
        assert result == expected

    def test_tonelada_para_saca(self):
        result = preco_tonelada_para_saca(2250)
        sacas_por_ton = Decimal("1000") / Decimal("60")
        expected = Decimal("2250") / sacas_por_ton
        assert result == expected

    def test_ida_e_volta(self):
        preco_sc = Decimal("135.00")
        preco_ton = preco_saca_para_tonelada(preco_sc)
        volta = preco_tonelada_para_saca(preco_ton)
        assert abs(volta - preco_sc) < Decimal("0.01")
