from __future__ import annotations

from decimal import Decimal

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize.units import (
    converter,
    preco_saca_para_tonelada,
    preco_tonelada_para_saca,
    sacas_para_toneladas,
    toneladas_para_sacas,
)
from tests.helpers import collect_failures, sem_excecao


class TestConverterBushel:
    def test_bu_para_kg_soja(self):
        with collect_failures() as check:
            with check("test_bu_para_kg_soja"):
                result = converter(1, "bu", "kg", produto="soja")
                assert result == Decimal("27.2155")
            with check("test_kg_para_bu_milho"):
                result = converter(Decimal("25.4012"), "kg", "bu", produto="milho")
                assert result == Decimal("1")

    def test_produto_invalido_raises(self):
        with collect_failures() as check:
            with (
                check("test_sem_produto_raises"),
                pytest.raises(InvalidParameterError, match="Produto.*Produtos: milho, soja, trigo"),
            ):
                converter(1, "bu", "kg")
            with (
                check("test_produto_invalido_raises"),
                pytest.raises(InvalidParameterError, match="não definido. Produtos: milho"),
            ):
                converter(1, "bu", "kg", produto="arroz")


class TestConverterMilTon:
    def test_mil_ton_para_ton(self):
        with collect_failures() as check:
            for case, value_1, value_2, value_3, expected in [
                ("test_mil_ton_para_ton", 1, "mil_ton", "ton", Decimal("1000")),
                ("test_ton_para_mil_ton", 1000, "ton", "mil_ton", Decimal("1")),
            ]:
                with check(case):
                    assert converter(value_1, value_2, value_3) == expected


class TestConverterHa:
    def test_ha_para_mil_ha(self):
        with collect_failures() as check:
            for case, value_1, value_2, value_3, expected in [
                ("test_mil_ha_para_ha", 1, "mil_ha", "ha", Decimal("1000")),
                ("test_ha_para_mil_ha", 1000, "ha", "mil_ha", Decimal("1")),
            ]:
                with check(case):
                    assert converter(value_1, value_2, value_3) == expected


class TestConverterInputTypes:
    def test_aceita_decimal(self):
        with collect_failures() as check:
            with check("test_aceita_float"):
                result = converter(1.5, "ton", "kg")
                assert result == Decimal("1500")
            with check("test_aceita_int"):
                result = converter(2, "ton", "kg")
                assert result == Decimal("2000")
            with check("test_aceita_decimal"):
                result = converter(Decimal("3.5"), "ton", "kg")
                assert result == Decimal("3500")


class TestConverterUnidadeDesconhecida:
    def test_destino_desconhecido_raises(self):
        with collect_failures() as check:
            with (
                check("test_origem_desconhecida_raises"),
                pytest.raises(InvalidParameterError, match="Unidades de massa: arroba, kg"),
            ):
                converter(1, "galao", "kg")
            with (
                check("test_destino_desconhecido_raises"),
                pytest.raises(InvalidParameterError, match="Unidades de massa: arroba, kg"),
            ):
                converter(1, "kg", "galao")


class TestSacasParaToneladas:
    def test_aceita_float(self):
        with collect_failures() as check:
            with check("test_padrao_60kg"):
                result = sacas_para_toneladas(100)
                assert result == Decimal("6.0")
                assert result == Decimal("6000") / Decimal("1000")
            with check("test_saca_50kg"):
                result = sacas_para_toneladas(100, peso_saca_kg=50)
                assert result == Decimal("5.0")
            with check("test_aceita_float"):
                result = sacas_para_toneladas(10.5)
                assert isinstance(result, Decimal)


@pytest.mark.parametrize(
    ("funcao", "entrada", "esperado"),
    [
        (sacas_para_toneladas, 10.5, Decimal("0.63")),
        (toneladas_para_sacas, 1.5, Decimal("25")),
        (preco_saca_para_tonelada, 120.0, Decimal("2000")),
        (preco_tonelada_para_saca, 2000.0, Decimal("120")),
    ],
)
def test_conversoes_de_saca_aceitam_float(funcao, entrada, esperado):
    with sem_excecao():
        resultado = funcao(entrada)
    assert resultado == esperado


def test_mesma_unidade_de_bushel_dispensa_produto():
    with sem_excecao():
        resultado = converter(10, "bu", "bu")
    assert resultado == Decimal("10")


@pytest.mark.parametrize(
    "funcao",
    [
        sacas_para_toneladas,
        toneladas_para_sacas,
        preco_saca_para_tonelada,
        preco_tonelada_para_saca,
    ],
)
@pytest.mark.parametrize("peso", [0, -60, True, "60"])
def test_peso_saca_nao_positivo_recusado(funcao, peso):
    with pytest.raises(InvalidParameterError, match="peso_saca_kg deve ser número positivo"):
        funcao(100, peso_saca_kg=peso)
