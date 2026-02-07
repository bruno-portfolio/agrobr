"""Testes para os modelos Pydantic de custo de produção CONAB."""

import pytest

from agrobr.conab.custo_producao.models import (
    CATEGORIAS_MAP,
    CULTURAS_MAP,
    CustoTotal,
    ItemCusto,
    classify_categoria,
    normalize_cultura,
)


class TestItemCusto:
    def test_basic_creation(self):
        item = ItemCusto(
            cultura="Soja",
            uf="mt",
            safra="2023/24",
            tecnologia="Alta",
            categoria="insumos",
            item="Sementes",
            unidade="kg",
            quantidade_ha=60.0,
            preco_unitario=8.50,
            valor_ha=510.0,
            participacao_pct=12.5,
        )

        assert item.cultura == "soja"
        assert item.uf == "MT"
        assert item.tecnologia == "alta"
        assert item.valor_ha == 510.0

    def test_normalization(self):
        item = ItemCusto(
            cultura="  SOJA  ",
            uf="mt",
            safra="2023/24",
            tecnologia="  ALTA ",
            categoria="insumos",
            item="Sementes",
            valor_ha=100.0,
        )

        assert item.cultura == "soja"
        assert item.uf == "MT"
        assert item.tecnologia == "alta"

    def test_optional_fields(self):
        item = ItemCusto(
            cultura="soja",
            uf="MT",
            safra="2023/24",
            categoria="insumos",
            item="Sementes",
            valor_ha=510.0,
        )

        assert item.unidade is None
        assert item.quantidade_ha is None
        assert item.preco_unitario is None
        assert item.participacao_pct is None

    def test_safra_pattern_validation(self):
        with pytest.raises(ValueError):
            ItemCusto(
                cultura="soja",
                uf="MT",
                safra="2023-2024",
                categoria="insumos",
                item="Sementes",
                valor_ha=100.0,
            )

    def test_valor_ha_must_be_positive(self):
        with pytest.raises(ValueError):
            ItemCusto(
                cultura="soja",
                uf="MT",
                safra="2023/24",
                categoria="insumos",
                item="Sementes",
                valor_ha=-100.0,
            )


class TestCustoTotal:
    def test_basic_creation(self):
        ct = CustoTotal(
            cultura="Soja",
            uf="mt",
            safra="2023/24",
            tecnologia="Alta",
            coe_ha=3800.0,
            cot_ha=4500.0,
            ct_ha=5200.0,
        )

        assert ct.cultura == "soja"
        assert ct.uf == "MT"
        assert ct.coe_ha == 3800.0
        assert ct.cot_ha == 4500.0
        assert ct.ct_ha == 5200.0

    def test_optional_cot_ct(self):
        ct = CustoTotal(
            cultura="soja",
            uf="MT",
            safra="2023/24",
            coe_ha=3800.0,
        )

        assert ct.cot_ha is None
        assert ct.ct_ha is None

    def test_model_dump(self):
        ct = CustoTotal(
            cultura="soja",
            uf="MT",
            safra="2023/24",
            coe_ha=3800.0,
            cot_ha=4500.0,
        )

        d = ct.model_dump()
        assert d["cultura"] == "soja"
        assert d["coe_ha"] == 3800.0
        assert d["cot_ha"] == 4500.0
        assert d["ct_ha"] is None


class TestClassifyCategoria:
    def test_insumos(self):
        assert classify_categoria("Sementes") == "insumos"
        assert classify_categoria("Fertilizantes de base") == "insumos"
        assert classify_categoria("Herbicidas") == "insumos"
        assert classify_categoria("Inseticidas") == "insumos"
        assert classify_categoria("Fungicidas") == "insumos"
        assert classify_categoria("Inoculante") == "insumos"

    def test_operacoes(self):
        assert classify_categoria("Preparo do solo") == "operacoes"
        assert classify_categoria("Colheita mecânica") == "operacoes"
        assert classify_categoria("Pulverizações") == "operacoes"
        assert classify_categoria("Transporte interno") == "operacoes"

    def test_mao_de_obra(self):
        assert classify_categoria("Mão de obra temporária") == "mao_de_obra"
        assert classify_categoria("Empreita") == "mao_de_obra"

    def test_custos_fixos(self):
        assert classify_categoria("Depreciação de máquinas") == "custos_fixos"
        assert classify_categoria("Manutenção periódica") == "custos_fixos"
        assert classify_categoria("Seguros") == "custos_fixos"

    def test_outros(self):
        assert classify_categoria("Assistência técnica") == "outros"
        assert classify_categoria("Arrendamento") == "outros"
        assert classify_categoria("CESSR / Funrural") == "outros"

    def test_unknown_is_outros(self):
        assert classify_categoria("Algo totalmente desconhecido") == "outros"


class TestNormalizeCultura:
    def test_known_cultures(self):
        assert normalize_cultura("Soja") == "soja"
        assert normalize_cultura("MILHO") == "milho"
        assert normalize_cultura("Café Arábica") == "cafe_arabica"
        assert normalize_cultura("Milho Safrinha") == "milho_safrinha"
        assert normalize_cultura("Arroz Irrigado") == "arroz_irrigado"

    def test_unknown_culture(self):
        assert normalize_cultura("Quinoa") == "quinoa"
        assert normalize_cultura("Grão de Bico") == "grão_de_bico"

    def test_culturas_map_completeness(self):
        assert len(CULTURAS_MAP) >= 10

    def test_categorias_map_completeness(self):
        assert len(CATEGORIAS_MAP) >= 15
