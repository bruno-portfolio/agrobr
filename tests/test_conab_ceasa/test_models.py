from __future__ import annotations

from agrobr.conab.ceasa.models import (
    CEASA_UF_MAP,
    COLUNAS_SAIDA,
    FRUTAS,
    HORTALICAS,
    PRODUTOS_PROHORT,
    UNIDADE_ESPECIAL,
    parse_ceasa_uf,
    parse_produto_unidade,
)


class TestParseProdutoUnidade:
    def test_kg(self):
        assert parse_produto_unidade("TOMATE (KG)") == ("TOMATE", "KG")

    def test_un(self):
        assert parse_produto_unidade("ABACAXI (UN)") == ("ABACAXI", "UN")

    def test_dz(self):
        assert parse_produto_unidade("OVOS (DZ)") == ("OVOS", "DZ")

    def test_multi_word(self):
        assert parse_produto_unidade("BANANA NANICA (KG)") == ("BANANA NANICA", "KG")

    def test_hyphen(self):
        assert parse_produto_unidade("COUVE-FLOR (UN)") == ("COUVE-FLOR", "UN")

    def test_whitespace(self):
        assert parse_produto_unidade("  TOMATE (KG)  ") == ("TOMATE", "KG")

    def test_no_unit_defaults_kg(self):
        assert parse_produto_unidade("TOMATE") == ("TOMATE", "KG")


class TestParseCeasaUf:
    def test_slash_pattern(self):
        assert parse_ceasa_uf("CEASA/PR - CURITIBA") == "PR"

    def test_ceagesp(self):
        assert parse_ceasa_uf("CEAGESP - SAO PAULO") == "SP"

    def test_ceasaminas(self):
        assert parse_ceasa_uf("CEASAMINAS - BELO HORIZONTE") == "MG"

    def test_ama_ba(self):
        assert parse_ceasa_uf("AMA/BA - JUAZEIRO") == "BA"

    def test_unknown_returns_none(self):
        assert parse_ceasa_uf("DESCONHECIDA") is None

    def test_all_43_mapped(self):
        assert len(CEASA_UF_MAP) == 43
        for name, uf in CEASA_UF_MAP.items():
            assert len(uf) == 2
            assert parse_ceasa_uf(name) == uf


class TestConstantes:
    def test_48_produtos(self):
        assert len(PRODUTOS_PROHORT) == 48

    def test_20_frutas(self):
        assert len(FRUTAS) == 20

    def test_28_hortalicas(self):
        assert len(HORTALICAS) == 28

    def test_categorias_cover_all(self):
        all_prods = set(FRUTAS) | set(HORTALICAS)
        assert all_prods == PRODUTOS_PROHORT

    def test_unidade_especial_count(self):
        assert len(UNIDADE_ESPECIAL) == 5

    def test_colunas_saida(self):
        assert "data" in COLUNAS_SAIDA
        assert "preco" in COLUNAS_SAIDA
        assert len(COLUNAS_SAIDA) == 7
