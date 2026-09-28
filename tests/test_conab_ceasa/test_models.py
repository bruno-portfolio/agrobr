from __future__ import annotations

from agrobr.conab.ceasa.models import (
    parse_ceasa_uf,
    parse_produto_unidade,
)


class TestParseProdutoUnidade:
    def test_no_unit_defaults_kg(self):
        assert parse_produto_unidade("TOMATE") == ("TOMATE", "KG")


class TestParseCeasaUf:
    def test_unknown_returns_none(self):
        assert parse_ceasa_uf("DESCONHECIDA") is None
