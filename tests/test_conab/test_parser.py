"""Testes do parser CONAB."""

from __future__ import annotations

from decimal import Decimal
from io import BytesIO
from pathlib import Path

import pytest

from agrobr.conab.parsers.v1 import ConabParserV1


SAMPLE_FILE = Path(__file__).parent.parent / "golden_data" / "conab" / "safra_sample" / "response.xlsx"


@pytest.fixture
def parser():
    """Fixture do parser."""
    return ConabParserV1()


@pytest.fixture
def sample_xlsx():
    """Fixture com arquivo XLSX de amostra."""
    if not SAMPLE_FILE.exists():
        pytest.skip(f"Arquivo de amostra não encontrado: {SAMPLE_FILE}")

    with open(SAMPLE_FILE, "rb") as f:
        return BytesIO(f.read())


class TestConabParserV1:
    """Testes do parser v1."""

    def test_parser_version(self, parser):
        """Testa versão do parser."""
        assert parser.version == 1
        assert parser.source == "conab"

    def test_parse_safra_soja(self, parser, sample_xlsx):
        """Testa parsing de dados de soja."""
        safras = parser.parse_safra_produto(
            xlsx=sample_xlsx,
            produto="soja",
        )

        assert len(safras) > 0

        for safra in safras:
            assert safra.fonte.value == "conab"
            assert safra.produto == "soja"
            assert safra.unidade_area == "mil_ha"
            assert safra.unidade_producao == "mil_ton"

    def test_parse_safra_milho(self, parser, sample_xlsx):
        """Testa parsing de dados de milho."""
        sample_xlsx.seek(0)
        safras = parser.parse_safra_produto(
            xlsx=sample_xlsx,
            produto="milho",
        )

        assert len(safras) > 0

        for safra in safras:
            assert safra.produto == "milho"

    def test_parse_safra_with_uf(self, parser, sample_xlsx):
        """Testa que retorna dados por UF."""
        sample_xlsx.seek(0)
        safras = parser.parse_safra_produto(
            xlsx=sample_xlsx,
            produto="soja",
        )

        ufs = {s.uf for s in safras if s.uf}

        assert "MT" in ufs or "PR" in ufs or "RS" in ufs

    def test_parse_safra_values_reasonable(self, parser, sample_xlsx):
        """Testa que valores estão em ranges razoáveis."""
        sample_xlsx.seek(0)
        safras = parser.parse_safra_produto(
            xlsx=sample_xlsx,
            produto="soja",
        )

        for safra in safras:
            if safra.area_plantada:
                assert safra.area_plantada > 0
                assert safra.area_plantada < 50000

            if safra.producao:
                assert safra.producao > 0
                # 200000 mil ton = 200M ton (Brasil total pode chegar a 150M+)
                assert safra.producao < 200000

            if safra.produtividade:
                assert safra.produtividade > 1000
                assert safra.produtividade < 6000

    def test_parse_suprimento(self, parser, sample_xlsx):
        """Testa parsing de balanço de oferta/demanda."""
        sample_xlsx.seek(0)
        suprimentos = parser.parse_suprimento(xlsx=sample_xlsx)

        assert len(suprimentos) > 0

        produtos_encontrados = {s["produto"] for s in suprimentos}

        # A aba Suprimento principal tem MILHO, ARROZ, FEIJÃO, ALGODÃO
        # Soja tem aba separada "Suprimento - Soja"
        assert any("MILHO" in p.upper() for p in produtos_encontrados)

    def test_parse_suprimento_filter_produto(self, parser, sample_xlsx):
        """Testa filtro por produto no suprimento."""
        sample_xlsx.seek(0)
        suprimentos = parser.parse_suprimento(xlsx=sample_xlsx, produto="soja")

        for s in suprimentos:
            assert "SOJA" in s["produto"].upper()

    def test_parse_suprimento_values(self, parser, sample_xlsx):
        """Testa que valores do suprimento estão corretos."""
        sample_xlsx.seek(0)
        suprimentos = parser.parse_suprimento(xlsx=sample_xlsx)

        for s in suprimentos:
            assert "safra" in s
            assert "/" in s["safra"]

            if s["producao"]:
                assert s["producao"] > 0

            if s["exportacao"]:
                assert s["exportacao"] >= 0

    def test_parse_brasil_total(self, parser, sample_xlsx):
        """Testa parsing de totais do Brasil."""
        sample_xlsx.seek(0)
        totais = parser.parse_brasil_total(xlsx=sample_xlsx)

        assert len(totais) > 0

    def test_parse_produto_invalido(self, parser, sample_xlsx):
        """Testa que produto inválido levanta erro."""
        sample_xlsx.seek(0)

        from agrobr.exceptions import ParseError

        with pytest.raises(ParseError):
            parser.parse_safra_produto(
                xlsx=sample_xlsx,
                produto="produto_inexistente",
            )

    def test_parse_decimal_conversion(self, parser):
        """Testa conversão de valores para Decimal."""
        assert parser._parse_decimal(123.45) == Decimal("123.45")
        assert parser._parse_decimal("123,45") == Decimal("123.45")
        assert parser._parse_decimal("1 234.5") == Decimal("1234.5")
        assert parser._parse_decimal(None) is None
        assert parser._parse_decimal("") is None
        assert parser._parse_decimal("-") is None

    def test_find_header_row(self, parser, sample_xlsx):
        """Testa encontrar linha de header."""
        import pandas as pd

        sample_xlsx.seek(0)
        df = pd.read_excel(sample_xlsx, sheet_name="Soja", header=None)

        header_row = parser._find_header_row(df)

        assert header_row is not None
        assert header_row >= 0
