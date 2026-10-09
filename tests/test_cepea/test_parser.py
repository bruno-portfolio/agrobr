from __future__ import annotations

from decimal import Decimal

import pytest
from bs4 import BeautifulSoup

from agrobr.cepea.parsers.v1 import CepeaParserV1
from agrobr.exceptions import ParseError


class TestCepeaParserV1:
    def setup_method(self):
        self.parser = CepeaParserV1()

    def test_can_parse_with_valid_html(self, sample_html_cepea):
        can_parse, confidence = self.parser.can_parse(sample_html_cepea)
        assert can_parse is True
        assert confidence >= 0.4

    def test_parse_decimal_formats(self):
        assert self.parser._parse_decimal("145,50") == Decimal("145.50")
        assert self.parser._parse_decimal("1.234,56") == Decimal("1234.56")
        assert self.parser._parse_decimal("R$ 145,50") == Decimal("145.50")
        assert self.parser._parse_decimal("invalid") is None
        assert self.parser._parse_decimal("-10") is None

    def test_parse_raises_on_empty_table(self, sample_html_empty):
        with pytest.raises(ParseError) as exc_info:
            self.parser.parse(sample_html_empty, "soja")

        assert "No tables found" in str(exc_info.value)


class TestCepeaParserV1EdgeCases:
    def setup_method(self):
        self.parser = CepeaParserV1()

    def test_find_data_table_multiplas_sem_titulo_falham(self):
        html = """
        <html><body>
        <table><tr><td>x</td></tr></table>
        <table>
            <tr><td>Col1</td><td>Col2</td></tr>
            <tr><td>01/02/2024</td><td>145,50</td></tr>
            <tr><td>02/02/2024</td><td>146,00</td></tr>
            <tr><td>03/02/2024</td><td>147,00</td></tr>
        </table>
        <p>CEPEA ESALQ indicador</p>
        </body></html>
        """
        with pytest.raises(ParseError, match="Could not identify data table"):
            self.parser.parse(html, "soja")

    def test_parse_decimal_dot_only(self):
        assert self.parser._parse_decimal(".") is None

    def test_parse_row_validation_error_skipped(self):
        html = """
        <html><body>
        <table class="indicador">
            <tr><th>Data</th><th>Valor R$</th></tr>
            <tr><td>01/02/2024</td><td>145,50</td></tr>
        </table>
        <p>CEPEA ESALQ indicador</p>
        </body></html>
        """
        with pytest.raises(ParseError, match="No valid indicators"):
            self.parser.parse(html, "x")

    def test_find_data_table_header_text_match(self):
        html = """
        <html><body>
        <table>
            <tr><th>Data</th><th>Valor</th></tr>
            <tr><td>01/02/2024</td><td>145,50</td></tr>
        </table>
        <p>CEPEA ESALQ indicador</p>
        </body></html>
        """
        assert self.parser._find_data_table(BeautifulSoup(html, "lxml"), "soja") is not None
        indicadores = self.parser.parse(html, "soja")
        assert len(indicadores) == 1

    def test_find_data_table_por_classe_sem_cabecalho_de_dados(self):
        soup = BeautifulSoup(
            '<table class="cotacao"><tr><th>Dia</th><th>Preço</th></tr>'
            "<tr><td>01/02/2024</td><td>145,50</td></tr></table>",
            "lxml",
        )
        assert self.parser._find_data_table(soup, "soja") is soup.find("table")

    def test_detect_unidade_header_saca(self):
        assert self.parser._detect_unidade("desconhecido", ["saca 60kg"]) == "BRL/sc60kg"

    def test_detect_unidade_header_arroba(self):
        assert self.parser._detect_unidade("desconhecido", ["arroba"]) == "BRL/@"

    def test_detect_unidade_header_kg(self):
        assert self.parser._detect_unidade("desconhecido", ["kg"]) == "BRL/kg"

    def test_detect_unidade_header_litro(self):
        assert self.parser._detect_unidade("desconhecido", ["litro"]) == "BRL/L"

    def test_detect_unidade_header_sc50(self):
        assert self.parser._detect_unidade("desconhecido", ["sc 50kg"]) == "BRL/sc50kg"

    def test_extract_fingerprint(self, sample_html_cepea):
        result = self.parser.extract_fingerprint(sample_html_cepea)
        assert isinstance(result, dict)
        assert "table_count" in result or "row_count" in result or len(result) > 0

    def test_parse_date_invalid_value_continues(self):
        assert self.parser._parse_date("31/02/2024") is None

    def test_parse_decimal_invalid_operation(self):
        assert self.parser._parse_decimal("...") is None

    def test_detect_unidade_header_unknown_default(self):
        assert self.parser._detect_unidade("desconhecido", ["xpto"]) == "BRL/sc60kg"
