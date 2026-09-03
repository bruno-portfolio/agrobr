"""Tests for CEPEA parser."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from agrobr.cepea.parsers.v1 import PRACAS, CepeaParserV1, _is_robusta
from agrobr.exceptions import ParseError


class TestCepeaParserV1:
    """Tests for CepeaParserV1."""

    def setup_method(self):
        self.parser = CepeaParserV1()

    def test_can_parse_with_valid_html(self, sample_html_cepea):
        can_parse, confidence = self.parser.can_parse(sample_html_cepea)
        assert can_parse is True
        assert confidence >= 0.4

    def test_can_parse_with_empty_html(self, sample_html_empty):
        can_parse, confidence = self.parser.can_parse(sample_html_empty)
        assert can_parse is False or confidence < 0.4

    def test_parse_extracts_indicadores(self, sample_html_cepea):
        indicadores = self.parser.parse(sample_html_cepea, "soja")

        assert len(indicadores) == 2

        first = indicadores[0]
        assert first.data == date(2024, 2, 1)
        assert first.valor == Decimal("145.50")
        assert first.produto == "soja"
        assert first.unidade == "BRL/sc60kg"
        assert first.praca == "Paranaguá/PR"

    def test_parse_raises_on_empty_table(self, sample_html_empty):
        with pytest.raises(ParseError) as exc_info:
            self.parser.parse(sample_html_empty, "soja")

        assert "No tables found" in str(exc_info.value)

    def test_parse_date_formats(self):
        assert self.parser._parse_date("01/02/2024") == date(2024, 2, 1)
        assert self.parser._parse_date("01-02-2024") == date(2024, 2, 1)
        assert self.parser._parse_date("2024-02-01") == date(2024, 2, 1)
        assert self.parser._parse_date("invalid") is None

    def test_parse_decimal_formats(self):
        assert self.parser._parse_decimal("145,50") == Decimal("145.50")
        assert self.parser._parse_decimal("1.234,56") == Decimal("1234.56")
        assert self.parser._parse_decimal("R$ 145,50") == Decimal("145.50")
        assert self.parser._parse_decimal("invalid") is None
        assert self.parser._parse_decimal("-10") is None

    def test_detect_unidade_soja(self):
        unidade = self.parser._detect_unidade("soja", [])
        assert unidade == "BRL/sc60kg"

    def test_detect_unidade_boi(self):
        unidade = self.parser._detect_unidade("boi", [])
        assert unidade == "BRL/@"

    def test_parser_metadata(self):
        assert self.parser.version == 1
        assert self.parser.source == "cepea"
        assert self.parser.valid_from == date(2024, 1, 1)
        assert self.parser.valid_until is None

    def test_parse_excludes_usd_column(self):
        html = """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr>
                <th></th>
                <th>Valor R$*</th>
                <th>Var./Dia</th>
                <th>Var./Mês</th>
                <th>Valor US$*</th>
            </tr>
            <tr>
                <td>10/03/2024</td>
                <td>145,50</td>
                <td>+0,5%</td>
                <td>+1,2%</td>
                <td>28,50</td>
            </tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "soja")
        assert len(indicadores) == 1
        assert indicadores[0].valor == Decimal("145.50")

    def test_parse_var_dia_not_matched_as_date(self):
        html = """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr>
                <th></th>
                <th>Valor R$*</th>
                <th>Var./Dia</th>
                <th>Var./Mês</th>
                <th>Valor US$*</th>
            </tr>
            <tr>
                <td>15/03/2024</td>
                <td>130,00</td>
                <td>+0,2%</td>
                <td>+0,8%</td>
                <td>25,00</td>
            </tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "milho")
        assert indicadores[0].data == date(2024, 3, 15)

    def test_parse_populates_praca(self):
        html = """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr><th></th><th>Valor R$*</th><th>Var./Dia</th><th>Var./Mês</th><th>Valor US$*</th></tr>
            <tr><td>01/01/2024</td><td>100,00</td><td>+0,1%</td><td>+0,5%</td><td>20,00</td></tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "soja")
        assert indicadores[0].praca == PRACAS["soja"]

    def test_parse_unknown_produto_praca_none(self):
        html = """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr><th></th><th>Valor R$*</th><th>Var./Dia</th><th>Var./Mês</th><th>Valor US$*</th></tr>
            <tr><td>01/01/2024</td><td>100,00</td><td>+0,1%</td><td>+0,5%</td><td>20,00</td></tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "desconhecido")
        assert indicadores[0].praca is None


class TestCepeaParserV1EdgeCases:
    def setup_method(self):
        self.parser = CepeaParserV1()

    def test_find_data_table_no_match_raises(self):
        html = """
        <html><body>
        <table><tr><td>A</td></tr></table>
        <table><tr><td>X</td></tr><tr><td>Y</td></tr></table>
        <p>CEPEA ESALQ indicador</p>
        </body></html>
        """
        with pytest.raises(ParseError, match="Could not identify data table"):
            self.parser.parse(html, "soja")

    def test_find_data_table_class_match(self):
        html = """
        <html><body>
        <table class="cotacao">
            <tr><th>Data</th><th>Valor R$</th></tr>
            <tr><td>01/02/2024</td><td>145,50</td></tr>
        </table>
        <p>CEPEA ESALQ indicador</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "soja")
        assert len(indicadores) == 1
        assert indicadores[0].valor == Decimal("145.50")

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
        indicadores = self.parser.parse(html, "soja")
        assert len(indicadores) == 1

    def test_find_data_table_largest_table_fallback(self):
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
        indicadores = self.parser.parse(html, "soja")
        assert len(indicadores) >= 2

    def test_parse_row_valor_fallback_scans_cells(self):
        html = """
        <html><body>
        <table class="indicador">
            <tr><th>Periodo</th><th>Cotacao</th></tr>
            <tr><td>01/02/2024</td><td>145,50</td></tr>
        </table>
        <p>CEPEA ESALQ indicador</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "soja")
        assert len(indicadores) == 1
        assert indicadores[0].valor == Decimal("145.50")

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

    def test_parse_date_two_digit_year(self):
        assert self.parser._parse_date("01/02/24") == date(2024, 2, 1)

    def test_parse_date_invalid_value_continues(self):
        assert self.parser._parse_date("31/02/2024") is None

    def test_parse_decimal_empty(self):
        assert self.parser._parse_decimal("") is None

    def test_parse_decimal_dash(self):
        assert self.parser._parse_decimal("-") is None

    def test_parse_decimal_dot_only(self):
        assert self.parser._parse_decimal(".") is None

    def test_parse_decimal_dot_separator(self):
        assert self.parser._parse_decimal("145.50") == Decimal("145.50")

    def test_parse_decimal_invalid_operation(self):
        assert self.parser._parse_decimal("...") is None

    def test_detect_unidade_header_saca(self):
        assert self.parser._detect_unidade("desconhecido", ["saca 60kg"]) == "BRL/sc60kg"

    def test_detect_unidade_header_sc50(self):
        assert self.parser._detect_unidade("desconhecido", ["sc 50kg"]) == "BRL/sc50kg"

    def test_detect_unidade_header_arroba(self):
        assert self.parser._detect_unidade("desconhecido", ["arroba"]) == "BRL/@"

    def test_detect_unidade_header_kg(self):
        assert self.parser._detect_unidade("desconhecido", ["kg"]) == "BRL/kg"

    def test_detect_unidade_header_litro(self):
        assert self.parser._detect_unidade("desconhecido", ["litro"]) == "BRL/L"

    def test_detect_unidade_header_unknown_default(self):
        assert self.parser._detect_unidade("desconhecido", ["xpto"]) == "BRL/sc60kg"

    def test_extract_fingerprint(self, sample_html_cepea):
        result = self.parser.extract_fingerprint(sample_html_cepea)
        assert isinstance(result, dict)
        assert "table_count" in result or "row_count" in result or len(result) > 0


class TestCepeaCafeRobusta:
    """Seleção entre arábica e robusta na página de café (2 tabelas, mesma URL)."""

    def setup_method(self):
        self.parser = CepeaParserV1()

    @staticmethod
    def _html_cafe_duas_tabelas() -> str:
        return """
        <html><body>
        <div class="imagenet-table-titulo">INDICADOR DO CAFÉ ARÁBICA CEPEA/ESALQ</div>
        <table id="imagenet-indicador1" class="imagenet-table">
            <tr><th></th><th>Valor R$</th><th>Var./Dia</th><th>Var./Mês</th><th>Valor US$</th></tr>
            <tr><td>16/06/2026</td><td>1.474,18</td><td>3,26%</td><td>-5,24%</td><td>289,74</td></tr>
        </table>
        <div class="imagenet-table-titulo">INDICADOR DO CAFÉ ROBUSTA CEPEA/ESALQ</div>
        <table id="imagenet-indicador2" class="imagenet-table">
            <tr><th></th><th>Valor R$</th><th>Var./Dia</th><th>Var./Mês</th><th>Valor US$</th></tr>
            <tr><td>16/06/2026</td><td>988,50</td><td>1,48%</td><td>3,77%</td><td>194,28</td></tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """

    def test_robusta_seleciona_tabela_robusta(self):
        inds = self.parser.parse(self._html_cafe_duas_tabelas(), "cafe_robusta")
        assert len(inds) == 1
        assert inds[0].valor == Decimal("988.50")
        assert inds[0].produto == "cafe_robusta"
        assert inds[0].praca == "Espírito Santo"
        assert inds[0].unidade == "BRL/sc60kg"

    def test_cafe_seleciona_tabela_arabica(self):
        inds = self.parser.parse(self._html_cafe_duas_tabelas(), "cafe")
        assert len(inds) == 1
        assert inds[0].valor == Decimal("1474.18")
        assert inds[0].praca == "São Paulo/SP"

    def test_cafe_arabica_seleciona_tabela_arabica(self):
        inds = self.parser.parse(self._html_cafe_duas_tabelas(), "cafe_arabica")
        assert inds[0].valor == Decimal("1474.18")

    def test_robusta_sem_titulo_nao_cai_em_arabica(self):
        html = """
        <html><body>
        <table id="imagenet-indicador1" class="imagenet-table">
            <tr><th></th><th>Valor R$</th><th>Var./Dia</th></tr>
            <tr><td>16/06/2026</td><td>1.474,18</td><td>3,26%</td></tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        with pytest.raises(ParseError):
            self.parser.parse(html, "cafe_robusta")

    def test_is_robusta_detection(self):
        assert _is_robusta("cafe_robusta")
        assert _is_robusta("INDICADOR DO CAFÉ ROBUSTA CEPEA/ESALQ")
        assert _is_robusta("conilon")
        assert _is_robusta("conillon")
        assert not _is_robusta("cafe")
        assert not _is_robusta("INDICADOR DO CAFÉ ARÁBICA CEPEA/ESALQ")


class TestCepeaBezerro:
    """Testes específicos para o indicador de bezerro com peso médio e preço em USD."""

    def setup_method(self):
        self.parser = CepeaParserV1()

    @staticmethod
    def _html_bezerro_completo() -> str:
        return """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr>
                <th></th>
                <th>Valor R$*</th>
                <th>Var./Dia</th>
                <th>Var./Mês</th>
                <th>Valor US$*</th>
                <th>Peso Médio (kg)</th>
            </tr>
            <tr>
                <td>01/03/2024</td>
                <td>280,50</td>
                <td>+0,8%</td>
                <td>+2,1%</td>
                <td>56,10</td>
                <td>185,00</td>
            </tr>
            <tr>
                <td>02/03/2024</td>
                <td>281,00</td>
                <td>+0,2%</td>
                <td>+2,3%</td>
                <td>56,20</td>
                <td>186,00</td>
            </tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """

    def test_bezerro_extrai_dados_completos(self):
        indicadores = self.parser.parse(self._html_bezerro_completo(), "bezerro")
        assert len(indicadores) == 2

        first = indicadores[0]
        assert first.produto == "bezerro"
        assert first.praca == "Mato Grosso do Sul"
        assert first.valor == Decimal("280.50")
        assert first.unidade == "BRL/@"
        assert "variacao" in first.meta
        assert "peso_medio" in first.meta
        assert "valor_usd" in first.meta
        assert first.meta["peso_medio"] == "185.00"
        assert first.meta["valor_usd"] == "56.10"

    def test_bezerro_unidade_correta(self):
        unidade = self.parser._detect_unidade("bezerro", [])
        assert unidade == "BRL/@"

    def test_bezerro_sem_peso_medio(self):
        html = """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr><th></th><th>Valor R$*</th><th>Var./Dia</th><th>Valor US$*</th></tr>
            <tr><td>01/03/2024</td><td>280,50</td><td>+0,8%</td><td>56,10</td></tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "bezerro")
        assert len(indicadores) == 1
        assert indicadores[0].valor == Decimal("280.50")
        assert "valor_usd" in indicadores[0].meta
        assert "peso_medio" not in indicadores[0].meta

    def test_bezerro_sem_usd(self):
        html = """
        <html><body>
        <table class="indicador" id="imagenet-indicador1">
            <tr><th></th><th>Valor R$*</th><th>Var./Dia</th><th>Peso Médio (kg)</th></tr>
            <tr><td>01/03/2024</td><td>280,50</td><td>+0,8%</td><td>185,00</td></tr>
        </table>
        <p>Indicador CEPEA/ESALQ</p>
        </body></html>
        """
        indicadores = self.parser.parse(html, "bezerro")
        assert len(indicadores) == 1
        assert indicadores[0].valor == Decimal("280.50")
        assert "peso_medio" in indicadores[0].meta
        assert "valor_usd" not in indicadores[0].meta
