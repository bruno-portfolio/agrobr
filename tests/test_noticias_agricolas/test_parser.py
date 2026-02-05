"""Testes para o parser do Notícias Agrícolas."""

from datetime import date
from decimal import Decimal

import pytest

from agrobr.constants import Fonte
from agrobr.exceptions import ParseError
from agrobr.noticias_agricolas.parser import (
    PRACAS,
    UNIDADES,
    _parse_date,
    _parse_valor,
    _parse_variacao,
    parse_indicador,
)


class TestParseFunctions:
    """Testes para funções auxiliares de parse."""

    def test_parse_date_valid(self):
        """Testa parse de data válida."""
        result = _parse_date("03/02/2026")
        assert result is not None
        assert result.day == 3
        assert result.month == 2
        assert result.year == 2026

    def test_parse_date_invalid(self):
        """Testa parse de data inválida."""
        assert _parse_date("invalid") is None
        assert _parse_date("32/13/2026") is None
        assert _parse_date("") is None

    def test_parse_valor_valid(self):
        """Testa parse de valor válido."""
        assert _parse_valor("124,55") == Decimal("124.55")
        assert _parse_valor("1.234,56") == Decimal("1234.56")
        assert _parse_valor("R$ 124,55") == Decimal("124.55")
        assert _parse_valor("  124,55  ") == Decimal("124.55")

    def test_parse_valor_invalid(self):
        """Testa parse de valor inválido."""
        assert _parse_valor("invalid") is None
        assert _parse_valor("") is None

    def test_parse_variacao_valid(self):
        """Testa parse de variação válida."""
        assert _parse_variacao("-0,26") == Decimal("-0.26")
        assert _parse_variacao("+0,16") == Decimal("0.16")
        assert _parse_variacao("0,23%") == Decimal("0.23")
        assert _parse_variacao("-0,26%") == Decimal("-0.26")

    def test_parse_variacao_invalid(self):
        """Testa parse de variação inválida."""
        assert _parse_variacao("invalid") is None
        assert _parse_variacao("") is None


class TestParseIndicador:
    """Testes para parse_indicador."""

    @pytest.fixture
    def sample_html(self):
        """HTML de exemplo com tabela de cotações."""
        return """
        <html>
        <body>
        <table class="cot-fisicas">
            <thead>
                <tr>
                    <th>Data</th>
                    <th>Valor R$</th>
                    <th>Variação (%)</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>03/02/2026</td>
                    <td>124,55</td>
                    <td>-0,26</td>
                </tr>
                <tr>
                    <td>02/02/2026</td>
                    <td>124,88</td>
                    <td>-0,02</td>
                </tr>
            </tbody>
        </table>
        </body>
        </html>
        """

    @pytest.fixture
    def sample_html_no_class(self):
        """HTML sem classe cot-fisicas (formato real atual do site)."""
        return """
        <html>
        <body>
        <table>
            <thead>
                <tr>
                    <th>Data</th>
                    <th>Valor R$</th>
                    <th>Variação (%)</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>04/02/2026</td>
                    <td>66,44</td>
                    <td>+0,24</td>
                </tr>
                <tr>
                    <td>03/02/2026</td>
                    <td>66,28</td>
                    <td>+0,17</td>
                </tr>
            </tbody>
        </table>
        </body>
        </html>
        """

    @pytest.fixture
    def sample_html_trigo(self):
        """HTML com tabela de trigo (4 colunas: Data, Região, R$/t, Variação)."""
        return """
        <html>
        <body>
        <table>
            <thead>
                <tr>
                    <th>Data</th>
                    <th>Região</th>
                    <th>R$/t</th>
                    <th>Variação (%)</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>04/02/2026</td>
                    <td>Paraná</td>
                    <td>1.176,58</td>
                    <td>+0,12</td>
                </tr>
                <tr>
                    <td>04/02/2026</td>
                    <td>Rio Grande do Sul</td>
                    <td>1.056,90</td>
                    <td>0,00</td>
                </tr>
            </tbody>
        </table>
        </body>
        </html>
        """

    def test_parse_indicador_success(self, sample_html):
        indicadores = parse_indicador(sample_html, "soja")

        assert len(indicadores) == 2

        ind1 = indicadores[0]
        assert ind1.fonte == Fonte.NOTICIAS_AGRICOLAS
        assert ind1.produto == "soja"
        assert ind1.data == date(2026, 2, 3)
        assert ind1.valor == Decimal("124.55")
        assert ind1.unidade == "BRL/sc60kg"
        assert ind1.praca == "Paranaguá/PR"
        assert ind1.meta["variacao_percentual"] == -0.26
        assert ind1.meta["fonte_original"] == "CEPEA/ESALQ"

        ind2 = indicadores[1]
        assert ind2.data == date(2026, 2, 2)
        assert ind2.valor == Decimal("124.88")

    def test_parse_indicador_no_class(self, sample_html_no_class):
        """Tabela sem classe cot-fisicas (formato atual do site)."""
        indicadores = parse_indicador(sample_html_no_class, "milho")

        assert len(indicadores) == 2
        assert indicadores[0].produto == "milho"
        assert indicadores[0].valor == Decimal("66.44")
        assert indicadores[0].unidade == "BRL/sc60kg"

    def test_parse_indicador_trigo_multi_regiao(self, sample_html_trigo):
        """Trigo tem coluna Região extra."""
        indicadores = parse_indicador(sample_html_trigo, "trigo")

        assert len(indicadores) == 2
        assert indicadores[0].praca == "Paraná"
        assert indicadores[0].valor == Decimal("1176.58")
        assert indicadores[0].unidade == "BRL/ton"
        assert indicadores[1].praca == "Rio Grande do Sul"
        assert indicadores[1].valor == Decimal("1056.90")

    def test_parse_indicador_empty_html(self):
        with pytest.raises(ParseError, match="No indicators found"):
            parse_indicador("<html></html>", "soja")

    def test_parse_indicador_no_table(self):
        html = "<html><body><p>Sem tabela</p></body></html>"
        with pytest.raises(ParseError, match="No tables found"):
            parse_indicador(html, "soja")

    def test_parse_indicador_different_products(self, sample_html):
        for produto in ["soja", "milho", "boi", "cafe"]:
            indicadores = parse_indicador(sample_html, produto)
            assert len(indicadores) == 2
            assert indicadores[0].produto == produto
            assert indicadores[0].unidade == UNIDADES[produto]
            assert indicadores[0].praca == PRACAS[produto]

    def test_parse_indicador_new_products(self, sample_html_no_class):
        """Novos produtos adicionados."""
        for produto in ["arroz", "acucar", "frango_congelado", "suino"]:
            indicadores = parse_indicador(sample_html_no_class, produto)
            assert len(indicadores) == 2
            assert indicadores[0].produto == produto
            assert indicadores[0].unidade == UNIDADES[produto]


class TestConstants:
    """Testes para constantes do parser."""

    def test_unidades_mapping(self):
        assert UNIDADES["soja"] == "BRL/sc60kg"
        assert UNIDADES["milho"] == "BRL/sc60kg"
        assert UNIDADES["boi"] == "BRL/@"
        assert UNIDADES["cafe"] == "BRL/sc60kg"
        assert UNIDADES["algodao"] == "cBRL/lb"
        assert UNIDADES["trigo"] == "BRL/ton"
        assert UNIDADES["arroz"] == "BRL/sc50kg"
        assert UNIDADES["acucar"] == "BRL/sc50kg"
        assert UNIDADES["etanol_hidratado"] == "BRL/L"
        assert UNIDADES["etanol_anidro"] == "BRL/L"
        assert UNIDADES["frango_congelado"] == "BRL/kg"
        assert UNIDADES["frango_resfriado"] == "BRL/kg"
        assert UNIDADES["suino"] == "BRL/kg"
        assert UNIDADES["leite"] == "BRL/L"
        assert UNIDADES["laranja_industria"] == "BRL/cx40.8kg"
        assert UNIDADES["laranja_in_natura"] == "BRL/cx40.8kg"

    def test_pracas_mapping(self):
        assert PRACAS["soja"] == "Paranaguá/PR"
        assert PRACAS["milho"] == "Campinas/SP"
        assert PRACAS["boi"] == "São Paulo/SP"
        assert PRACAS["cafe"] == "São Paulo/SP"
        assert PRACAS["trigo"] is None
        assert PRACAS["leite"] is None
        assert PRACAS["arroz"] == "Rio Grande do Sul"

    def test_all_na_produtos_have_unidade(self):
        """Todo produto mapeado no NA deve ter unidade definida."""
        from agrobr.constants import NOTICIAS_AGRICOLAS_PRODUTOS

        for produto in NOTICIAS_AGRICOLAS_PRODUTOS:
            assert produto in UNIDADES, f"Falta unidade para '{produto}'"
