"""Pytest configuration and fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent
CASSETTES_DIR = FIXTURES_DIR / "cassettes"
GOLDEN_DATA_DIR = FIXTURES_DIR / "golden_data"


@pytest.fixture
def sample_html_cepea() -> str:
    """HTML mínimo para testes de parsing CEPEA."""
    return """
    <html>
    <head><title>CEPEA - Indicador</title></head>
    <body>
        <div id="content">
            <table class="indicador" id="tblIndicador">
                <tr>
                    <th>Data</th>
                    <th>Valor (R$/sc 60kg)</th>
                    <th>Variação</th>
                </tr>
                <tr>
                    <td>01/02/2024</td>
                    <td>145,50</td>
                    <td>+0,5%</td>
                </tr>
                <tr>
                    <td>31/01/2024</td>
                    <td>144,78</td>
                    <td>-0,3%</td>
                </tr>
            </table>
        </div>
        <p>Indicador CEPEA/ESALQ</p>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_empty() -> str:
    """HTML sem tabelas para testar erros."""
    return "<html><body><p>No data available</p></body></html>"


@pytest.fixture
def sample_html_invalid_structure() -> str:
    """HTML com estrutura inválida."""
    return """
    <html>
    <body>
        <table>
            <tr><td>Random data</td></tr>
        </table>
    </body>
    </html>
    """


@pytest.fixture
def cassettes_dir() -> Path:
    """Diretório para cassettes VCR."""
    CASSETTES_DIR.mkdir(parents=True, exist_ok=True)
    return CASSETTES_DIR


@pytest.fixture
def golden_data_dir() -> Path:
    """Diretório para golden data."""
    return GOLDEN_DATA_DIR


@pytest.fixture
def mock_indicador_data() -> dict:
    """Dados mockados de indicador para testes."""
    return {
        "fonte": "cepea",
        "produto": "soja",
        "praca": None,
        "data": "2024-02-01",
        "valor": "145.50",
        "unidade": "BRL/sc60kg",
        "metodologia": "indicador_esalq",
        "revisao": 0,
    }
