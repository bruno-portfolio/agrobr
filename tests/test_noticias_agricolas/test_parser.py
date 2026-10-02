from decimal import Decimal
from pathlib import Path

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.noticias_agricolas import parser


@pytest.mark.parametrize(
    "produto", ["inexistente", "", " ", None, 1, [], "frango", "chicken", "etanol", "ethanol"]
)
def test_produto_invalido_recusado_antes_do_parsing(produto):
    with pytest.raises(InvalidParameterError, match="Produto.*Válidos"):
        parser.parse_indicador("<html></html>", produto)


def test_produto_acentuado_preserva_valor_unidade_e_praca_publicados():
    arquivo = (
        Path(__file__).resolve().parents[1] / "golden_data/reconciliacao_r6_20260918/na_cafe.html"
    )
    indicadores = parser.parse_indicador(arquivo.read_text(encoding="utf-8"), " CAFÉ ")
    primeiro = indicadores[0]
    assert primeiro.data.isoformat() == "2026-09-17"
    assert primeiro.produto == "cafe"
    assert primeiro.valor == Decimal("1541.97")
    assert primeiro.unidade == "BRL/sc60kg"
    assert primeiro.praca == "São Paulo/SP"
