from __future__ import annotations

from io import BytesIO
from unittest import mock

import pandas as pd
import pytest

from agrobr.conab.parsers.v1 import ConabParserV1


@pytest.mark.parametrize("product", ["algodao", "feijao", "trigo"])
def test_supply_accepts_source_product_and_period_spelling(product):
    label = {"algodao": "ALGODÃO", "feijao": "FEIJÃO", "trigo": "TRIGO"}[product]
    period = "2026 **" if product == "trigo" else "2025/26"
    rows = [
        [
            "PRODUTO",
            "SAFRA",
            "",
            "ESTOQUE INICIAL",
            "PRODUÇÃO",
            "IMPORTAÇÃO",
            "SUPRIMENTO",
            "CONSUMO",
            "EXPORTAÇÃO",
            "DEMANDA TOTAL",
            "ESTOQUE FINAL",
        ],
        [label, period, "jul/26", 10, 100, 1, 111, 50, 40, 90, 21],
        [None, None, "ago/26", 10, 120, 1, 131, 50, 40, 90, 41],
    ]
    frame = pd.DataFrame(rows)
    with mock.patch("agrobr.conab.parsers.v1.read_excel_safe", return_value=frame):
        result = ConabParserV1()._parse_suprimento_long(BytesIO(), product)
    assert len(result) == 1
    assert result[0]["producao"] == 120
    assert result[0]["levantamento"] == "ago/26"
    assert result[0]["safra"] == ("2026" if product == "trigo" else "2025/26")
