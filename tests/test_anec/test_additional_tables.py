from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from agrobr.anec import parser
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures


def _word(text: str, center: float, top: float) -> dict[str, Any]:
    return {"text": text, "x0": center - 5, "x1": center + 5, "top": top, "bottom": top + 5}


def _monthly_header(top: float) -> list[dict[str, Any]]:
    return [
        _word("Soybean", 200, top),
        _word("Soybean", 245, top),
        _word("Meal", 255, top),
        _word("Maize", 300, top),
        _word("Wheat", 400, top),
        _word("DDGS", 450, top),
        _word("Sorghum", 475, top),
        _word("Total", 490, top),
        _word("Products", 510, top),
    ]


def test_zero_publicado_nao_vira_ausente():
    monthly = parser._parse_monthly_shipments(
        [
            _word("Monthly shipments 2026", 200, 0),
            *_monthly_header(10),
            _word("January", 50, 20),
            _word("0", 220, 20),
            _word("100", 420, 20),
            _word("100", 520, 20),
        ]
    ).set_index("produto")
    comparison = parser._parse_yoy_comparison(
        [
            [
                _word("Soybeans", 225, 0),
                _word("2025", 200, 10),
                _word("2026*", 300, 10),
                _word("January*", 50, 20),
                _word("0", 320, 20),
            ]
        ],
        [0],
    )
    with collect_failures() as check:
        with check("mensal"):
            assert monthly["valor_ton"].isna().to_dict() == {
                "soybean": False,
                "soybean_meal": True,
                "maize": True,
                "wheat": False,
                "ddgs": True,
                "sorghum": True,
            }
            assert monthly["valor_ton"].fillna(-1).to_dict() == {
                "soybean": 0.0,
                "soybean_meal": -1,
                "maize": -1,
                "wheat": 100.0,
                "ddgs": -1,
                "sorghum": -1,
            }
        with check("comparação"):
            valores = comparison[["valor_base_ton", "valor_comparacao_ton"]]
            assert valores.isna().to_numpy().tolist() == [[True, False]]
            assert valores.fillna(-1).to_numpy().tolist() == [[-1, 0.0]]
            assert comparison["eh_estimativa"].tolist() == [True]


def test_comparison_incompatible_years_raise():
    words = [
        _word("Soybeans", 225, 0),
        _word("2024", 200, 10),
        _word("2026*", 300, 10),
        _word("January", 50, 20),
        _word("100", 220, 20),
    ]

    with pytest.raises(ParseError, match="incompatíveis"):
        parser._parse_yoy_comparison([words], [0])


def test_comparison_ano_no_rodape_nao_substitui_cabecalho_ausente():
    words = [
        _word("Soybeans", 225, 0),
        _word("ANEC", 30, 5),
        _word("Statistics", 100, 5),
        _word("2026", 200, 5),
        _word("January", 50, 20),
        _word("100", 220, 20),
    ]
    with pytest.raises(ParseError, match="anos ausentes"):
        parser._parse_yoy_comparison([words], [0])


def test_destination_missing_period_is_not_inferred():
    with collect_failures() as check:
        for period, expected in [
            ("2025", (2025, None, None)),
            ("", (None, None, None)),
            ("2026 (mar-jan)", (2026, None, None)),
        ]:
            with check(period):
                words = [
                    _word(f"Brazilian Soybeans Importers: {period}", 200, 0),
                    _word("Destination", 100, 10),
                    _word("%", 300, 10),
                    _word("CHINA", 100, 20),
                    _word("0%", 300, 20),
                ]
                frame = parser._parse_destinations([words], [0])
                row = frame.loc[0, ["ano", "mes_inicio", "mes_fim"]]
                assert [None if pd.isna(v) else int(v) for v in row] == list(expected)
                assert frame["share_pct"].isna().tolist() == [False]
                assert frame["share_pct"].fillna(-1).tolist() == [0.0]


def test_empty_additional_tables_preserve_schema():
    comparison = parser._parse_yoy_comparison([], [])
    destinations = parser._parse_destinations([], [])

    assert comparison.empty
    assert str(comparison["ano_base"].dtype) == "Int64"
    assert str(comparison["eh_estimativa"].dtype) == "bool"
    assert destinations.empty
    assert str(destinations["mes_fim"].dtype) == "Int64"
