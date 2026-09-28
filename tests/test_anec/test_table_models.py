from __future__ import annotations

from typing import Any

import pydantic
import pytest

from agrobr.anec import models, parser
from agrobr.exceptions import ParseError
from tests.helpers import collect_failures


def _monthly(**overrides: Any) -> dict[str, Any]:
    return {
        "ano": 2026,
        "mes": 4,
        "produto": "soybean",
        "valor_ton": None,
        "eh_estimativa": True,
        "valor_min_ton": None,
        "valor_max_ton": None,
    } | overrides


def _word(text: str, center: float, top: float) -> dict[str, Any]:
    return {"text": text, "x0": center - 5, "x1": center + 5, "top": top, "bottom": top + 5}


def test_monthly_rejects_ambiguous_or_inverted_range():
    with collect_failures() as check:
        for overrides in [
            {"valor_min_ton": 1.0},
            {"valor_min_ton": 2.0, "valor_max_ton": 1.0},
            {"valor_ton": 1.5, "valor_min_ton": 1.0, "valor_max_ton": 2.0},
        ]:
            with check(overrides), pytest.raises(pydantic.ValidationError):
                models.ANECMonthlyShipment.model_validate(_monthly(**overrides))


def test_monthly_parser_translates_row_validation_error():
    words = [
        _word("Monthly shipments 1899", 200, 0),
        _word("Soybean", 200, 10),
        _word("Soybean", 245, 10),
        _word("Meal", 255, 10),
        _word("Maize", 300, 10),
        _word("Wheat", 400, 10),
        _word("DDGS", 450, 10),
        _word("Sorghum", 475, 10),
        _word("Total", 490, 10),
        _word("Products", 510, 10),
        _word("January", 50, 20),
        _word("100", 220, 20),
    ]
    with pytest.raises(ParseError, match="ANECMonthlyShipment") as exc:
        parser._parse_monthly_shipments(words)
    assert exc.value.source == "anec"
    assert exc.value.parser_version == parser.PARSER_VERSION
    assert isinstance(exc.value.__cause__, pydantic.ValidationError)
